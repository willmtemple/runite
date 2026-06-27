//! Public scheduler entry points generic over a per-platform [`Runtime`].
//!
//! Each platform's `runtime.rs` defines a marker type that implements
//! [`Runtime`] and re-exports these functions with the platform type fixed,
//! so callers continue to write `runite::queue_macrotask(..)` without any
//! turbofish.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::driver_backend::{DriverBackend, Notifier};
use super::future_task::{FutureTask, JoinState, TaskShared};
use super::handles::{
    IntervalHandle, JoinHandle, ThreadHandle, TimeoutHandle, WorkerHandle, YieldNow,
};
use super::state::{
    ChildWorker, IntervalEntry, MacroTask, ThreadShared, WorkerCompletion, install_thread,
    lock_queue, teardown_thread, try_with_installed_thread, with_current_thread,
    with_installed_thread,
};
use super::timer::{TimerKind, TimerNode};
use super::{IntervalCallback, LocalTask, MICROTASK_STARVATION_THRESHOLD};
use crate::trace_targets;

/// Per-platform glue trait.
///
/// Each platform (Linux x86_64, macOS aarch64) implements this on a private
/// marker type and uses it to monomorphize the public scheduler functions.
///
/// The trait surface is intentionally tiny: shared state is fully
/// type-erased through `Box<dyn DriverBackend>` and `Box<dyn Notifier>`, so
/// the only platform-specific behaviour the scheduler ever needs to know
/// about is **how to mint a fresh driver + notifier pair** and **how to
/// resolve `now` from the monotonic clock**.
#[doc(hidden)]
pub trait Runtime: 'static {
    fn create_driver_pair() -> io::Result<(Box<dyn DriverBackend>, Box<dyn Notifier>)>;
    fn monotonic_now() -> io::Result<Duration>;
}

// -- Public functions --------------------------------------------------------

/// Returns a handle for the current runtime thread.
///
/// If the current thread has not yet entered the runtime, the runtime state
/// is initialized lazily.
///
/// # Panics
///
/// Panics if the runtime cannot initialize its driver for the current thread.
pub fn current_thread_handle<R: Runtime>() -> ThreadHandle {
    with_current_thread::<R, _>(|state| state.handle())
}

pub(crate) fn try_current_thread_handle() -> Option<ThreadHandle> {
    try_with_installed_thread(|state| state.map(|s| s.handle()))
}

/// Runs `f` with access to the current driver, downcast through
/// [`DriverBackend::as_any`]. Returns `None` if the driver type does not
/// match `T`. Used by per-platform shims to expose driver-specific entry
/// points (e.g. `cancel_operation`, `cancel_fd_readiness`).
pub(crate) fn with_current_driver_any<R: Runtime, T: Any, U>(f: impl FnOnce(&T) -> U) -> U {
    with_current_thread::<R, _>(|state| {
        let any = state.driver.as_any();
        let typed = any
            .downcast_ref::<T>()
            .expect("driver type mismatch in with_current_driver");
        f(typed)
    })
}

/// Queues a macrotask on the current runtime thread.
///
/// The task runs after all currently-queued macrotasks, and after all
/// microtasks.
///
/// # Panics
///
/// Panics if the runtime cannot initialize its state for the current thread.
pub fn queue_task<R: Runtime, F>(task: F)
where
    F: FnOnce() + 'static,
{
    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::SCHEDULER,
        event = "queue_task",
        queue = "local_macro",
        "queueing local macrotask"
    );
    push_local_macrotask::<R>(Box::new(task));
}

/// Queues a microtask on the current runtime thread.
///
/// Microtasks run before the next macrotask turn, mirroring JavaScript-style
/// event loop semantics.
///
/// # Panics
///
/// Panics if the runtime cannot initialize its state for the current thread.
pub fn queue_microtask<R: Runtime, F>(task: F)
where
    F: FnOnce() + 'static,
{
    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::SCHEDULER,
        event = "queue_microtask",
        queue = "local_micro",
        "queueing local microtask"
    );
    with_current_thread::<R, _>(|state| {
        state
            .local_microtasks
            .borrow_mut()
            .push_back(Box::new(task));
    });
}

/// Schedules a one-shot timer on the current runtime thread.
///
/// # Panics
///
/// Panics if the runtime cannot initialize its state for the current thread.
pub fn timeout<R: Runtime, F>(delay: Duration, callback: F) -> TimeoutHandle
where
    F: FnOnce() + 'static,
{
    let id = allocate_timer_id::<R>();
    let deadline = deadline_from_now::<R>(delay);
    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "timeout",
        timer_id = id,
        delay_ns = delay.as_nanos() as u64,
        deadline_ns = deadline.as_nanos() as u64,
        "scheduling timeout"
    );
    let timer = TimerNode::timeout(id, deadline, Box::new(callback));

    let generation = with_current_thread::<R, _>(|state| {
        state.timers.borrow_mut().insert(timer);
        state.generation
    });
    rearm_thread_timer::<R>();

    TimeoutHandle { id, generation }
}

/// Cancels a timeout previously created by [`timeout`].
///
/// Cancelling a handle whose originating runtime thread has already torn down,
/// or whose handle was created on a different thread, is a silent no-op.
pub fn cancel_timeout(handle: &TimeoutHandle) {
    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "cancel_timeout",
        timer_id = handle.id,
        "cancelling timeout"
    );
    clear_timer(handle.generation, handle.id);
}

/// Schedules a repeating timer on the current runtime thread.
///
/// The callback is invoked once per interval until the handle is cancelled.
///
/// # Panics
///
/// Panics if the runtime cannot initialize its state for the current thread.
pub fn interval<R: Runtime, F>(delay: Duration, callback: F) -> IntervalHandle
where
    F: FnMut() + 'static,
{
    let id = allocate_timer_id::<R>();

    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "interval",
        timer_id = id,
        delay_ns = delay.as_nanos() as u64,
        "scheduling interval"
    );

    let callback: IntervalCallback = Rc::new(RefCell::new(Box::new(callback)));
    let generation = with_current_thread::<R, _>(|state| {
        state.live_intervals.borrow_mut().insert(
            id,
            IntervalEntry {
                callback: Rc::clone(&callback),
                interval: delay,
            },
        );
        state.generation
    });

    if delay.is_zero() {
        // A zero-delay interval would spin the OS timer at 100% CPU if armed
        // through the kernel. Instead it self-schedules as a macrotask each
        // turn, the same path a non-zero interval falls into when its handler
        // has already overshot the next deadline by the time it returns.
        let scheduled = deadline_from_now::<R>(Duration::ZERO);
        schedule_interval_macrotask::<R>(id, scheduled);
    } else {
        let deadline = deadline_from_now::<R>(delay);
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::TIMER,
            event = "interval_deadline",
            timer_id = id,
            deadline_ns = deadline.as_nanos() as u64,
            "interval deadline computed"
        );
        let timer = TimerNode::interval(id, deadline);
        with_current_thread::<R, _>(|state| state.timers.borrow_mut().insert(timer));
        rearm_thread_timer::<R>();
    }

    IntervalHandle { id, generation }
}

/// Cancels an interval previously created by [`interval`].
///
/// Cancelling a handle whose originating runtime thread has already torn down,
/// or whose handle was created on a different thread, is a silent no-op.
pub fn cancel_interval(handle: &IntervalHandle) {
    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "cancel_interval",
        timer_id = handle.id,
        "cancelling interval"
    );
    clear_timer(handle.generation, handle.id);
}

/// Queues a future on the current runtime thread.
///
/// The future is scheduled immediately and can be awaited through the returned
/// [`JoinHandle`].
///
/// The future will be driven to completion regardless of whether the join
/// handle is polled or dropped, so this function can be used as a convenient
/// way to spawn detached async tasks on the current thread.
///
/// # Panics
///
/// Panics if the runtime cannot initialize its state for the current thread.
pub fn queue_future<R: Runtime, F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    #[cfg(debug_assertions)]
    tracing::trace!(
        target: trace_targets::ASYNC,
        event = "queue_future",
        "queueing local future"
    );
    // Force thread-state lazy-init before constructing the task, so the
    // waker's `with_installed_thread` precondition holds before any wake can
    // fire.
    with_current_thread::<R, _>(|_| {});

    let shared = Rc::new(TaskShared::new());
    let state = Rc::new(JoinState::new(Rc::clone(&shared)));
    let completion = Rc::clone(&state);
    let task = Rc::new(FutureTask {
        future: RefCell::new(Some(Box::pin(async move {
            let output = future.await;
            completion.complete(output);
        }))),
        queued: Cell::new(false),
        shared: Rc::clone(&shared),
    });
    shared.set_task(&task);

    task.schedule();

    JoinHandle { state }
}

/// Spawns a worker runtime thread.
///
/// `initial_task` is queued onto the worker as its first macrotask.
/// `on_exit` runs on the parent runtime thread after the worker shuts down.
///
/// # Panics
///
/// Panics if the worker thread or its driver cannot be created.
pub fn spawn_worker<R: Runtime, Init, Exit>(initial_task: Init, on_exit: Exit) -> WorkerHandle
where
    Init: FnOnce() + Send + 'static,
    Exit: FnOnce() + 'static,
{
    tracing::debug!(
        target: trace_targets::RUNTIME,
        event = "spawn_worker",
        "spawning runtime worker thread"
    );
    let (driver, notifier) = R::create_driver_pair().expect("worker driver should initialize");
    let shared = Arc::new(ThreadShared::new(notifier));
    let handle = ThreadHandle {
        shared: Arc::clone(&shared),
    };
    let completion = Arc::new(WorkerCompletion {
        finished: AtomicBool::new(false),
        parent_event: with_current_thread::<R, _>(|parent| parent.handle()),
    });

    with_current_thread::<R, _>(|parent| {
        parent.children.borrow_mut().push(ChildWorker {
            completion: Arc::clone(&completion),
            on_exit: Some(Box::new(on_exit)),
        });
    });

    let worker_completion = Arc::clone(&completion);
    std::thread::Builder::new()
        .name("runite-worker".into())
        .spawn(move || {
            install_thread(shared, driver, Some(worker_completion));
            queue_task::<R, _>(initial_task);
            run::<R>();
        })
        .expect("worker thread should spawn");

    WorkerHandle {
        thread: handle,
        completion,
    }
}

/// Returns a future that yields back to the runtime scheduler once.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// Runs the current runtime thread until no work, timers, child workers, or
/// async operations remain.
///
/// This is the main event-loop entry point used by the proc-macro entry
/// attributes.
///
/// # Panics
///
/// Panics if runtime initialization fails or if the underlying driver returns
/// an unexpected error.
pub fn run<R: Runtime>() {
    let _span = tracing::debug_span!(
        target: trace_targets::RUNTIME,
        "runtime.run"
    )
    .entered();
    tracing::debug!(
        target: trace_targets::RUNTIME,
        event = "run_enter",
        "entering runtime event loop"
    );
    with_current_thread::<R, _>(|_| {});

    loop {
        drain_all::<R>();

        let mut microtasks_run: u64 = 0;
        while let Some(task) = pop_microtask() {
            task();
            microtasks_run += 1;
            drain_all::<R>();
        }
        if microtasks_run >= MICROTASK_STARVATION_THRESHOLD {
            tracing::warn!(
                target: trace_targets::SCHEDULER,
                event = "microtask_starvation",
                count = microtasks_run,
                "microtask queue ran {microtasks_run} tasks in a single turn; macrotask handlers may be starved",
            );
        }

        if let Some(task) = pop_macrotask::<R>() {
            task();
            continue;
        }

        drain_all::<R>();

        if has_ready_work() {
            continue;
        }

        if !with_installed_thread(|state| state.try_begin_shutdown()) {
            continue;
        }

        drain_all::<R>();

        if has_ready_work() {
            with_installed_thread(|state| {
                state.shared.closing.store(false, Ordering::Release);
            });
            continue;
        }

        let busy = with_installed_thread(|state| {
            !state.timers.borrow().is_empty()
                || state.has_live_children()
                || state.has_live_async_operations()
        });

        if busy {
            with_installed_thread(|state| {
                state.shared.closing.store(false, Ordering::Release);
                #[cfg(debug_assertions)]
                tracing::trace!(
                    target: trace_targets::RUNTIME,
                    event = "run_wait",
                    pending_timers = !state.timers.borrow().is_empty(),
                    live_children = state.has_live_children(),
                    live_async = state.has_live_async_operations(),
                    "runtime waiting for more work"
                );
                state.driver.wait().expect("driver wait should succeed");
            });
            continue;
        }

        // Atomically commit to exit: set `closed` while holding the remote
        // queue lock. `enqueue_macro` also checks `closed` under this same
        // lock, so there is no window in which a task can be accepted after
        // we decide to exit. If a task snuck in between the `has_ready_work`
        // check above and acquiring the lock, we abort and process it first.
        let (committed, worker_completion) = with_installed_thread(|state| {
            let remote = lock_queue(&state.shared.remote_macrotasks);
            if remote.is_empty() {
                state.shared.closed.store(true, Ordering::Release);
                (true, state.worker_completion.clone())
            } else {
                (false, None)
            }
        });

        if !committed {
            with_installed_thread(|state| {
                state.shared.closing.store(false, Ordering::Release);
            });
            continue;
        }

        if let Some(completion) = worker_completion {
            completion.finished.store(true, Ordering::Release);
            completion.parent_event.shared.notify();
        }

        with_installed_thread(|state| state.shared.notify());
        tracing::debug!(
            target: trace_targets::RUNTIME,
            event = "run_exit",
            "runtime event loop exiting"
        );
        teardown_thread();
        return;
    }
}

/// Drains ready work on the current runtime thread without blocking for
/// future work.
///
/// Unlike [`run`], this returns as soon as there are no immediately runnable
/// microtasks or macrotasks left. It is intended for host integrations that
/// need to re-enter the scheduler while an outer platform loop remains active.
pub fn run_until_stalled<R: Runtime>() {
    with_current_thread::<R, _>(|_| {});

    loop {
        drain_all::<R>();

        let mut microtasks_run: u64 = 0;
        while let Some(task) = pop_microtask() {
            task();
            microtasks_run += 1;
            drain_all::<R>();
        }
        if microtasks_run >= MICROTASK_STARVATION_THRESHOLD {
            tracing::warn!(
                target: trace_targets::SCHEDULER,
                event = "microtask_starvation",
                count = microtasks_run,
                "microtask queue ran {microtasks_run} tasks in a single turn; macrotask handlers may be starved",
            );
        }

        if let Some(task) = pop_macrotask::<R>() {
            task();
            continue;
        }

        drain_all::<R>();

        if has_ready_work() {
            continue;
        }

        with_installed_thread(|state| {
            state.shared.closing.store(false, Ordering::Release);
        });
        return;
    }
}

/// Drains already-queued work on the current runtime thread without polling
/// the driver for timers or I/O readiness.
///
/// This is intended for host integrations that need to flush application work
/// from inside a host callback without re-entering timer callbacks.
pub fn run_ready_tasks<R: Runtime>() {
    with_current_thread::<R, _>(|_| {});

    loop {
        drain_remote_tasks::<R>();
        drain_completed_workers::<R>();

        let mut microtasks_run: u64 = 0;
        while let Some(task) = pop_microtask() {
            task();
            microtasks_run += 1;
            drain_remote_tasks::<R>();
            drain_completed_workers::<R>();
        }
        if microtasks_run >= MICROTASK_STARVATION_THRESHOLD {
            tracing::warn!(
                target: trace_targets::SCHEDULER,
                event = "microtask_starvation",
                count = microtasks_run,
                "microtask queue ran {microtasks_run} tasks in a single turn; macrotask handlers may be starved",
            );
        }

        if let Some(task) = pop_macrotask::<R>() {
            task();
            continue;
        }

        drain_remote_tasks::<R>();
        drain_completed_workers::<R>();

        if has_ready_work() {
            continue;
        }

        with_installed_thread(|state| {
            state.shared.closing.store(false, Ordering::Release);
        });
        return;
    }
}

// -- Internal scheduler primitives ------------------------------------------

fn drain_all<R: Runtime>() {
    drain_driver_events::<R>();
    drain_remote_tasks::<R>();
    drain_completed_workers::<R>();
}

fn drain_driver_events<R: Runtime>() {
    loop {
        let ready =
            with_installed_thread(|state| state.driver.poll().expect("driver poll should succeed"));

        let Some(ready) = ready else {
            break;
        };

        if ready.wake {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::DRIVER,
                event = "drain_wake",
                "draining driver wake notifications"
            );
            with_installed_thread(|state| {
                let _ = state.driver.drain_wake();
            });
        }
        if ready.timer {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::TIMER,
                event = "drain_timer",
                "draining expired runtime timers"
            );
            with_installed_thread(|state| {
                let _ = state.driver.drain_timer();
            });
            dispatch_expired_timers::<R>();
        }
    }
}

fn drain_remote_tasks<R: Runtime>() {
    // Swap the entire remote queue under the lock and release immediately,
    // minimizing the time the lock is held and avoiding per-item allocation.
    let drained = with_installed_thread(|state| {
        let mut remote = lock_queue(&state.shared.remote_macrotasks);
        std::mem::take(&mut *remote)
    });

    if !drained.is_empty() {
        with_installed_thread(move |state| {
            let mut local = state.local_macrotasks.borrow_mut();
            for task in drained {
                // SendTask (Box<dyn FnOnce() + Send>) coerces to LocalTask
                // (Box<dyn FnOnce()>) by dropping the Send bound.
                let task: LocalTask = task;
                local.push_back(make_macro_task::<R>(task));
            }
        });
    }
}

fn drain_completed_workers<R: Runtime>() {
    let exited = with_installed_thread(|state| {
        let mut exited = Vec::new();
        let mut children = state.children.borrow_mut();
        let mut index = 0;
        while index < children.len() {
            if children[index].completion.finished.load(Ordering::Acquire) {
                let child = children.swap_remove(index);
                exited.push(child);
            } else {
                index += 1;
            }
        }
        exited
    });

    if exited.is_empty() {
        return;
    }

    with_installed_thread(move |state| {
        let mut local = state.local_macrotasks.borrow_mut();
        for mut child in exited {
            if let Some(task) = child.on_exit.take() {
                local.push_back(make_macro_task::<R>(task));
            }
        }
    });
}

fn pop_microtask() -> Option<LocalTask> {
    with_installed_thread(|state| state.local_microtasks.borrow_mut().pop_front())
}

fn pop_macrotask<R: Runtime>() -> Option<LocalTask> {
    let entry = with_installed_thread(|state| state.local_macrotasks.borrow_mut().pop_front())?;
    #[cfg(debug_assertions)]
    {
        let now = deadline_from_now::<R>(Duration::ZERO);
        let wait = now.saturating_sub(entry.queued_at);
        tracing::trace!(
            target: trace_targets::SCHEDULER,
            event = "macrotask_dequeued",
            wait_ns = wait.as_nanos() as u64,
            "macrotask dequeued after waiting in queue"
        );
    }
    let _phantom: core::marker::PhantomData<R> = core::marker::PhantomData;
    Some(entry.task)
}

fn push_local_macrotask<R: Runtime>(task: LocalTask) {
    with_current_thread::<R, _>(|state| {
        state
            .local_macrotasks
            .borrow_mut()
            .push_back(make_macro_task::<R>(task));
    });
}

fn make_macro_task<R: Runtime>(task: LocalTask) -> MacroTask {
    let _phantom: core::marker::PhantomData<R> = core::marker::PhantomData;
    MacroTask {
        task,
        #[cfg(debug_assertions)]
        queued_at: deadline_from_now::<R>(Duration::ZERO),
    }
}

fn has_ready_work() -> bool {
    with_installed_thread(|state| {
        if !state.local_microtasks.borrow().is_empty()
            || !state.local_macrotasks.borrow().is_empty()
        {
            return true;
        }

        if !lock_queue(&state.shared.remote_macrotasks).is_empty() {
            return true;
        }

        false
    })
}

fn allocate_timer_id<R: Runtime>() -> usize {
    with_current_thread::<R, _>(|state| {
        let id = state.next_timer_id.get();
        let next = id.checked_add(1).expect("timer ID space exhausted");
        state.next_timer_id.set(next);
        id
    })
}

fn clear_timer(generation: u64, id: usize) {
    let should_rearm = try_with_installed_thread(|state| {
        let Some(state) = state else {
            return false;
        };
        if state.generation != generation {
            // Stale handle from a different `ThreadState` instance — either
            // a torn-down runtime that happened to reuse an address, or a
            // handle smuggled from a different thread. Either way, there is
            // nothing to remove here.
            return false;
        }
        // Drop the live-interval entry first. Any macrotask already queued for
        // this interval (or the currently running handler about to re-arm
        // itself) will look up the entry and bail out when it is gone.
        state.live_intervals.borrow_mut().remove(&id);
        state.timers.borrow_mut().remove(id).is_some()
    });
    if should_rearm {
        // Re-arming uses the installed accessor — by construction we just
        // observed an installed thread above.
        rearm_thread_timer_installed();
    }
}

/// Pushes a macrotask that fires one tick of the interval identified by `id`.
///
/// `scheduled_deadline` is the deadline that this tick is logically scheduled
/// for. After the handler returns, the next deadline (`scheduled_deadline +
/// interval`) is compared against the current time:
///   * if it has already elapsed, the next tick is enqueued immediately as a
///     macrotask, preserving JS-like "at most once per turn" semantics without
///     spinning an OS timer (this is the only path zero-delay intervals ever
///     take);
///   * otherwise the interval is reinserted into the timer heap and the
///     driver's timer is rearmed.
fn schedule_interval_macrotask<R: Runtime>(id: usize, scheduled_deadline: Duration) {
    push_local_macrotask::<R>(Box::new(move || {
        let Some(callback) = with_installed_thread(|state| {
            state
                .live_intervals
                .borrow()
                .get(&id)
                .map(|entry| Rc::clone(&entry.callback))
        }) else {
            // Interval was cleared before this turn ran.
            return;
        };

        (callback.borrow_mut())();

        // The handler may have cleared its own interval (or a chained one);
        // re-check liveness and pull the current interval duration.
        let interval = match with_installed_thread(|state| {
            state
                .live_intervals
                .borrow()
                .get(&id)
                .map(|entry| entry.interval)
        }) {
            Some(interval) => interval,
            None => return,
        };

        let next_deadline = scheduled_deadline
            .checked_add(interval)
            .unwrap_or(Duration::MAX);
        let now = deadline_from_now::<R>(Duration::ZERO);

        if now >= next_deadline {
            // Deadline already elapsed by the time the handler finished;
            // re-enqueue directly without round-tripping through an OS timer.
            schedule_interval_macrotask::<R>(id, next_deadline);
        } else {
            let node = TimerNode::interval(id, next_deadline);
            with_installed_thread(|state| state.timers.borrow_mut().insert(node));
            rearm_thread_timer_installed();
        }
    }));
}

fn dispatch_expired_timers<R: Runtime>() {
    let now = deadline_from_now::<R>(Duration::ZERO);
    let due = with_installed_thread(|state| state.timers.borrow_mut().pop_due(now));

    if due.is_empty() {
        rearm_thread_timer_installed();
        return;
    }

    for timer in due {
        match timer.kind {
            TimerKind::Timeout(callback) => push_local_macrotask::<R>(callback),
            TimerKind::Interval => {
                // The reschedule decision is deferred until after the handler
                // runs (see `schedule_interval_macrotask`), so that an
                // overshot deadline can re-enqueue as a macrotask rather than
                // rearming a past-deadline kernel timer.
                schedule_interval_macrotask::<R>(timer.id, timer.deadline);
            }
        }
    }

    rearm_thread_timer_installed();
}

fn rearm_thread_timer<R: Runtime>() {
    with_current_thread::<R, _>(|state| {
        let deadline = state.timers.borrow().peek_deadline();
        state
            .driver
            .rearm_timer(deadline)
            .expect("driver timer rearm should succeed");
    });
}

fn rearm_thread_timer_installed() {
    with_installed_thread(|state| {
        let deadline = state.timers.borrow().peek_deadline();
        state
            .driver
            .rearm_timer(deadline)
            .expect("driver timer rearm should succeed");
    });
}

fn deadline_from_now<R: Runtime>(delay: Duration) -> Duration {
    R::monotonic_now()
        .expect("monotonic clock should be available")
        .checked_add(delay)
        .unwrap_or(Duration::MAX)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use super::super::driver_backend::Notifier;
    use super::super::handles::QueueError;
    use super::*;

    struct TestNotifier;

    impl Notifier for TestNotifier {
        fn notify(&self) -> io::Result<()> {
            Ok(())
        }
    }

    fn handle_with_capacity(capacity: usize) -> ThreadHandle {
        ThreadHandle {
            shared: Arc::new(ThreadShared::with_remote_capacity(
                Box::new(TestNotifier),
                capacity,
            )),
        }
    }

    #[test]
    fn bounded_remote_queue_accepts_up_to_capacity() {
        let handle = handle_with_capacity(4);

        for _ in 0..4 {
            assert!(handle.queue_task(|| {}).is_ok());
        }

        assert!(matches!(handle.queue_task(|| {}), Err(QueueError::Full)));
    }

    #[test]
    fn closed_thread_returns_closed_error() {
        let handle = handle_with_capacity(4);
        handle.shared.closed.store(true, Ordering::Release);

        assert!(matches!(handle.queue_task(|| {}), Err(QueueError::Closed)));
    }
}

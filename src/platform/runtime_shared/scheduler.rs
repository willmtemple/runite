//! Public scheduler entry points generic over a per-platform [`Runtime`].
//!
//! Each platform's `runtime.rs` defines a marker type that implements
//! [`Runtime`] and re-exports these functions with the platform type fixed,
//! so callers continue to write `runite::queue_macrotask(..)` without any
//! turbofish.

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::driver_backend::{DriverBackend, Notifier};
use super::future_task::{FutureTask, JoinState, TaskShared, cancel_tasks_for_shutdown};
use super::handles::{
    IntervalHandle, JoinHandle, ThreadHandle, TimeoutHandle, WorkerHandle, WorkerJoinError,
    YieldNow,
};
use super::state::{
    ChildWorker, IntervalEntry, MacroTask, ThreadShared, WorkerCompletion, describe_panic,
    install_thread, lock_queue, thread_teardown_guard, try_with_installed_thread,
    with_current_thread, with_installed_thread,
};
use super::timer::{TimerKind, TimerNode};
use super::{IntervalCallback, LocalTask, MICROTASK_STARVATION_THRESHOLD};
use crate::trace_targets;

/// Per-platform glue trait.
///
/// Each platform (Linux, macOS aarch64) implements this on a private
/// marker type and uses it to monomorphize the public scheduler functions.
///
/// The trait surface is intentionally tiny: shared state is fully
/// type-erased through `Box<dyn DriverBackend>` and `Box<dyn Notifier>`, so
/// the only platform-specific behaviour the scheduler ever needs to know
/// about is **how to mint a fresh driver + notifier pair**, **how to resolve
/// `now` from the monotonic clock**, and **how to start worker and reaper
/// threads**.
#[doc(hidden)]
pub trait Runtime: 'static {
    fn create_driver_pair() -> io::Result<(Box<dyn DriverBackend>, Box<dyn Notifier>)>;
    fn monotonic_now() -> io::Result<Duration>;

    fn spawn_worker_thread(task: super::SendTask) -> io::Result<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("runite-worker".into())
            .spawn(task)
    }

    fn spawn_worker_reaper(task: super::SendTask) -> io::Result<()> {
        let reaper = std::thread::Builder::new()
            .name("runite-worker-reaper".into())
            .spawn(task)?;
        drop(reaper);
        Ok(())
    }
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
        state.live_timeouts.borrow_mut().insert(id);
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
/// The future remains scheduled regardless of whether the join handle is
/// polled or dropped, so this function can be used to spawn detached async
/// tasks. If `run()` reaches quiescence while the task has no
/// scheduler-visible source of progress, it is shutdown-cancelled.
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
    // Force thread-state lazy-init before constructing the task (so the
    // waker's `with_installed_thread` precondition holds before any wake can
    // fire), and allocate this task's registry id and an owner handle for its
    // `Send + Sync` waker.
    let (id, owner) = with_current_thread::<R, _>(|state| {
        let id = state.next_task_id.get();
        state
            .next_task_id
            .set(id.checked_add(1).expect("task ID space exhausted"));
        (id, state.handle())
    });

    let shared = Rc::new(TaskShared::new());
    let state = Rc::new(JoinState::new(Rc::clone(&shared)));
    let completion = Rc::clone(&state);
    let task = FutureTask::new(
        Box::pin(async move {
            let output = future.await;
            completion.complete(output);
        }),
        Rc::clone(&shared),
        id,
        owner,
    );
    shared.set_task(&task);

    // Register before scheduling so a wake fired during the first poll can find
    // the task; the registry holds the runtime's strong reference until the
    // task completes or is aborted.
    with_current_thread::<R, _>(|state| {
        state.tasks.borrow_mut().insert(id, Rc::clone(&task));
    });

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
/// Panics if the worker thread, its non-runtime reaper, or its driver cannot be
/// created.
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
    let completion = Arc::new(WorkerCompletion::new(with_current_thread::<R, _>(
        |parent| parent.handle(),
    )));

    let (worker_sender, worker_receiver) =
        std::sync::mpsc::sync_channel::<std::thread::JoinHandle<()>>(1);
    let reaper_completion = Arc::clone(&completion);
    R::spawn_worker_reaper(Box::new(move || {
        let Ok(worker_thread) = worker_receiver.recv() else {
            return;
        };
        let thread_panicked = match worker_thread.join() {
            Ok(()) => false,
            Err(payload) => {
                discard_caught_panic(payload);
                true
            }
        };
        reaper_completion.publish_after_join(thread_panicked);
    }))
    .expect("worker reaper should spawn");

    let worker_completion = Arc::clone(&completion);
    let worker_thread = R::spawn_worker_thread(Box::new(move || {
        let teardown = thread_teardown_guard();
        let setup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            install_thread(shared, driver, Some(Arc::clone(&worker_completion)));
        }));
        let mut outcome = match setup {
            Ok(()) => {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    queue_task::<R, _>(initial_task);
                    run::<R>();
                })) {
                    Ok(()) => Ok(()),
                    Err(payload) => {
                        discard_caught_panic(payload);
                        Err(WorkerJoinError::RuntimePanicked)
                    }
                }
            }
            Err(payload) => {
                discard_caught_panic(payload);
                Err(WorkerJoinError::SetupPanicked)
            }
        };

        // Explicit teardown reports any panic isolated while releasing
        // runtime-owned values. The reaper publishes this result only after
        // the OS thread, including its remaining TLS destructors, has exited.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| teardown.teardown())) {
            Ok(Ok(())) => {}
            Ok(Err(_)) => outcome = Err(WorkerJoinError::RuntimePanicked),
            Err(payload) => {
                discard_caught_panic(payload);
                outcome = Err(WorkerJoinError::RuntimePanicked);
            }
        }
        worker_completion.record_worker_outcome(outcome);
    }))
    .expect("worker thread should spawn");
    worker_sender
        .send(worker_thread)
        .expect("worker reaper should remain available for its worker");

    // Register only after the thread was created successfully. A failed
    // spawn panics by contract, but must not leave a child whose completion
    // can never become ready and permanently strand the parent event loop.
    with_current_thread::<R, _>(|parent| {
        parent.children.borrow_mut().push(ChildWorker {
            completion: Arc::clone(&completion),
            on_exit: Some(Box::new(on_exit)),
        });
    });
    WorkerHandle {
        thread: handle,
        completion,
    }
}

/// Returns a future that yields back to the runtime scheduler once.
pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

fn discard_caught_panic(payload: Box<dyn Any + Send>) {
    if let Err(drop_payload) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
    {
        // A panic payload with a panicking destructor must not prevent worker
        // completion publication or trigger a double-panic abort.
        std::mem::forget(drop_payload);
    }
}

enum IdleCommit {
    Retry,
    CancelTasks(Vec<Rc<FutureTask>>),
    MainIdle,
    WorkerClosed,
}

/// Runs the current runtime thread until no work, timers, child workers, or
/// async operations remain.
///
/// This is the main event-loop entry point used by the proc-macro entry
/// attributes. On an ordinary/user thread, reaching idle returns without
/// destroying state or driver so sequential entries reuse them. A runtime-owned
/// worker instead atomically commits `closed` against its remote queue before
/// returning. Pending spawned tasks with no scheduler-visible source of
/// progress complete with `JoinError::Cancelled`. Workers perform full cleanup
/// before returning from their thread function. Arbitrary Unix threads do so
/// at final TLS teardown; on Windows the loader-lock-safe TLS fallback only
/// publishes closure and retains the remaining state.
///
/// # Panics
///
/// Panics if runtime initialization fails or if the underlying driver returns
/// an unexpected error.
pub fn run<R: Runtime>() {
    with_current_thread::<R, _>(|_| {});
    let _event_loop = EventLoopGuard::enter();
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

    loop {
        // One iteration of this loop is one turn.
        let _turn = TurnGuard::begin();

        drain_all::<R>();

        drain_microtasks::<R>();

        if let Some(task) = pop_macrotask::<R>() {
            run_guarded(task);
            continue;
        }

        drain_all::<R>();

        if has_ready_work() {
            continue;
        }

        if !with_installed_thread(|state| state.try_begin_idle_probe()) {
            continue;
        }

        // From here `closing` is `true`. This guard restores it to `false` on
        // any early exit from the shutdown-probe region below — including a
        // panic unwind — and is disarmed only once we commit to exiting.
        let mut closing_reset = ClosingResetGuard::new();

        drain_all::<R>();

        if has_ready_work() {
            continue;
        }

        #[cfg(test)]
        with_installed_thread(|state| state.shared.run_after_idle_ready_check());

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

        #[cfg(test)]
        with_installed_thread(|state| {
            if state.worker_completion.is_some() {
                state.shared.run_before_worker_idle_close();
            }
        });

        let worker_closed = match commit_idle() {
            IdleCommit::Retry => {
                // A completion or remote task raced the preliminary probes.
                // `closing_reset` restores the flag before retrying.
                continue;
            }
            IdleCommit::CancelTasks(tasks) => {
                // Extraction was committed under the queue lock. Release that
                // lock before invoking arbitrary wakers or destructors.
                cancel_tasks_for_shutdown(tasks);
                continue;
            }
            IdleCommit::MainIdle => {
                closing_reset.disarm();
                false
            }
            IdleCommit::WorkerClosed => {
                closing_reset.disarm();
                true
            }
        };

        tracing::debug!(
            target: trace_targets::RUNTIME,
            event = "run_exit",
            worker_closed,
            "runtime event loop reached idle"
        );
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
    let _event_loop = EventLoopGuard::enter();

    loop {
        // One iteration of this loop is one turn.
        let _turn = TurnGuard::begin();

        drain_all::<R>();

        drain_microtasks::<R>();

        if let Some(task) = pop_macrotask::<R>() {
            run_guarded(task);
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
    let _event_loop = EventLoopGuard::enter();

    loop {
        // One iteration of this loop is one turn.
        let _turn = TurnGuard::begin();

        drain_remote_tasks::<R>();
        drain_completed_workers::<R>();

        drain_microtasks::<R>();

        if let Some(task) = pop_macrotask::<R>() {
            run_guarded(task);
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

/// Drives the current thread's event loop until `future` resolves, then returns
/// its output.
///
/// Unlike [`run`], which runs until the loop is fully idle, `block_on` returns
/// as soon as the supplied future completes; any other tasks still queued on the
/// thread are left in place for a later `run`/`block_on`. The future is driven in
/// place (not spawned), so it may borrow local state and need not be `Send` or
/// `'static`.
///
/// # Panics
///
/// Panics if runtime initialization fails, if the driver returns an unexpected
/// error, or if called re-entrantly from within a task already running on this
/// thread (see the reentrancy guard shared with [`run`]).
pub fn block_on<R: Runtime, F: Future>(future: F) -> F::Output {
    with_current_thread::<R, _>(|_| {});
    let _event_loop = EventLoopGuard::enter();

    let owner = with_installed_thread(|state| state.handle());
    // `woken` starts true so the future is polled once before the loop parks.
    let block_waker = Arc::new(BlockOnWaker {
        woken: AtomicBool::new(true),
        owner,
    });
    let waker = Waker::from(Arc::clone(&block_waker));
    let mut context = Context::from_waker(&waker);

    let mut future = core::pin::pin!(future);

    loop {
        // One iteration of this loop is one turn.
        let _turn = TurnGuard::begin();

        // Poll the top-level future whenever it may have made progress.
        if block_waker.woken.swap(false, Ordering::AcqRel)
            && let Poll::Ready(output) = future.as_mut().poll(&mut context)
        {
            return output;
        }

        // Pump one turn of the loop, mirroring `run` minus the idle-shutdown
        // probe: `block_on` exits on the future, not on loop quiescence.
        drain_all::<R>();

        drain_microtasks::<R>();

        if let Some(task) = pop_macrotask::<R>() {
            run_guarded(task);
            continue;
        }

        drain_all::<R>();

        // If the future was woken during draining, or there is more ready work,
        // handle it before considering a blocking wait.
        if block_waker.woken.load(Ordering::Acquire) || has_ready_work() {
            continue;
        }

        // Nothing runnable and the future is pending: block for external events
        // (I/O completions, timers, cross-thread wakes), then re-check.
        with_installed_thread(|state| {
            state.driver.wait().expect("driver wait should succeed");
        });
    }
}

/// Waker for the top-level [`block_on`] future. Marks that the future should be
/// re-polled and, for a cross-thread wake, nudges the driver so a parked
/// `block_on` loop wakes up.
struct BlockOnWaker {
    woken: AtomicBool,
    owner: ThreadHandle,
}

impl Wake for BlockOnWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        // A same-thread wake happens while the loop is actively running (never
        // parked in `driver.wait`), so the flag alone suffices. A cross-thread
        // wake may find the loop parked; request a durable driver notification.
        // Transient notifier failures are retried without losing the `woken`
        // state.
        if !self.owner.is_current() {
            self.owner.shared.notify();
        }
    }
}

// -- Internal scheduler primitives ------------------------------------------

/// Runs the microtask queue to exhaustion (one checkpoint), warning if the
/// checkpoint crosses the starvation threshold while a macrotask is actually
/// waiting to run.
///
/// The warning fires from *inside* the loop, at most once per checkpoint: a
/// checkpoint that never empties — a task chain that keeps scheduling new
/// microtasks without ever yielding to a macro turn, the exact pathology this
/// warning exists to flag — would never reach an after-the-loop check at all.
/// A long checkpoint with nothing queued behind it starves no one, so the
/// waiting-macrotask condition is re-checked at each threshold multiple rather
/// than warning on count alone.
fn drain_microtasks<R: Runtime>() {
    let mut microtasks_run: u64 = 0;
    let mut warned = false;
    while let Some(task) = pop_microtask() {
        run_guarded(task);
        microtasks_run += 1;
        if !warned
            && microtasks_run.is_multiple_of(MICROTASK_STARVATION_THRESHOLD)
            && macrotask_waiting::<R>()
        {
            warned = true;
            tracing::warn!(
                target: trace_targets::SCHEDULER,
                event = "microtask_starvation",
                threshold = MICROTASK_STARVATION_THRESHOLD,
                microtasks_run,
                "a single microtask checkpoint has run {microtasks_run} tasks without yielding while macrotasks (timers, I/O, cross-thread work) are waiting; macrotask handlers are being starved",
            );
        }
    }
}

/// Returns whether a macrotask is waiting to run on this thread: a queued
/// local or remote macrotask, or a timer whose deadline has already passed.
///
/// I/O completions parked in the driver are invisible without polling it —
/// which the scheduler deliberately does only once per turn — so they do not
/// count here.
fn macrotask_waiting<R: Runtime>() -> bool {
    let now = deadline_from_now::<R>(Duration::ZERO);
    with_installed_thread(|state| {
        !state.local_macrotasks.borrow().is_empty()
            || state
                .timers
                .borrow()
                .peek_deadline()
                .is_some_and(|deadline| deadline <= now)
            || !lock_queue(&state.shared.remote_macrotasks).is_empty()
    })
}

/// Runs one scheduled unit of work (a macrotask or microtask closure) with a
/// panic firewall.
///
/// A panic escaping a scheduled closure — a timer/interval callback, a
/// `queue_task`/`queue_microtask` closure, a worker `on_exit` handler, or a
/// task poll — is caught here so it cannot unwind the event loop and take down
/// the runtime thread. Spawned-future polls additionally convert their panic
/// into `JoinError::Panicked` inside `FutureTask::poll`, so by the time such a
/// microtask reaches this guard it no longer panics; this is the backstop for
/// every *other* scheduled closure. The panic is still surfaced through the
/// process panic hook.
fn run_guarded(task: LocalTask) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task)) {
        tracing::error!(
            target: trace_targets::SCHEDULER,
            event = "scheduled_task_panicked",
            panic = describe_panic(&*payload),
            "scheduled task panicked; isolating panic to keep the event loop running",
        );
    }
}

/// RAII guard that marks the current thread as actively driving its event loop
/// and clears the mark on drop.
///
/// Constructing it via [`enter`](Self::enter) panics if a driver loop is
/// already running on this thread — that is, if [`run`], [`run_until_stalled`],
/// [`run_ready_tasks`], or [`block_on`] is (transitively) re-entered from inside
/// a task poll or scheduled callback. Re-entry would drive the same
/// microtask/macrotask queues from two stack frames at once and corrupt
/// scheduling state, so it is rejected up front. The panic is subject to the
/// per-task firewall, so a task that illegally re-enters resolves to
/// `JoinError::Panicked` rather than taking down the outer loop.
/// Process-wide source of turn identifiers.
///
/// Starts at 1 so a zero value can never be mistaken for a real turn. One
/// relaxed increment per *turn* — not per task, not per microtask — which is
/// far below the cost of the driver drain that opens the same turn.
static NEXT_TURN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_TURN: std::cell::Cell<Option<TurnId>> = const { std::cell::Cell::new(None) };
}

/// Identifies one turn of an event loop.
///
/// See [`crate::current_turn`]. Deliberately opaque: consumers join records on
/// equality, and keeping the numbering scheme private leaves it changeable.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TurnId(u64);

impl std::fmt::Display for TurnId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Returns the identifier of the turn currently being driven, if any.
pub fn current_turn() -> Option<TurnId> {
    CURRENT_TURN.with(std::cell::Cell::get)
}

/// Marks one iteration of an event loop as a turn.
///
/// Restores the previous value rather than clearing, so the mechanism does not
/// depend on `EventLoopGuard`'s non-reentrancy assertion staying in place.
struct TurnGuard(Option<TurnId>);

impl TurnGuard {
    fn begin() -> Self {
        let previous = CURRENT_TURN.with(std::cell::Cell::get);
        let id = TurnId(NEXT_TURN.fetch_add(1, Ordering::Relaxed));
        CURRENT_TURN.with(|current| current.set(Some(id)));
        Self(previous)
    }
}

impl Drop for TurnGuard {
    fn drop(&mut self) {
        CURRENT_TURN.with(|current| current.set(self.0));
    }
}

struct EventLoopGuard;

impl EventLoopGuard {
    fn enter() -> Self {
        with_installed_thread(|state| {
            assert!(
                !state.tearing_down.get(),
                "runite: cannot enter the runtime event loop during thread teardown",
            );
            assert!(
                !state.in_event_loop.replace(true),
                "runite: cannot re-enter the runtime event loop; `run`, \
                 `block_on`, `run_until_stalled`, and `run_ready_tasks` must not be \
                 called from within a task or callback already running on this \
                 runtime thread",
            );
        });
        EventLoopGuard
    }
}

impl Drop for EventLoopGuard {
    fn drop(&mut self) {
        // Best-effort: an explicit thread-scope teardown may already have
        // removed state while unwinding an owned runtime thread.
        try_with_installed_thread(|state| {
            if let Some(state) = state {
                state.in_event_loop.set(false);
            }
        });
    }
}

/// Resets the current thread's `closing` flag on drop.
///
/// `run()` sets `closing` while it probes for a run-to-idle return. On every
/// non-idle path the flag must return to `false` so the loop can be re-entered.
/// This guard makes that reset happen even if a panic unwinds through the
/// probe. Runtime-owned workers may instead atomically commit `closed`; other
/// threads leave final closure to their teardown owner.
struct ClosingResetGuard {
    armed: bool,
}

impl ClosingResetGuard {
    fn new() -> Self {
        Self { armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClosingResetGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        try_with_installed_thread(|state| {
            if let Some(state) = state {
                state.shared.closing.store(false, Ordering::Release);
            }
        });
    }
}

/// Reap all external events into the local queues: poll the driver for I/O
/// completions and expired timers, splice in cross-thread (remote) tasks, and
/// collect exited workers.
///
/// Everything this enqueues is a **macrotask** — an I/O completion (CQE /
/// readiness) takes a macro turn, as do timers, remote tasks, and worker
/// exits. None of them can run during a microtask checkpoint. The run loops
/// therefore call this **once per turn**, before draining microtasks, rather
/// than after every microtask: re-polling mid-checkpoint cost one syscall per
/// microtask without changing observable ordering (the reaped macrotasks run
/// after the checkpoint either way). This mirrors the JS event loop's poll
/// phase. A runaway microtask chain that never yields the checkpoint will, by
/// design, starve these events — exactly as `Promise.resolve().then` recursion
/// starves a browser; the `MICROTASK_STARVATION_THRESHOLD` warning flags it.
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
    let mut exited = with_installed_thread(|state| {
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

    let callbacks = exited
        .iter_mut()
        .filter_map(|child| child.on_exit.take())
        .collect::<Vec<_>>();

    with_installed_thread(move |state| {
        let mut local = state.local_macrotasks.borrow_mut();
        for task in callbacks {
            local.push_back(make_macro_task::<R>(task));
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

fn commit_idle() -> IdleCommit {
    with_installed_thread(|state| {
        let remote = lock_queue(&state.shared.remote_macrotasks);

        // A cross-thread completion enqueues its wake while holding this lock
        // and only then decrements `pending_ops`. Therefore whichever side
        // acquires the lock first makes either the queue or liveness recheck
        // non-empty, preventing cancellation in the completion window.
        if !remote.is_empty() || state.has_live_async_operations() {
            return IdleCommit::Retry;
        }

        let tasks = std::mem::take(&mut *state.tasks.borrow_mut())
            .into_values()
            .collect::<Vec<_>>();
        if !tasks.is_empty() {
            return IdleCommit::CancelTasks(tasks);
        }

        if state.worker_completion.is_some() {
            state.shared.closed.store(true, Ordering::Release);
            IdleCommit::WorkerClosed
        } else {
            state.shared.closing.store(false, Ordering::Release);
            IdleCommit::MainIdle
        }
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
    let cleared = try_with_installed_thread(|state| {
        let state = state?;
        if state.generation != generation {
            // Stale handle from a different `ThreadState` instance — either
            // a torn-down runtime that happened to reuse an address, or a
            // handle smuggled from a different thread. Either way, there is
            // nothing to remove here.
            return None;
        }
        // Remove all scheduler-visible bookkeeping before dropping user
        // callbacks. A panicking capture destructor must not leave another
        // timer armed or an interval marked live.
        state.live_timeouts.borrow_mut().remove(&id);
        let interval = state.live_intervals.borrow_mut().remove(&id);
        let timer = state.timers.borrow_mut().remove(id);
        Some((timer, interval))
    });

    let Some((timer, interval)) = cleared else {
        return;
    };
    if timer.is_some() {
        rearm_thread_timer_installed();
    }
    if let Some(timer) = timer {
        drop_timer_value(timer, "cancelled timer callback");
    }
    if let Some(interval) = interval {
        drop_timer_value(interval, "cancelled interval callback");
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

        let callback_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (callback.borrow_mut())();
        }));
        if let Err(payload) = callback_result {
            let removed = with_installed_thread(|state| {
                state.timers.borrow_mut().remove(id);
                state.live_intervals.borrow_mut().remove(&id)
            });
            if let Some(removed) = removed {
                drop_timer_value(removed, "panicking interval callback");
            }
            drop_timer_value(callback, "panicking interval callback");
            std::panic::resume_unwind(payload);
        }

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
            TimerKind::Timeout(callback) => {
                let id = timer.id;
                push_local_macrotask::<R>(Box::new(move || {
                    let live =
                        with_installed_thread(|state| state.live_timeouts.borrow_mut().remove(&id));
                    if live {
                        callback();
                    }
                }));
            }
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

fn drop_timer_value<T>(value: T, kind: &'static str) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
        tracing::error!(
            target: trace_targets::TIMER,
            event = "timer_drop_panicked",
            kind,
            panic = describe_panic(&*payload),
            "timer-owned value panicked from Drop; timer bookkeeping is already terminal",
        );
    }
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
            assert!(handle.queue_macrotask(|| {}).is_ok());
        }

        assert!(matches!(
            handle.queue_macrotask(|| {}),
            Err(QueueError::Full)
        ));
    }

    #[test]
    fn closed_thread_returns_closed_error() {
        let handle = handle_with_capacity(4);
        handle.shared.closed.store(true, Ordering::Release);

        assert!(matches!(
            handle.queue_macrotask(|| {}),
            Err(QueueError::Closed)
        ));
    }

    #[test]
    fn internal_wakes_bypass_remote_queue_capacity() {
        let handle = handle_with_capacity(1);

        // A user macrotask fills the queue to capacity; the next is rejected.
        assert!(handle.queue_macrotask(|| {}).is_ok());
        assert!(matches!(
            handle.queue_macrotask(|| {}),
            Err(QueueError::Full)
        ));

        // Internal completion wakes are accepted even past capacity: dropping
        // one would strand an awaiting future whose result is already stored.
        assert!(handle.queue_internal_wake(|| {}).is_ok());
        assert!(handle.queue_internal_wake(|| {}).is_ok());
    }

    #[test]
    fn internal_wakes_still_reject_when_closed() {
        let handle = handle_with_capacity(1);
        handle.shared.closed.store(true, Ordering::Release);

        assert!(matches!(
            handle.queue_internal_wake(|| {}),
            Err(QueueError::Closed)
        ));
    }
}

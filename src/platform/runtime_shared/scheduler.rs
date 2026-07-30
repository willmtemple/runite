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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use super::config::RuntimeConfig;
use super::driver_backend::{DriverBackend, Notifier};
use super::future_task::{FutureTask, JoinState, TaskShared, cancel_tasks_for_shutdown};
use super::handles::{
    IntervalHandle, JoinHandle, ThreadHandle, TimeoutHandle, WorkerHandle, WorkerJoinError,
    YieldNow,
};
use super::state::{
    ChildWorker, IntervalEntry, MacroTask, RuntimeCounters, RuntimeId, ThreadShared, ThreadState,
    WorkerCompletion, describe_panic, install_thread, lock_queue, thread_teardown_guard,
    try_ensure_current_thread, try_install_configured_thread, try_with_installed_thread,
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
    fn create_driver_pair(
        config: RuntimeConfig,
    ) -> io::Result<(Box<dyn DriverBackend>, Box<dyn Notifier>)>;
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
    tracing::trace!(
        target: trace_targets::SCHEDULER,
        event = "queue_task",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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
    tracing::trace!(
        target: trace_targets::SCHEDULER,
        event = "queue_microtask",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "timeout",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "cancel_timeout",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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

    tracing::trace!(
        target: trace_targets::TIMER,
        event = "interval",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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
        tracing::trace!(
            target: trace_targets::TIMER,
            event = "interval_deadline",
            runtime_id = trace_runtime_id(),
            turn_id = trace_turn_id(),
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
    tracing::trace!(
        target: trace_targets::TIMER,
        event = "cancel_interval",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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
    tracing::trace!(
        target: trace_targets::ASYNC,
        event = "queue_future",
        runtime_id = trace_runtime_id(),
        turn_id = trace_turn_id(),
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
/// The worker inherits the spawning thread's [`RuntimeConfig`]. A thread that
/// has not been configured through [`crate::Builder`] passes on the defaults,
/// which is what it is running with itself.
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
        runtime_id = trace_runtime_id(),
        "spawning runtime worker thread"
    );
    // Read the parent's configuration without forcing its runtime to exist:
    // the parent is installed a few lines below anyway, and installing it here
    // would reorder driver creation between parent and child.
    let config = try_with_installed_thread(|state| {
        state.map_or_else(RuntimeConfig::default, |state| state.config)
    });
    let (driver, notifier) =
        R::create_driver_pair(config).expect("worker driver should initialize");
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
            install_thread(shared, driver, Some(Arc::clone(&worker_completion)), config);
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
        runtime_id = trace_runtime_id(),
        "entering runtime event loop"
    );

    loop {
        // One iteration of this loop is one turn.
        let _turn = TurnGuard::begin(ENTRY_RUN);

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
                tracing::trace!(
                    target: trace_targets::RUNTIME,
                    event = "run_wait",
                    runtime_id = trace_runtime_id(),
                    turn_id = trace_turn_id(),
                    pending_timers = !state.timers.borrow().is_empty(),
                    live_children = state.has_live_children(),
                    live_async = state.has_live_async_operations(),
                    "runtime waiting for more work"
                );
                park_in_driver(state);
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
                continue;
            }
            IdleCommit::CancelTasks(tasks) => {
                // Extraction was committed under the queue lock. Release that
                // lock before invoking arbitrary wakers or destructors.
                cancel_tasks_for_shutdown(tasks);
                continue;
            }
            IdleCommit::MainIdle => false,
            IdleCommit::WorkerClosed => true,
        };

        tracing::debug!(
            target: trace_targets::RUNTIME,
            event = "run_exit",
            runtime_id = trace_runtime_id(),
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
        let _turn = TurnGuard::begin(ENTRY_UNTIL_STALLED);

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
        let _turn = TurnGuard::begin(ENTRY_READY_TASKS);

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
    block_on_installed::<R, F>(future)
}

/// Validates `config` and installs the current thread's runtime from it.
///
/// Backs [`crate::Builder::build`]; see that method for the contract this
/// reports through `io::Result`.
pub fn build_runtime<R: Runtime>(config: RuntimeConfig) -> io::Result<()> {
    config.validate()?;
    try_install_configured_thread::<R>(config)
}

/// Fallible counterpart to [`block_on`]: reports driver-creation failure rather
/// than panicking on it.
///
/// Only *startup* is fallible. Once the runtime is installed, this is
/// `block_on`, and any error from the future itself is the future's own.
pub fn try_block_on<R: Runtime, F: Future>(future: F) -> io::Result<F::Output> {
    try_ensure_current_thread::<R>()?;
    Ok(block_on_installed::<R, F>(future))
}

fn block_on_installed<R: Runtime, F: Future>(future: F) -> F::Output {
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
        let _turn = TurnGuard::begin(ENTRY_BLOCK_ON);

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
        with_installed_thread(park_in_driver);
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
    let started = turn_timestamp();
    let mut microtasks_run: u64 = 0;
    let mut warned = false;
    // The counter lives on `ThreadShared`, so one handle taken at the first
    // microtask keeps the rest of the checkpoint off the thread-local lookup
    // that `with_installed_thread` performs; an empty checkpoint takes none.
    // The bump stays inside the loop so a microtask that reads
    // `metrics::snapshot` sees the checkpoint's progress rather than its
    // starting value.
    let mut shared: Option<Arc<ThreadShared>> = None;
    while let Some(task) = pop_microtask() {
        run_guarded(task);
        microtasks_run += 1;
        let shared =
            shared.get_or_insert_with(|| with_installed_thread(|state| Arc::clone(&state.shared)));
        RuntimeCounters::bump(&shared.counters.microtasks_run);
        if !warned
            && microtasks_run.is_multiple_of(MICROTASK_STARVATION_THRESHOLD)
            && macrotask_waiting::<R>()
        {
            warned = true;
            TURN.with(|turn| turn.starvation_warned.set(true));
            tracing::warn!(
                target: trace_targets::SCHEDULER,
                event = "microtask_starvation",
                runtime_id = trace_runtime_id(),
                turn_id = trace_turn_id(),
                threshold = MICROTASK_STARVATION_THRESHOLD,
                microtasks_run,
                "a single microtask checkpoint has run {microtasks_run} tasks without yielding while macrotasks (timers, I/O, cross-thread work) are waiting; macrotask handlers are being starved",
            );
        }
    }
    if let Some(started) = started {
        let drained = turn_elapsed(started);
        TURN.with(|turn| {
            turn.microtask_drain
                .set(turn.microtask_drain.get() + drained)
        });
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
            runtime_id = trace_runtime_id(),
            turn_id = trace_turn_id(),
            panic = describe_panic(&*payload),
            "scheduled task panicked; isolating panic to keep the event loop running",
        );
    }
}

/// Process-wide source of turn identifiers.
///
/// Starts at 1 so a zero value can never be mistaken for a real turn. One
/// relaxed increment per *turn* — not per task, not per microtask — which is
/// far below the cost of the driver drain that opens the same turn.
static NEXT_TURN: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_TURN: Cell<Option<TurnId>> = const { Cell::new(None) };
    /// What the turn in progress has done so far. Every field is written by
    /// the site that did the work, so a turn in which nothing happened writes
    /// nothing.
    static TURN: TurnActivity = const { TurnActivity::new() };
    /// How long the loop was last parked in the driver.
    ///
    /// Carried across the turn boundary because a park belongs to the turn its
    /// wake *begins*, not to the turn that performed it: "this turn woke after
    /// 4ms because a timer fired" is the sentence a consumer needs, and
    /// splitting the wait from its reason across two records makes it
    /// unanswerable.
    static PARKED: Cell<Option<Duration>> = const { Cell::new(None) };
}

/// Identifies one turn of an event loop.
///
/// See [`crate::current_turn`]. The [`Display`](std::fmt::Display) rendering
/// is the same text that appears in the `turn_id` field of runite's trace
/// events, so application records can be joined against runite's; that
/// correspondence is the promise, not the numbering behind it. Nothing else
/// about the value is specified — in particular, the gap between two turn ids
/// is not a count of turns, because the counter is process-wide and every
/// runtime thread draws from it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TurnId(u64);

impl std::fmt::Display for TurnId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Registers a closure to run when this thread's runtime is torn down.
pub fn on_shutdown<F: FnOnce() + 'static>(hook: F) {
    with_installed_thread(|state| {
        state.shutdown_hooks.borrow_mut().push(Box::new(hook));
    });
}

/// Tears this thread's runtime down now, running shutdown hooks.
pub fn shutdown() {
    super::state::shutdown_current_thread();
}

/// Returns the identifier of the turn currently being driven, if any.
pub fn current_turn() -> Option<TurnId> {
    CURRENT_TURN.with(Cell::get)
}

/// Returns the identifier of the runtime installed on the calling thread.
pub fn current_runtime_id() -> Option<RuntimeId> {
    try_with_installed_thread(|state| state.map(|state| state.shared.runtime_id))
}

/// Reads the monotonic clock the runtime schedules its own deadlines on.
///
/// # Panics
///
/// Panics if the platform clock cannot be read, which the runtime already
/// treats as unrecoverable everywhere it arms a deadline.
pub fn monotonic_now<R: Runtime>() -> Duration {
    R::monotonic_now().expect("monotonic clock should be available")
}

/// The `runtime_id` field value for a trace event emitted from this thread.
///
/// A thread-local read, and `tracing` evaluates field expressions only for an
/// event some subscriber wants, so a dormant build never performs it. `None`
/// off a runtime thread.
pub(crate) fn trace_runtime_id() -> Option<u64> {
    current_runtime_id().map(|id| id.0)
}

/// The `turn_id` field value for a trace event emitted from this thread.
///
/// `None` outside a turn, which is the honest answer for work queued from a
/// foreign thread or from outside the loop entirely.
pub(crate) fn trace_turn_id() -> Option<u64> {
    current_turn().map(|id| id.0)
}

/// Whether anything is collecting the per-turn record.
///
/// `tracing::enabled!` is a relaxed load of a shared static and a compare —
/// the same check the trace macros make before evaluating their fields — so a
/// dormant build reaches none of the work this gates: no clock read for the
/// driver park, no queue-depth sampling, and in particular no lock on the
/// cross-thread queue.
///
/// It is a *different* callsite from the record it guards, and it carries no
/// fields, so it answers only the target and the level. A subscriber whose
/// interest depends on which fields a callsite declares can therefore accept
/// the record's callsite and decline this one, in which case the sample is
/// never taken and the record is never emitted. There is no way to ask
/// `tracing` "would you want *that* event" without emitting it, and answering
/// the question by building the record first is the cost this exists to
/// avoid — so the contract is that turn records are selected by target and
/// level (`runite::runtime` at `TRACE`), not by field predicate.
fn turn_records_enabled() -> bool {
    tracing::enabled!(target: trace_targets::RUNTIME, tracing::Level::TRACE)
}

/// Reads the monotonic clock, but only while something is collecting turn
/// records.
///
/// Every clock read the turn machinery performs goes through this function and
/// [`turn_elapsed`], which is what makes the dormant cost testable rather than
/// merely asserted: both count themselves under `cfg(test)`, so removing the
/// gate fails
/// [`dormant_turn_records_cost_nothing`](super::test_support::dormant_turn_records_cost_nothing).
/// A new site calling `Instant::now` directly is not counted and would not
/// fail it — which is exactly how the four reads got here — so the count is
/// worth something only for as long as these two stay the turn path's only
/// clock reads.
///
/// The gating matters more here than anywhere else in the turn path because
/// there are four reads per turn — one either side of the microtask drain, one
/// either side of the turn — and they fire whether or not the microtask queue
/// had anything in it. Ungated they measured ~465 marginal instructions per
/// turn against ~1320 for the whole dormant turn without them. `Instant::now`
/// is a vDSO call at best and a real syscall on a host whose clocksource is
/// `hpet` or `acpi_pm`, against a `tracing::enabled!` that is a relaxed load of
/// a shared static and a compare.
fn turn_timestamp() -> Option<Instant> {
    turn_records_enabled().then(|| {
        note_turn_sample();
        Instant::now()
    })
}

/// Closes a span opened by [`turn_timestamp`].
///
/// Separate from `Instant::elapsed` only so the second read of the pair is
/// counted too.
fn turn_elapsed(started: Instant) -> Duration {
    note_turn_sample();
    started.elapsed()
}

#[cfg(test)]
thread_local! {
    /// How many times this thread has done work that only the turn record
    /// needs: sampling the queue depths (which locks the cross-thread queue)
    /// and timing the driver park.
    ///
    /// Exists because "no record was emitted" does not establish "no work was
    /// done", and the work is the entire cost.
    /// [`test_support::dormant_turn_records_cost_nothing`](super::test_support::dormant_turn_records_cost_nothing)
    /// asserts this stays at zero under a subscriber that declines TRACE, so
    /// deleting the gate fails a test instead of silently putting a mutex lock
    /// on every iteration of every runite loop.
    static TURN_SAMPLES: Cell<u64> = const { Cell::new(0) };
}

/// Records that the turn machinery did something a dormant loop must not do.
#[cfg(test)]
fn note_turn_sample() {
    TURN_SAMPLES.with(|samples| samples.set(samples.get() + 1));
}

#[cfg(not(test))]
fn note_turn_sample() {}

/// Samples taken on this thread since the counter was last reset.
#[cfg(test)]
pub(crate) fn turn_samples_taken() -> u64 {
    TURN_SAMPLES.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_turn_samples() {
    TURN_SAMPLES.with(|samples| samples.set(0));
}

/// Waits until the turn-record gate answers `want` on this thread.
///
/// `tracing` caches each callsite's interest process-wide, and while only one
/// dispatcher is registered it resolves that cache against whichever thread
/// happens to reach the callsite first (tracing-core's
/// `Dispatchers::rebuilder`). Every runite loop reaches this gate, so a test
/// running concurrently on a thread with no subscriber can pin it to
/// `Interest::never` for the rest of the process, and a scoped subscriber
/// installed afterwards cannot override a cached answer. Rebuilding from a
/// thread that does have the subscriber installed re-resolves it; the loop
/// covers the window where another thread is registering the callsite at the
/// same moment and would otherwise win the race afterwards.
///
/// Establishing the precondition, not asserting the property: the sample
/// counter still has to reach zero on its own.
#[cfg(test)]
pub(crate) fn settle_turn_record_gate(want: bool) {
    for _ in 0..64 {
        if turn_records_enabled() == want {
            return;
        }
        tracing::callsite::rebuild_interest_cache();
        std::thread::yield_now();
    }
    panic!("turn-record gate would not settle to {want}");
}

/// What the turn in progress has done, accumulated by the sites that did it.
///
/// Separate from [`RuntimeCounters`] because those are cumulative for the life
/// of the thread; these reset every turn. Only the quantities that cannot be
/// recovered by differencing a cumulative counter live here.
struct TurnActivity {
    /// Time spent draining microtasks. Accumulated because a turn may drain
    /// more than once.
    microtask_drain: Cell<Duration>,
    /// Timers taken off the heap and queued as macrotasks.
    timers_dispatched: Cell<u64>,
    /// Cross-thread macrotasks moved onto the local queue.
    remote_adopted: Cell<u64>,
    /// Worker exit callbacks queued.
    worker_exits: Cell<u64>,
    /// Cross-thread wake notifications drained from the driver.
    notifications: Cell<u64>,
    /// The driver reported an expired timer. Distinct from
    /// `timers_dispatched`: an expiry whose timers were all cancelled
    /// dispatches nothing and still woke the loop.
    timer_ready: Cell<bool>,
    /// The driver reported a cross-thread wake notification.
    wake_ready: Cell<bool>,
    /// The driver dispatched at least one I/O completion. Read from the
    /// driver rather than from the `operations_completed` delta, which the
    /// blocking pool also moves.
    io_ready: Cell<bool>,
    /// A microtask checkpoint crossed [`MICROTASK_STARVATION_THRESHOLD`] with
    /// macrotasks waiting behind it.
    starvation_warned: Cell<bool>,
}

impl TurnActivity {
    const fn new() -> Self {
        Self {
            microtask_drain: Cell::new(Duration::ZERO),
            timers_dispatched: Cell::new(0),
            remote_adopted: Cell::new(0),
            worker_exits: Cell::new(0),
            notifications: Cell::new(0),
            timer_ready: Cell::new(false),
            wake_ready: Cell::new(false),
            io_ready: Cell::new(false),
            starvation_warned: Cell::new(false),
        }
    }

    fn reset(&self) {
        self.microtask_drain.set(Duration::ZERO);
        self.timers_dispatched.set(0);
        self.remote_adopted.set(0);
        self.worker_exits.set(0);
        self.notifications.set(0);
        self.timer_ready.set(false);
        self.wake_ready.set(false);
        self.io_ready.set(false);
        self.starvation_warned.set(false);
    }
}

/// Adds to one of the turn's counters, naming it by projection so each drain
/// site reads as the thing it counted.
fn count_in_turn(select: impl FnOnce(&TurnActivity) -> &Cell<u64>, amount: u64) {
    TURN.with(|activity| {
        let counter = select(activity);
        counter.set(counter.get().saturating_add(amount));
    });
}

// Which entry point drove a turn. Reported so `wait_ns == 0` can be read
// correctly: a host driving the loop with `run_ready_tasks` never parks, and
// its turns are not the runtime choosing to stay runnable.
const ENTRY_RUN: &str = "run";
const ENTRY_BLOCK_ON: &str = "block_on";
const ENTRY_UNTIL_STALLED: &str = "run_until_stalled";
const ENTRY_READY_TASKS: &str = "run_ready_tasks";

/// Marks one iteration of an event loop as a turn, and reports what it did.
///
/// The turn *id* is restored on drop rather than cleared, so a nested turn
/// would leave the outer one correctly identified. The per-turn *activity* is
/// a single thread-local, and a nested turn resets it: the outer record would
/// then report only what happened after the inner one finished. That is
/// unreachable — `EventLoopGuard` rejects re-entering a driver loop — and
/// making it reachable means revisiting this, not just the id.
struct TurnGuard {
    id: TurnId,
    previous: Option<TurnId>,
    entry: &'static str,
    /// When the turn began, taken only while something is collecting turn
    /// records. `None` is the dormant path: no clock read here, none at the
    /// close, and consequently no `microtask_bound_turns` maintenance.
    started: Option<Instant>,
    /// Everything sampled at the start of the turn — present only while
    /// something is collecting turn records. `None` is the dormant path, and
    /// beyond the branch that produced it that path does no extra work at all.
    opening: Option<TurnOpening>,
}

/// Turn-start sample, differenced against the same quantities at turn end.
struct TurnOpening {
    parked: Option<Duration>,
    microtask_depth: usize,
    macrotask_depth: usize,
    remote_depth: usize,
    microtasks_run: u64,
    macrotasks_run: u64,
    task_polls: u64,
    operations_completed: u64,
}

/// Turn-end sample. Gathered before the record is emitted so no `RefCell`
/// borrow is held while a subscriber runs — a subscriber is free to call back
/// into the runtime.
struct TurnClosing {
    runtime_id: u64,
    microtask_depth: usize,
    macrotask_depth: usize,
    remote_depth: usize,
    microtasks_run: u64,
    macrotasks_run: u64,
    task_polls: u64,
    operations_completed: u64,
}

/// What the turn drained, read out of [`TurnActivity`] in one go so the
/// emitting code is not a wall of `Cell::get`.
struct TurnDrained {
    timers_dispatched: u64,
    remote_adopted: u64,
    worker_exits: u64,
    notifications: u64,
    timer_ready: bool,
    wake_ready: bool,
    io_ready: bool,
    starvation_warned: bool,
}

impl TurnDrained {
    fn of(activity: &TurnActivity) -> Self {
        Self {
            timers_dispatched: activity.timers_dispatched.get(),
            remote_adopted: activity.remote_adopted.get(),
            worker_exits: activity.worker_exits.get(),
            notifications: activity.notifications.get(),
            timer_ready: activity.timer_ready.get(),
            wake_ready: activity.wake_ready.get(),
            io_ready: activity.io_ready.get(),
            starvation_warned: activity.starvation_warned.get(),
        }
    }
}

impl TurnGuard {
    fn begin(entry: &'static str) -> Self {
        let previous = CURRENT_TURN.with(Cell::get);
        let id = TurnId(NEXT_TURN.fetch_add(1, Ordering::Relaxed));
        CURRENT_TURN.with(|current| current.set(Some(id)));
        TURN.with(TurnActivity::reset);
        // Taken unconditionally: a park timed while a subscriber was installed
        // must not be attributed to some much later turn if the subscriber
        // goes away in between.
        let parked = PARKED.with(Cell::take);

        // This one read decides whether the turn is timed at all; the close
        // consults `started` rather than asking `tracing` again, so a
        // subscriber installed mid-turn cannot produce a record whose
        // `runnable_ns` was never measured.
        let started = turn_timestamp();

        let opening = try_with_installed_thread(|state| {
            let state = state?;
            RuntimeCounters::bump(&state.shared.counters.turns);
            // An untimed turn has no record to open: `runnable_ns` is the one
            // field nothing else can supply.
            started?;
            note_turn_sample();
            let counters = &state.shared.counters;
            Some(TurnOpening {
                parked,
                microtask_depth: state.local_microtasks.borrow().len(),
                macrotask_depth: state.local_macrotasks.borrow().len(),
                remote_depth: state.shared.remote_queue_depth(),
                microtasks_run: counters.microtasks_run.load(Ordering::Relaxed),
                macrotasks_run: counters.macrotasks_run.load(Ordering::Relaxed),
                task_polls: counters.task_polls.load(Ordering::Relaxed),
                operations_completed: counters.operations_completed.load(Ordering::Relaxed),
            })
        });

        Self {
            id,
            previous,
            entry,
            started,
            opening,
        }
    }

    /// Emits the per-turn record.
    ///
    /// `elapsed` spans the whole loop iteration, which includes any park the
    /// turn performed at its own end; that park is subtracted so `runnable_ns`
    /// is time spent on work, and is reported instead as the *next* turn's
    /// `wait_ns`.
    fn emit(&self, opening: &TurnOpening, close: TurnClose) {
        let TurnClose {
            elapsed,
            microtask_drain,
            microtask_bound,
        } = close;
        let parked_here = PARKED.with(Cell::get).unwrap_or(Duration::ZERO);
        let drained = TURN.with(TurnDrained::of);

        let Some(closing) = try_with_installed_thread(|state| {
            let state = state?;
            note_turn_sample();
            let counters = &state.shared.counters;
            Some(TurnClosing {
                runtime_id: state.shared.runtime_id.0,
                microtask_depth: state.local_microtasks.borrow().len(),
                macrotask_depth: state.local_macrotasks.borrow().len(),
                remote_depth: state.shared.remote_queue_depth(),
                microtasks_run: counters.microtasks_run.load(Ordering::Relaxed),
                macrotasks_run: counters.macrotasks_run.load(Ordering::Relaxed),
                task_polls: counters.task_polls.load(Ordering::Relaxed),
                operations_completed: counters.operations_completed.load(Ordering::Relaxed),
            })
        }) else {
            // The state was torn down inside the turn; there is nothing left to
            // attribute the record to.
            return;
        };

        tracing::trace!(
            target: trace_targets::RUNTIME,
            event = "turn",
            runtime_id = closing.runtime_id,
            turn_id = self.id.0,
            entry = self.entry,
            wake = wake_reason(opening.parked.is_some(), &drained),
            wait_ns = opening.parked.unwrap_or(Duration::ZERO).as_nanos() as u64,
            runnable_ns = elapsed.saturating_sub(parked_here).as_nanos() as u64,
            microtask_ns = microtask_drain.as_nanos() as u64,
            microtasks = closing.microtasks_run.saturating_sub(opening.microtasks_run),
            macrotasks = closing.macrotasks_run.saturating_sub(opening.macrotasks_run),
            task_polls = closing.task_polls.saturating_sub(opening.task_polls),
            // Cumulative-counter delta, so it counts every async operation of
            // this runtime that reached a terminal result inside the turn's
            // wall-clock window — including ones finished on a blocking-pool
            // thread. `wake` deliberately does not read it.
            operations_completed = closing
                .operations_completed
                .saturating_sub(opening.operations_completed),
            timers = drained.timers_dispatched,
            remote_adopted = drained.remote_adopted,
            worker_exits = drained.worker_exits,
            notifications = drained.notifications,
            microtask_bound,
            microtask_starvation = drained.starvation_warned,
            microtask_depth_before = opening.microtask_depth,
            microtask_depth_after = closing.microtask_depth,
            macrotask_depth_before = opening.macrotask_depth,
            macrotask_depth_after = closing.macrotask_depth,
            remote_depth_before = opening.remote_depth,
            remote_depth_after = closing.remote_depth,
            "event loop turn completed"
        );
    }
}

/// What the turn looked like from [`TurnGuard::drop`], where the numbers the
/// record needs are already computed for the runtime's own counters.
struct TurnClose {
    elapsed: Duration,
    microtask_drain: Duration,
    microtask_bound: bool,
}

/// Classifies what began a turn, from what the turn itself observed.
///
/// A turn that did not park was not woken by anything: the loop went round
/// again, which covers a turn continuing existing work, a host-driven turn,
/// and the first turn after entering the loop. Reporting one of those as an
/// I/O or timer wake would be a guess, and the whole point of the field is to
/// not guess — so `queued` outranks every cause.
///
/// Given a park, the causes come from the driver's own readiness bits, never
/// from a counter delta: `operations_completed` moves on blocking-pool threads
/// too, so a turn whose wall-clock window merely overlapped a pool completion
/// would otherwise be labelled an I/O wake. A wake can carry more than one
/// kind of event, so one has to be named. A timer expiry wins, because "which
/// timer stops this process sleeping" is the question an idle-cost
/// investigation asks; I/O beats a bare notification for the same reason, a
/// notification usually being the delivery mechanism for something else. The
/// per-source counts on the same record say what else the wake carried.
///
/// `spurious` — parked, and the driver gave nothing — is the remaining honest
/// answer, and exists so an unexplained wake is reported as one rather than
/// blamed on whatever happened nearby.
fn wake_reason(parked: bool, drained: &TurnDrained) -> &'static str {
    if !parked {
        "queued"
    } else if drained.timer_ready {
        "timer"
    } else if drained.io_ready {
        "io"
    } else if drained.wake_ready {
        "notify"
    } else {
        "spurious"
    }
}

impl Drop for TurnGuard {
    fn drop(&mut self) {
        // Timing the turn is what makes "the reactive graph is what this turn
        // spent its time on" answerable, and it was also most of the cost of
        // the turn machinery — four clock reads with the microtask drain's
        // pair, ~465 instructions against ~1320 for a whole dormant turn. So
        // it is bought rather than assumed: nothing here runs unless something
        // is collecting, and `microtask_bound_turns` is the price, since a
        // clock-derived counter cannot be maintained without the clock.
        let close = self.started.map(|started| {
            let elapsed = turn_elapsed(started);
            let microtask_drain = TURN.with(|activity| activity.microtask_drain.get());
            TurnClose {
                elapsed,
                microtask_drain,
                // The same predicate the record reports, so the counter and
                // the record cannot invite two interpretations.
                microtask_bound: microtask_drain * 2 > elapsed,
            }
        });
        try_with_installed_thread(|state| {
            if let Some(state) = state {
                if close.as_ref().is_some_and(|close| close.microtask_bound) {
                    RuntimeCounters::bump(&state.shared.counters.microtask_bound_turns);
                }
                state.observe_peaks();
            }
        });
        if let (Some(opening), Some(close)) = (self.opening.take(), close) {
            self.emit(&opening, close);
        }
        CURRENT_TURN.with(|current| current.set(self.previous));
    }
}

/// Blocks in the driver, timing the park for the turn its wake will begin.
///
/// The clock is read only while something is collecting turn records. Two
/// reads either side of a blocking syscall are nothing next to the syscall
/// itself, but a dormant build should pay for neither.
fn park_in_driver(state: &ThreadState) {
    let started = turn_timestamp();
    state.driver.wait().expect("driver wait should succeed");
    if let Some(started) = started {
        PARKED.with(|parked| parked.set(Some(turn_elapsed(started))));
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

        if ready.io {
            TURN.with(|turn| turn.io_ready.set(true));
        }
        if ready.wake {
            tracing::trace!(
                target: trace_targets::DRIVER,
                event = "drain_wake",
                runtime_id = trace_runtime_id(),
                turn_id = trace_turn_id(),
                "draining driver wake notifications"
            );
            let notifications = with_installed_thread(|state| state.driver.drain_wake());
            TURN.with(|turn| turn.wake_ready.set(true));
            count_in_turn(|turn| &turn.notifications, notifications.unwrap_or(0));
        }
        if ready.timer {
            tracing::trace!(
                target: trace_targets::TIMER,
                event = "drain_timer",
                runtime_id = trace_runtime_id(),
                turn_id = trace_turn_id(),
                "draining expired runtime timers"
            );
            with_installed_thread(|state| {
                let _ = state.driver.drain_timer();
            });
            TURN.with(|turn| turn.timer_ready.set(true));
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
        count_in_turn(|turn| &turn.remote_adopted, drained.len() as u64);
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

    count_in_turn(|turn| &turn.worker_exits, exited.len() as u64);

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
    let entry = with_installed_thread(|state| {
        let entry = state.local_macrotasks.borrow_mut().pop_front();
        if entry.is_some() {
            RuntimeCounters::bump(&state.shared.counters.macrotasks_run);
        }
        entry
    })?;
    if let Some(queued_at) = entry.queued_at {
        let wait = deadline_from_now::<R>(Duration::ZERO).saturating_sub(queued_at);
        tracing::trace!(
            target: trace_targets::SCHEDULER,
            event = "macrotask_dequeued",
            runtime_id = trace_runtime_id(),
            turn_id = trace_turn_id(),
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
        queued_at: queue_timestamp::<R>(),
    }
}

/// Reads the monotonic clock, but only if a subscriber is collecting the
/// scheduler traces that would report it.
///
/// `tracing::enabled!` is the same check the trace macros make before
/// evaluating their fields — a relaxed load of a shared static and a compare —
/// so with no subscriber installed this costs a not-taken branch and no
/// syscall.
fn queue_timestamp<R: Runtime>() -> Option<Duration> {
    tracing::enabled!(target: trace_targets::SCHEDULER, tracing::Level::TRACE)
        .then(|| deadline_from_now::<R>(Duration::ZERO))
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

    count_in_turn(|turn| &turn.timers_dispatched, due.len() as u64);

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
    monotonic_now::<R>()
        .checked_add(delay)
        .unwrap_or(Duration::MAX)
}

fn drop_timer_value<T>(value: T, kind: &'static str) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
        tracing::error!(
            target: trace_targets::TIMER,
            event = "timer_drop_panicked",
            runtime_id = trace_runtime_id(),
            turn_id = trace_turn_id(),
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

    /// A rejection is what a consumer chasing backpressure counts, so it is
    /// reported through `metrics::snapshot().counters.remote_tasks_rejected`.
    /// Nothing else in the suite can drive that counter: the default queue
    /// holds 65536 tasks.
    #[test]
    fn a_refused_remote_task_is_counted() {
        let handle = handle_with_capacity(1);
        let rejected = &handle.shared.counters.remote_tasks_rejected;

        assert!(handle.queue_macrotask(|| {}).is_ok());
        assert_eq!(rejected.load(Ordering::Relaxed), 0);

        assert!(matches!(
            handle.queue_macrotask(|| {}),
            Err(QueueError::Full)
        ));
        assert_eq!(
            rejected.load(Ordering::Relaxed),
            1,
            "the refused task should be counted"
        );
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

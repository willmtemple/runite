//! `ThreadState`, `ThreadShared`, the per-thread TLS slot, install / teardown
//! helpers and worker-completion bookkeeping.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::ptr;
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use super::driver_backend::{DriverBackend, Notifier};
use super::future_task::FutureTask;
use super::handles::QueueError;
use super::scheduler::Runtime;
use super::timer::TimerHeap;
use super::{LocalTask, LocalTaskQueue, MacroTaskQueue, SendTask};
use crate::trace_targets;

/// Process-wide counter used to produce a unique `generation` for every
/// `ThreadState` instance installed on any thread. Each `install_thread` /
/// lazy-init bumps the counter, so a stale `TimeoutHandle` cannot collide
/// with a freshly installed state — even one that happens to land at the
/// same address as the torn-down one.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static REMOTE_QUEUE_CAPACITY: OnceLock<usize> = OnceLock::new();

const DEFAULT_REMOTE_QUEUE_CAPACITY: usize = 65_536;
const MAX_REMOTE_QUEUE_CAPACITY: usize = 1 << 24;

thread_local! {
    /// Pointer to the heap-allocated `ThreadState` owned by the current thread,
    /// or null if the runtime has not been installed on this thread yet.
    ///
    /// Stored as a raw pointer rather than a `Box` so that the runtime can be
    /// torn down explicitly via `teardown_thread()`; `LocalKey` destructors run
    /// after `main` returns, which is too late for ordering the runtime
    /// shutdown protocol against `WorkerCompletion` notification.
    ///
    /// `ThreadState` is fully type-erased (the driver and notifier are
    /// `Box<dyn _>`), so a single TLS slot serves every `Runtime` impl in a
    /// given build.
    pub(super) static CURRENT_THREAD: Cell<*mut ThreadState> = const { Cell::new(ptr::null_mut()) };
}

pub(crate) struct MacroTask {
    pub(crate) task: LocalTask,
    /// Wall time at which this task entered the local queue. Populated only
    /// in debug builds; used to emit a trace event reporting queue-wait time.
    #[cfg(debug_assertions)]
    pub(crate) queued_at: Duration,
}

pub(crate) struct IntervalEntry {
    pub(crate) callback: super::IntervalCallback,
    pub(crate) interval: Duration,
}

pub(crate) type LiveIntervals = RefCell<HashMap<usize, IntervalEntry>>;

pub(crate) struct RemoteQueue {
    inner: Mutex<VecDeque<SendTask>>,
    capacity: usize,
    warned_full: AtomicBool,
}

impl RemoteQueue {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            capacity: capacity.clamp(1, MAX_REMOTE_QUEUE_CAPACITY),
            warned_full: AtomicBool::new(false),
        }
    }
}

pub(crate) struct ThreadState {
    pub(crate) driver: Box<dyn DriverBackend>,
    pub(crate) shared: Arc<ThreadShared>,
    pub(crate) worker_completion: Option<Arc<WorkerCompletion>>,
    pub(crate) local_microtasks: RefCell<LocalTaskQueue>,
    pub(crate) local_macrotasks: RefCell<MacroTaskQueue<MacroTask>>,
    pub(crate) timers: RefCell<TimerHeap>,
    /// Tracks every live interval (zero and non-zero delay alike). An entry
    /// is present iff the interval has not been cleared; a missing entry tells
    /// any in-flight macrotask copy of the callback to bail out instead of
    /// firing. Intervals may simultaneously sit in the timer heap (waiting
    /// for their next deadline) or be pending in the macrotask queue (when
    /// they overshot their deadline during the previous handler); the live
    /// map is what makes `cancel_interval` work uniformly across both states.
    pub(crate) live_intervals: LiveIntervals,
    pub(crate) next_timer_id: Cell<usize>,
    /// Registry of every live spawned task on this thread, keyed by task id.
    /// This holds the runtime's strong reference to each `FutureTask` from
    /// spawn until it completes or is aborted, so a `Send + Sync` waker can
    /// reschedule a task by id without holding an `Rc` across threads (see
    /// [`FutureTask`](super::future_task::FutureTask)). Ids are never reused, so
    /// a wake for a completed task simply finds no entry.
    pub(crate) tasks: RefCell<HashMap<u64, Rc<FutureTask>>>,
    pub(crate) next_task_id: Cell<u64>,
    /// `true` while one of the driver loops (`run`, `run_until_stalled`,
    /// `run_ready_tasks`) is active on this thread. Used to detect and reject
    /// re-entrant driver calls (e.g. calling `run()` from inside a task poll),
    /// which would double-drive the same queues and corrupt scheduling state.
    pub(crate) in_event_loop: Cell<bool>,
    pub(crate) children: RefCell<Vec<ChildWorker>>,
    /// Unique generation token issued by `NEXT_GENERATION` when this state was
    /// installed on this thread. Used to detect stale `TimeoutHandle` and
    /// `IntervalHandle` references after the originating state was torn down
    /// (or after a handle is presented to a different runtime thread).
    pub(crate) generation: u64,
}

impl ThreadState {
    fn new(
        shared: Arc<ThreadShared>,
        driver: Box<dyn DriverBackend>,
        worker_completion: Option<Arc<WorkerCompletion>>,
        generation: u64,
    ) -> Self {
        Self {
            driver,
            shared,
            worker_completion,
            local_microtasks: RefCell::new(VecDeque::new()),
            local_macrotasks: RefCell::new(VecDeque::new()),
            timers: RefCell::new(TimerHeap::new()),
            live_intervals: RefCell::new(HashMap::new()),
            next_timer_id: Cell::new(1),
            tasks: RefCell::new(HashMap::new()),
            next_task_id: Cell::new(1),
            in_event_loop: Cell::new(false),
            children: RefCell::new(Vec::new()),
            generation,
        }
    }

    pub(crate) fn handle(&self) -> super::ThreadHandle {
        super::ThreadHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    pub(crate) fn has_live_children(&self) -> bool {
        !self.children.borrow().is_empty()
    }

    pub(crate) fn has_live_async_operations(&self) -> bool {
        self.shared.pending_ops.load(Ordering::Acquire) != 0
    }

    pub(crate) fn try_begin_shutdown(&self) -> bool {
        self.shared
            .closing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

pub(crate) struct ThreadShared {
    notifier: Box<dyn Notifier>,
    // The microtask queue is strictly thread-local; only macrotasks may be
    // enqueued from remote threads, keeping the microtask queue free from
    // cross-thread interference.
    pub(crate) remote_macrotasks: RemoteQueue,
    pub(crate) pending_ops: AtomicUsize,
    pub(crate) closing: AtomicBool,
    pub(crate) closed: AtomicBool,
}

impl ThreadShared {
    pub(crate) fn new(notifier: Box<dyn Notifier>) -> Self {
        Self::with_remote_capacity(notifier, remote_queue_capacity())
    }

    pub(crate) fn with_remote_capacity(notifier: Box<dyn Notifier>, capacity: usize) -> Self {
        Self {
            notifier,
            remote_macrotasks: RemoteQueue::new(capacity),
            pending_ops: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    /// Enqueues a cross-thread **user** macrotask, applying the bounded-queue
    /// capacity limit as backpressure. Returns [`QueueError::Full`] when the
    /// queue is at capacity.
    pub(crate) fn enqueue_macro(&self, task: SendTask) -> Result<(), QueueError> {
        self.enqueue(task, true)
    }

    /// Enqueues an internal cross-thread **wake** (an I/O/channel completion
    /// wake, or a spawned-task waker firing from another thread), bypassing the
    /// capacity limit.
    ///
    /// These must never be dropped for backpressure: a completion stores its
    /// result *before* queueing the wake, and a task waker's wake is a task's
    /// only scheduling signal, so a dropped wake strands the target forever.
    /// Unlike user macrotasks, their count is naturally bounded — one pending
    /// wake per in-flight operation or per live task (each coalesced by its own
    /// scheduled flag) — so the queue cannot grow without a matching amount of
    /// genuine outstanding work. Only a `closed` thread rejects.
    pub(crate) fn enqueue_internal_wake(&self, task: SendTask) -> Result<(), QueueError> {
        self.enqueue(task, false)
    }

    fn enqueue(&self, task: SendTask, enforce_capacity: bool) -> Result<(), QueueError> {
        // Check `closed` under the queue lock so that the exit path can
        // atomically set `closed` while holding the same lock, eliminating
        // the window where a task is accepted but then stranded at shutdown.
        let mut queue = lock_queue(&self.remote_macrotasks);
        if self.closed.load(Ordering::Acquire) {
            return Err(QueueError::Closed);
        }
        if enforce_capacity && queue.len() >= self.remote_macrotasks.capacity {
            if !self
                .remote_macrotasks
                .warned_full
                .swap(true, Ordering::AcqRel)
            {
                tracing::warn!(
                    target: trace_targets::SCHEDULER,
                    event = "remote_queue_full",
                    capacity = self.remote_macrotasks.capacity,
                    "cross-thread macrotask queue is full; rejecting remote task"
                );
            }
            return Err(QueueError::Full);
        }
        queue.push_back(task);
        drop(queue);
        // Notify after releasing the lock. By this point `closed` is still
        // false (we verified under the lock) and teardown_thread has not run,
        // so the ring is guaranteed to be alive.
        self.notify();
        Ok(())
    }

    pub(crate) fn notify(&self) {
        if let Err(error) = self.notifier.notify() {
            // BrokenPipe is expected during shutdown (ring already closed).
            // Any other error is unexpected; log it rather than panicking so
            // the runtime continues to make progress.
            if error.kind() != io::ErrorKind::BrokenPipe {
                tracing::error!(
                    target: trace_targets::DRIVER,
                    event = "notify_error",
                    ?error,
                    "unexpected error sending thread notification"
                );
            }
        }
    }
}

pub(crate) struct ChildWorker {
    pub(crate) completion: Arc<WorkerCompletion>,
    pub(crate) on_exit: Option<LocalTask>,
}

pub(crate) struct WorkerCompletion {
    pub(crate) finished: AtomicBool,
    pub(crate) parent_event: super::ThreadHandle,
}

pub(crate) fn lock_queue(queue: &RemoteQueue) -> MutexGuard<'_, VecDeque<SendTask>> {
    queue.inner.lock().expect("runtime queue poisoned")
}

/// Best-effort extraction of a human-readable message from a caught panic
/// payload (the `Box<dyn Any + Send>` returned by [`std::panic::catch_unwind`]).
/// The standard library uses `&'static str` for `panic!("literal")` and
/// `String` for formatted panics; anything else is opaque.
pub(crate) fn describe_panic(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "Box<dyn Any>"
    }
}

fn remote_queue_capacity() -> usize {
    *REMOTE_QUEUE_CAPACITY.get_or_init(|| {
        std::env::var("RUNITE_REMOTE_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|capacity| *capacity >= 1)
            .map(|capacity| capacity.min(MAX_REMOTE_QUEUE_CAPACITY))
            .unwrap_or(DEFAULT_REMOTE_QUEUE_CAPACITY)
    })
}

/// Lazy-initializing accessor. Use from any public entry point on the
/// scheduler — initializes a fresh `ThreadState` on first use.
pub(crate) fn with_current_thread<R: Runtime, T>(f: impl FnOnce(&ThreadState) -> T) -> T {
    let ptr = CURRENT_THREAD.with(|cell| {
        let mut ptr = cell.get();
        if ptr.is_null() {
            let (driver, notifier) =
                R::create_driver_pair().expect("runtime driver should initialize");
            let shared = Arc::new(ThreadShared::new(notifier));
            let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
            let state = Box::new(ThreadState::new(shared, driver, None, generation));
            let raw = Box::into_raw(state);
            // SAFETY: `raw` was just produced by `Box::into_raw` and has not
            // been published to any other code yet.
            unsafe {
                (*raw).driver.bind_current_thread();
            }
            cell.set(raw);
            ptr = raw;
        }
        ptr
    });
    // SAFETY: `ptr` is non-null per the lazy-init branch above and points to a
    // `ThreadState` that lives until `teardown_thread()` is called on this
    // same thread. The borrow is confined to `f`.
    unsafe { f(&*ptr) }
}

/// Non-initializing accessor. Use from contexts that are guaranteed to run
/// only on a thread the scheduler has already installed (waker callbacks,
/// internal scheduler helpers invoked after a public entry point).
///
/// # Panics
///
/// Panics if no thread state is installed on the calling thread.
pub(crate) fn with_installed_thread<T>(f: impl FnOnce(&ThreadState) -> T) -> T {
    let ptr = CURRENT_THREAD.with(|cell| cell.get());
    assert!(!ptr.is_null(), "runtime state not installed on this thread");
    // SAFETY: `ptr` is non-null and points to a `ThreadState` owned by this
    // thread until `teardown_thread()` runs.
    unsafe { f(&*ptr) }
}

pub(crate) fn try_with_installed_thread<T>(f: impl FnOnce(Option<&ThreadState>) -> T) -> T {
    let ptr = CURRENT_THREAD.with(|cell| cell.get());
    if ptr.is_null() {
        f(None)
    } else {
        // SAFETY: `ptr` is non-null and points to a `ThreadState` owned by
        // this thread until `teardown_thread()` runs.
        unsafe { f(Some(&*ptr)) }
    }
}

pub(crate) fn install_thread(
    shared: Arc<ThreadShared>,
    driver: Box<dyn DriverBackend>,
    worker_completion: Option<Arc<WorkerCompletion>>,
) {
    CURRENT_THREAD.with(|cell| {
        debug_assert!(cell.get().is_null(), "thread runtime already installed");
        let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        let state = Box::new(ThreadState::new(
            shared,
            driver,
            worker_completion,
            generation,
        ));
        let raw = Box::into_raw(state);
        // SAFETY: `raw` is a freshly-allocated `ThreadState` owned by this
        // thread; nothing else can observe it until we publish via `cell.set`.
        unsafe {
            (*raw).driver.bind_current_thread();
        }
        cell.set(raw);
    });
}

pub(crate) fn teardown_thread() {
    CURRENT_THREAD.with(|cell| {
        let ptr = cell.replace(ptr::null_mut());
        if !ptr.is_null() {
            // SAFETY: `ptr` is a `Box::into_raw` value previously installed on
            // this thread and never aliased after install; reclaiming it on
            // teardown is sound.
            unsafe {
                (*ptr).driver.unbind_current_thread();
                drop(Box::from_raw(ptr));
            }
        }
    });
}

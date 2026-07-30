//! `ThreadState`, `ThreadShared`, the per-thread TLS slot, install / teardown
//! helpers and worker-completion bookkeeping.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io;
use std::panic::resume_unwind;
use std::ptr;
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use super::config::RuntimeConfig;
use super::driver_backend::{DriverBackend, Notifier};
use super::future_task::{FutureTask, cancel_tasks_for_shutdown};
use super::handles::{QueueError, WorkerJoinError};
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
const NOTIFY_ATTEMPTS: usize = 2;

thread_local! {
    /// Non-owning fast-path pointer used by scheduler accessors. This key has
    /// no destructor; ownership lives in `THREAD_OWNER`.
    static CURRENT_THREAD: Cell<*const ThreadState> = const { Cell::new(ptr::null()) };

    /// RAII owner for the current thread's runtime state. It is deliberately
    /// initialized only *after* `DriverBackend::bind_current_thread`: TLS
    /// destructors run in reverse initialization order, so platforms that
    /// permit full TLS cleanup can unbind while driver-specific TLS is alive.
    static THREAD_OWNER: ThreadOwner = const { ThreadOwner::new() };

    /// Guards against re-entry and lazy reinstallation while the owned state
    /// is being torn down. This key has no destructor, so it remains readable
    /// from user destructors and driver drop code.
    static THREAD_PHASE: Cell<ThreadPhase> = const { Cell::new(ThreadPhase::Empty) };
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ThreadPhase {
    Empty,
    Active,
    TearingDown,
    Terminated,
}

struct ThreadOwner {
    state: RefCell<Option<Rc<ThreadState>>>,
}

impl ThreadOwner {
    const fn new() -> Self {
        Self {
            state: RefCell::new(None),
        }
    }

    fn install(&self, state: Rc<ThreadState>) -> *const ThreadState {
        let ptr = Rc::as_ptr(&state);
        let previous = self.state.borrow_mut().replace(state);
        assert!(previous.is_none(), "thread runtime already installed");
        ptr
    }

    fn take(&self) -> Option<Rc<ThreadState>> {
        self.state.borrow_mut().take()
    }
}

impl Drop for ThreadOwner {
    fn drop(&mut self) {
        if let Some(state) = self.state.get_mut().take() {
            #[cfg(windows)]
            mark_closed_last_resort(state);
            #[cfg(not(windows))]
            let _ = finalize_thread(state, true);
        } else {
            set_thread_phase(ThreadPhase::Terminated);
        }
    }
}

/// Ensures runtime-owned threads perform full teardown before their thread
/// function returns. This is required on Windows, where TLS destructors run
/// under the loader lock and may not execute arbitrary user destructors or
/// blocking driver cleanup safely.
#[must_use = "the guard must live until the runtime-owned thread is ready to exit"]
pub(crate) struct ThreadTeardownGuard {
    armed: bool,
}

#[derive(Debug)]
pub(crate) struct ThreadTeardownError;

impl ThreadTeardownGuard {
    pub(crate) fn teardown(mut self) -> Result<(), ThreadTeardownError> {
        self.armed = false;
        teardown_owned_thread(true)
    }
}

impl Drop for ThreadTeardownGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = teardown_owned_thread(true);
        }
    }
}

pub(crate) fn thread_teardown_guard() -> ThreadTeardownGuard {
    ThreadTeardownGuard { armed: true }
}

pub(crate) struct MacroTask {
    pub(crate) task: LocalTask,
    /// Monotonic time at which this task entered the local queue, used to
    /// report queue-wait time when the task is dequeued.
    ///
    /// `None` unless a subscriber was actually collecting scheduler traces at
    /// the moment of the push. Reading the clock is a real syscall — around
    /// 20-30ns on every backend — and this is per macrotask, so it must not
    /// happen just because the build has tracing linked in. The check that
    /// produces this is the same not-taken branch every other trace site pays.
    ///
    /// The consequence is that queue-wait timing begins once a subscriber is
    /// installed rather than retroactively: tasks already queued at that
    /// moment are dequeued without it.
    pub(crate) queued_at: Option<Duration>,
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

/// Cumulative, monotonic activity counts for one runtime thread.
#[derive(Debug, Default)]
pub(crate) struct RuntimeCounters {
    pub(crate) turns: AtomicU64,
    pub(crate) operations_completed: AtomicU64,
    pub(crate) tasks_cancelled: AtomicU64,
    pub(crate) coalesced_wakes: AtomicU64,
    /// Turns whose microtask drain took more of the turn than everything else
    /// put together. Two clock reads per *turn* — not per microtask — which is
    /// far below the driver poll that opens the same turn.
    pub(crate) microtask_bound_turns: AtomicU64,
    pub(crate) task_polls: AtomicU64,
    pub(crate) task_wakes: AtomicU64,
    pub(crate) microtasks_run: AtomicU64,
    pub(crate) macrotasks_run: AtomicU64,
    pub(crate) remote_tasks_rejected: AtomicU64,
}

impl RuntimeCounters {
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Highest value each gauge has reached on this runtime thread.
#[derive(Debug, Default)]
pub(crate) struct RuntimePeaks {
    pub(crate) live_tasks: AtomicUsize,
    pub(crate) ready_tasks: AtomicUsize,
    pub(crate) microtask_queue_depth: AtomicUsize,
    pub(crate) local_macrotask_queue_depth: AtomicUsize,
    pub(crate) outstanding_operations: AtomicUsize,
    pub(crate) armed_timers: AtomicUsize,
}

impl RuntimePeaks {
    /// Raises `peak` to `value` if it is higher.
    ///
    /// A relaxed compare-and-swap loop rather than a fetch-max, which is not
    /// available for `AtomicUsize`. Contention is nil: a peak is only ever
    /// written from its own runtime thread.
    pub(crate) fn observe(peak: &AtomicUsize, value: usize) {
        let mut seen = peak.load(Ordering::Relaxed);
        while value > seen {
            match peak.compare_exchange_weak(seen, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(current) => seen = current,
            }
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
    /// Timeout ids remain live after expiry while their callbacks wait in the
    /// macrotask queue. Cancellation removes the id, allowing the queued
    /// wrapper to suppress an expired callback that has not run yet.
    pub(crate) live_timeouts: RefCell<HashSet<usize>>,
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
    /// Closures to run when this thread's runtime is torn down.
    ///
    /// Deliberately keyed to teardown rather than to `run()` returning: a host
    /// that drives the loop with `run_ready_tasks` returns constantly and means
    /// nothing by it, so a hook defined as "runs when the entry point returns"
    /// would fire spuriously there and never for the case it exists for.
    pub(crate) shutdown_hooks: RefCell<Vec<Box<dyn FnOnce()>>>,
    /// `true` while one of the driver loops (`run`, `run_until_stalled`,
    /// `run_ready_tasks`) is active on this thread. Used to detect and reject
    /// re-entrant driver calls (e.g. calling `run()` from inside a task poll),
    /// which would double-drive the same queues and corrupt scheduling state.
    pub(crate) in_event_loop: Cell<bool>,
    /// Set before any terminal callbacks or destructors run. Driver entry
    /// guards reject re-entry while ordinary TLS access remains available to
    /// cancellation/drop code.
    pub(crate) tearing_down: Cell<bool>,
    /// Records panics isolated by nested cleanup helpers while explicit thread
    /// teardown is in progress. Runtime-owned workers surface this through
    /// `WorkerJoinError::RuntimePanicked`; TLS fallback cleanup only logs it.
    pub(crate) teardown_panicked: Cell<bool>,
    pub(crate) children: RefCell<Vec<ChildWorker>>,
    /// Unique generation token issued by `NEXT_GENERATION` when this state was
    /// installed on this thread. Used to detect stale `TimeoutHandle` and
    /// `IntervalHandle` references after the originating state was torn down
    /// (or after a handle is presented to a different runtime thread).
    pub(crate) generation: u64,
    /// Configuration the driver above was created from. Retained so
    /// [`spawn_worker`](super::scheduler::spawn_worker) can hand the same
    /// configuration to the child it creates.
    pub(crate) config: RuntimeConfig,
}

impl ThreadState {
    fn new(
        shared: Arc<ThreadShared>,
        driver: Box<dyn DriverBackend>,
        worker_completion: Option<Arc<WorkerCompletion>>,
        generation: u64,
        config: RuntimeConfig,
    ) -> Self {
        Self {
            driver,
            shared,
            worker_completion,
            local_microtasks: RefCell::new(VecDeque::new()),
            local_macrotasks: RefCell::new(VecDeque::new()),
            timers: RefCell::new(TimerHeap::new()),
            live_timeouts: RefCell::new(HashSet::new()),
            live_intervals: RefCell::new(HashMap::new()),
            next_timer_id: Cell::new(1),
            tasks: RefCell::new(HashMap::new()),
            next_task_id: Cell::new(1),
            shutdown_hooks: RefCell::new(Vec::new()),
            in_event_loop: Cell::new(false),
            tearing_down: Cell::new(false),
            teardown_panicked: Cell::new(false),
            children: RefCell::new(Vec::new()),
            generation,
            config,
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

    /// Driver operations submitted and not yet terminally completed.
    pub(crate) fn outstanding_operations(&self) -> usize {
        self.shared.pending_ops.load(Ordering::Acquire)
    }

    /// Tasks queued for polling but not yet polled.
    pub(crate) fn ready_tasks(&self) -> usize {
        self.shared.ready_tasks.load(Ordering::Acquire)
    }

    /// Records the peaks of gauges that are cheapest to sample at a turn
    /// boundary rather than at every mutation: queue depths and live tasks
    /// move constantly, and sampling them once per turn costs nothing while
    /// still catching the shape of a backlog.
    pub(crate) fn observe_peaks(&self) {
        let peaks = &self.shared.peaks;
        RuntimePeaks::observe(&peaks.live_tasks, self.tasks.borrow().len());
        RuntimePeaks::observe(&peaks.ready_tasks, self.ready_tasks());
        RuntimePeaks::observe(
            &peaks.microtask_queue_depth,
            self.local_microtasks.borrow().len(),
        );
        RuntimePeaks::observe(
            &peaks.local_macrotask_queue_depth,
            self.local_macrotasks.borrow().len(),
        );
        RuntimePeaks::observe(&peaks.outstanding_operations, self.outstanding_operations());
        RuntimePeaks::observe(&peaks.armed_timers, self.timers.borrow().len());
    }
}

pub(crate) struct ThreadShared {
    notifier: Box<dyn Notifier>,
    // The microtask queue is strictly thread-local; only macrotasks may be
    // enqueued from remote threads, keeping the microtask queue free from
    // cross-thread interference.
    pub(crate) remote_macrotasks: RemoteQueue,
    pub(crate) pending_ops: AtomicUsize,
    /// Cumulative activity counters. Maintained at the mutation sites that
    /// perform the work, so reading them walks nothing. Relaxed throughout:
    /// these are for attribution, never for synchronization, and a reader that
    /// observes a count one behind has still learned what it needed.
    pub(crate) counters: RuntimeCounters,
    /// Tasks currently sitting in the microtask queue waiting to be polled.
    ///
    /// Maintained rather than derived: counting them would mean walking the
    /// task registry, and a snapshot that walks is work that distorts the idle
    /// measurement it exists to take.
    pub(crate) ready_tasks: AtomicUsize,
    /// High-water marks for the gauges worth knowing the worst case of.
    ///
    /// A peak is neither a level nor a total: it answers "how bad did this
    /// get", which neither of the others can, and which is exactly the
    /// question after an incident. Updated where the corresponding gauge
    /// rises, so reading one is a load.
    pub(crate) peaks: RuntimePeaks,
    pub(crate) closed: AtomicBool,
    notification_requested: AtomicU64,
    notification_delivered: AtomicU64,
    notification_retry_running: AtomicBool,
    notification_retry_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    #[cfg(test)]
    before_worker_idle_close: Mutex<Option<SendTask>>,
    #[cfg(test)]
    after_idle_ready_check: Mutex<Option<SendTask>>,
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
            counters: RuntimeCounters::default(),
            ready_tasks: AtomicUsize::new(0),
            peaks: RuntimePeaks::default(),
            closed: AtomicBool::new(false),
            notification_requested: AtomicU64::new(0),
            notification_delivered: AtomicU64::new(0),
            notification_retry_running: AtomicBool::new(false),
            notification_retry_thread: Mutex::new(None),
            #[cfg(test)]
            before_worker_idle_close: Mutex::new(None),
            #[cfg(test)]
            after_idle_ready_check: Mutex::new(None),
        }
    }

    /// Enqueues a cross-thread **user** macrotask, applying the bounded-queue
    /// capacity limit as backpressure. Returns [`QueueError::Full`] when the
    /// queue is at capacity or the target driver cannot be notified. Errors
    /// are transactional: the task is removed before returning.
    pub(crate) fn enqueue_macro(self: &Arc<Self>, task: SendTask) -> Result<(), QueueError> {
        self.enqueue(task, true, false)
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
    /// genuine outstanding work. Notification is retried once; if both
    /// immediate attempts fail, the wake remains queued and a single helper
    /// retries with bounded backoff until delivery or final thread closure. A
    /// `closed` thread returns [`QueueError::Closed`].
    pub(crate) fn enqueue_internal_wake(
        self: &Arc<Self>,
        task: SendTask,
    ) -> Result<(), QueueError> {
        self.enqueue(task, false, true)
    }

    fn enqueue(
        self: &Arc<Self>,
        task: SendTask,
        enforce_capacity: bool,
        durable_notification: bool,
    ) -> Result<(), QueueError> {
        // `closed`, queue insertion and the initial notification are
        // serialized by the same lock. Final thread teardown therefore orders
        // cleanly against enqueue. User work rolls back on notification
        // failure; internal wakes stay queued behind a durable retry.
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
            RuntimeCounters::bump(&self.counters.remote_tasks_rejected);
            return Err(QueueError::Full);
        }
        queue.push_back(task);

        if durable_notification {
            self.request_notification();
            return Ok(());
        }

        match self.notify_with_retry() {
            Ok(()) => Ok(()),
            Err(error) => {
                let rejected = queue
                    .pop_back()
                    .expect("just-enqueued runtime task should remain at the back");
                drop(queue);
                tracing::error!(
                    target: trace_targets::DRIVER,
                    event = "notify_rejected",
                    ?error,
                    "rejecting queued work because the runtime could not be notified"
                );
                drop(rejected);
                Err(QueueError::Full)
            }
        }
    }

    pub(crate) fn notify(self: &Arc<Self>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.request_notification();
    }

    fn notify_with_retry(&self) -> io::Result<()> {
        let mut last_error = None;
        for attempt in 0..NOTIFY_ATTEMPTS {
            match self.notifier.notify() {
                Ok(()) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < NOTIFY_ATTEMPTS {
                        std::thread::yield_now();
                    }
                }
            }
        }
        Err(last_error.expect("notification attempts must be non-zero"))
    }

    /// Records a durable notification request. A successful attempt covers
    /// all state published before its generation; failures are retried by one
    /// detached helper until a notification succeeds or final teardown closes
    /// the target. Internal wake closures remain queued throughout.
    fn request_notification(self: &Arc<Self>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let generation = self
            .notification_requested
            .fetch_add(1, Ordering::AcqRel)
            .checked_add(1)
            .expect("runtime notification generation exhausted");

        match self.notify_with_retry() {
            Ok(()) => {
                self.notification_delivered
                    .fetch_max(generation, Ordering::AcqRel);
                if self.has_pending_notification() {
                    self.start_notification_retry();
                }
            }
            Err(error) => {
                if error.kind() != io::ErrorKind::BrokenPipe {
                    tracing::error!(
                        target: trace_targets::DRIVER,
                        event = "notify_retry_scheduled",
                        ?error,
                        "runtime notification failed; retaining work and scheduling durable retry"
                    );
                }
                self.start_notification_retry();
            }
        }
    }

    fn has_pending_notification(&self) -> bool {
        self.notification_delivered.load(Ordering::Acquire)
            < self.notification_requested.load(Ordering::Acquire)
    }

    fn start_notification_retry(self: &Arc<Self>) {
        if self.closed.load(Ordering::Acquire)
            || !self.has_pending_notification()
            || self.notification_retry_running.swap(true, Ordering::AcqRel)
        {
            return;
        }

        let shared = Arc::clone(self);
        let mut retry_thread = self
            .notification_retry_thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spawn = std::thread::Builder::new()
            .name("runite-notify-retry".into())
            .spawn(move || shared.notification_retry_loop());
        match spawn {
            Ok(handle) => {
                *retry_thread = Some(handle);
            }
            Err(error) => {
                self.notification_retry_running
                    .store(false, Ordering::Release);
                panic!("runite: failed to start durable notification retry thread: {error}");
            }
        }
    }

    fn notification_retry_loop(self: Arc<Self>) {
        loop {
            let mut delay = Duration::from_millis(1);
            while !self.closed.load(Ordering::Acquire) && self.has_pending_notification() {
                let generation = self.notification_requested.load(Ordering::Acquire);
                match self.notify_with_retry() {
                    Ok(()) => {
                        self.notification_delivered
                            .fetch_max(generation, Ordering::AcqRel);
                        delay = Duration::from_millis(1);
                    }
                    Err(_) => {
                        std::thread::sleep(delay);
                        delay = (delay * 2).min(Duration::from_millis(50));
                    }
                }
            }

            self.notification_retry_running
                .store(false, Ordering::Release);
            if self.closed.load(Ordering::Acquire)
                || !self.has_pending_notification()
                || self
                    .notification_retry_running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return;
            }
        }
    }

    /// Depth of the cross-thread macrotask queue.
    ///
    /// Takes the queue lock briefly; there is no cheaper honest answer, and a
    /// snapshot is not on a hot path.
    pub(crate) fn remote_queue_depth(&self) -> usize {
        lock_queue(&self.remote_macrotasks).len()
    }

    fn close(&self) -> VecDeque<SendTask> {
        let mut queue = lock_queue(&self.remote_macrotasks);
        self.closed.store(true, Ordering::Release);
        self.notification_delivered.store(
            self.notification_requested.load(Ordering::Acquire),
            Ordering::Release,
        );
        std::mem::take(&mut *queue)
    }

    fn join_notification_retry(&self) -> bool {
        let retry_thread = self
            .notification_retry_thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(retry_thread) = retry_thread
            && let Err(payload) = retry_thread.join()
        {
            tracing::error!(
                target: trace_targets::RUNTIME,
                event = "notification_retry_join_panicked",
                panic = describe_panic(&*payload),
                "notification retry thread panicked during runtime teardown",
            );
            return true;
        }
        false
    }

    #[cfg(windows)]
    fn mark_closed_without_cleanup(&self) {
        self.closed.store(true, Ordering::Release);
        self.notification_delivered.store(
            self.notification_requested.load(Ordering::Acquire),
            Ordering::Release,
        );
    }

    #[cfg(test)]
    pub(crate) fn set_before_worker_idle_close(&self, hook: impl FnOnce() + Send + 'static) {
        *self
            .before_worker_idle_close
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(crate) fn run_before_worker_idle_close(&self) {
        let hook = self
            .before_worker_idle_close
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    pub(crate) fn set_after_idle_ready_check(&self, hook: impl FnOnce() + Send + 'static) {
        *self
            .after_idle_ready_check
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(hook));
    }

    #[cfg(test)]
    pub(crate) fn run_after_idle_ready_check(&self) {
        let hook = self
            .after_idle_ready_check
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(hook) = hook {
            hook();
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
    state: Mutex<WorkerCompletionState>,
}

struct WorkerCompletionState {
    worker_outcome: Option<Result<(), WorkerJoinError>>,
    outcome: Option<Result<(), WorkerJoinError>>,
    next_waiter_id: u64,
    waiters: BTreeMap<u64, WorkerWaiter>,
}

struct WorkerWaiter {
    active: Arc<AtomicBool>,
    waker: Waker,
}

impl WorkerCompletion {
    pub(crate) fn new(parent_event: super::ThreadHandle) -> Self {
        Self {
            finished: AtomicBool::new(false),
            parent_event,
            state: Mutex::new(WorkerCompletionState {
                worker_outcome: None,
                outcome: None,
                next_waiter_id: 1,
                waiters: BTreeMap::new(),
            }),
        }
    }

    /// Records the result of running and explicitly tearing down the worker.
    /// The result is not observable until the non-runtime reaper joins the OS
    /// thread and calls [`publish_after_join`](Self::publish_after_join).
    pub(crate) fn record_worker_outcome(&self, outcome: Result<(), WorkerJoinError>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(
            state.worker_outcome.is_none(),
            "worker outcome recorded twice"
        );
        state.worker_outcome = Some(outcome);
    }

    /// Publishes completion only after the reaper has joined the OS thread, so
    /// parent callbacks and join futures cannot observe pre-TLS-exit state.
    pub(crate) fn publish_after_join(&self, thread_panicked: bool) {
        let waiters = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            debug_assert!(state.outcome.is_none(), "worker outcome published twice");
            let outcome = if thread_panicked {
                Err(WorkerJoinError::RuntimePanicked)
            } else {
                state
                    .worker_outcome
                    .take()
                    .unwrap_or(Err(WorkerJoinError::RuntimePanicked))
            };
            state.outcome = Some(outcome);
            self.finished.store(true, Ordering::Release);
            std::mem::take(&mut state.waiters)
                .into_values()
                .collect::<Vec<_>>()
        };

        wake_worker_joiners(waiters);
        self.parent_event.shared.notify();
    }

    pub(crate) fn poll_join(
        &self,
        waiter_id: &mut Option<u64>,
        waiter_active: &mut Option<Arc<AtomicBool>>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkerJoinError>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(outcome) = state.outcome {
            if let Some(active) = waiter_active.take() {
                active.store(false, Ordering::Release);
            }
            if let Some(id) = waiter_id.take() {
                state.waiters.remove(&id);
            }
            return Poll::Ready(outcome);
        }

        let id = match *waiter_id {
            Some(id) => id,
            None => {
                let id = state.next_waiter_id;
                state.next_waiter_id = id.checked_add(1).expect("worker waiter ID space exhausted");
                *waiter_id = Some(id);
                *waiter_active = Some(Arc::new(AtomicBool::new(true)));
                id
            }
        };
        let active = waiter_active
            .as_ref()
            .expect("pending worker waiter should have a liveness token");
        state.waiters.insert(
            id,
            WorkerWaiter {
                active: Arc::clone(active),
                waker: cx.waker().clone(),
            },
        );
        Poll::Pending
    }

    pub(crate) fn remove_waiter(&self, id: u64) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waiters
            .remove(&id);
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waiters
            .len()
    }
}

fn wake_worker_joiners(waiters: Vec<WorkerWaiter>) {
    for waiter in waiters {
        if !waiter.active.swap(false, Ordering::AcqRel) {
            continue;
        }
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waiter.waker.wake()))
        {
            tracing::error!(
                target: trace_targets::RUNTIME,
                event = "worker_join_waker_panicked",
                panic = describe_panic(&*payload),
                "worker join waker panicked; worker completion remains observable",
            );
        }
    }
}

pub(crate) fn lock_queue(queue: &RemoteQueue) -> MutexGuard<'_, VecDeque<SendTask>> {
    queue
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
///
/// # Panics
///
/// Panics if the platform driver cannot be created. Entry points that want to
/// report that failure instead use [`try_ensure_current_thread`] first.
pub(crate) fn with_current_thread<R: Runtime, T>(f: impl FnOnce(&ThreadState) -> T) -> T {
    if let Err(error) = try_ensure_current_thread::<R>() {
        panic!("runtime driver should initialize: {error:?}");
    }
    with_installed_thread(f)
}

/// Installs this thread's runtime state if it is not installed already,
/// reporting driver-creation failure instead of panicking.
///
/// Idempotent: a thread that already has state installed returns `Ok(())`
/// without touching the driver.
pub(crate) fn try_ensure_current_thread<R: Runtime>() -> io::Result<()> {
    if !current_thread_ptr().is_null() {
        return Ok(());
    }
    install_lazy_state::<R>(RuntimeConfig::default())
}

/// Installs this thread's runtime state from an explicit configuration.
///
/// Unlike [`try_ensure_current_thread`] this is *not* idempotent: a thread that
/// already has a runtime reports [`io::ErrorKind::AlreadyExists`]. The driver
/// described by `config` was created when the existing state was installed, so
/// there is nothing left for a second configuration to affect, and quietly
/// accepting one would be the silently-ignored knob that
/// [`crate::Builder`] is shaped to prevent.
pub(crate) fn try_install_configured_thread<R: Runtime>(config: RuntimeConfig) -> io::Result<()> {
    if !current_thread_ptr().is_null() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "this thread already has a runite runtime; it can only be configured before it starts",
        ));
    }
    install_lazy_state::<R>(config)
}

fn install_lazy_state<R: Runtime>(config: RuntimeConfig) -> io::Result<()> {
    assert!(
        matches!(thread_phase(), ThreadPhase::Empty),
        "runite: runtime state is unavailable during thread teardown"
    );
    let (driver, notifier) = R::create_driver_pair(config)?;
    let shared = Arc::new(ThreadShared::new(notifier));
    let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
    install_owned_state(Box::new(ThreadState::new(
        shared, driver, None, generation, config,
    )));
    Ok(())
}

/// Non-initializing accessor. Use from contexts that are guaranteed to run
/// only on a thread the scheduler has already installed (waker callbacks,
/// internal scheduler helpers invoked after a public entry point).
///
/// # Panics
///
/// Panics if no thread state is installed on the calling thread.
pub(crate) fn with_installed_thread<T>(f: impl FnOnce(&ThreadState) -> T) -> T {
    let ptr = current_thread_ptr();
    assert!(!ptr.is_null(), "runtime state not installed on this thread");
    // SAFETY: `ptr` is non-null and points to a `ThreadState` owned by this
    // thread until final TLS teardown.
    unsafe { f(&*ptr) }
}

pub(crate) fn try_with_installed_thread<T>(f: impl FnOnce(Option<&ThreadState>) -> T) -> T {
    let ptr = current_thread_ptr();
    if ptr.is_null() {
        f(None)
    } else {
        // SAFETY: `ptr` is non-null and points to a `ThreadState` owned by
        // this thread until final TLS teardown.
        unsafe { f(Some(&*ptr)) }
    }
}

pub(crate) fn mark_teardown_panicked() {
    try_with_installed_thread(|state| {
        if let Some(state) = state
            && state.tearing_down.get()
        {
            state.teardown_panicked.set(true);
        }
    });
}

pub(crate) fn install_thread(
    shared: Arc<ThreadShared>,
    driver: Box<dyn DriverBackend>,
    worker_completion: Option<Arc<WorkerCompletion>>,
    config: RuntimeConfig,
) {
    debug_assert!(
        current_thread_ptr().is_null(),
        "thread runtime already installed"
    );
    assert!(
        matches!(thread_phase(), ThreadPhase::Empty),
        "runite: cannot install runtime state during thread teardown"
    );
    let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
    install_owned_state(Box::new(ThreadState::new(
        shared,
        driver,
        worker_completion,
        generation,
        config,
    )));
}

#[cfg(test)]
pub(crate) fn teardown_thread() {
    let _ = teardown_owned_thread(false);
}

fn current_thread_ptr() -> *const ThreadState {
    CURRENT_THREAD.try_with(Cell::get).unwrap_or(ptr::null())
}

fn install_owned_state(state: Box<ThreadState>) -> *const ThreadState {
    let state = Rc::<ThreadState>::from(state);
    // Bind first. This ordering is what makes driver-specific TLS outlive the
    // `THREAD_OWNER` destructor at OS-thread exit.
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        state.driver.bind_current_thread();
    })) {
        set_thread_phase(ThreadPhase::TearingDown);
        let rejected = state.shared.close();
        let _ = state.shared.join_notification_retry();
        for task in rejected {
            let _ = drop_user_value(task, "remote macrotask after driver bind failure");
        }
        if let Err(unbind_payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.driver.unbind_current_thread();
        })) {
            tracing::error!(
                target: trace_targets::DRIVER,
                event = "driver_unbind_after_bind_failure_panicked",
                panic = describe_panic(&*unbind_payload),
                "runtime driver panicked while rolling back a failed bind",
            );
        }
        let _ = drop_user_value(state, "runtime state after driver bind failure");
        set_thread_phase(ThreadPhase::Empty);
        resume_unwind(payload);
    }
    let ptr = THREAD_OWNER
        .try_with(|owner| owner.install(state))
        .expect("runtime TLS unavailable during thread teardown");
    CURRENT_THREAD
        .try_with(|cell| cell.set(ptr))
        .expect("runtime TLS unavailable during thread teardown");
    set_thread_phase(ThreadPhase::Active);
    ptr
}

/// Tears down this thread's runtime now, from the caller's own stack.
///
/// Returns `false` if there was nothing installed to tear down.
pub(crate) fn shutdown_current_thread() -> bool {
    let installed = try_with_installed_thread(|state| {
        if let Some(state) = state {
            assert!(
                !state.in_event_loop.get(),
                "runite: cannot shut the runtime down from within a task or \
                 callback running on it; call `shutdown` after `run` returns",
            );
            assert!(
                !state.tearing_down.get(),
                "runite: the runtime is already tearing down",
            );
            true
        } else {
            false
        }
    });
    if !installed {
        return false;
    }
    // Not a final exit: the thread outlives this call and goes on running its
    // own code, so it is left in the same phase it started in. Marking it
    // terminated here would make the next runtime call — `block_on`,
    // `try_block_on`, `Builder::build` — assert instead of installing a fresh
    // runtime, which is the reuse `shutdown` documents.
    let _ = teardown_owned_thread(false);
    true
}

fn teardown_owned_thread(final_exit: bool) -> Result<(), ThreadTeardownError> {
    let state = THREAD_OWNER.try_with(ThreadOwner::take).ok().flatten();
    if let Some(state) = state {
        finalize_thread(state, final_exit)
    } else if final_exit {
        set_thread_phase(ThreadPhase::Terminated);
        Ok(())
    } else {
        Ok(())
    }
}

fn finalize_thread(state: Rc<ThreadState>, final_exit: bool) -> Result<(), ThreadTeardownError> {
    let _ = CURRENT_THREAD.try_with(|cell| cell.set(Rc::as_ptr(&state)));
    state.tearing_down.set(true);
    state.teardown_panicked.set(false);
    set_thread_phase(ThreadPhase::TearingDown);
    let shared = Arc::clone(&state.shared);
    let remote_tasks = shared.close();
    let teardown_failed = Cell::new(false);
    if shared.join_notification_retry() {
        teardown_failed.set(true);
    }

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut pointer_reset = CurrentPointerReset { armed: true };
        // Before anything is torn down: hooks exist to observe a live runtime
        // one last time, and running them after the tasks are cancelled would
        // hand them a runtime that can no longer do anything.
        let hooks = std::mem::take(&mut *state.shutdown_hooks.borrow_mut());
        for hook in hooks {
            if run_shutdown_hook(hook) {
                teardown_failed.set(true);
            }
        }
        cancel_all_registered_tasks(&state);
        if state.teardown_panicked.get() {
            teardown_failed.set(true);
        }

        for task in remote_tasks {
            if drop_user_value(task, "remote macrotask") {
                teardown_failed.set(true);
            }
        }
        let local_microtasks = {
            let mut tasks = state.local_microtasks.borrow_mut();
            std::mem::take(&mut *tasks)
        };
        for task in local_microtasks {
            if drop_user_value(task, "local microtask") {
                teardown_failed.set(true);
            }
        }
        let local_macrotasks = {
            let mut tasks = state.local_macrotasks.borrow_mut();
            std::mem::take(&mut *tasks)
        };
        for task in local_macrotasks {
            if drop_user_value(task.task, "local macrotask") {
                teardown_failed.set(true);
            }
        }
        state.live_timeouts.borrow_mut().clear();
        let timers = {
            let mut timers = state.timers.borrow_mut();
            std::mem::replace(&mut *timers, TimerHeap::new())
        };
        if drop_user_value(timers, "timer callbacks") {
            teardown_failed.set(true);
        }
        let intervals = {
            let mut intervals = state.live_intervals.borrow_mut();
            std::mem::take(&mut *intervals)
        };
        for (_, interval) in intervals {
            if drop_user_value(interval, "interval callback") {
                teardown_failed.set(true);
            }
        }
        let children = {
            let mut children = state.children.borrow_mut();
            std::mem::take(&mut *children)
        };
        for mut child in children {
            if let Some(on_exit) = child.on_exit.take()
                && drop_user_value(on_exit, "worker exit callback")
            {
                teardown_failed.set(true);
            }
        }

        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.driver.unbind_current_thread();
        })) {
            tracing::error!(
                target: trace_targets::DRIVER,
                event = "driver_unbind_panicked",
                panic = describe_panic(&*payload),
                "runtime driver panicked while unbinding during thread teardown",
            );
            teardown_failed.set(true);
        }

        if state.teardown_panicked.get() {
            teardown_failed.set(true);
        }
        pointer_reset.clear();
        drop(state);
    }));

    let _ = CURRENT_THREAD.try_with(|cell| cell.set(ptr::null()));
    set_thread_phase(if final_exit {
        ThreadPhase::Terminated
    } else {
        ThreadPhase::Empty
    });
    if let Err(payload) = outcome {
        teardown_failed.set(true);
        tracing::error!(
            target: trace_targets::RUNTIME,
            event = "thread_teardown_panicked",
            panic = describe_panic(&*payload),
            "runtime state panicked during final thread teardown; isolating panic",
        );
    }

    if teardown_failed.get() {
        Err(ThreadTeardownError)
    } else {
        Ok(())
    }
}

struct CurrentPointerReset {
    armed: bool,
}

impl CurrentPointerReset {
    fn clear(&mut self) {
        let _ = CURRENT_THREAD.try_with(|cell| cell.set(ptr::null()));
        self.armed = false;
    }
}

impl Drop for CurrentPointerReset {
    fn drop(&mut self) {
        if self.armed {
            let _ = CURRENT_THREAD.try_with(|cell| cell.set(ptr::null()));
        }
    }
}

fn cancel_all_registered_tasks(state: &ThreadState) {
    loop {
        let tasks = {
            let mut tasks = state.tasks.borrow_mut();
            std::mem::take(&mut *tasks)
                .into_values()
                .collect::<Vec<_>>()
        };
        if tasks.is_empty() {
            return;
        }
        cancel_tasks_for_shutdown(tasks);
    }
}

fn thread_phase() -> ThreadPhase {
    THREAD_PHASE
        .try_with(Cell::get)
        .unwrap_or(ThreadPhase::Terminated)
}

fn set_thread_phase(phase: ThreadPhase) {
    let _ = THREAD_PHASE.try_with(|current| current.set(phase));
}

#[cfg(windows)]
fn mark_closed_last_resort(state: Rc<ThreadState>) {
    state.tearing_down.set(true);
    set_thread_phase(ThreadPhase::TearingDown);
    state.shared.mark_closed_without_cleanup();
    let _ = CURRENT_THREAD.try_with(|cell| cell.set(ptr::null()));
    set_thread_phase(ThreadPhase::Terminated);

    // Windows invokes TLS destructors while holding the loader lock. Running
    // user `Drop` code, joining helpers, or closing an IOCP here can deadlock
    // process shutdown. Runtime-owned worker threads use
    // `ThreadTeardownGuard` and never reach this fallback. Arbitrary user
    // threads still publish `closed` so external handles fail safely, but the
    // remaining state is deliberately leaked.
    let _ = Rc::into_raw(state);
}

/// Runs one shutdown hook, isolating a panic the way task panics are isolated.
///
/// A hook that panics must not abort teardown: the remaining hooks still have
/// resources to release, and a half-torn-down runtime is worse than a reported
/// panic.
fn run_shutdown_hook(hook: Box<dyn FnOnce()>) -> bool {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(hook)) {
        tracing::error!(
            target: trace_targets::RUNTIME,
            event = "shutdown_hook_panicked",
            panic = describe_panic(&*payload),
            "a runtime shutdown hook panicked; isolating panic and continuing teardown",
        );
        true
    } else {
        false
    }
}

fn drop_user_value<T>(value: T, kind: &'static str) -> bool {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value))) {
        tracing::error!(
            target: trace_targets::RUNTIME,
            event = "teardown_drop_panicked",
            kind,
            panic = describe_panic(&*payload),
            "runtime-owned value panicked from Drop during thread teardown; isolating panic",
        );
        true
    } else {
        false
    }
}

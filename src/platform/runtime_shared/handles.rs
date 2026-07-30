//! Public handle types: `ThreadHandle`, `WorkerHandle`, `TimeoutHandle`,
//! `IntervalHandle`, `JoinHandle`, `YieldNow`.
//!
//! All handles are non-generic — driver and notifier are erased at the
//! [`ThreadShared`](super::state::ThreadShared) level — so the per-platform
//! `runtime.rs` modules can `pub use` them directly without any aliasing.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use super::future_task::{JoinState, TaskShared};
use super::state::{ThreadShared, WorkerCompletion};
use crate::trace_targets;

/// Returned by [`ThreadHandle::queue_macrotask`] when the target runtime has
/// terminated or cannot currently accept and wake remote work.
#[derive(Debug)]
#[non_exhaustive]
pub enum QueueError {
    /// The target thread has committed to final shutdown; no further work can
    /// be queued.
    Closed,
    /// The cross-thread macrotask queue is at capacity, or both attempts to
    /// notify the target driver failed. The task was not accepted; callers
    /// decide whether to retry, drop the work, or panic.
    Full,
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("target runtime thread is closed"),
            Self::Full => f.write_str("target runtime thread cannot currently accept remote work"),
        }
    }
}

impl std::error::Error for QueueError {}

#[derive(Clone)]
/// A cloneable, `Send` handle for queueing macrotasks onto a specific runtime
/// thread from any thread.
///
/// Obtained from [`current_thread_handle`](crate::current_thread_handle) or
/// [`WorkerHandle::thread`]. Use [`queue_macrotask`](Self::queue_macrotask) to send work
/// across threads; the closure runs as a macrotask on the target thread's event
/// loop after that thread has drained its microtask queue.
pub struct ThreadHandle {
    pub(crate) shared: Arc<ThreadShared>,
}

impl std::fmt::Debug for ThreadHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadHandle")
            .finish_non_exhaustive()
    }
}

/// A handle to a worker runtime thread spawned with
/// [`spawn_worker`](crate::spawn_worker).
///
/// Lets the parent thread queue work onto the worker
/// ([`queue_macrotask`](Self::queue_macrotask)), observe its lifecycle
/// ([`is_finished`](Self::is_finished)), and obtain a plain
/// [`ThreadHandle`] to it ([`thread`](Self::thread)). Queued work enters the
/// worker's macrotask queue and runs only after the worker drains its
/// microtasks.
pub struct WorkerHandle {
    pub(crate) thread: ThreadHandle,
    pub(crate) completion: Arc<WorkerCompletion>,
}

impl std::fmt::Debug for WorkerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerHandle")
            .finish_non_exhaustive()
    }
}

/// Future returned by [`WorkerHandle::join`].
///
/// Dropping this future before the worker exits unregisters its waker. It does
/// not cancel or detach the worker; another join future may observe the stored
/// result later. While pending, it also keeps every runtime that has polled it
/// live so run-to-quiescence cannot cancel a valid worker observation.
#[must_use = "futures do nothing unless polled or awaited"]
pub struct WorkerJoin {
    completion: Arc<WorkerCompletion>,
    waiter_id: Option<u64>,
    waiter_active: Option<Arc<AtomicBool>>,
    liveness: Vec<WorkerJoinLiveness>,
}

impl std::fmt::Debug for WorkerJoin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("WorkerJoin").finish_non_exhaustive()
    }
}

struct WorkerJoinLiveness {
    thread: ThreadHandle,
}

impl WorkerJoinLiveness {
    fn new(thread: ThreadHandle) -> Self {
        thread.begin_async_operation();
        Self { thread }
    }

    fn belongs_to(&self, thread: &ThreadHandle) -> bool {
        Arc::ptr_eq(&self.thread.shared, &thread.shared)
    }
}

impl Drop for WorkerJoinLiveness {
    fn drop(&mut self) {
        self.thread.finish_async_operation();
    }
}

/// Error returned by [`WorkerHandle::join`].
///
/// The panic itself is still reported through the process panic hook. Its
/// platform-specific payload is intentionally not exposed, keeping this error
/// portable and `Copy`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkerJoinError {
    /// The worker panicked while installing its runtime driver.
    SetupPanicked,
    /// The worker runtime panicked outside the per-task/callback panic
    /// firewall while driving its event loop or tearing down.
    RuntimePanicked,
}

impl WorkerJoinError {
    /// Returns `true` if the worker panicked during runtime setup.
    pub fn is_setup_panicked(&self) -> bool {
        matches!(self, Self::SetupPanicked)
    }

    /// Returns `true` if the worker panicked while running or tearing down.
    pub fn is_runtime_panicked(&self) -> bool {
        matches!(self, Self::RuntimePanicked)
    }
}

impl std::fmt::Display for WorkerJoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetupPanicked => f.write_str("worker runtime setup panicked"),
            Self::RuntimePanicked => f.write_str("worker runtime panicked"),
        }
    }
}

impl std::error::Error for WorkerJoinError {}

#[derive(Clone)]
/// Handle returned by [`time::set_timeout`](crate::time::set_timeout).
///
/// Cancelling this handle from a different runtime thread than the one that
/// created it is a no-op rather than a panic: the `generation` field uniquely
/// identifies the originating `ThreadState`, so a stale handle simply fails
/// the generation check and is silently ignored.
pub struct TimeoutHandle {
    pub(crate) id: usize,
    pub(crate) generation: u64,
}

impl std::fmt::Debug for TimeoutHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TimeoutHandle")
            .finish_non_exhaustive()
    }
}

impl TimeoutHandle {
    /// Cancels the pending timeout.
    ///
    /// Cancellation still suppresses a callback whose deadline has expired
    /// but whose macrotask has not started. Once the callback begins, this is a
    /// no-op.
    ///
    /// Dropping a `TimeoutHandle` does **not** cancel the timeout; the handle is
    /// a cloneable cancellation token, so you must keep it and call `cancel` to
    /// stop the callback from firing.
    pub fn cancel(&self) {
        super::scheduler::cancel_timeout(self);
    }

    /// Wraps this token in a guard that cancels the timeout when dropped.
    ///
    /// For a timeout whose lifetime belongs to a scope rather than to the
    /// program. See [`CancelOnDrop`].
    pub fn cancel_on_drop(self) -> CancelOnDrop<Self> {
        CancelOnDrop {
            handle: self,
            thread_affine: PhantomData,
        }
    }
}

#[derive(Clone)]
/// Handle returned by [`time::set_interval`](crate::time::set_interval).
///
/// Cancelling this handle from a different runtime thread than the one that
/// created it is a no-op rather than a panic; see [`TimeoutHandle`] for the
/// generation-token rationale.
pub struct IntervalHandle {
    pub(crate) id: usize,
    pub(crate) generation: u64,
}

impl std::fmt::Debug for IntervalHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IntervalHandle")
            .finish_non_exhaustive()
    }
}

impl IntervalHandle {
    /// Cancels the repeating timer, preventing any further callback
    /// invocations. Cancelling an already-cancelled interval is a no-op.
    ///
    /// Dropping an `IntervalHandle` does **not** cancel the interval; the handle
    /// is a cloneable cancellation token, so you must keep it and call `cancel`
    /// to stop the repeating callback (and to let the runtime exit).
    pub fn cancel(&self) {
        super::scheduler::cancel_interval(self);
    }

    /// Wraps this token in a guard that cancels the interval when dropped.
    ///
    /// Particularly relevant for intervals: an uncancelled interval keeps the
    /// runtime alive, so a leaked one prevents `run()` from ever returning.
    /// See [`CancelOnDrop`].
    pub fn cancel_on_drop(self) -> CancelOnDrop<Self> {
        CancelOnDrop {
            handle: self,
            thread_affine: PhantomData,
        }
    }
}

/// A timer handle that cancels when it is dropped.
///
/// Created by [`TimeoutHandle::cancel_on_drop`] or
/// [`IntervalHandle::cancel_on_drop`]. The plain handles are cloneable
/// cancellation *tokens* — dropping one leaves the timer running, matching
/// JavaScript's `setInterval`/`clearInterval` and this crate's own
/// [`JoinHandle`], which detaches on drop. That is the right default for a
/// timer whose lifetime is not tied to any particular value, and the wrong one
/// for a timer that belongs to a scope.
///
/// This wrapper is the second case: hold it for as long as the timer should
/// run, and let it fall out of scope to stop. It is deliberately **not**
/// `Clone` — two owners of a cancel-on-drop guard would mean the first drop
/// wins, which is not a useful contract.
///
/// It is also **not** `Send`, unlike the tokens it wraps. Cancelling a timer
/// from a thread other than the one that armed it is a documented no-op, which
/// a caller who spelled out `handle.cancel()` can reason about — but a guard
/// exists precisely so nobody spells the cancellation out. A guard moved to
/// another thread would drop there, cancel nothing, and leave an interval
/// keeping the original runtime alive forever. Use [`into_inner`](Self::into_inner)
/// to get the `Send` token back if a token really is what you want to move.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// runite::spawn(async {
///     let ticker = runite::time::set_interval(Duration::from_millis(1), || {})
///         .cancel_on_drop();
///     // ... work that the ticker accompanies ...
///     drop(ticker); // stops here, rather than outliving the scope
/// });
/// runite::run();
/// ```
#[derive(Debug)]
#[must_use = "the timer is cancelled as soon as this guard is dropped"]
pub struct CancelOnDrop<H: TimerCancel> {
    handle: H,
    /// Binds the guard to the thread that created it; see the type docs.
    thread_affine: PhantomData<Rc<()>>,
}

impl<H: TimerCancel> CancelOnDrop<H> {
    /// Returns the underlying token without cancelling.
    ///
    /// Use this to hand the timer back to a longer-lived owner: the guard is
    /// consumed, so nothing cancels, and the returned token behaves as it did
    /// before it was wrapped.
    pub fn into_inner(self) -> H {
        // A type with a `Drop` impl cannot be destructured, so the guard is
        // neutralised rather than taken apart: `ManuallyDrop` suppresses the
        // cancellation and the token is cloned back out. That is what the
        // `Clone` bound on `TimerCancel` buys — no `unsafe` here, and no
        // `Option` field forcing a fallible `Deref` on the guard.
        let this = std::mem::ManuallyDrop::new(self);
        this.handle.clone()
    }

    /// Cancels the timer now rather than at the end of the scope.
    pub fn cancel(self) {
        drop(self);
    }
}

impl<H: TimerCancel> std::ops::Deref for CancelOnDrop<H> {
    type Target = H;

    fn deref(&self) -> &H {
        &self.handle
    }
}

impl<H: TimerCancel> Drop for CancelOnDrop<H> {
    fn drop(&mut self) {
        self.handle.cancel_timer();
    }
}

/// Timer tokens that a [`CancelOnDrop`] guard can stop.
///
/// Sealed in practice: implemented only for [`TimeoutHandle`] and
/// [`IntervalHandle`], whose `cancel` is idempotent and thread-safe by way of
/// the generation check.
///
/// `Clone` is a bound because a timer token only *names* a timer — duplicating
/// one cannot change what cancelling does, which is why both handles are
/// already `Clone` — and it is what lets [`CancelOnDrop::into_inner`] hand the
/// token back out of a `Drop` type without `unsafe`.
pub trait TimerCancel: Clone {
    /// Cancels the timer this token identifies.
    fn cancel_timer(&self);
}

impl TimerCancel for TimeoutHandle {
    fn cancel_timer(&self) {
        self.cancel();
    }
}

impl TimerCancel for IntervalHandle {
    fn cancel_timer(&self) {
        self.cancel();
    }
}

/// Handle returned by `spawn`.
///
/// Awaiting a join handle yields `Result<T, JoinError>` rather than the queued
/// future's output directly: `Ok(output)` contains the future's output, while
/// [`Err(JoinError::Aborted)`](crate::task::JoinError) means the task was
/// aborted via [`abort`](Self::abort) before it completed, and
/// [`Err(JoinError::Cancelled)`](crate::task::JoinError) means `run()` reached
/// quiescence with no scheduler-visible event capable of waking the task.
///
/// Dropping a `JoinHandle` does **not** itself cancel the task — it continues
/// detached until completion, explicit abort, or shutdown cancellation at
/// `run()` quiescence. Use [`abort`](Self::abort) (or an [`AbortHandle`]) to
/// cancel explicitly.
pub struct JoinHandle<T> {
    pub(crate) state: Rc<JoinState<T>>,
}

impl<T> std::fmt::Debug for JoinHandle<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("JoinHandle").finish_non_exhaustive()
    }
}

impl<T> JoinHandle<T> {
    /// Aborts the task.
    ///
    /// Once the abort is observed, the task's future is dropped without being
    /// polled again. Dropping the future may cancel runtime interest in driver
    /// operations it was awaiting, but underlying OS work may still complete.
    /// A subsequent await of this handle resolves to
    /// [`Err(JoinError::Aborted)`](crate::task::JoinError). Aborting a task that
    /// has already completed is a no-op.
    pub fn abort(&self) {
        self.state.shared.abort();
    }

    /// Returns `true` once the task has completed, been aborted, been
    /// shutdown-cancelled, or panicked.
    pub fn is_finished(&self) -> bool {
        self.state.shared.is_finished()
    }

    /// Returns a cheap, cloneable handle that can abort this task from elsewhere
    /// without holding the `JoinHandle` (and thus without the ability to await
    /// the output).
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle {
            shared: Rc::clone(&self.state.shared),
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, crate::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.poll(cx)
    }
}

/// Cloneable handle that can abort a queued task without joining it.
///
/// Obtained from [`JoinHandle::abort_handle`]. Like the runtime's futures, this
/// handle is `!Send` and only valid on the runtime thread that created the
/// task. This differs from Tokio's `Send` abort handles: runite tasks are local
/// and the handle is backed by `Rc`, so abort requests cannot be sent directly
/// across threads. From another thread, use [`ThreadHandle::queue_macrotask`] to
/// schedule a closure on the owning runtime thread and abort from there.
#[derive(Clone)]
pub struct AbortHandle {
    shared: Rc<TaskShared>,
}

impl std::fmt::Debug for AbortHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AbortHandle")
            .finish_non_exhaustive()
    }
}

impl AbortHandle {
    /// Aborts the associated task. See [`JoinHandle::abort`].
    pub fn abort(&self) {
        self.shared.abort();
    }

    /// Returns `true` once the associated task has reached any terminal state.
    pub fn is_finished(&self) -> bool {
        self.shared.is_finished()
    }
}

/// Future returned by `yield_now`.
///
/// Awaiting this future will immediately yield control back to the runtime
/// scheduler, allowing other queued microtasks to run before the current task
/// continues executing. Note that continuations of futures run as
/// microtasks, so this can only yield to other microtasks and not to
/// macrotasks (driver events such as file or network I/O, timers, or channel
/// messages). To yield to macrotasks, you must allow the flow of execution
/// to return to the runtime event loop and flush the full microtask queue,
/// for example by awaiting a timer.
#[must_use = "futures do nothing unless awaited or polled"]
pub struct YieldNow {
    pub(crate) yielded: bool,
}

impl std::fmt::Debug for YieldNow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("YieldNow").finish_non_exhaustive()
    }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl ThreadHandle {
    /// Queues a macrotask onto this runtime thread.
    ///
    /// Remote tasks are first drained into the target thread's local macrotask
    /// queue. They run in that queue only after the target thread drains all
    /// ready microtasks.
    ///
    /// Returns [`QueueError::Closed`] if the target thread is already closed,
    /// or [`QueueError::Full`] if the queue is at capacity or the target driver
    /// could not be notified. On either error the closure was not accepted.
    pub fn queue_macrotask<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        let result = self.shared.enqueue_macro(Box::new(task));
        tracing::trace!(
            target: trace_targets::SCHEDULER,
            event = "queue_remote_task",
            // Both ends, because a cross-thread post is the one event where
            // "which runtime" has two answers. The sender is `None` when the
            // posting thread has no runtime of its own.
            runtime_id = super::scheduler::trace_runtime_id(),
            turn_id = super::scheduler::trace_turn_id(),
            to_runtime_id = self.shared.runtime_id.0,
            queue = "remote_macro",
            queued = result.is_ok(),
            "queueing remote macrotask"
        );
        result
    }

    /// Queues an internal cross-thread wake onto this runtime thread, bypassing
    /// the bounded-queue capacity limit.
    ///
    /// Used by the completion machinery and the task waker to deliver a wake to
    /// its owning thread. Unlike [`queue_macrotask`](Self::queue_macrotask)
    /// this bypasses capacity and retries notification once immediately. If
    /// notification remains unavailable, the accepted wake stays queued while
    /// one helper retries with bounded backoff until delivery or final thread
    /// closure. It therefore never returns [`QueueError::Full`]. The number of
    /// accepted wakes is bounded by in-flight operations and live tasks rather
    /// than by user input. See
    /// [`ThreadShared::enqueue_internal_wake`](super::state::ThreadShared::enqueue_internal_wake).
    pub(crate) fn queue_internal_wake<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.shared.enqueue_internal_wake(Box::new(task))
    }

    /// Returns `true` once the target runtime thread has committed to final
    /// shutdown.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Returns `true` iff this handle refers to the runtime thread currently
    /// executing this code.
    ///
    /// Returns `false` when called from a thread that has no runtime state
    /// installed (e.g. a `std::thread::spawn`'d worker or a blocking-pool
    /// thread), so callers can safely use this as a "may I dispatch a
    /// microtask?" probe — a `false` result always means "no; you must go
    /// through the cross-thread macrotask path".
    pub fn is_current(&self) -> bool {
        super::state::try_with_installed_thread(|state| {
            state
                .map(|s| Arc::ptr_eq(&self.shared, &s.shared))
                .unwrap_or(false)
        })
    }

    pub(crate) fn begin_async_operation(&self) {
        self.shared.pending_ops.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn finish_async_operation(&self) {
        super::state::RuntimeCounters::bump(&self.shared.counters.operations_completed);
        let previous = self.shared.pending_ops.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "async operation count underflow");
        // The notification exists to make a *parked* thread re-evaluate
        // quiescence. A thread running this code is not parked -- it is
        // dispatching the completion -- and will re-evaluate on its own next
        // turn, so notifying itself only costs a wake round trip. On Linux that
        // is an `IORING_OP_MSG_RING` to its own ring plus the `io_uring_enter`
        // to submit it, per completion, which is what collapses submission
        // batches back to size one.
        //
        // Skipping it cannot strand the durable-retry protocol: that tracks
        // delivered-vs-requested generations, and this path mints no
        // generation. The waker itself has already taken its own same-thread
        // fast path in `CompletionState::queue_wake`.
        if self.is_current() {
            return;
        }
        self.shared.notify();
    }
}

impl WorkerHandle {
    /// Queues a macrotask onto the worker thread.
    ///
    /// The closure is sent through the worker's remote queue, then runs as a
    /// macrotask after the worker drains its microtasks.
    ///
    /// Returns [`QueueError::Closed`] if the worker has already shut down, or
    /// [`QueueError::Full`] if it cannot currently accept and notify the work.
    pub fn queue_macrotask<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread.queue_macrotask(task)
    }

    /// Returns `true` once the worker OS thread, including its TLS destructors,
    /// has fully exited.
    pub fn is_finished(&self) -> bool {
        self.completion.finished.load(Ordering::Acquire)
    }

    /// Waits for the worker's runtime state and OS thread to finish.
    ///
    /// The result remains stored after completion, so this method may be
    /// awaited repeatedly. Worker setup and runtime panics are isolated to the
    /// worker thread and returned as [`WorkerJoinError`] values. A panic from a
    /// scheduled task or callback remains governed by the runtime's ordinary
    /// per-task panic isolation and does not itself fail the worker. OS joining
    /// runs on a dedicated non-runtime reaper, so polling this future never
    /// blocks a runtime thread.
    pub fn join(&self) -> WorkerJoin {
        WorkerJoin {
            completion: Arc::clone(&self.completion),
            waiter_id: None,
            waiter_active: None,
            liveness: Vec::new(),
        }
    }

    /// Returns a generic [`ThreadHandle`] for the worker thread.
    pub fn thread(&self) -> ThreadHandle {
        self.thread.clone()
    }
}

impl Future for WorkerJoin {
    type Output = Result<(), WorkerJoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(current) = super::scheduler::try_current_thread_handle()
            && !this
                .liveness
                .iter()
                .any(|liveness| liveness.belongs_to(&current))
        {
            this.liveness.push(WorkerJoinLiveness::new(current));
        }

        let result = this
            .completion
            .poll_join(&mut this.waiter_id, &mut this.waiter_active, cx);
        if result.is_ready() {
            this.liveness.clear();
        }
        result
    }
}

impl Drop for WorkerJoin {
    fn drop(&mut self) {
        if let Some(active) = self.waiter_active.take() {
            active.store(false, Ordering::Release);
        }
        if let Some(id) = self.waiter_id.take() {
            self.completion.remove_waiter(id);
        }
        self.liveness.clear();
    }
}

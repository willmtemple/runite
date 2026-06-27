//! Public handle types: `ThreadHandle`, `WorkerHandle`, `TimeoutHandle`,
//! `IntervalHandle`, `JoinHandle`, `YieldNow`.
//!
//! All handles are non-generic — driver and notifier are erased at the
//! [`ThreadShared`](super::state::ThreadShared) level — so the per-platform
//! `runtime.rs` modules can `pub use` them directly without any aliasing.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use super::future_task::{JoinState, TaskShared};
use super::state::{ThreadShared, WorkerCompletion};
use crate::trace_targets;

/// Returned by [`ThreadHandle::queue_macrotask`] when the target runtime is shutting
/// down or its cross-thread macrotask queue is full.
#[derive(Debug)]
pub enum QueueError {
    /// The target thread has finished shutting down; no further work can be queued.
    Closed,
    /// The cross-thread macrotask queue is at capacity. Try again later; callers
    /// decide whether to retry, drop the work, or panic.
    Full,
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("target runtime thread is closed"),
            Self::Full => f.write_str("target runtime thread remote queue is full"),
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

impl TimeoutHandle {
    /// Cancels the pending timeout. If the callback has already fired, this is
    /// a no-op.
    ///
    /// Dropping a `TimeoutHandle` does **not** cancel the timeout; the handle is
    /// a cloneable cancellation token, so you must keep it and call `cancel` to
    /// stop the callback from firing.
    pub fn cancel(&self) {
        super::scheduler::cancel_timeout(self);
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
}

/// Handle returned by `spawn`.
///
/// Awaiting a join handle yields `Result<T, JoinError>` rather than the queued
/// future's output directly: `Ok(output)` contains the future's output, while
/// [`Err(JoinError::Aborted)`](crate::task::JoinError) means the task was
/// aborted via [`abort`](Self::abort) before it completed.
///
/// Dropping a `JoinHandle` does **not** cancel the task — it continues to run
/// to completion detached. Use [`abort`](Self::abort) (or an
/// [`AbortHandle`]) to cancel.
pub struct JoinHandle<T> {
    pub(crate) state: Rc<JoinState<T>>,
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

    /// Returns `true` once the task has completed or been aborted.
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

impl AbortHandle {
    /// Aborts the associated task. See [`JoinHandle::abort`].
    pub fn abort(&self) {
        self.shared.abort();
    }

    /// Returns `true` once the associated task has completed or been aborted.
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
pub struct YieldNow {
    pub(crate) yielded: bool,
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
    /// Returns [`QueueError::Closed`] if the target thread is already closed, or
    /// [`QueueError::Full`] if the cross-thread macrotask queue is at capacity.
    pub fn queue_macrotask<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        let result = self.shared.enqueue_macro(Box::new(task));
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::SCHEDULER,
            event = "queue_remote_task",
            queue = "remote_macro",
            queued = result.is_ok(),
            "queueing remote macrotask"
        );
        result
    }

    /// Returns `true` if the target runtime thread has shut down.
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

    #[allow(dead_code)]
    pub(crate) fn begin_async_operation(&self) {
        self.shared.pending_ops.fetch_add(1, Ordering::AcqRel);
    }

    #[allow(dead_code)]
    pub(crate) fn finish_async_operation(&self) {
        let previous = self.shared.pending_ops.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "async operation count underflow");
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
    /// [`QueueError::Full`] if its cross-thread macrotask queue is at capacity.
    pub fn queue_macrotask<F>(&self, task: F) -> Result<(), QueueError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.thread.queue_macrotask(task)
    }

    /// Returns `true` once the worker thread has fully exited.
    pub fn is_finished(&self) -> bool {
        self.completion.finished.load(Ordering::Acquire)
    }

    /// Returns a generic [`ThreadHandle`] for the worker thread.
    pub fn thread(&self) -> ThreadHandle {
        self.thread.clone()
    }
}

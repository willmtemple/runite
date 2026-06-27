#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::platform::current::runtime::{
    QueueError, ThreadHandle, current_thread_handle, queue_microtask, queue_task,
};
use crate::trace_targets;

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    /// Per-thread count of same-thread (local macrotask) wakes observed by
    /// `queue_wake` on this thread. Used by unit tests to assert that
    /// same-thread completions take the local macrotask path.
    pub(crate) static LOCAL_WAKE_COUNT: Cell<u64> = const { Cell::new(0) };
    /// Per-thread count of cross-thread macrotask wakes observed by
    /// `queue_wake` on this thread. Per-thread (not global) so concurrent
    /// tests do not pollute each other's measurements.
    pub(crate) static REMOTE_WAKE_COUNT: Cell<u64> = const { Cell::new(0) };
}

type CancelCallback = Box<dyn FnOnce() + Send + 'static>;

/// How a same-thread completion wake is scheduled on the owner's run loop.
///
/// This mirrors the JavaScript event-loop distinction between the microtask
/// checkpoint and a macro turn, and is chosen by whoever *creates* the
/// completion — the completion primitive itself is source-agnostic:
///
/// * [`WakeClass::Microtask`] — an **in-process resolution** (a channel send,
///   `Notify`, an mpsc slot opening). These are the `Promise.resolve` analogs:
///   they run on the current microtask checkpoint, before the loop takes
///   another macro turn. Same-thread channel ops use this.
/// * [`WakeClass::Macrotask`] — an **I/O completion** (a CQE / readiness
///   event). Like a Node poll-phase callback, it always defers to a macro
///   turn so it cannot preempt an in-flight microtask checkpoint and cannot
///   starve the timer/macrotask queues. All I/O ops use this.
///
/// The distinction only applies to the same-thread wake path. A cross-thread
/// wake is always a macrotask: it is an external event by definition, and the
/// only way to cross the thread boundary is the owner's notify queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WakeClass {
    Microtask,
    Macrotask,
}

struct CompletionState<T> {
    owner: ThreadHandle,
    wake_class: WakeClass,
    interested: AtomicBool,
    finished: AtomicBool,
    wake_queued: AtomicBool,
    result: Mutex<Option<T>>,
    waker: Mutex<Option<Waker>>,
    cancel: Mutex<Option<CancelCallback>>,
}

impl<T: Send + 'static> CompletionState<T> {
    fn queue_wake(self: &Arc<Self>) {
        if self.wake_queued.swap(true, Ordering::AcqRel) {
            return;
        }

        // Same-thread completion. The completer is running on the owner's
        // runtime thread, so we can wake without the cross-thread machinery
        // (no remote-queue mutex, no driver notify, no MSG_RING syscall) by
        // enqueueing directly. This is the common case for purely local I/O on
        // Linux, where the io_uring CQE handler invokes `finish` on the same
        // thread that owns the future, and for same-thread channel sends.
        //
        // The queue we enqueue onto depends on the completion's `wake_class`
        // (see [`WakeClass`]): an I/O completion takes a macro turn, an
        // in-process resolution joins the current microtask checkpoint.
        if self.owner.is_current() {
            #[cfg(test)]
            LOCAL_WAKE_COUNT.with(|c| c.set(c.get() + 1));

            let state = Arc::clone(self);
            let wake = move || {
                state.wake_queued.store(false, Ordering::Release);
                if let Some(waker) = state.waker.lock().unwrap().take() {
                    waker.wake();
                }
            };
            match self.wake_class {
                WakeClass::Microtask => queue_microtask(wake),
                WakeClass::Macrotask => queue_task(wake),
            }
            return;
        }

        // Slow path: cross-thread completion. Queue as a macrotask on the
        // owner thread; on Linux this routes through the notifier
        // (`IORING_OP_MSG_RING`), on macOS through the eventfd-equivalent
        // pipe wakeup.
        #[cfg(test)]
        REMOTE_WAKE_COUNT.with(|c| c.set(c.get() + 1));

        let state = Arc::clone(self);
        match self.owner.queue_macrotask(move || {
            state.wake_queued.store(false, Ordering::Release);
            if let Some(waker) = state.waker.lock().unwrap().take() {
                waker.wake();
            }
        }) {
            Ok(()) => {}
            Err(QueueError::Closed) => {
                self.wake_queued.store(false, Ordering::Release);
            }
            Err(QueueError::Full) => {
                // Do not block or retry from a kernel/blocking completion callback.
                // Dropping this wake is preferable to deadlocking the producer.
                tracing::error!(
                    target: trace_targets::SCHEDULER,
                    event = "completion_wake_dropped",
                    "dropping cross-thread completion wake because the remote queue is full"
                );
                self.wake_queued.store(false, Ordering::Release);
            }
        }
    }
}

pub(crate) struct CompletionFuture<T> {
    state: Arc<CompletionState<T>>,
}

pub(crate) struct CompletionHandle<T> {
    state: Arc<CompletionState<T>>,
}

impl<T> Clone for CompletionHandle<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

pub(crate) fn completion<T: Send + 'static>(
    owner: ThreadHandle,
    wake_class: WakeClass,
) -> (CompletionFuture<T>, CompletionHandle<T>) {
    owner.begin_async_operation();
    let state = Arc::new(CompletionState {
        owner,
        wake_class,
        interested: AtomicBool::new(true),
        finished: AtomicBool::new(false),
        wake_queued: AtomicBool::new(false),
        result: Mutex::new(None),
        waker: Mutex::new(None),
        cancel: Mutex::new(None),
    });

    (
        CompletionFuture {
            state: Arc::clone(&state),
        },
        CompletionHandle { state },
    )
}

/// Build a completion owned by the current runtime thread for an **I/O
/// operation**. The wake therefore takes a macro turn ([`WakeClass::Macrotask`]),
/// matching JS poll-phase semantics. Channel/in-process waiters must instead
/// call [`completion`] with [`WakeClass::Microtask`].
pub(crate) fn completion_for_current_thread<T: Send + 'static>()
-> (CompletionFuture<T>, CompletionHandle<T>) {
    completion(current_thread_handle(), WakeClass::Macrotask)
}

impl<T: Send + 'static> CompletionHandle<T> {
    pub(crate) fn complete(self, value: T) {
        self.finish(Some(value));
    }

    pub(crate) fn finish(self, value: Option<T>) {
        if self.state.finished.swap(true, Ordering::AcqRel) {
            return;
        }

        let interested = self.state.interested.load(Ordering::Acquire);
        if interested {
            *self.state.result.lock().unwrap() = value;
            self.state.queue_wake();
        }

        let _ = self.state.cancel.lock().unwrap().take();
        self.state.owner.finish_async_operation();
    }

    pub(crate) fn set_cancel(&self, cancel: impl FnOnce() + Send + 'static) {
        *self.state.cancel.lock().unwrap() = Some(Box::new(cancel));
    }

    pub(crate) fn is_interested(&self) -> bool {
        self.state.interested.load(Ordering::Acquire)
    }
}

impl<T> Future for CompletionFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(value) = self.state.result.lock().unwrap().take() {
            return Poll::Ready(value);
        }

        *self.state.waker.lock().unwrap() = Some(cx.waker().clone());

        if let Some(value) = self.state.result.lock().unwrap().take() {
            let _ = self.state.waker.lock().unwrap().take();
            return Poll::Ready(value);
        }

        Poll::Pending
    }
}

impl<T> Drop for CompletionFuture<T> {
    fn drop(&mut self) {
        if !self.state.interested.swap(false, Ordering::AcqRel) {
            return;
        }

        let _ = self.state.result.lock().unwrap().take();
        let _ = self.state.waker.lock().unwrap().take();

        if self.state.finished.load(Ordering::Acquire) {
            return;
        }

        if let Some(cancel) = self.state.cancel.lock().unwrap().take() {
            // Delegate to the cancel callback (e.g. submit an io_uring cancel).
            // The actual I/O completion will eventually call handle.finish(),
            // which decrements pending_ops.
            cancel();
        } else {
            // No cancel callback was registered — this happens when submit_operation
            // failed before set_cancel could be called, leaving no path through
            // which finish() would run. Decrement pending_ops directly so the
            // runtime does not stall indefinitely waiting for an operation that
            // will never complete. The swap is atomic: if finish() races to set
            // finished first, the swap returns true and we skip the decrement.
            if !self.state.finished.swap(true, Ordering::AcqRel) {
                self.state.owner.finish_async_operation();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::{queue_macrotask, run, spawn};

    /// Same-thread completion must take the local macrotask path and resolve.
    /// I/O completions always defer to a macro turn (JS poll-phase semantics),
    /// so the same-thread fast path enqueues a *local macrotask*, not a
    /// microtask. We use thread-local counters so this test does not race
    /// against any other concurrently-running completion-touching test.
    #[test]
    fn local_completion_uses_local_macrotask_path() {
        LOCAL_WAKE_COUNT.with(|c| c.set(0));
        REMOTE_WAKE_COUNT.with(|c| c.set(0));

        let observed = Arc::new(Mutex::new(None::<i32>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                let (future, handle) = completion_for_current_thread::<i32>();

                spawn(async move {
                    let value = future.await;
                    *observed.lock().unwrap() = Some(value);
                });

                // Complete on the same runtime thread that owns the future.
                // `queue_wake` therefore runs on this thread and bumps the
                // thread-local LOCAL counter via the local-macrotask path.
                handle.complete(42);
            });
        }

        run();

        assert_eq!(*observed.lock().unwrap(), Some(42));

        let local = LOCAL_WAKE_COUNT.with(|c| c.get());
        let remote = REMOTE_WAKE_COUNT.with(|c| c.get());
        assert_eq!(
            local, 1,
            "expected exactly one local macrotask wake on this thread, got {local}"
        );
        assert_eq!(
            remote, 0,
            "local completion must not hit the cross-thread macrotask path, \
             got {remote} remote wakes on this thread"
        );
    }

    /// Cross-thread completion must take the macrotask path and resolve. The
    /// REMOTE counter is bumped on the spawned thread (where `queue_wake`
    /// runs), so the worker reads its own thread-local counters before
    /// exiting and reports them back via a shared atomic for the assertion.
    #[test]
    fn cross_thread_completion_uses_macrotask_path() {
        let observed = Arc::new(Mutex::new(None::<i32>));
        let worker_local = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let worker_remote = Arc::new(std::sync::atomic::AtomicU64::new(0));
        // The worker stores its thread-local wake counters *after* it calls
        // `complete`, but `complete` is what unblocks `run()`. Stash the join
        // handle so we can join the worker before reading the counters; the
        // join provides the happens-before edge that makes the stores visible
        // and avoids a race where the main thread reads them as zero.
        let worker_handle = Arc::new(Mutex::new(None::<std::thread::JoinHandle<()>>));

        {
            let observed = Arc::clone(&observed);
            let worker_local = Arc::clone(&worker_local);
            let worker_remote = Arc::clone(&worker_remote);
            let worker_handle = Arc::clone(&worker_handle);
            queue_macrotask(move || {
                let (future, handle) = completion_for_current_thread::<i32>();

                let join = std::thread::spawn(move || {
                    // Reset this thread's counters so we measure only the
                    // increments produced by this `complete` call.
                    LOCAL_WAKE_COUNT.with(|c| c.set(0));
                    REMOTE_WAKE_COUNT.with(|c| c.set(0));

                    handle.complete(7);

                    worker_local.store(
                        LOCAL_WAKE_COUNT.with(|c| c.get()),
                        std::sync::atomic::Ordering::Release,
                    );
                    worker_remote.store(
                        REMOTE_WAKE_COUNT.with(|c| c.get()),
                        std::sync::atomic::Ordering::Release,
                    );
                });
                *worker_handle.lock().unwrap() = Some(join);

                spawn(async move {
                    let value = future.await;
                    *observed.lock().unwrap() = Some(value);
                });
            });
        }

        run();

        // Join the worker so its counter stores are complete and visible
        // before we assert on them.
        worker_handle
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap();

        assert_eq!(*observed.lock().unwrap(), Some(7));

        let local_on_worker = worker_local.load(std::sync::atomic::Ordering::Acquire);
        let remote_on_worker = worker_remote.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(
            remote_on_worker, 1,
            "expected exactly one remote macrotask wake on the worker, got {remote_on_worker}"
        );
        // The std::thread worker has no runtime state installed, so
        // `is_current()` returns false and we must not have taken the
        // local same-thread fast path.
        assert_eq!(
            local_on_worker, 0,
            "cross-thread completion must not hit the local same-thread path, \
             got {local_on_worker} local wakes on the worker"
        );
    }

    /// Direct unit test for `ThreadHandle::is_current`: must return true on
    /// the owner thread and false from a thread with no runtime installed.
    #[test]
    fn is_current_reflects_owner_thread() {
        let handle = current_thread_handle();
        assert!(
            handle.is_current(),
            "handle of current thread must be current"
        );

        let h = handle.clone();
        let result = std::thread::spawn(move || h.is_current()).join().unwrap();
        assert!(
            !result,
            "handle must not be reported as current on a non-runtime thread"
        );
    }
}

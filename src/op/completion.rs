#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::platform::current::runtime::{
    QueueError, ThreadHandle, current_thread_handle, queue_microtask,
};
use crate::trace_targets;

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    /// Per-thread count of microtask wakes observed by `queue_wake` on this
    /// thread. Used by unit tests to assert that same-thread completions take
    /// the microtask fast path.
    pub(crate) static LOCAL_WAKE_COUNT: Cell<u64> = const { Cell::new(0) };
    /// Per-thread count of cross-thread macrotask wakes observed by
    /// `queue_wake` on this thread. Per-thread (not global) so concurrent
    /// tests do not pollute each other's measurements.
    pub(crate) static REMOTE_WAKE_COUNT: Cell<u64> = const { Cell::new(0) };
}

type CancelCallback = Box<dyn FnOnce() + Send + 'static>;

struct CompletionState<T> {
    owner: ThreadHandle,
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

        // Fast path: same-thread completion. The completer is running on the
        // owner's runtime thread, so we can wake by enqueueing a microtask
        // directly — no remote-queue mutex, no driver notify, no MSG_RING
        // syscall. This is the common case for purely local I/O on Linux,
        // where the io_uring CQE handler invokes `finish` on the same thread
        // that owns the future.
        if self.owner.is_current() {
            #[cfg(test)]
            LOCAL_WAKE_COUNT.with(|c| c.set(c.get() + 1));

            let state = Arc::clone(self);
            queue_microtask(move || {
                state.wake_queued.store(false, Ordering::Release);
                if let Some(waker) = state.waker.lock().unwrap().take() {
                    waker.wake();
                }
            });
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
) -> (CompletionFuture<T>, CompletionHandle<T>) {
    owner.begin_async_operation();
    let state = Arc::new(CompletionState {
        owner,
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

pub(crate) fn completion_for_current_thread<T: Send + 'static>()
-> (CompletionFuture<T>, CompletionHandle<T>) {
    completion(current_thread_handle())
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

    /// Same-thread completion must take the microtask fast path and resolve.
    /// We use thread-local counters so this test does not race against any
    /// other concurrently-running completion-touching test.
    #[test]
    fn local_completion_uses_microtask_path() {
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
                // thread-local LOCAL counter.
                handle.complete(42);
            });
        }

        run();

        assert_eq!(*observed.lock().unwrap(), Some(42));

        let local = LOCAL_WAKE_COUNT.with(|c| c.get());
        let remote = REMOTE_WAKE_COUNT.with(|c| c.get());
        assert_eq!(
            local, 1,
            "expected exactly one local microtask wake on this thread, got {local}"
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

        {
            let observed = Arc::clone(&observed);
            let worker_local = Arc::clone(&worker_local);
            let worker_remote = Arc::clone(&worker_remote);
            queue_macrotask(move || {
                let (future, handle) = completion_for_current_thread::<i32>();

                std::thread::spawn(move || {
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

                spawn(async move {
                    let value = future.await;
                    *observed.lock().unwrap() = Some(value);
                });
            });
        }

        run();

        assert_eq!(*observed.lock().unwrap(), Some(7));

        let local_on_worker = worker_local.load(std::sync::atomic::Ordering::Acquire);
        let remote_on_worker = worker_remote.load(std::sync::atomic::Ordering::Acquire);
        assert_eq!(
            remote_on_worker, 1,
            "expected exactly one remote macrotask wake on the worker, got {remote_on_worker}"
        );
        // The std::thread worker has no runtime state installed, so
        // `is_current()` returns false and we must not have taken the
        // microtask fast path.
        assert_eq!(
            local_on_worker, 0,
            "cross-thread completion must not hit the local microtask path, \
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

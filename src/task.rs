//! Task spawning primitives.
//!
//! Currently exposes a single entry point: [`spawn_blocking`], which moves a
//! blocking closure onto the shared OS-thread pool and returns a future that
//! resolves to the closure's return value.
//!
//! In-runtime async work should use [`crate::queue_future`] instead; this
//! module exists for code that must call blocking syscalls or run CPU-heavy
//! computations without stalling the event loop.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use std::io;

use crate::channel::oneshot;
use crate::sys::blocking;

/// Future returned by [`spawn_blocking`].
///
/// Awaiting it yields the closure's return value. If the worker pool dropped
/// the task without completing it (for example because the process is shutting
/// down), the future resolves to [`JoinError::Cancelled`].
///
/// This handle is itself a future, so it is normally `.await`ed from a future
/// scheduled on a runtime thread.
pub struct BlockingJoinHandle<R: Send + 'static> {
    inner: Pin<Box<dyn Future<Output = Result<R, oneshot::RecvError>> + Send + 'static>>,
}

/// Error returned by awaiting a join handle.
///
/// Produced both by [`BlockingJoinHandle`] (when a blocking-pool worker exits
/// without delivering a value) and by [`crate::JoinHandle`] (when a queued
/// future is aborted before it completes). A queued task's join output is
/// `Result<T, JoinError>`, so callers should handle these errors when awaiting
/// any join handle.
///
/// Use [`JoinError::is_cancelled`] and [`JoinError::is_aborted`] when the caller
/// only needs to distinguish the category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinError {
    /// The worker exited without producing a value.
    ///
    /// This is used for blocking tasks whose result channel closes before the
    /// worker delivers a value, such as during runtime shutdown or panic
    /// unwinding.
    Cancelled,
    /// The task was aborted before it completed.
    ///
    /// This is returned by [`crate::JoinHandle`] when
    /// [`JoinHandle::abort`](crate::JoinHandle::abort) or an
    /// [`AbortHandle`](crate::AbortHandle) cancels the queued future.
    Aborted,
}

impl JoinError {
    /// Returns `true` if the task was aborted before completion.
    ///
    /// This is true only for [`JoinError::Aborted`].
    pub fn is_aborted(&self) -> bool {
        matches!(self, JoinError::Aborted)
    }

    /// Returns `true` if a blocking-pool worker was cancelled (e.g. during
    /// runtime shutdown) without producing a value.
    ///
    /// This is true only for [`JoinError::Cancelled`].
    pub fn is_cancelled(&self) -> bool {
        matches!(self, JoinError::Cancelled)
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => f.write_str("blocking task was cancelled"),
            JoinError::Aborted => f.write_str("task was aborted"),
        }
    }
}

impl std::error::Error for JoinError {}

impl<R: Send + 'static> Future for BlockingJoinHandle<R> {
    type Output = Result<R, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError::Cancelled)),
        }
    }
}

/// Runs `f` on the shared blocking worker pool.
///
/// The returned future resolves with the closure's return value. If the pool's
/// bounded queue is full, returns [`io::ErrorKind::WouldBlock`] synchronously.
///
/// `f` runs on a real OS thread; it may call blocking syscalls freely. Avoid
/// touching any per-runtime-thread state from inside `f` — this is a pool
/// thread, not a runtime thread.
///
/// # Examples
///
/// ```
/// use std::sync::{
///     Arc,
///     atomic::{AtomicUsize, Ordering},
/// };
///
/// let observed = Arc::new(AtomicUsize::new(0));
/// let observed_task = Arc::clone(&observed);
///
/// runite::queue_future(async move {
///     let handle = runite::spawn_blocking(|| 40usize + 2).expect("blocking task should queue");
///     let value = handle.await.expect("blocking task should complete");
///     observed_task.store(value, Ordering::SeqCst);
/// });
///
/// runite::run();
///
/// assert_eq!(observed.load(Ordering::SeqCst), 42);
/// ```
pub fn spawn_blocking<F, R>(f: F) -> io::Result<BlockingJoinHandle<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (sender, mut receiver) = oneshot::channel::<R>();
    blocking::spawn_blocking(move || {
        let value = f();
        let _ = sender.send(value);
    })?;
    let inner: Pin<Box<dyn Future<Output = Result<R, oneshot::RecvError>> + Send + 'static>> =
        Box::pin(async move { receiver.recv().await });
    Ok(BlockingJoinHandle { inner })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{queue_future, run, run_until_stalled};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn spawn_blocking_returns_value() {
        let result = Arc::new(AtomicUsize::new(0));
        let result_clone = Arc::clone(&result);

        // Run on a dedicated runtime thread and drive with a blocking `run()`,
        // which parks until the blocking-pool worker's cross-thread completion
        // wake lands. This is deterministic, unlike polling `run_until_stalled`
        // with a timed retry loop (which was flaky under parallel test load).
        std::thread::spawn(move || {
            queue_future(async move {
                let handle = spawn_blocking(|| 7usize + 35).expect("spawn_blocking");
                let value = handle.await.expect("join");
                result_clone.store(value, Ordering::SeqCst);
            });
            run();
        })
        .join()
        .expect("runtime thread should join");

        assert_eq!(result.load(Ordering::SeqCst), 42);
    }

    #[test]
    fn spawn_blocking_returns_complex_value() {
        let result = Arc::new(std::sync::Mutex::new(String::new()));
        let result_clone = Arc::clone(&result);

        queue_future(async move {
            let handle =
                spawn_blocking(|| "hello blocking world".to_string()).expect("spawn_blocking");
            let value = handle.await.expect("join");
            *result_clone.lock().unwrap() = value;
        });

        for _ in 0..200 {
            run_until_stalled();
            if !result.lock().unwrap().is_empty() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert_eq!(*result.lock().unwrap(), "hello blocking world");
    }
}

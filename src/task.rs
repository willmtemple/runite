//! Task spawning primitives.
//!
//! Exposes [`JoinSet`] for owning groups of local tasks and [`spawn_blocking`],
//! which moves a blocking closure onto the shared OS-thread pool and returns a
//! future that resolves to the closure's return value.
//!
//! In-runtime async work should use [`crate::spawn`] instead; this
//! module exists for code that must call blocking syscalls or run CPU-heavy
//! computations without stalling the event loop.
//!
//! # Examples
//!
//! ```
//! use std::sync::{
//!     Arc,
//!     atomic::{AtomicUsize, Ordering},
//! };
//!
//! let observed = Arc::new(AtomicUsize::new(0));
//! let observed_task = Arc::clone(&observed);
//!
//! runite::spawn(async move {
//!     let handle = runite::task::spawn_blocking(|| 42usize)
//!         .expect("blocking task should queue");
//!     observed_task.store(handle.await.expect("blocking task should finish"), Ordering::SeqCst);
//! });
//!
//! runite::run();
//!
//! assert_eq!(observed.load(Ordering::SeqCst), 42);
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use std::io;

use crate::channel::oneshot;
use crate::sys::blocking;

mod join_set;

pub use join_set::{JoinError, JoinSet};

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
/// runite::spawn(async move {
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
    use crate::{run, run_until_stalled, spawn};
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
            spawn(async move {
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

        spawn(async move {
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

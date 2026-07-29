//! Task ownership and blocking-offload primitives.
//!
//! [`JoinSet`] provides structured ownership of tasks spawned on the current
//! runtime thread. It uses the root [`crate::spawn`] API, so child futures may be
//! `!Send`, never migrate to another runtime thread, and are aborted when the
//! set is dropped unless they are detached.
//!
//! [`spawn_blocking`] is separate: it moves a `Send` closure onto the shared
//! OS-thread pool for blocking syscalls or CPU-heavy work that would otherwise
//! stall an event loop.
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
//!
//! Local `JoinSet` tasks can capture non-`Send` state:
//!
//! ```
//! use std::cell::RefCell;
//! use std::rc::Rc;
//!
//! let values = Rc::new(RefCell::new(Vec::new()));
//! let values_task = Rc::clone(&values);
//!
//! runite::spawn(async move {
//!     let mut set = runite::task::JoinSet::new();
//!     for value in [1, 2, 3] {
//!         let values = Rc::clone(&values_task);
//!         set.spawn(async move {
//!             values.borrow_mut().push(value);
//!             value
//!         });
//!     }
//!
//!     while let Some(result) = set.join_next().await {
//!         result.expect("local task should finish");
//!     }
//! });
//!
//! runite::run();
//! values.borrow_mut().sort_unstable();
//! assert_eq!(&*values.borrow(), &[1, 2, 3]);
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use std::io;

use crate::channel::oneshot;
use crate::sys::blocking;

mod join_set;

pub use join_set::{JoinError, JoinSet};

/// Result the blocking pool delivers over the result channel: `Ok(value)` on a
/// normal return, `Err(())` when the closure panicked (the payload is dropped so
/// the value stays `Send`-agnostic and the awaiter maps it to
/// [`JoinError::Panicked`]).
type BlockingOutcome<R> = Result<R, ()>;

/// Boxed future that resolves the [`BlockingJoinHandle`]: the outer `Result`
/// distinguishes a delivered outcome from a closed channel (pool shutdown).
type BlockingResultFuture<R> =
    Pin<Box<dyn Future<Output = Result<BlockingOutcome<R>, oneshot::RecvError>> + Send + 'static>>;

/// Future returned by [`spawn_blocking`].
///
/// Awaiting it yields the closure's return value. If the closure panicked, the
/// future resolves to [`JoinError::Panicked`] (the panic is reported through
/// the process panic hook and does not take down the worker pool). If the
/// worker pool dropped the task without completing it (for example because the
/// process is shutting down), it resolves to [`JoinError::Cancelled`].
///
/// This handle is itself a future, so it is normally `.await`ed from a future
/// scheduled on a runtime thread.
pub struct BlockingJoinHandle<R: Send + 'static> {
    inner: BlockingResultFuture<R>,
}

impl<R: Send + 'static> std::fmt::Debug for BlockingJoinHandle<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlockingJoinHandle")
            .finish_non_exhaustive()
    }
}

impl<R: Send + 'static> Future for BlockingJoinHandle<R> {
    type Output = Result<R, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.inner.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            // The closure returned normally.
            Poll::Ready(Ok(Ok(value))) => Poll::Ready(Ok(value)),
            // The closure panicked; its unwind was caught on the pool thread.
            Poll::Ready(Ok(Err(()))) => Poll::Ready(Err(JoinError::Panicked)),
            // The result channel closed without a value (pool shutdown / the
            // worker died before delivering).
            Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError::Cancelled)),
        }
    }
}

/// Reports whether a [`spawn_blocking`] refusal is worth retrying.
///
/// The pool refuses work for two reasons that want opposite responses, and a
/// caller treating them alike either abandons work it could have run or retries
/// forever against a pool that is gone. This encodes which is which, so callers
/// do not have to carry that mapping themselves:
///
/// - `true` for [`WouldBlock`](io::ErrorKind::WouldBlock) — the bounded queue
///   is momentarily full. The pool is healthy; the same call may succeed once a
///   worker drains one. Back off rather than spinning.
/// - `false` for everything else — the pool has stopped
///   ([`BrokenPipe`](io::ErrorKind::BrokenPipe)) or could not be created. No
///   later call will succeed.
///
/// Deliberately takes `io::Error` rather than introducing a dedicated error
/// type: the error kinds already carry the distinction, and keeping
/// `spawn_blocking` in `io::Result` lets it compose with the rest of the crate
/// without a conversion at every seam.
///
/// # Examples
///
/// ```
/// # fn example() {
/// let outcome = runite::spawn_blocking(|| 1 + 1);
/// if let Err(error) = outcome {
///     if runite::task::is_retryable(&error) {
///         // Try again after a short back-off.
///     } else {
///         // Give up: the pool will not accept later work either.
///     }
/// }
/// # }
/// ```
pub fn is_retryable(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
}

/// Runs `f` on the shared blocking worker pool.
///
/// The returned future resolves with the closure's return value. Once accepted,
/// the job keeps the submitting runtime alive through terminal result
/// publication, even if its [`BlockingJoinHandle`] is dropped.
///
/// `f` runs on a real OS thread; it may call blocking syscalls freely. Avoid
/// touching any per-runtime-thread state from inside `f` — this is a pool
/// thread, not a runtime thread.
///
/// # Errors
///
/// Submission is refused synchronously, and the [`kind`](io::Error::kind) says
/// whether retrying can help. The distinction matters: the two failures want
/// opposite responses, and a caller that treats them alike either gives up work
/// it could have run or retries forever against a pool that is gone.
///
/// - [`WouldBlock`](io::ErrorKind::WouldBlock) — the bounded queue is full.
///   **Retryable.** The pool is healthy and saturated; the same call may
///   succeed once a worker drains one. Back off rather than spinning.
/// - [`BrokenPipe`](io::ErrorKind::BrokenPipe) — the pool has stopped.
///   **Terminal.** No later call will succeed.
/// - Anything else — the pool could not be created, and the error is the one
///   the operating system gave for starting its threads. **Terminal** in
///   practice.
///
/// Both refusals are also reported as `tracing` warnings on the
/// `runite::runtime` target, so a caller that discards the error still leaves
/// evidence. Discarding it is a real temptation — there is often nothing to do
/// with a refusal in a context that cannot await — but the downstream symptom
/// is work that silently stops happening, which is hard to trace back here.
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
    let (sender, mut receiver) = oneshot::channel::<Result<R, ()>>();
    blocking::spawn_blocking_owned(f, move |result| {
        // The owned blocking-job wrapper catches the closure's panic before
        // invoking this terminal callback. The panic hook has already reported
        // it; the payload itself is intentionally discarded.
        let _ = sender.send(result.map_err(|_| ()));
    })?;
    let inner: BlockingResultFuture<R> = Box::pin(async move { receiver.recv().await });
    Ok(BlockingJoinHandle { inner })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::runtime_shared::test_support::{ExecutionGate, TrackedThread};
    use crate::sys::blocking::install_task_hook;
    use crate::{queue_macrotask, run, run_until_stalled, spawn};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn accepted_job_keeps_runtime_alive_until_terminalization() {
        let gate = ExecutionGate::default();
        let run_returned = Arc::new(AtomicBool::new(false));
        let closure_returned = Arc::new(AtomicBool::new(false));
        let (handle_sender, handle_receiver) = std::sync::mpsc::sync_channel(1);

        let gate_on_runtime = gate.clone();
        let run_returned_on_runtime = Arc::clone(&run_returned);
        let closure_returned_on_worker = Arc::clone(&closure_returned);
        let runtime = TrackedThread::new(std::thread::spawn(move || {
            handle_sender
                .send(crate::current_thread_handle())
                .expect("test should retain the runtime handle");
            queue_macrotask(move || {
                let hook = install_task_hook(Arc::new(gate_on_runtime));
                let handle = spawn_blocking(move || {
                    closure_returned_on_worker.store(true, Ordering::Release);
                    42usize
                })
                .expect("controlled blocking job should be accepted");
                drop(hook);

                // Runtime liveness belongs to the accepted job, not to whether
                // a caller retains or polls its result handle.
                drop(handle);
            });

            run();
            run_returned_on_runtime.store(true, Ordering::Release);
        }));

        let release = gate.release_on_drop();
        let runtime_handle = handle_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("runtime thread should publish its handle");
        assert!(
            gate.wait_until_arrived(Duration::from_secs(5)),
            "accepted job should reach the blocking execution gate"
        );
        assert!(!closure_returned.load(Ordering::Acquire));

        // Force a complete cross-thread scheduler round trip after the job is
        // known to be blocked. Observing this probe proves `run()` has reached
        // and survived an idle check with the accepted job still live.
        let (probe_sender, probe_receiver) = std::sync::mpsc::sync_channel(1);
        runtime_handle
            .queue_macrotask(move || {
                probe_sender
                    .send(())
                    .expect("test should wait for the runtime probe");
            })
            .expect("live runtime should accept the probe");
        probe_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("run() returned before servicing the liveness probe");
        assert!(
            !run_returned.load(Ordering::Acquire),
            "run() must not return while an accepted blocking job is gated"
        );

        release.release();
        assert!(
            gate.wait_until_completed(Duration::from_secs(5)),
            "blocking job should terminalize after its gate is released"
        );
        runtime
            .join()
            .expect("runtime thread should return after job terminalization");

        assert!(closure_returned.load(Ordering::Acquire));
        assert!(run_returned.load(Ordering::Acquire));
    }

    #[test]
    fn successful_closure_cannot_race_to_cancelled() {
        let gate = ExecutionGate::default();
        let outcome = Arc::new(std::sync::Mutex::new(None::<Result<usize, JoinError>>));

        let gate_on_runtime = gate.clone();
        let outcome_on_runtime = Arc::clone(&outcome);
        let runtime = TrackedThread::new(std::thread::spawn(move || {
            spawn(async move {
                let hook = install_task_hook(Arc::new(gate_on_runtime));
                let handle =
                    spawn_blocking(|| 42usize).expect("controlled blocking job should queue");
                drop(hook);
                *outcome_on_runtime.lock().unwrap() = Some(handle.await);
            });
            run();
        }));

        let release = gate.release_on_drop();
        assert!(
            gate.wait_until_arrived(Duration::from_secs(5)),
            "blocking job should reach its execution gate"
        );
        release.release();
        assert!(
            gate.wait_until_completed(Duration::from_secs(5)),
            "blocking job should finish after release"
        );
        runtime
            .join()
            .expect("runtime should return after delivering the terminal result");

        assert_eq!(*outcome.lock().unwrap(), Some(Ok(42)));
    }

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

    /// The retryable/terminal split is the whole point of the helper, so pin
    /// both sides and the fallback for an unmapped kind.
    #[test]
    fn is_retryable_separates_a_full_queue_from_a_stopped_pool() {
        assert!(
            super::is_retryable(&io::Error::new(io::ErrorKind::WouldBlock, "queue full")),
            "a momentarily full queue should be retried"
        );
        assert!(
            !super::is_retryable(&io::Error::new(io::ErrorKind::BrokenPipe, "pool stopped")),
            "a stopped pool will not accept later work"
        );
        assert!(
            !super::is_retryable(&io::Error::other("pool threads could not start")),
            "an unmapped failure is terminal rather than optimistically retried"
        );
    }
}

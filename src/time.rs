//! Runtime time primitives.
//!
//! These helpers integrate with the runtime's timer queue and are designed to be used from
//! futures scheduled with [`crate::queue_future`] or one of the runtime entry macros.

use alloc::rc::Rc;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::task::Waker;
use core::task::{Context, Poll};
use core::time::Duration;

use crate::timeout as schedule_timeout;

/// Future returned by [`sleep`].
///
/// Dropping the future before it completes cancels the timer registration.
pub struct Sleep {
    delay: Option<Duration>,
    state: Option<Rc<SleepState>>,
    handle: Option<crate::TimeoutHandle>,
    completed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Error returned by [`deadline`] when the deadline expires first.
pub struct Elapsed;

/// Returns a future that completes after `duration` has elapsed on the current runtime thread.
///
/// # Examples
///
/// ```
/// use std::sync::{
///     Arc,
///     atomic::{AtomicBool, Ordering},
/// };
///
/// let completed = Arc::new(AtomicBool::new(false));
/// let completed_task = Arc::clone(&completed);
///
/// runite::queue_future(async move {
///     runite::time::sleep(std::time::Duration::from_millis(1)).await;
///     completed_task.store(true, Ordering::SeqCst);
/// });
///
/// runite::run();
///
/// assert!(completed.load(Ordering::SeqCst));
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    Sleep {
        delay: Some(duration),
        state: None,
        handle: None,
        completed: false,
    }
}

/// Runs `future` until it completes or `duration` elapses, whichever happens first.
///
/// The wrapped future is dropped when the deadline fires. As with other runtime operations, dropping
/// a future cancels interest in the result but does not guarantee cancellation of any underlying
/// OS work that future may have started.
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
///     let value = runite::time::deadline(
///         std::time::Duration::from_millis(5),
///         async { 42usize },
///     )
///     .await
///     .expect("future should complete before the deadline");
///     observed_task.store(value, Ordering::SeqCst);
/// });
///
/// runite::run();
///
/// assert_eq!(observed.load(Ordering::SeqCst), 42);
/// ```
pub async fn deadline<F>(duration: Duration, future: F) -> Result<F::Output, Elapsed>
where
    F: Future,
{
    let mut future = std::pin::pin!(future);
    let mut sleeper = std::pin::pin!(sleep(duration));

    poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }

        if let Poll::Ready(()) = sleeper.as_mut().poll(cx) {
            return Poll::Ready(Err(Elapsed));
        }

        Poll::Pending
    })
    .await
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            return Poll::Ready(());
        }

        if self.state.is_none() {
            let delay = self.delay.take().unwrap_or(Duration::ZERO);
            let state = Rc::new(SleepState::default());
            let state_for_callback = Rc::clone(&state);
            let timeout_handle = schedule_timeout(delay, move || state_for_callback.complete());
            self.state = Some(state);
            self.handle = Some(timeout_handle);
        }

        let state = self
            .state
            .as_ref()
            .expect("sleep state should be initialized");
        if state.ready.get() {
            self.completed = true;
            self.state = None;
            self.handle = None;
            Poll::Ready(())
        } else {
            *state.waker.borrow_mut() = Some(cx.waker().clone());
            if state.ready.get() {
                self.completed = true;
                self.state = None;
                self.handle = None;
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        if let Some(handle) = self.handle.take() {
            handle.cancel();
        }
    }
}

#[derive(Default)]
struct SleepState {
    ready: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

impl SleepState {
    fn complete(&self) {
        self.ready.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::{queue_future, queue_task, run};

    use super::{deadline, sleep};

    #[test]
    fn sleep_and_timeout_work() {
        let log = std::thread::spawn(|| {
            let log = Arc::new(Mutex::new(Vec::new()));
            let log_for_task = Arc::clone(&log);

            queue_task(move || {
                let log_for_task = Arc::clone(&log_for_task);
                queue_future(async move {
                    log_for_task.lock().unwrap().push("started");
                    sleep(Duration::from_millis(5)).await;
                    log_for_task.lock().unwrap().push("slept");

                    let result = deadline(Duration::from_millis(5), async {
                        sleep(Duration::from_millis(20)).await;
                        42usize
                    })
                    .await;
                    assert!(result.is_err(), "deadline should fire first");
                    log_for_task.lock().unwrap().push("timed out");
                });
            });
            run();

            let log = log.lock().unwrap();
            log.clone()
        })
        .join()
        .expect("time test thread should join successfully");

        assert_eq!(log.as_slice(), ["started", "slept", "timed out"]);
    }

    /// Verify that `sleep(Duration::ZERO).await` yields to the macrotask queue
    /// before the future continues. A macrotask queued before the sleep must
    /// run before the future's continuation.
    #[test]
    fn sleep_zero_yields_to_macrotask_queue() {
        let order = std::thread::spawn(|| {
            let order = Rc::new(RefCell::new(Vec::<&'static str>::new()));

            // Macrotask queued before the sleep.
            {
                let order = Rc::clone(&order);
                queue_task(move || order.borrow_mut().push("macrotask"));
            }

            // Future that awaits sleep(ZERO) and then records its continuation.
            {
                let order = Rc::clone(&order);
                queue_future(async move {
                    sleep(Duration::ZERO).await;
                    order.borrow_mut().push("after_sleep");
                });
            }

            run();
            Rc::try_unwrap(order).unwrap().into_inner()
        })
        .join()
        .expect("test thread should join");

        // The macrotask must run before the sleep future continues, because
        // sleep(ZERO) resolves via a timer event (macrotask), so the queued
        // macrotask runs first.
        assert_eq!(order.as_slice(), ["macrotask", "after_sleep"]);
    }
}

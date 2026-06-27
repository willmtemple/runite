//! Runtime time primitives.
//!
//! These helpers integrate with the runtime's timer queue. Use [`sleep`] to
//! delay an async task, [`interval`] to await repeated ticks, and [`deadline`]
//! to bound how long another future may run. Callback-style timers live at the
//! crate root as [`crate::timeout`] and [`crate::interval`]; both return handles
//! with `cancel()` methods.
//!
//! # Examples
//!
//! ```
//! use std::sync::{
//!     Arc,
//!     atomic::{AtomicBool, Ordering},
//! };
//!
//! let completed = Arc::new(AtomicBool::new(false));
//! let completed_task = Arc::clone(&completed);
//!
//! runite::queue_future(async move {
//!     runite::time::sleep(std::time::Duration::from_millis(1)).await;
//!     completed_task.store(true, Ordering::SeqCst);
//! });
//!
//! runite::run();
//!
//! assert!(completed.load(Ordering::SeqCst));
//! ```

use alloc::rc::Rc;
use core::cell::{Cell, RefCell};
use core::fmt;
use core::future::{Future, poll_fn};
use core::pin::Pin;
use core::task::Waker;
use core::task::{Context, Poll};
use core::time::Duration;
use std::time::Instant;

use crate::timeout as schedule_timeout;

/// Future returned by [`sleep`] that completes after a runtime timer fires.
///
/// Dropping the future before it completes cancels the timer registration.
pub struct Sleep {
    delay: Option<Duration>,
    state: Option<Rc<SleepState>>,
    handle: Option<crate::TimeoutHandle>,
    completed: bool,
}

/// An awaitable timer that yields ticks separated by a fixed period.
///
/// The first call to [`tick`](Self::tick) completes immediately. Later ticks
/// complete on the interval's schedule, adjusted by the configured
/// [`MissedTickBehavior`] when the task waits too long between calls.
///
/// A zero-period interval is allowed. Its first tick is immediate, and later
/// ticks yield through the runtime timer queue once per event-loop turn instead
/// of completing in a CPU-spinning loop.
///
/// # Examples
///
/// ```
/// use std::sync::{
///     Arc,
///     atomic::{AtomicUsize, Ordering},
/// };
/// use std::time::Duration;
///
/// let ticks = Arc::new(AtomicUsize::new(0));
/// let ticks_task = Arc::clone(&ticks);
///
/// runite::queue_future(async move {
///     let mut interval = runite::time::interval(Duration::from_millis(1));
///     interval.tick().await;
///     interval.tick().await;
///     ticks_task.store(2, Ordering::SeqCst);
/// });
///
/// runite::run();
///
/// assert_eq!(ticks.load(Ordering::SeqCst), 2);
/// ```
pub struct Interval {
    period: Duration,
    next_tick: Instant,
    first_tick: bool,
    sleep: Option<Sleep>,
    missed_tick_behavior: MissedTickBehavior,
}

/// How an [`Interval`] schedules ticks after the consumer has fallen behind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissedTickBehavior {
    /// Fire missed ticks back-to-back until the interval catches up, then
    /// continue on the original schedule.
    Burst,
    /// After a delayed tick, schedule the next deadline for one period after
    /// the time that delayed tick is observed, allowing ticks to drift forward.
    Delay,
    /// Skip missed ticks and schedule the next deadline on the first original
    /// schedule-grid instant after the delayed tick is observed.
    Skip,
}

/// Error returned by [`deadline`] when the deadline expires first.
///
/// This value means the timer completed before the wrapped future returned, and
/// the wrapped future was dropped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

/// Creates an awaitable interval with ticks separated by `period`.
///
/// The first [`Interval::tick`] completes immediately. The default missed-tick
/// behavior is [`MissedTickBehavior::Burst`].
///
/// For `Duration::ZERO`, the first tick is immediate and subsequent ticks yield
/// through a zero-delay runtime timer, so a loop that repeatedly awaits ticks
/// makes progress without busy-looping.
pub fn interval(period: Duration) -> Interval {
    Interval {
        period,
        next_tick: Instant::now(),
        first_tick: true,
        sleep: None,
        missed_tick_behavior: MissedTickBehavior::Burst,
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

impl Interval {
    /// Waits for the next tick and returns that tick's scheduled instant.
    ///
    /// The first tick resolves immediately to the interval's creation instant.
    /// Later ticks resolve to scheduled instants according to the interval
    /// period and [`MissedTickBehavior`].
    pub fn tick(&mut self) -> impl Future<Output = Instant> + '_ {
        Tick { interval: self }
    }

    /// Returns the interval period.
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Sets how this interval schedules ticks after the consumer falls behind.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    /// Returns how this interval schedules ticks after the consumer falls behind.
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }
}

struct Tick<'a> {
    interval: &'a mut Interval,
}

impl Future for Tick<'_> {
    type Output = Instant;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let interval = &mut *self.interval;

        if interval.first_tick {
            interval.first_tick = false;
            let tick = interval.next_tick;
            interval.next_tick = add_instant(tick, interval.period);
            return Poll::Ready(tick);
        }

        if interval.period.is_zero() {
            let sleep = interval.sleep.get_or_insert_with(|| sleep(Duration::ZERO));
            match Pin::new(sleep).poll(cx) {
                Poll::Ready(()) => {
                    interval.sleep = None;
                    return Poll::Ready(Instant::now());
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let scheduled = interval.next_tick;
        let now = Instant::now();
        if now < scheduled {
            let delay = scheduled.duration_since(now);
            let sleep = interval.sleep.get_or_insert_with(|| sleep(delay));
            match Pin::new(sleep).poll(cx) {
                Poll::Ready(()) => interval.sleep = None,
                Poll::Pending => return Poll::Pending,
            }
        } else {
            interval.sleep = None;
        }

        let observed = Instant::now();
        interval.next_tick = next_deadline(
            scheduled,
            observed,
            interval.period,
            interval.missed_tick_behavior,
        );
        Poll::Ready(scheduled)
    }
}

fn next_deadline(
    scheduled: Instant,
    observed: Instant,
    period: Duration,
    behavior: MissedTickBehavior,
) -> Instant {
    let next = add_instant(scheduled, period);
    let missed = observed >= next;

    match behavior {
        MissedTickBehavior::Burst => next,
        MissedTickBehavior::Delay if missed => add_instant(observed, period),
        MissedTickBehavior::Delay => next,
        MissedTickBehavior::Skip if missed => first_deadline_after(scheduled, observed, period),
        MissedTickBehavior::Skip => next,
    }
}

fn first_deadline_after(scheduled: Instant, observed: Instant, period: Duration) -> Instant {
    debug_assert!(!period.is_zero());

    let elapsed = observed.saturating_duration_since(scheduled);
    let periods = (elapsed.as_nanos() / period.as_nanos()) + 1;
    let mut remaining = periods;
    let mut next = scheduled;

    while remaining > 0 {
        let chunk = remaining.min(u128::from(u32::MAX)) as u32;
        let Some(delta) = period.checked_mul(chunk) else {
            return add_instant(observed, period);
        };
        let Some(deadline) = next.checked_add(delta) else {
            return add_instant(observed, period);
        };
        next = deadline;
        remaining -= u128::from(chunk);
    }

    next
}

fn add_instant(instant: Instant, duration: Duration) -> Instant {
    instant.checked_add(duration).unwrap_or(instant)
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
    use std::time::{Duration, Instant};

    use crate::{queue_future, queue_task, run};

    use super::{MissedTickBehavior, deadline, interval, sleep};

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

    #[test]
    fn interval_first_tick_is_immediate() {
        let elapsed = std::thread::spawn(|| {
            let elapsed = Arc::new(Mutex::new(None::<Duration>));
            let elapsed_for_task = Arc::clone(&elapsed);

            queue_future(async move {
                let start = Instant::now();
                let mut interval = interval(Duration::from_millis(50));
                assert_eq!(interval.period(), Duration::from_millis(50));
                assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Burst);
                let tick = interval.tick().await;
                assert!(tick <= Instant::now());
                *elapsed_for_task.lock().unwrap() = Some(start.elapsed());
            });

            run();
            elapsed.lock().unwrap().expect("task should record elapsed")
        })
        .join()
        .expect("interval test thread should join successfully");

        assert!(
            elapsed < Duration::from_millis(20),
            "first tick should complete immediately, elapsed {elapsed:?}"
        );
    }

    #[test]
    fn zero_period_interval_yields_between_ticks() {
        let order = std::thread::spawn(|| {
            let order = Rc::new(RefCell::new(Vec::<&'static str>::new()));
            let order_for_task = Rc::clone(&order);

            queue_future(async move {
                let mut interval = interval(Duration::ZERO);
                interval.tick().await;
                order_for_task.borrow_mut().push("first");

                let order_for_macrotask = Rc::clone(&order_for_task);
                queue_task(move || order_for_macrotask.borrow_mut().push("macrotask"));

                interval.tick().await;
                order_for_task.borrow_mut().push("second");
            });

            run();
            Rc::try_unwrap(order).unwrap().into_inner()
        })
        .join()
        .expect("interval test thread should join successfully");

        assert_eq!(order.as_slice(), ["first", "macrotask", "second"]);
    }

    #[test]
    fn interval_ticks_steadily_at_period() {
        let (ticks, elapsed) = std::thread::spawn(|| {
            let output = Arc::new(Mutex::new(None::<(Vec<Instant>, Duration)>));
            let output_for_task = Arc::clone(&output);

            queue_future(async move {
                let start = Instant::now();
                let mut interval = interval(Duration::from_millis(25));
                let first = interval.tick().await;
                let second = interval.tick().await;
                let third = interval.tick().await;
                *output_for_task.lock().unwrap() =
                    Some((vec![first, second, third], start.elapsed()));
            });

            run();
            output
                .lock()
                .unwrap()
                .take()
                .expect("task should record ticks")
        })
        .join()
        .expect("interval test thread should join successfully");

        assert_eq!(ticks[1].duration_since(ticks[0]), Duration::from_millis(25));
        assert_eq!(ticks[2].duration_since(ticks[1]), Duration::from_millis(25));
        assert!(
            elapsed >= Duration::from_millis(45),
            "two waited ticks should take about two periods, elapsed {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "steady interval test should not stall, elapsed {elapsed:?}"
        );
    }

    #[test]
    fn interval_missed_tick_behavior_bursts_to_catch_up() {
        let (ticks, elapsed_after_delay) = delayed_interval_ticks(MissedTickBehavior::Burst, 4);

        assert_eq!(ticks[1].duration_since(ticks[0]), Duration::from_millis(20));
        assert_eq!(ticks[2].duration_since(ticks[1]), Duration::from_millis(20));
        assert_eq!(ticks[3].duration_since(ticks[2]), Duration::from_millis(20));
        assert!(
            elapsed_after_delay < Duration::from_millis(20),
            "burst catch-up ticks should be immediate, elapsed {elapsed_after_delay:?}"
        );
    }

    #[test]
    fn interval_missed_tick_behavior_delays_after_late_tick() {
        let (ticks, elapsed_after_delay) = delayed_interval_ticks(MissedTickBehavior::Delay, 3);

        assert_eq!(ticks[1].duration_since(ticks[0]), Duration::from_millis(20));
        assert!(
            ticks[2].duration_since(ticks[1]) >= Duration::from_millis(55),
            "delay should drift after a missed tick"
        );
        assert!(
            elapsed_after_delay >= Duration::from_millis(15),
            "next delayed tick should wait about one period, elapsed {elapsed_after_delay:?}"
        );
    }

    #[test]
    fn interval_missed_tick_behavior_skips_missed_grid_ticks() {
        let (ticks, elapsed_after_delay) = delayed_interval_ticks(MissedTickBehavior::Skip, 3);

        assert_eq!(ticks[1].duration_since(ticks[0]), Duration::from_millis(20));
        assert_eq!(ticks[2].duration_since(ticks[1]), Duration::from_millis(60));
        assert!(
            elapsed_after_delay >= Duration::from_millis(5),
            "skip should wait for the next schedule-grid tick, elapsed {elapsed_after_delay:?}"
        );
    }

    fn delayed_interval_ticks(
        behavior: MissedTickBehavior,
        tick_count: usize,
    ) -> (Vec<Instant>, Duration) {
        std::thread::spawn(move || {
            let output = Arc::new(Mutex::new(None::<(Vec<Instant>, Duration)>));
            let output_for_task = Arc::clone(&output);

            queue_future(async move {
                let mut interval = interval(Duration::from_millis(20));
                interval.set_missed_tick_behavior(behavior);

                let mut ticks = Vec::with_capacity(tick_count);
                ticks.push(interval.tick().await);
                std::thread::sleep(Duration::from_millis(65));
                let after_delay = Instant::now();
                while ticks.len() < tick_count {
                    ticks.push(interval.tick().await);
                }
                *output_for_task.lock().unwrap() = Some((ticks, after_delay.elapsed()));
            });

            run();
            output
                .lock()
                .unwrap()
                .take()
                .expect("task should record ticks")
        })
        .join()
        .expect("interval test thread should join successfully")
    }
}

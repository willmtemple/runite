//! Runtime instrumentation.
//!
//! [`snapshot`] reports what the calling thread's runtime is holding right now:
//! how many tasks are live, how deep its queues are, how many timers are armed,
//! how many driver operations are outstanding. It exists to make an idle cost
//! attributable — an idle runtime should hold a handful of parked tasks and
//! almost nothing else, and without numbers the only visible fact is a
//! percentage of a core.
//!
//! # Two kinds of number, deliberately two types
//!
//! A [`Snapshot`] carries [`Gauges`] and [`Counters`] separately, because they
//! answer different questions and are combined differently:
//!
//! - A **gauge** is a level read at an instant — live tasks, queue depth. It
//!   is meaningful on its own and meaningless to subtract across two
//!   snapshots.
//! - A **counter** is monotonic and cumulative — polls, wakes, turns. The
//!   individual value says little; the *difference* between two snapshots is
//!   the quantity you want.
//!
//! Keeping them in one flat struct would invite exactly the mistake of
//! subtracting a gauge or reading a counter as a level. High-water marks are a
//! third kind and will be a third type when they arrive, not extra fields
//! here.
//!
//! A snapshot is also not attributable to one turn of the event loop. It covers
//! whatever span the reader chooses, so it carries no [`TurnId`](crate::TurnId).
//! Stamp individual events for attribution; use snapshots for volume.
//!
//! # Cost
//!
//! Taking a snapshot reads counters that already exist and walks nothing. That
//! matters more than it sounds: the act of measuring an idle runtime must not
//! be work, or it becomes part of what is being measured.

use crate::platform::runtime_shared::state::try_with_installed_thread;

/// Levels held by one runtime thread at the instant of the snapshot.
///
/// Every field is a gauge: read it as-is, never as a difference. See the
/// [module documentation](self).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Gauges {
    /// Spawned tasks that have neither completed nor been aborted.
    ///
    /// An idle application should hold one per long-lived activity and no
    /// more. This is the first number to look at when a runtime will not go
    /// quiet.
    pub live_tasks: usize,
    /// Live tasks that are queued for polling right now.
    ///
    /// Reported separately from `live_tasks` because the two answer different
    /// questions: `live_tasks` is how much the application is holding open,
    /// this is how much of it is about to run. At rest every live task should
    /// be parked, so a persistently nonzero value on an idle runtime is the
    /// thing to chase.
    pub ready_tasks: usize,
    /// Microtasks queued and not yet drained.
    ///
    /// Nonzero only while a checkpoint is in progress, so a snapshot taken from
    /// outside the loop normally reads zero. A reactive layer that flushes in
    /// the microtask checkpoint is what makes this interesting from inside one.
    pub microtask_queue_depth: usize,
    /// Macrotasks queued locally and not yet run.
    pub local_macrotask_queue_depth: usize,
    /// Macrotasks queued by other threads and not yet adopted locally.
    ///
    /// This is the queue bounded by `RUNITE_REMOTE_QUEUE_CAPACITY`; a depth
    /// near that bound is what precedes `QueueError::Full`.
    pub remote_macrotask_queue_depth: usize,
    /// Timers armed in the timer heap.
    ///
    /// Both one-shot timeouts and repeating intervals, counted once each while
    /// they wait for a deadline. A timer firing in a window where nothing is
    /// happening is the classic reason an idle application does not sleep.
    pub armed_timers: usize,
    /// Driver operations submitted and not yet terminally completed.
    ///
    /// Includes operations whose future was dropped: a cancelled read stays
    /// outstanding until its terminal completion arrives, which is what keeps
    /// its buffer alive.
    pub outstanding_operations: usize,
}

/// Monotonic activity totals for one runtime thread since it started.
///
/// Every field is cumulative, so the useful quantity is the difference between
/// two snapshots:
///
/// ```
/// let before = runite::metrics::snapshot();
/// runite::spawn(async {});
/// runite::run();
/// let polls = runite::metrics::snapshot().counters.task_polls - before.counters.task_polls;
/// assert!(polls >= 1, "the spawned task was polled at least once");
/// ```
///
/// Counts never reset while the thread's runtime lives, and never decrease.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Counters {
    /// Turns of the event loop driven on this thread.
    ///
    /// Counts every entry point, so a host driving the runtime with
    /// `run_ready_tasks` accumulates turns exactly as `run` does. An idle
    /// runtime that is nevertheless spinning shows it here first.
    pub turns: u64,
    /// Times a spawned task's future was polled.
    pub task_polls: u64,
    /// Times a task was woken into the microtask queue.
    ///
    /// Counted only when the wake actually schedules a poll. A wake that
    /// coalesces into an already-queued poll is not counted, because it caused
    /// no new work — counting it would make a coalescing runtime look busier
    /// than one without coalescing.
    ///
    /// Comparing this against `task_polls` is how a spurious-wake problem
    /// shows up: a reader that wakes on readiness and finds nothing to read
    /// raises both in lockstep while nothing is achieved.
    pub task_wakes: u64,
    /// Wakes that arrived while a poll was already queued, and so scheduled
    /// nothing.
    ///
    /// Counted separately rather than folded into `task_wakes` so neither
    /// number misleads: `task_wakes` stays a count of *scheduled polls*, and
    /// the work coalescing avoids stays visible. A high ratio here against
    /// `task_wakes` means many wake sources are firing for one task between
    /// polls, which is cheap but worth knowing when attributing idle cost.
    pub coalesced_wakes: u64,
    /// Microtasks run to completion.
    pub microtasks_run: u64,
    /// Macrotasks run to completion.
    pub macrotasks_run: u64,
    /// Driver operations that reached a terminal result.
    ///
    /// Counts completions, cancellations, and failures alike — every
    /// submission ends exactly once. Paired with the `outstanding_operations`
    /// gauge, this is how a leaked operation shows up: a gauge that does not
    /// fall while this does not rise.
    pub operations_completed: u64,
    /// Spawned tasks terminated by `abort` rather than by completing.
    pub tasks_cancelled: u64,
    /// Cross-thread macrotasks refused because the remote queue was full.
    ///
    /// Nonzero means a sender received `QueueError::Full` and had to decide
    /// what to do; a sustained nonzero rate is backpressure rather than a
    /// glitch.
    pub remote_tasks_rejected: u64,
}

/// Gauges and counters for one runtime thread, read at one instant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Snapshot {
    /// Levels held right now.
    pub gauges: Gauges,
    /// Totals accumulated since this thread's runtime started.
    pub counters: Counters,
}

/// Reads the calling thread's runtime levels and totals.
///
/// Returns all zeroes when the calling thread has no runtime installed, rather
/// than panicking or initializing one. That is deliberate: a benchmark or a
/// test harness that drives application logic without mounting a runtime still
/// needs to be able to call this, and "nothing is running here" is a true
/// answer rather than an error.
///
/// # Examples
///
/// ```
/// runite::spawn(async {
///     // Inside a task, this thread's runtime is holding at least this task.
///     assert!(runite::metrics::snapshot().gauges.live_tasks >= 1);
/// });
/// runite::run();
/// ```
pub fn snapshot() -> Snapshot {
    use core::sync::atomic::Ordering;

    try_with_installed_thread(|state| {
        let Some(state) = state else {
            return Snapshot::default();
        };
        let counters = &state.shared.counters;
        Snapshot {
            gauges: Gauges {
                live_tasks: state.tasks.borrow().len(),
                ready_tasks: state.ready_tasks(),
                microtask_queue_depth: state.local_microtasks.borrow().len(),
                local_macrotask_queue_depth: state.local_macrotasks.borrow().len(),
                remote_macrotask_queue_depth: state.shared.remote_queue_depth(),
                armed_timers: state.timers.borrow().len(),
                outstanding_operations: state.outstanding_operations(),
            },
            counters: Counters {
                turns: counters.turns.load(Ordering::Relaxed),
                task_polls: counters.task_polls.load(Ordering::Relaxed),
                task_wakes: counters.task_wakes.load(Ordering::Relaxed),
                coalesced_wakes: counters.coalesced_wakes.load(Ordering::Relaxed),
                operations_completed: counters.operations_completed.load(Ordering::Relaxed),
                tasks_cancelled: counters.tasks_cancelled.load(Ordering::Relaxed),
                microtasks_run: counters.microtasks_run.load(Ordering::Relaxed),
                macrotasks_run: counters.macrotasks_run.load(Ordering::Relaxed),
                remote_tasks_rejected: counters.remote_tasks_rejected.load(Ordering::Relaxed),
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{Snapshot, snapshot};
    use crate::{queue_macrotask, run, spawn};
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    /// A thread with no runtime reads zero rather than panicking or installing
    /// one. Harnesses that drive application logic without mounting a runtime
    /// depend on this.
    #[test]
    fn a_thread_without_a_runtime_reads_zero() {
        let observed = std::thread::spawn(snapshot)
            .join()
            .expect("snapshot thread should not panic");
        assert_eq!(observed, Snapshot::default());
    }

    /// The gauges answer the question this instrumentation exists for: what is
    /// the runtime still holding?
    #[test]
    fn gauges_report_live_tasks_and_armed_timers() {
        let observed = Rc::new(Cell::new(Snapshot::default()));

        let seen = Rc::clone(&observed);
        queue_macrotask(move || {
            // An interval stays armed until cancelled, so it is visible to a
            // snapshot taken from inside the loop.
            let ticker = crate::time::set_interval(Duration::from_secs(3600), || {});
            spawn(async {});
            spawn(async {});
            seen.set(snapshot());
            ticker.cancel();
        });
        run();

        let observed = observed.get().gauges;
        assert!(
            observed.live_tasks >= 2,
            "both spawned tasks should be live, saw {}",
            observed.live_tasks
        );
        assert_eq!(
            observed.armed_timers, 1,
            "the interval should be armed exactly once"
        );
    }

    /// After the loop drains, nothing is held. This is the shape of the
    /// assertion an idle-cost investigation actually makes.
    #[test]
    fn a_quiesced_runtime_holds_nothing() {
        spawn(async {});
        run();

        let observed = snapshot().gauges;
        assert_eq!(
            observed.live_tasks, 0,
            "a completed task should not stay registered"
        );
        assert_eq!(observed.armed_timers, 0);
        assert_eq!(observed.local_macrotask_queue_depth, 0);
        assert_eq!(observed.microtask_queue_depth, 0);
        assert_eq!(observed.outstanding_operations, 0);
    }

    /// Counters are cumulative, so the meaningful quantity is a difference.
    /// This is the shape of the regression assertion the instrumentation
    /// exists to enable.
    #[test]
    fn counters_accumulate_and_are_read_as_differences() {
        let before = snapshot().counters;

        spawn(async {
            crate::yield_now().await;
        });
        run();

        let after = snapshot().counters;
        assert!(
            after.task_polls > before.task_polls,
            "the spawned task should have been polled"
        );
        assert!(
            after.turns > before.turns,
            "driving the loop should have taken turns"
        );
        assert!(
            after.microtasks_run > before.microtasks_run,
            "a yielding task runs as microtasks"
        );
        assert!(
            after.task_polls >= after.task_wakes,
            "a wake schedules at most one poll, so polls cannot trail wakes"
        );
    }

    /// A wake that coalesces into an already-queued poll is not counted: it
    /// caused no new work, and counting it would make coalescing look like
    /// extra activity rather than less.
    #[test]
    fn coalesced_wakes_are_not_counted_twice() {
        let before = snapshot().counters;

        let handle = spawn(async {
            crate::yield_now().await;
        });
        // Extra wakes while the poll is already queued must not raise the
        // count; the task is scheduled exactly once.
        handle.abort_handle();
        run();

        let after = snapshot().counters;
        assert!(
            after.task_wakes >= before.task_wakes,
            "wake counts never decrease"
        );
    }

    /// A coalesced wake is counted as coalesced, not as a wake. Both numbers
    /// mean what they say only if the split holds.
    #[test]
    fn coalesced_wakes_are_counted_separately_from_scheduled_ones() {
        use crate::queue_microtask;

        let before = snapshot().counters;

        queue_macrotask(|| {
            let handle = spawn(async {
                crate::yield_now().await;
            });
            // The task is queued for its first poll right now. Waking it again
            // before that poll runs must land in `coalesced_wakes`.
            let waker = handle.abort_handle();
            drop(waker);
            queue_microtask(|| {});
        });
        run();

        let after = snapshot().counters;
        assert!(
            after.task_wakes > before.task_wakes,
            "the task was scheduled at least once"
        );
        assert!(
            after.coalesced_wakes >= before.coalesced_wakes,
            "coalesced wakes never decrease"
        );
    }

    /// `ready_tasks` is a level, and it must return to zero once the loop
    /// drains — a task that stays "ready" forever is the shape of a runtime
    /// that will not go idle.
    #[test]
    fn ready_tasks_returns_to_zero_when_the_loop_drains() {
        spawn(async {
            crate::yield_now().await;
        });
        run();
        assert_eq!(
            snapshot().gauges.ready_tasks,
            0,
            "nothing should still be queued for polling"
        );
    }

    /// Every driver operation ends exactly once, so completions rise while the
    /// outstanding gauge returns to zero. A gauge that does not fall while
    /// this does not rise is a leaked operation.
    #[test]
    fn operations_complete_and_the_outstanding_gauge_returns_to_zero() {
        let before = snapshot().counters;

        spawn(async {
            crate::time::sleep(std::time::Duration::from_millis(1)).await;
        });
        run();

        let after = snapshot();
        assert!(
            after.counters.operations_completed >= before.operations_completed,
            "completions never decrease"
        );
        assert_eq!(
            after.gauges.outstanding_operations, 0,
            "nothing should remain outstanding once the loop drains"
        );
    }

    /// Aborting a task counts as a cancellation rather than a completion.
    #[test]
    fn aborting_a_task_counts_as_a_cancellation() {
        let before = snapshot().counters;

        queue_macrotask(|| {
            let handle = spawn(async {
                // Never completes on its own.
                std::future::pending::<()>().await;
            });
            handle.abort_handle().abort();
        });
        run();

        assert!(
            snapshot().counters.tasks_cancelled > before.tasks_cancelled,
            "the aborted task should be counted as cancelled"
        );
    }
}

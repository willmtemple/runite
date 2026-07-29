//! Runtime instrumentation.
//!
//! [`snapshot`] reports what the calling thread's runtime is holding right now:
//! how many tasks are live, how deep its queues are, how many timers are armed,
//! how many driver operations are outstanding. It exists to make an idle cost
//! attributable — an idle runtime should hold a handful of parked tasks and
//! almost nothing else, and without numbers the only visible fact is a
//! percentage of a core.
//!
//! # What these numbers are
//!
//! Everything here is a **gauge**: a level read at an instant, meaningful on
//! its own and meaningless to subtract across two snapshots. Cumulative
//! counters (polls, wakes, completions) and high-water marks are deliberately
//! *not* mixed in — a consumer that cannot tell a running total from a level
//! will misreport it, so they will arrive as separate types rather than as
//! extra fields here.
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
/// Obtained from [`snapshot`]. Every field is a gauge; see the [module
/// documentation](self) for why counters and peaks are not mixed in.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct Gauges {
    /// Spawned tasks that have neither completed nor been aborted.
    ///
    /// An idle application should hold one per long-lived activity and no
    /// more. This is the first number to look at when a runtime will not go
    /// quiet.
    pub live_tasks: usize,
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

/// Reads the calling thread's runtime levels.
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
/// // No runtime on this thread yet: everything reads zero.
/// assert_eq!(runite::metrics::snapshot(), runite::metrics::Gauges::default());
///
/// runite::spawn(async {
///     // Inside a task, this thread's runtime is holding at least this task.
///     assert!(runite::metrics::snapshot().live_tasks >= 1);
/// });
/// runite::run();
/// ```
pub fn snapshot() -> Gauges {
    try_with_installed_thread(|state| {
        let Some(state) = state else {
            return Gauges::default();
        };
        Gauges {
            live_tasks: state.tasks.borrow().len(),
            microtask_queue_depth: state.local_microtasks.borrow().len(),
            local_macrotask_queue_depth: state.local_macrotasks.borrow().len(),
            remote_macrotask_queue_depth: state.shared.remote_queue_depth(),
            armed_timers: state.timers.borrow().len(),
            outstanding_operations: state.outstanding_operations(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{Gauges, snapshot};
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
        assert_eq!(observed, Gauges::default());
    }

    /// The gauges answer the question this instrumentation exists for: what is
    /// the runtime still holding?
    #[test]
    fn gauges_report_live_tasks_and_armed_timers() {
        let observed = Rc::new(Cell::new(Gauges::default()));

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

        let observed = observed.get();
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

        let observed = snapshot();
        assert_eq!(
            observed.live_tasks, 0,
            "a completed task should not stay registered"
        );
        assert_eq!(observed.armed_timers, 0);
        assert_eq!(observed.local_macrotask_queue_depth, 0);
        assert_eq!(observed.microtask_queue_depth, 0);
        assert_eq!(observed.outstanding_operations, 0);
    }
}

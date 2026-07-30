//! One record per event-loop turn, and none at all when nobody is listening.
//!
//! The turn record is the cheapest useful unit of runtime attribution — far
//! cheaper than a record per task or per completion — but it is still one per
//! turn, and a loop can take millions of them. So the dormant path must not
//! sample queue depths, must not lock the cross-thread queue, and must not time
//! the driver park. These tests pin both halves: what a collector receives, and
//! that a subscriber which declines TRACE receives nothing.

#![cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, span};

/// The fields of one `event = "turn"` record that these tests assert on.
#[derive(Clone, Debug, Default)]
struct TurnRecord {
    runtime_id: u64,
    turn_id: u64,
    entry: String,
    wake: String,
    wait_ns: u64,
    runnable_ns: u64,
    timers: u64,
}

struct TurnWatcher {
    records: Arc<Mutex<Vec<TurnRecord>>>,
    /// Whether to claim interest at all. A subscriber that declines TRACE is
    /// how an application that only wants warnings looks to the runtime.
    collect: bool,
}

impl tracing::Subscriber for TurnWatcher {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        if self.collect {
            *metadata.level() <= Level::TRACE
        } else {
            *metadata.level() <= Level::WARN
        }
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = TurnVisitor {
            is_turn: false,
            record: TurnRecord::default(),
        };
        event.record(&mut visitor);
        if visitor.is_turn {
            self.records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(visitor.record);
        }
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

struct TurnVisitor {
    is_turn: bool,
    record: TurnRecord,
}

impl TurnVisitor {
    fn text(&mut self, name: &str, value: &str) {
        match name {
            "event" if value == "turn" => self.is_turn = true,
            "entry" => self.record.entry = value.to_owned(),
            "wake" => self.record.wake = value.to_owned(),
            _ => {}
        }
    }
}

impl Visit for TurnVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.text(field.name(), value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "runtime_id" => self.record.runtime_id = value,
            "turn_id" => self.record.turn_id = value,
            "wait_ns" => self.record.wait_ns = value,
            "runnable_ns" => self.record.runnable_ns = value,
            "timers" => self.record.timers = value,
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `&'static str` fields normally arrive through `record_str`, but a
        // subscriber must not assume which visit method a given tracing version
        // routes them through.
        let rendered = format!("{value:?}");
        self.text(field.name(), rendered.trim_matches('"'));
    }
}

/// Runs `workload` under a subscriber and returns whatever turn records it saw.
fn records_from(collect: bool, workload: impl FnOnce()) -> Vec<TurnRecord> {
    let records = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::with_default(
        TurnWatcher {
            records: Arc::clone(&records),
            collect,
        },
        workload,
    );
    // Taken out through the shared handle rather than by unwrapping the `Arc`:
    // `tracing` may still be holding the dispatcher after the default guard is
    // released, and sole ownership is not this test's claim.
    let mut collected = records
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::take(&mut *collected)
}

/// A collector receives a record per turn, stamped with the runtime and turn
/// it belongs to. Without both identities a merged timeline cannot tell two
/// runtime threads apart.
#[test]
fn a_collecting_subscriber_receives_identified_turn_records() {
    let mut runtime_id = None;
    let records = records_from(true, || {
        runtime_id = runite::block_on(async {
            runite::yield_now().await;
            runite::current_runtime_id()
        });
    });

    assert!(
        !records.is_empty(),
        "a TRACE subscriber should receive turn records"
    );

    let expected = runtime_id
        .expect("block_on installs a runtime")
        .to_string()
        .parse::<u64>()
        .expect("RuntimeId renders as its numeric identity");
    for record in &records {
        assert_eq!(
            record.runtime_id, expected,
            "every turn on one thread belongs to that thread's runtime"
        );
        assert_ne!(record.turn_id, 0, "a turn is never identified as zero");
        assert_eq!(record.entry, "block_on", "block_on drove these turns");
    }

    let mut ids = records
        .iter()
        .map(|record| record.turn_id)
        .collect::<Vec<_>>();
    let observed = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), observed, "turn ids are never reused");
    assert!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "turn ids increase with the turns they name"
    );
}

/// The point of the exercise: a subscriber that declines TRACE gets nothing,
/// and so the turn loop never samples a queue depth or times a park.
#[test]
fn a_subscriber_that_declines_trace_receives_no_turn_records() {
    let records = records_from(false, || {
        runite::block_on(async {
            runite::yield_now().await;
        });
    });

    assert!(
        records.is_empty(),
        "a WARN-only subscriber should receive no turn records"
    );
}

/// A turn woken by a timer says so, and says how long the loop was parked
/// before it. Attributing an idle wake to the wrong source is worse than not
/// attributing it, which is why `spurious` exists as its own answer.
#[test]
fn a_turn_woken_by_a_timer_reports_the_timer_and_the_park() {
    const NAP: Duration = Duration::from_millis(50);

    let records = records_from(true, || {
        runite::block_on(runite::time::sleep(NAP));
    });

    let woken = records
        .iter()
        .max_by_key(|record| record.wait_ns)
        .expect("the sleeping loop should have produced turn records");
    assert!(
        woken.wait_ns as u128 >= NAP.as_nanos() / 2,
        "the park before the wake should be reported, saw {}ns",
        woken.wait_ns
    );
    assert_eq!(
        woken.wake, "timer",
        "a sleep is woken by its timer, not by a spurious or notified wake"
    );
    assert!(
        woken.timers >= 1,
        "the wake should carry the timer it dispatched"
    );

    // The turn that performed the park is the one before it. Its `runnable_ns`
    // must not include the park: this is the regression that would make an
    // idle loop look like it is doing 50ms of work per turn.
    let parked = records
        .iter()
        .filter(|record| record.turn_id < woken.turn_id)
        .max_by_key(|record| record.turn_id)
        .expect("a park is always performed by an earlier turn");
    assert!(
        parked.runnable_ns * 2 < woken.wait_ns,
        "the park must not be counted as runnable work, saw {}ns runnable against a {}ns park",
        parked.runnable_ns,
        woken.wait_ns
    );
}

/// A turn that never parked is classified as continuing work rather than as a
/// wake that did not happen.
#[test]
fn a_turn_that_never_parked_is_not_reported_as_a_wake() {
    let records = records_from(true, || {
        runite::queue_macrotask(|| {});
        runite::run_ready_tasks();
    });

    assert!(!records.is_empty(), "run_ready_tasks takes turns too");
    for record in &records {
        assert_eq!(record.entry, "run_ready_tasks");
        assert_eq!(
            record.wake, "queued",
            "a host-driven turn parks in no driver and is woken by nothing"
        );
        assert_eq!(record.wait_ns, 0, "nothing waited, so nothing is reported");
    }
}

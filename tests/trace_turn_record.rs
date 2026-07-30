//! One record per event-loop turn, and an honest `wake` on every one of them.
//!
//! What these pin is the classification. A wake reason that is guessed rather
//! than observed is worse than no wake reason at all, so `timer`, `io`,
//! `notify` and `queued` each get a test that fails if the runtime starts
//! inferring them from something the turn did not see. `spurious` is the
//! remainder — a park the driver could not explain — and there is no portable
//! way to provoke one, so it is pinned only by the others: it is what a turn
//! reports when none of them apply.
//!
//! The other half of the feature — that a loop with no collector samples no
//! queue depth, takes no lock on the cross-thread queue, and does not time the
//! driver park — cannot be pinned from out here, because the cost is invisible
//! from the outside: an integration test can only observe that no event
//! arrived, which `tracing` would arrange on its own with the gate deleted.
//! That property is counted directly in
//! `platform::runtime_shared::test_support::dormant_turn_records_cost_nothing`.

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

/// The `turn_id` field carries the same value [`runite::current_turn`] returns
/// inside that turn.
///
/// This is the join the API exists for: an application stamps its own record
/// with `current_turn()` and lines it up against runite's. Neither half is
/// worth anything if they can drift, and nothing else here compares them —
/// the tests above check `runtime_id` against the API but only check `turn_id`
/// against itself.
#[test]
fn the_record_turn_id_is_what_current_turn_returns() {
    let mut stamped = None;
    let records = records_from(true, || {
        stamped = runite::block_on(async { runite::current_turn() });
    });

    let stamped = stamped
        .expect("a task body runs inside a turn")
        .to_string()
        .parse::<u64>()
        .expect("TurnId renders as the numeric identity the field carries");
    let ids = records
        .iter()
        .map(|record| record.turn_id)
        .collect::<Vec<_>>();
    assert!(
        ids.contains(&stamped),
        "the turn the task stamped should be one of the turns recorded: \
         stamped {stamped}, recorded {ids:?}"
    );
}

/// A subscriber that declines TRACE receives no turn records.
///
/// This says nothing about what the loop *did* — `tracing` would filter the
/// event on its own — only that the record does not reach an application that
/// did not ask for it. The cost claim is pinned in the crate's own tests.
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

/// Work finishing on the blocking pool must not forge an I/O wake.
///
/// `operations_completed` is bumped by whichever thread terminalizes the
/// operation, so a blocking-pool job finishing while a host loop happens to be
/// mid-turn moves a counter the turn had nothing to do with. Classifying from
/// that delta labelled turns that never parked, never polled a driver and
/// dispatched nothing as `io`. The wake reason comes from the driver's own
/// readiness bits instead, and a turn that did not park is `queued` whatever
/// else moved.
#[test]
fn a_blocking_pool_completion_is_not_reported_as_an_io_wake() {
    const JOBS: u32 = 32;
    /// Generous upper bound on a pool that is working, not a target.
    const CHURN: Duration = Duration::from_secs(30);

    let records = records_from(true, || {
        // Installs this thread's runtime, so `spawn_blocking` has an owner to
        // report its completions to.
        runite::run_ready_tasks();

        let finished = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let jobs = (0..JOBS)
            .map(|index| {
                let finished = Arc::clone(&finished);
                runite::spawn_blocking(move || {
                    // Staggered so completions land throughout the host loop
                    // below rather than all in one turn.
                    std::thread::sleep(Duration::from_micros(u64::from(index) * 400));
                    finished.fetch_add(1, std::sync::atomic::Ordering::Release);
                })
                .expect("the blocking pool should accept the job")
            })
            .collect::<Vec<_>>();

        // Spin the host loop until every job has reported in. Deliberately not
        // a fixed time budget: the blocking pool is bounded at 2..=32 workers,
        // so the wall time to retire `JOBS` staggered sleeps depends on how
        // many cores the machine has. A budget wide enough for a two-worker
        // machine is mostly idle everywhere else, and one tuned on a developer
        // box fails in CI — which is exactly what happened on macOS, where 19
        // of 32 had finished when a 150ms budget expired.
        let deadline = std::time::Instant::now() + CHURN;
        while finished.load(std::sync::atomic::Ordering::Acquire) < JOBS {
            runite::run_ready_tasks();
            assert!(
                std::time::Instant::now() < deadline,
                "blocking jobs did not retire within {CHURN:?}; the pool is stuck, \
                 not merely slow"
            );
        }
        // Keep taking turns after the last completion, so the window in which a
        // counter could be misattributed is covered on both sides.
        for _ in 0..64 {
            runite::run_ready_tasks();
        }
        drop(jobs);
    });

    assert!(!records.is_empty(), "the host loop should have taken turns");
    for record in &records {
        assert_eq!(
            record.wake, "queued",
            "a turn that never parked cannot have been woken by I/O"
        );
    }
}

/// A turn woken by a real completion says `io`, and says so because the driver
/// reported one rather than because a counter moved.
#[test]
fn a_turn_woken_by_a_completion_reports_io() {
    use runite::io::AsyncReadExt as _;
    use std::io::Write as _;

    const LATENCY: Duration = Duration::from_millis(30);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("loopback should bind");
    let address = listener
        .local_addr()
        .expect("bound listener has an address");

    // A plain OS thread, so nothing about the peer touches the runtime under
    // test: the only thing that can wake it is the completion of its own read.
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("the runtime should connect");
        std::thread::sleep(LATENCY);
        stream.write_all(b"x").expect("peer write should succeed");
        std::thread::sleep(LATENCY);
    });

    let records = records_from(true, || {
        runite::block_on(async move {
            let mut stream = runite::net::TcpStream::connect(address)
                .await
                .expect("loopback connect should succeed");
            let mut byte = [0u8; 1];
            stream
                .read_exact(&mut byte)
                .await
                .expect("the peer should deliver a byte");
        });
    });
    peer.join().expect("peer thread should exit normally");

    let woken = records
        .iter()
        .filter(|record| record.wake == "io")
        .max_by_key(|record| record.wait_ns)
        .expect("the read completion should have woken a turn");
    assert!(
        woken.wait_ns > 0,
        "an I/O wake follows a park, so the park it ended is reported with it"
    );
}

/// A turn woken by a cross-thread notification says `notify`, not `io`.
///
/// A blocking-pool job posts its result over the remote queue and rings the
/// driver's wake; nothing reaches the completion path, so nothing about this
/// wake is an I/O completion even though an async operation of this runtime
/// did finish inside the same turn.
#[test]
fn a_turn_woken_by_a_cross_thread_post_reports_notify() {
    const LATENCY: Duration = Duration::from_millis(30);

    let records = records_from(true, || {
        runite::block_on(async {
            runite::spawn_blocking(|| std::thread::sleep(LATENCY))
                .expect("the blocking pool should accept the job")
                .await
                .expect("the blocking job should not panic");
        });
    });

    let woken = records
        .iter()
        .max_by_key(|record| record.wait_ns)
        .expect("the waiting loop should have produced turn records");
    assert!(
        woken.wait_ns as u128 >= LATENCY.as_nanos() / 2,
        "the park before the wake should be reported, saw {}ns",
        woken.wait_ns
    );
    assert_eq!(
        woken.wake, "notify",
        "a remote post wakes the driver's notifier, not its completion path"
    );
}

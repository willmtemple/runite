//! The microtask-starvation warning: a long microtask checkpoint warns only
//! when a macrotask is actually waiting behind it.

#![cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, span};

/// Minimal subscriber that flips a flag when the scheduler emits the
/// `microtask_starvation` warning.
struct StarvationFlag(Arc<AtomicBool>);

impl tracing::Subscriber for StarvationFlag {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= Level::WARN
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut saw_starvation = false;
        event.record(&mut FindStarvation(&mut saw_starvation));
        if saw_starvation {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

struct FindStarvation<'a>(&'a mut bool);

impl Visit for FindStarvation<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" && value == "microtask_starvation" {
            *self.0 = true;
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

/// Runs one microtask checkpoint of `remaining + 1` chained microtasks: each
/// task schedules the next, so the checkpoint cannot empty until the chain
/// bottoms out.
fn microtask_chain(remaining: u32) {
    if remaining > 0 {
        runite::queue_microtask(move || microtask_chain(remaining - 1));
    }
}

/// Comfortably past the 1000-task starvation threshold.
const CHAIN_LENGTH: u32 = 2500;

#[test]
fn long_checkpoint_with_waiting_macrotask_warns() {
    let warned = Arc::new(AtomicBool::new(false));
    tracing::subscriber::with_default(StarvationFlag(Arc::clone(&warned)), || {
        runite::queue_macrotask(|| {});
        runite::queue_microtask(|| microtask_chain(CHAIN_LENGTH));
        runite::run();
    });
    assert!(
        warned.load(Ordering::SeqCst),
        "a runaway checkpoint with a queued macrotask should warn"
    );
}

#[test]
fn long_checkpoint_with_idle_queues_stays_quiet() {
    let warned = Arc::new(AtomicBool::new(false));
    tracing::subscriber::with_default(StarvationFlag(Arc::clone(&warned)), || {
        runite::queue_microtask(|| microtask_chain(CHAIN_LENGTH));
        runite::run();
    });
    assert!(
        !warned.load(Ordering::SeqCst),
        "a long checkpoint with no waiting macrotask starves nothing and should not warn"
    );
}

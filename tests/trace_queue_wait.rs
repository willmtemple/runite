//! Queue-wait timing is collected only when something is collecting it.
//!
//! `macrotask_dequeued` reports how long a task sat in the local queue, which
//! means stamping every push with the monotonic clock. That is a real syscall
//! (~20-30ns on every backend) on a per-macrotask path, so it must not happen
//! merely because tracing is linked in — only when a subscriber is actually
//! taking the event. These tests pin both halves of that.

#![cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, span};

/// Records whether a `macrotask_dequeued` event arrived, and what `wait_ns` it
/// carried.
struct DequeueWatcher {
    seen: Arc<AtomicBool>,
    wait_ns: Arc<AtomicU64>,
    /// Whether to claim interest in the event at all. A subscriber that
    /// declines TRACE is how a real application that only wants warnings
    /// looks to the runtime.
    collect: bool,
}

impl tracing::Subscriber for DequeueWatcher {
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
        let mut visitor = FindDequeue {
            is_dequeue: false,
            wait_ns: None,
        };
        event.record(&mut visitor);
        if visitor.is_dequeue {
            self.seen.store(true, Ordering::SeqCst);
            if let Some(wait) = visitor.wait_ns {
                self.wait_ns.store(wait, Ordering::SeqCst);
            }
        }
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

struct FindDequeue {
    is_dequeue: bool,
    wait_ns: Option<u64>,
}

impl Visit for FindDequeue {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" && value == "macrotask_dequeued" {
            self.is_dequeue = true;
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "wait_ns" {
            self.wait_ns = Some(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `event = "..."` is a `&'static str` field, but a subscriber must not
        // assume which visit method a given tracing version routes it through.
        if field.name() == "event" && format!("{value:?}").contains("macrotask_dequeued") {
            self.is_dequeue = true;
        }
    }
}

fn run_one_macrotask() {
    let ran = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&ran);
    runite::queue_macrotask(move || flag.store(true, Ordering::SeqCst));
    runite::run();
    assert!(ran.load(Ordering::SeqCst), "the macrotask should have run");
}

/// The event reaches a subscriber that wants it, carrying a queue-wait time.
#[test]
fn a_collecting_subscriber_receives_the_queue_wait_time() {
    let seen = Arc::new(AtomicBool::new(false));
    let wait_ns = Arc::new(AtomicU64::new(u64::MAX));

    tracing::subscriber::with_default(
        DequeueWatcher {
            seen: Arc::clone(&seen),
            wait_ns: Arc::clone(&wait_ns),
            collect: true,
        },
        run_one_macrotask,
    );

    assert!(
        seen.load(Ordering::SeqCst),
        "a TRACE subscriber should receive macrotask_dequeued"
    );
    assert_ne!(
        wait_ns.load(Ordering::SeqCst),
        u64::MAX,
        "the event should carry a wait_ns field"
    );
}

/// A subscriber that declines TRACE gets nothing — and, the point of the
/// exercise, the push side never reads the clock to produce it.
#[test]
fn a_subscriber_that_declines_trace_receives_nothing() {
    let seen = Arc::new(AtomicBool::new(false));
    let wait_ns = Arc::new(AtomicU64::new(u64::MAX));

    tracing::subscriber::with_default(
        DequeueWatcher {
            seen: Arc::clone(&seen),
            wait_ns: Arc::clone(&wait_ns),
            collect: false,
        },
        run_one_macrotask,
    );

    assert!(
        !seen.load(Ordering::SeqCst),
        "a WARN-only subscriber should not receive macrotask_dequeued"
    );
}

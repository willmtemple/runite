//! Shared helpers for runite criterion benchmarks.
//!
//! Like the integration tests, runtime futures are `!Send` and must be built on
//! the runtime thread. Each helper spawns a single runtime thread, builds the
//! work future there, and times it internally so that thread-spawn cost is not
//! included in the measurement.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Runs `make_future` on a dedicated runtime thread, timing only the awaited
/// work (not runtime spin-up or thread creation). Returns the elapsed duration,
/// suitable for criterion's `iter_custom`.
pub fn time_on_runtime<Fut>(make_future: impl FnOnce() -> Fut + Send + 'static) -> Duration
where
    Fut: Future<Output = ()> + 'static,
{
    std::thread::spawn(move || {
        let slot: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::ZERO));
        let writer = Arc::clone(&slot);
        runite::spawn(async move {
            let start = Instant::now();
            make_future().await;
            *writer.lock().expect("timing slot poisoned") = start.elapsed();
        });
        runite::run();
        *slot.lock().expect("timing slot poisoned")
    })
    .join()
    .expect("runtime benchmark thread panicked")
}

/// [`time_on_runtime`] with `subscriber` installed as the runtime thread's
/// default for the whole loop.
///
/// Exists so one workload can be measured with diagnostics off and on. The
/// price of an instrumentation feature is the difference between the two runs,
/// and only the off run answers what a shipping application pays.
// This module is compiled into all three bench binaries; only `runtime` uses
// this half of it.
#[allow(dead_code)]
pub fn time_on_runtime_collecting<S, Fut>(
    subscriber: S,
    make_future: impl FnOnce() -> Fut + Send + 'static,
) -> Duration
where
    S: tracing::Subscriber + Send + Sync + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::spawn(move || {
        tracing::subscriber::with_default(subscriber, || {
            let slot: Arc<Mutex<Duration>> = Arc::new(Mutex::new(Duration::ZERO));
            let writer = Arc::clone(&slot);
            runite::spawn(async move {
                let start = Instant::now();
                make_future().await;
                *writer.lock().expect("timing slot poisoned") = start.elapsed();
            });
            runite::run();
            *slot.lock().expect("timing slot poisoned")
        })
    })
    .join()
    .expect("runtime benchmark thread panicked")
}

/// Accepts every callsite and discards every event.
///
/// The point is to pay for building the record and nothing beyond it: a
/// formatting subscriber would measure `tracing`'s formatter, not runite's.
#[allow(dead_code)]
pub struct AcceptEverything;

impl tracing::Subscriber for AcceptEverything {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

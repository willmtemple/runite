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

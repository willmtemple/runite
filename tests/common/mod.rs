//! Shared helpers for runite integration tests.
//!
//! The runtime is event-loop-per-thread: [`runite::run`] installs thread-local
//! state on the calling thread and drives the loop until every task completes.
//! Each helper runs the runtime on a freshly spawned OS thread so tests stay
//! isolated from cargo's test thread pool and from one another.
//!
//! Runtime futures are intentionally `!Send` (they live on a single event
//! loop), so the future must be *constructed on the runtime thread*. The helper
//! therefore takes a `Send` factory closure and builds the future inside the
//! spawned thread; only the closure and the `Send` output cross the boundary.

use std::future::Future;
use std::sync::{Arc, Mutex};

/// Builds a future on a dedicated runtime thread via `make_future`, drives it to
/// completion, and returns its output. Panics if the runtime thread panics or
/// the future never resolves.
pub fn block_on<Fut, T>(make_future: impl FnOnce() -> Fut + Send + 'static) -> T
where
    Fut: Future<Output = T> + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let writer = Arc::clone(&slot);
        runite::queue_future(async move {
            let value = make_future().await;
            *writer.lock().expect("result slot poisoned") = Some(value);
        });
        runite::run();
        slot.lock()
            .expect("result slot poisoned")
            .take()
            .expect("runtime drained without resolving the future")
    })
    .join()
    .expect("runtime thread panicked")
}

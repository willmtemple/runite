//! Tests for the `block_on` entry point.
//!
//! `block_on` drives the current thread's event loop until the supplied future
//! resolves and returns its output. Each test runs on a freshly spawned OS
//! thread so `block_on` installs and drives an isolated runtime.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use runite::QueueError;

struct PendingDrop {
    dropped: Arc<AtomicBool>,
}

impl Future for PendingDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

/// Returns the future's output and can borrow non-`'static` local state (the
/// future is driven in place, not spawned).
#[test]
fn returns_value_and_borrows_locals() {
    let output = std::thread::spawn(|| {
        let greeting = String::from("hello");
        runite::block_on(async { format!("{greeting} world") })
    })
    .join()
    .expect("runtime thread should not panic");

    assert_eq!(output, "hello world");
}

/// Drives real asynchronous I/O — here a timer — to completion.
#[test]
fn drives_timers() {
    let elapsed = std::thread::spawn(|| {
        runite::block_on(async {
            let start = Instant::now();
            runite::time::sleep(Duration::from_millis(10)).await;
            start.elapsed()
        })
    })
    .join()
    .expect("runtime thread should not panic");

    assert!(
        elapsed >= Duration::from_millis(10),
        "block_on should have driven the 10ms sleep, only {elapsed:?} elapsed"
    );
}

/// Returns as soon as the supplied future completes, even if other spawned
/// tasks are still pending — unlike `run`, which drains the whole loop. If it
/// waited for loop quiescence this test would hang on the never-completing task.
#[test]
fn returns_before_unfinished_background_tasks() {
    let value = std::thread::spawn(|| {
        runite::spawn(async { std::future::pending::<()>().await });
        runite::block_on(async { 99u32 })
    })
    .join()
    .expect("runtime thread should not panic");

    assert_eq!(value, 99);
}

/// Re-entering the event loop via a nested `block_on` is rejected (the panic
/// propagates out of the direct driver rather than being swallowed).
#[test]
fn nested_block_on_panics() {
    let result = std::thread::spawn(|| {
        std::panic::catch_unwind(|| {
            runite::block_on(async {
                runite::block_on(async {});
            });
        })
    })
    .join()
    .expect("runtime thread should not panic at the OS-thread boundary");

    assert!(
        result.is_err(),
        "a nested block_on must panic via the reentrancy guard"
    );
}

/// Runtime ownership is tied to the OS thread rather than to a single driver
/// entry. The final TLS teardown closes external handles even when the thread
/// only used `block_on` and never called `run`.
#[test]
fn thread_exit_after_block_on_closes_external_handle() {
    let handle = std::thread::spawn(|| {
        let handle = runite::current_thread_handle();
        runite::block_on(async {});
        assert!(handle.is_current());
        assert!(!handle.is_closed());
        handle
    })
    .join()
    .expect("runtime thread should exit cleanly");

    assert!(handle.is_closed());
    assert!(!handle.is_current());
    assert!(matches!(
        handle.queue_macrotask(|| {}),
        Err(QueueError::Closed)
    ));
}

/// Unix may safely run full fallback cleanup from its TLS destructor. Windows
/// invokes TLS destructors under the loader lock, so arbitrary user threads
/// publish `Closed` but deliberately leak residual driver/task state; owned
/// runite workers use an explicit pre-return teardown guard instead.
#[test]
fn ordinary_thread_exit_uses_platform_safe_cleanup() {
    let dropped = Arc::new(AtomicBool::new(false));
    let dropped_by_future = Arc::clone(&dropped);

    std::thread::spawn(move || {
        runite::spawn(PendingDrop {
            dropped: dropped_by_future,
        });
        runite::block_on(async {});
    })
    .join()
    .expect("ordinary runtime thread should exit cleanly");

    #[cfg(not(windows))]
    assert!(
        dropped.load(Ordering::Acquire),
        "Unix TLS teardown should perform full task cleanup"
    );
    #[cfg(windows)]
    assert!(
        !dropped.load(Ordering::Acquire),
        "Windows loader-lock fallback must not run arbitrary task destructors"
    );
}

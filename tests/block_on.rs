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

/// `try_block_on` is `block_on` with a reportable startup boundary. On a
/// machine where the driver initializes, it behaves identically and the future
/// is driven the same way.
#[test]
fn try_block_on_matches_block_on_when_startup_succeeds() {
    let value = runite::try_block_on(async {
        let mut total = 0;
        for step in 1..=4 {
            total += step;
            runite::yield_now().await;
        }
        total
    })
    .expect("startup should succeed on a machine that can run the tests");
    assert_eq!(value, 10);
}

/// Startup is the only fallible part. An error produced *by* the future is the
/// future's own, and arrives inside `Ok`.
#[test]
fn try_block_on_does_not_absorb_the_future_s_own_error() {
    let outcome: std::io::Result<std::io::Result<()>> = runite::try_block_on(async {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the future's own error",
        ))
    });

    let inner = outcome.expect("startup should succeed");
    let error = inner.expect_err("the future's error should survive");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

/// Repeated entry works: the runtime is installed once and reused, so a second
/// call does not attempt to create a second driver on the same thread.
#[test]
fn try_block_on_reuses_an_installed_runtime() {
    assert_eq!(runite::try_block_on(async { 1 }).expect("first"), 1);
    assert_eq!(runite::try_block_on(async { 2 }).expect("second"), 2);
    // And it interoperates with the panicking entry point on the same thread.
    assert_eq!(runite::block_on(async { 3 }), 3);
}

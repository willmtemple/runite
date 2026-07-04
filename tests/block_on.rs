//! Tests for the `block_on` entry point.
//!
//! `block_on` drives the current thread's event loop until the supplied future
//! resolves and returns its output. Each test runs on a freshly spawned OS
//! thread so `block_on` installs and drives an isolated runtime.

use std::time::{Duration, Instant};

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

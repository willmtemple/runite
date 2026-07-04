//! Tests for the `#[runite::test]` / `#[runite::main]` attribute macros.

use std::time::Duration;

/// The generated `#[test]` drives the async body to completion, including real
/// async I/O (a timer).
#[runite::test]
async fn drives_async_body() {
    let before = std::time::Instant::now();
    runite::time::sleep(Duration::from_millis(5)).await;
    assert!(before.elapsed() >= Duration::from_millis(5));
}

/// A test body may return a `Termination` type (here `Result`) so it can use
/// `?`; `Ok` reports success.
#[runite::test]
async fn supports_result_return() -> Result<(), Box<dyn std::error::Error>> {
    runite::time::sleep(Duration::from_millis(1)).await;
    let value: u32 = "42".parse()?;
    assert_eq!(value, 42);
    Ok(())
}

/// A spawned task on the test's loop runs to completion within the test.
#[runite::test]
async fn can_spawn_tasks() {
    let handle = runite::spawn(async { 7u32 + 8 });
    assert_eq!(handle.await.expect("spawned task should finish"), 15);
}

/// Attributes below `#[runite::test]` (here `#[should_panic]`) are forwarded to
/// the generated test wrapper.
#[runite::test]
#[should_panic = "expected boom"]
async fn forwards_should_panic() {
    panic!("expected boom");
}

/// The `crate = "..."` argument selects the path to the runite crate, so a
/// renamed dependency still works. Here we spell the real crate name.
#[runite::test(crate = "runite")]
async fn honors_crate_path_argument() {
    let handle = runite::spawn(async { 21u32 * 2 });
    assert_eq!(handle.await.expect("spawned task should finish"), 42);
}

//! When each shape of `#[runite::main]` / `#[runite::test]` installs the
//! thread's runtime relative to the annotated body.
//!
//! Three of the four shapes have a runtime before the body runs, so a
//! `Builder::build()` in the body reports `AlreadyExists`. The fourth — a bare
//! attribute on a *synchronous* `fn` — runs the body first and only then drains
//! with `run()`, so a `build()` there succeeds. The documentation used to state
//! the refusal as an absolute; these tests pin which shape is which so it
//! cannot drift back.
//!
//! The odd one out lives in `entry_bare_sync_runtime_timing.rs`, alone in its
//! own binary: it is the one case that needs a thread nothing has installed a
//! runtime on, and a binary with a single test has that under `--test-threads=1`
//! too.

use std::io::ErrorKind;

/// A bare `async` body is driven by `block_on`, which installs the runtime
/// before it polls anything.
#[runite::test]
async fn a_bare_async_body_already_has_a_runtime() {
    let error = runite::Builder::new()
        .build()
        .expect_err("block_on installed the runtime before this body was polled");
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
}

/// A configured attribute builds the runtime it names before the body, which
/// is the entire reason the settings are reachable from an entry point.
#[runite::test(ring_entries = 32)]
async fn a_configured_async_body_already_has_a_runtime() {
    let error = runite::Builder::new()
        .build()
        .expect_err("the attribute's own builder ran first");
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
}

#[runite::test(ring_entries = 32)]
fn a_configured_sync_body_already_has_a_runtime() {
    let error = runite::Builder::new()
        .build()
        .expect_err("the attribute's own builder ran first");
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
}

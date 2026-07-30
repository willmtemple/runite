//! The one entry-point shape whose body runs before its runtime exists.
//!
//! `#[runite::test]`/`#[runite::main]` on a *synchronous* `fn`, with no
//! settings, expands to "run the body, then `run()`". Nothing has installed a
//! runtime when the body starts, so a `Builder::build()` there succeeds — the
//! opposite of what the surrounding documentation used to state as an
//! absolute, and the reason it is now stated with the exception.
//!
//! This is the whole binary. The test needs a thread no other test has
//! installed a runtime on, and a single-test binary has one however libtest is
//! asked to schedule it. The other three shapes are covered in `macros.rs`,
//! where two of them have to sit under `cfg(target_os = "linux")` because
//! `ring_entries` is the only setting the attribute takes and it does not exist
//! off Linux.

/// If this ever starts failing, the attribute has begun installing a runtime
/// before a bare synchronous body — at which point the absolute claim becomes
/// true again and the documentation sites that now carry the exception (README,
/// ARCHITECTURE, `docs/MIGRATING-0.3.md`, `Builder`'s rustdoc and the two
/// attribute rustdocs) should drop it.
#[runite::test]
fn a_bare_sync_body_runs_before_its_runtime_exists() {
    assert!(
        runite::current_runtime_id().is_none(),
        "nothing should have installed a runtime on this thread yet",
    );

    let runtime = runite::Builder::new()
        .build()
        .expect("a bare synchronous body runs before anything installs a runtime");

    // Not merely accepted: the runtime it created is the thread's, and works.
    assert_eq!(runtime.block_on(async { 6 * 7 }), 42);
}

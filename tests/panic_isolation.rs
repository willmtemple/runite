//! Regression tests for runtime panic isolation (release plan task 2.1).
//!
//! A panic that unwinds out of a spawned future, a scheduled closure, or a
//! blocking-pool job must not tear down the event loop thread. Before this fix
//! a single `panic!` inside any of those propagated out of `run()`, killing the
//! runtime thread (and, for a worker thread, hanging the parent that waited on
//! its completion). The runtime now catches such unwinds, keeps the loop
//! running, and reports `JoinError::Panicked` (or, for a blocking job, the same
//! variant) to the awaiter while the process panic hook still surfaces the
//! panic message.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

mod common;

use common::block_on;

/// A spawned task that panics resolves its join handle to
/// `JoinError::Panicked`, and the event loop keeps running so a later task
/// still completes.
#[test]
fn task_panic_resolves_join_handle_and_loop_survives() {
    let (was_panicked, follow_up_value) = block_on(|| async {
        let panicker = runite::spawn(async {
            panic!("task boom");
        });
        let err = panicker
            .await
            .expect_err("a panicking task must not yield a value");

        // The loop survived the panic: a task spawned afterwards still runs.
        let follow_up = runite::spawn(async { 7u32 });
        let value = follow_up
            .await
            .expect("follow-up task should complete after an isolated panic");

        (err.is_panicked(), value)
    });

    assert!(
        was_panicked,
        "panicking task should resolve to JoinError::Panicked"
    );
    assert_eq!(follow_up_value, 7);
}

/// A panic inside a `spawn_blocking` closure resolves the blocking join handle
/// to `JoinError::Panicked` (distinct from `Cancelled`) and leaves the pool
/// usable for a subsequent job.
#[test]
fn blocking_task_panic_resolves_to_panicked() {
    let (was_panicked, follow_up_value) = block_on(|| async {
        let handle = runite::spawn_blocking(|| -> u32 { panic!("blocking boom") })
            .expect("blocking task should queue");
        let err = handle
            .await
            .expect_err("a panicking blocking task must not yield a value");

        // The pool worker survived: another blocking job still runs.
        let follow_up = runite::spawn_blocking(|| 11u32).expect("second blocking task should queue");
        let value = follow_up
            .await
            .expect("follow-up blocking task should complete");

        (err.is_panicked(), value)
    });

    assert!(
        was_panicked,
        "panicking blocking task should resolve to JoinError::Panicked"
    );
    assert_eq!(follow_up_value, 11);
}

/// A panic inside a scheduled macrotask closure is isolated: `run()` returns
/// normally and a macrotask queued after the panicking one still executes.
#[test]
fn scheduled_closure_panic_does_not_kill_loop() {
    let follow_up_ran = Arc::new(AtomicBool::new(false));
    let follow_up_writer = Arc::clone(&follow_up_ran);

    std::thread::spawn(move || {
        runite::queue_macrotask(|| panic!("macrotask boom"));
        runite::queue_macrotask(move || follow_up_writer.store(true, Ordering::SeqCst));
        runite::run();
    })
    .join()
    .expect("an isolated macrotask panic must not unwind out of run()");

    assert!(
        follow_up_ran.load(Ordering::SeqCst),
        "a macrotask queued after a panicking one should still run"
    );
}

/// A panicking task does not strand tasks spawned before it: independent work
/// on the same loop completes regardless of the panic.
#[test]
fn panic_does_not_strand_sibling_tasks() {
    let completed = Arc::new(AtomicU32::new(0));
    let completed_writer = Arc::clone(&completed);

    block_on(move || async move {
        let a = runite::spawn(async { 1u32 });
        let panicker = runite::spawn(async { panic!("sibling boom") });
        let b = runite::spawn(async { 2u32 });

        completed_writer.fetch_add(a.await.expect("task a completes"), Ordering::SeqCst);
        assert!(panicker.await.is_err());
        completed_writer.fetch_add(b.await.expect("task b completes"), Ordering::SeqCst);
    });

    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

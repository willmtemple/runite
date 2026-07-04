//! Regression tests for the event-loop reentrancy guard.
//!
//! The driver loops (`run`, `run_until_stalled`, `run_ready_tasks`) drive the
//! thread's microtask/macrotask queues. Re-entering any of them from inside a
//! task poll or scheduled callback would drive the same queues from two stack
//! frames at once and corrupt scheduling state. The runtime now rejects such
//! re-entry with a panic, which — thanks to the per-task panic firewall — is
//! isolated to the offending task (it resolves to `JoinError::Panicked`) rather
//! than taking down the outer loop.

mod common;

use common::block_on;

/// Calling `run()` from within a running task is rejected: the task resolves to
/// `JoinError::Panicked`, and the surrounding loop keeps running.
#[test]
fn nested_run_is_rejected() {
    let (was_panicked, follow_up) = block_on(|| async {
        let nested = runite::spawn(async {
            // Illegal: re-enter the driver from within a task poll.
            runite::run();
        });
        let err = nested
            .await
            .expect_err("re-entering run() from a task must not yield a value");

        // The outer loop survived the rejected re-entry.
        let follow_up = runite::spawn(async { 5u32 })
            .await
            .expect("a task spawned after the rejected re-entry should still run");

        (err.is_panicked(), follow_up)
    });

    assert!(
        was_panicked,
        "nested run() should be rejected as JoinError::Panicked"
    );
    assert_eq!(follow_up, 5);
}

/// Re-entering `run_until_stalled()` from within a task is likewise rejected.
#[test]
fn nested_run_until_stalled_is_rejected() {
    let was_panicked = block_on(|| async {
        let nested = runite::spawn(async {
            runite::run_until_stalled();
        });
        nested
            .await
            .expect_err("re-entering run_until_stalled() must not yield a value")
            .is_panicked()
    });

    assert!(was_panicked);
}

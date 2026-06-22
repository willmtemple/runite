//! End-to-end task cancellation tests: `JoinHandle::abort`, `AbortHandle`, and
//! that aborting a task parked on a driver operation cancels the in-flight op
//! without resolving its output.

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use common::block_on;
use runite::task::JoinError;
use runite::time::sleep;

#[test]
fn abort_before_first_poll_yields_aborted() {
    let result = block_on(|| async {
        let handle = runite::queue_future(async {
            sleep(Duration::from_secs(3600)).await;
            42usize
        });
        // Abort before the spawned task has had any chance to run.
        handle.abort();
        handle.await
    });

    assert_eq!(result, Err(JoinError::Aborted));
}

#[test]
fn abort_while_parked_on_timer_cancels_without_completing() {
    let (result, ran_body) = block_on(|| async {
        let ran_body = Rc::new(Cell::new(false));
        let flag = Rc::clone(&ran_body);

        let handle = runite::queue_future(async move {
            // Park on a long timer; the abort must drop this future before it
            // ever observes the sleep completing.
            sleep(Duration::from_secs(3600)).await;
            flag.set(true);
            7usize
        });

        // Let the task reach its suspension point on the timer.
        sleep(Duration::from_millis(20)).await;
        handle.abort();
        let result = handle.await;

        // Give the loop a moment; if the timer op were still live and resolved
        // the task, `ran_body` would flip.
        sleep(Duration::from_millis(20)).await;
        (result, ran_body.get())
    });

    assert_eq!(result, Err(JoinError::Aborted));
    assert!(!ran_body, "aborted task body must not run past the await");
}

#[test]
fn abort_handle_cancels_from_elsewhere() {
    let result = block_on(|| async {
        let handle = runite::queue_future(async {
            sleep(Duration::from_secs(3600)).await;
            1usize
        });
        let abort = handle.abort_handle();

        sleep(Duration::from_millis(10)).await;
        assert!(!abort.is_finished());
        abort.abort();
        assert!(abort.is_finished());

        handle.await
    });

    assert_eq!(result, Err(JoinError::Aborted));
}

#[test]
fn completed_task_reports_finished_and_abort_is_noop() {
    let value = block_on(|| async {
        let handle = runite::queue_future(async { 99usize });

        // Drive the task to completion before joining.
        sleep(Duration::from_millis(10)).await;
        assert!(handle.is_finished());

        // Aborting an already-finished task must not turn its output into an
        // error.
        handle.abort();
        handle.await
    });

    assert_eq!(value, Ok(99));
}

#[test]
fn join_handle_resolves_to_output_when_not_aborted() {
    let value = block_on(|| async {
        let handle = runite::queue_future(async {
            sleep(Duration::from_millis(5)).await;
            "done"
        });
        handle.await
    });

    assert_eq!(value, Ok("done"));
}

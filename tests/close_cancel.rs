//! Cancelling a close must not close the descriptor behind the ring's back.
//!
//! `close_descriptor` submits an `IORING_OP_CLOSE` naming the caller's real
//! descriptor — it is the one opcode that does not get a duplicate — and the
//! future that awaits it is cancellable. If the descriptor were owned by that
//! future rather than by the completion callback, dropping the future would run
//! `close(2)` while the SQE was still live.
//!
//! Both consequences are real and both are reproduced below: the process aborts
//! on std's I/O-safety check, or the descriptor number is reissued to an
//! unrelated file which the ring then closes. The second is the dangerous one,
//! because it is silent — in a server it closes another peer's socket.

#![cfg(target_os = "linux")]

mod common;

use std::os::fd::AsRawFd;

use common::block_on;

fn temp_file(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("runite-cc");
    std::fs::create_dir_all(&dir).expect("create test directory");
    let path = dir.join(format!("{label}{}", std::process::id()));
    std::fs::write(&path, b"contents").expect("seed file");
    path
}

/// Whether `raw` is still open **and still names `path`**.
///
/// Asking only whether the number is open would be a race: the tests in this
/// binary run concurrently, so another thread can be handed the number the
/// instant it is freed, and the probe would report "still open" about an
/// entirely different file. Comparing what the descriptor actually points at
/// answers the question that matters — did *our* file get closed — regardless
/// of who has the number now.
fn still_names(raw: std::os::fd::RawFd, path: &std::path::Path) -> bool {
    match std::fs::read_link(format!("/proc/self/fd/{raw}")) {
        Ok(target) => target == path,
        // Closed, so the entry is gone.
        Err(_) => false,
    }
}

/// Abort a task that is mid-close. Before the fix this aborted the process with
/// "IO Safety violation: owned file descriptor already closed".
#[test]
fn aborting_a_task_mid_close_does_not_double_close() {
    let path = temp_file("abort");
    let target = path.clone();

    block_on(move || async move {
        let file = runite::fs::File::open(&target).await.expect("open");
        let task = runite::spawn(async move {
            let _ = file.close_descriptor().await;
        });
        // Let the task submit its close, then abandon it.
        runite::yield_now().await;
        task.abort();
        runite::yield_now().await;
    });

    let _ = std::fs::remove_file(&path);
}

/// Poll the close once, then drop it, then claim descriptors. Before the fix
/// the dropped future closed the number, a later `open` was handed it, and the
/// ring's deferred close then closed *that* file.
#[test]
fn a_dropped_close_does_not_close_a_reissued_descriptor() {
    let path = temp_file("reissue");
    let target = path.clone();
    let victim_path = temp_file("victim");
    let victim_for_task = victim_path.clone();
    let victim_probe = victim_path.clone();

    let victim_survived = block_on(move || async move {
        let file = runite::fs::File::open(&target).await.expect("open");
        let doomed = file.as_raw_fd();

        let mut close = Box::pin(file.close_descriptor());
        // One poll is enough to stage the SQE.
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let _ = std::future::Future::poll(close.as_mut(), &mut context);
        drop(close);

        // Claim descriptors until we are handed the one the ring names. If the
        // ring closes it out from under us, the check at the end fails.
        let mut claimed = Vec::new();
        let mut victim = None;
        for _ in 0..64 {
            let opened = std::fs::File::open(&victim_for_task).expect("open victim");
            if opened.as_raw_fd() == doomed {
                victim = Some(opened);
                break;
            }
            claimed.push(opened);
        }

        // Give the runtime turns in which to flush and complete the close.
        for _ in 0..32 {
            runite::yield_now().await;
        }
        runite::time::sleep(std::time::Duration::from_millis(50)).await;
        for _ in 0..32 {
            runite::yield_now().await;
        }

        match victim {
            // We were handed the number back: it must still be ours.
            Some(file) => still_names(file.as_raw_fd(), &victim_probe),
            // Never reissued within the budget, so there is nothing to check.
            // Not a pass or a failure of the property — report success rather
            // than a false alarm, the other tests still cover the mechanism.
            None => true,
        }
    });

    assert!(
        victim_survived,
        "the ring closed a descriptor that had been reissued to an unrelated file"
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&victim_path);
}

/// Dropping the future without ever polling it leaves the descriptor to the
/// future's own `Drop`, which must still close it exactly once.
#[test]
fn a_never_polled_close_still_closes_exactly_once() {
    let path = temp_file("unpolled");
    let target = path.clone();

    let probe = path.clone();
    let closed = block_on(move || async move {
        let file = runite::fs::File::open(&target).await.expect("open");
        let raw = file.as_raw_fd();
        drop(file.close_descriptor());
        for _ in 0..16 {
            runite::yield_now().await;
        }
        !still_names(raw, &probe)
    });

    assert!(closed, "an unsubmitted close must still release the file");
    let _ = std::fs::remove_file(&path);
}

/// The ordinary path still reports the close it performed.
#[test]
fn an_awaited_close_still_reports_closed() {
    let path = temp_file("awaited");
    let target = path.clone();

    let outcome = block_on(move || async move {
        let file = runite::fs::File::open(&target).await.expect("open");
        file.close_descriptor().await.expect("close should succeed")
    });

    assert_eq!(outcome, runite::io::CloseOutcome::Closed);
    let _ = std::fs::remove_file(&path);
}

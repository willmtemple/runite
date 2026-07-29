//! Adopting an already-running process by pid: `Child::from_pid`.
//!
//! The motivating case is a process started through some other API — a
//! `std::process::Command` spawned for something runite does not model — whose
//! exit still has to be awaited without parking a thread.

mod common;

use std::time::Duration;

use common::block_on;
use runite::process::Child;

#[cfg(unix)]
fn sleeper(seconds: u32) -> std::process::Child {
    std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("sleep {seconds}"))
        .spawn()
        .expect("sleeper should spawn")
}

/// `timeout /T` refuses to run when stdin is not a console, which it is not
/// under a test harness, so it exits immediately instead of waiting. Pinging
/// loopback `n + 1` times waits about `n` seconds and needs no console.
#[cfg(windows)]
fn sleeper(seconds: u32) -> std::process::Child {
    std::process::Command::new("cmd")
        .args(["/C", "ping", "-n", &(seconds + 1).to_string(), "127.0.0.1"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("sleeper should spawn")
}

fn exits_with(code: &str) -> std::process::Child {
    #[cfg(unix)]
    let mut command = std::process::Command::new("sh");
    #[cfg(unix)]
    command.arg("-c").arg(format!("exit {code}"));
    #[cfg(windows)]
    let mut command = std::process::Command::new("cmd");
    #[cfg(windows)]
    command.args(["/C", "exit", code]);
    command.spawn().expect("child should spawn")
}

/// A process started outside runite can be awaited through the reactor, and
/// reports the exit status the process actually produced.
#[test]
fn adopted_child_reports_its_exit_status() {
    let started = exits_with("3");
    let pid = started.id();
    // runite reaps it; nothing else may.
    std::mem::forget(started);

    let status = block_on(move || async move {
        let mut child = Child::from_pid(pid).expect("running child should adopt");
        assert_eq!(child.id(), Some(pid));
        child.wait().await.expect("adopted child should exit")
    });

    assert_eq!(status.code(), Some(3));
}

/// The wait is event-driven rather than a poll loop: a child that outlives the
/// call still completes, and the runtime is free in the meantime.
#[test]
fn adopted_child_wait_completes_for_a_running_process() {
    let started = sleeper(1);
    let pid = started.id();
    std::mem::forget(started);

    let (status, elapsed) = block_on(move || async move {
        let mut child = Child::from_pid(pid).expect("running child should adopt");
        assert!(
            child.try_wait().expect("try_wait should succeed").is_none(),
            "a sleeping child should not report a status yet"
        );

        let start = std::time::Instant::now();
        let status = child.wait().await.expect("adopted child should exit");
        (status, start.elapsed())
    });

    assert!(status.success(), "the sleeper should exit cleanly");
    assert!(
        elapsed < Duration::from_secs(30),
        "wait should complete when the process exits, took {elapsed:?}"
    );
}

/// Killing through the adopted handle works, and the status reflects it.
#[test]
fn adopted_child_can_be_killed() {
    let started = sleeper(30);
    let pid = started.id();
    std::mem::forget(started);

    let status = block_on(move || async move {
        let mut child = Child::from_pid(pid).expect("running child should adopt");
        child.kill().expect("adopted child should be killable");
        child.wait().await.expect("killed child should be reaped")
    });

    assert!(
        !status.success(),
        "a killed process should not report success"
    );
}

/// Adopting something that is not there fails at adoption rather than producing
/// a handle whose `wait` never completes.
#[test]
fn adopting_a_nonexistent_process_fails() {
    // A pid that cannot be running: the kernel never allocates 0 as a
    // user-visible process, and the Windows System Idle Process is not
    // openable for synchronization.
    let error = Child::from_pid(0).expect_err("pid 0 should not adopt");
    let _ = error;

    let reaped = exits_with("0");
    let pid = reaped.id();
    // Reap it here, so the pid is genuinely gone before adoption.
    let mut reaped = reaped;
    reaped.wait().expect("child should be reaped");

    // Racy by nature: the pid may already have been reused. Only assert when
    // adoption fails, which is the case worth pinning.
    if let Err(error) = Child::from_pid(pid) {
        assert!(
            error.raw_os_error().is_some(),
            "adoption failure should carry an OS error, got {error:?}"
        );
    }
}

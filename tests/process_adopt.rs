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

/// Pins what the item doc promises about the unsupported Unix case: adopting a
/// process that is not a direct child *succeeds*, because neither a pidfd nor a
/// signal-0 probe can tell parentage — and then every operation that needs
/// `waitpid` fails at once with `ECHILD`. In particular `wait` does not wait for
/// the exit, and `kill` never reaches `kill(2)`.
///
/// The doc used to say `wait` "fails once the process exits", which would have
/// let a supervisor believe it had a working exit watch.
#[cfg(unix)]
#[test]
fn adopting_a_non_child_fails_every_wait_immediately() {
    /// Reaps the orphan however the test ends, including on a panic.
    struct Orphan(libc::pid_t);
    impl Drop for Orphan {
        fn drop(&mut self) {
            // SAFETY: `SIGKILL` takes no pointer arguments.
            unsafe { libc::kill(self.0, libc::SIGKILL) };
        }
    }

    // `sh` starts the sleeper in the background and exits, so init reparents it
    // and it is a live process this one is not the parent of. The sleeper's
    // standard streams go to the null device: it would otherwise inherit the
    // captured pipe and hold it open for its whole 30 seconds, and `output`
    // reads to end of input.
    let launcher = std::process::Command::new("sh")
        .arg("-c")
        .arg("sleep 30 </dev/null >/dev/null 2>&1 & echo $!")
        .output()
        .expect("launcher should run");
    let pid: libc::pid_t = String::from_utf8_lossy(&launcher.stdout)
        .trim()
        .parse()
        .expect("sh should print the background pid");
    let orphan = Orphan(pid);

    // SAFETY: signal 0 delivers nothing and takes no pointer arguments.
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        0,
        "the orphan should be alive before adoption"
    );

    let pid_u32 = u32::try_from(pid).expect("a pid should fit in u32");

    // `Child` is `!Send`, so it is built and used entirely on the runtime
    // thread; only the observations come back out.
    let (try_wait_error, kill_error, alive_after_kill, wait_error, elapsed) =
        block_on(move || async move {
            let mut child = Child::from_pid(pid_u32).expect("a live non-child still adopts");
            let try_wait_error = child
                .try_wait()
                .expect_err("try_wait on a non-child should fail");
            let kill_error = child.kill().expect_err("kill on a non-child should fail");
            // SAFETY: signal 0 delivers nothing and takes no pointer arguments.
            let alive_after_kill = unsafe { libc::kill(pid, 0) } == 0;

            let start = std::time::Instant::now();
            let wait_error = child.wait().await.expect_err("wait on a non-child fails");
            (
                try_wait_error,
                kill_error,
                alive_after_kill,
                wait_error,
                start.elapsed(),
            )
        });

    assert_eq!(try_wait_error.raw_os_error(), Some(libc::ECHILD));
    assert_eq!(kill_error.raw_os_error(), Some(libc::ECHILD));
    assert!(
        alive_after_kill,
        "kill returned ECHILD without ever signalling, so the orphan is alive"
    );
    assert_eq!(wait_error.raw_os_error(), Some(libc::ECHILD));
    assert!(
        elapsed < Duration::from_secs(5),
        "wait should fail at once rather than watch for the exit, took {elapsed:?}"
    );

    drop(orphan);
}

/// `from_pid`'s `# Errors` promises `InvalidInput` on Unix for a `pid` that is
/// not a process identifier at all, separately from the OS error a lookup
/// failure produces. Only the latter is worth retrying or logging as "gone", so
/// a caller that distinguishes them needs the boundary to stay where the doc
/// puts it.
#[cfg(unix)]
#[test]
fn an_out_of_range_pid_is_rejected_as_invalid_input() {
    // The `raw_os_error` assertions are the load-bearing ones. Both of these
    // values also make the kernel answer `EINVAL`, which is itself
    // `InvalidInput` — so checking the kind alone would pass just as well with
    // the validation deleted. Absence of an OS error is what shows the argument
    // was rejected before any syscall, which is what the doc claims.
    let zero = Child::from_pid(0).expect_err("zero is not a process identifier");
    assert_eq!(zero.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        zero.raw_os_error().is_none(),
        "validation happens before any syscall, so there is no OS error to carry: {zero:?}"
    );

    // `pid_t` is 32-bit signed on every Unix runite builds for, so this is
    // beyond it while still fitting the `u32` parameter.
    let huge = Child::from_pid(u32::MAX).expect_err("u32::MAX exceeds pid_t");
    assert_eq!(huge.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        huge.raw_os_error().is_none(),
        "a value beyond `pid_t` never reaches the kernel: {huge:?}"
    );
}

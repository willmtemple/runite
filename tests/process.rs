//! End-to-end tests for the public `runite::process` API.

mod common;

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::block_on;
use runite::io::{AsyncReadExt, AsyncWriteExt};
use runite::process::{Command, Stdio};
use runite::time;

fn artifact_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos();
    let dir = std::env::current_dir()
        .expect("test should run from the repository")
        .join("target")
        .join("runite-process-tests")
        .join(format!("{label}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test artifact directory should be created");
    dir
}

/// A child that fills the stderr pipe buffer before writing stdout must not
/// deadlock `Command::output`, which captures stdout. Without concurrently
/// draining the caller-piped stderr, the child blocks writing stderr while the
/// runtime waits to read stdout, deadlocking the event loop.
#[test]
fn output_drains_piped_stderr_without_deadlock() {
    let result = block_on(|| async {
        // Write 200 KiB to stderr (well past the OS pipe buffer), then "done"
        // to stdout.
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("head -c 200000 /dev/zero >&2; printf done")
            .stderr(Stdio::piped());
        time::timeout(Duration::from_secs(10), command.output()).await
    });

    let output = result
        .expect("output must not deadlock when stderr is piped")
        .expect("command should succeed");
    assert_eq!(output.stdout, b"done");
    // The 200KB written to stderr is captured, proving concurrent draining.
    assert_eq!(output.stderr.len(), 200_000);
}

#[test]
fn command_builder_applies_args_env_and_current_dir() {
    let work_dir = artifact_dir("current-dir");
    let canonical_dir = work_dir
        .canonicalize()
        .expect("artifact directory should be canonical");
    // MSYS `sh` reports a Unix-style `pwd` by default; `pwd -W` prints the
    // Windows drive path with forward slashes. Normalize the canonicalized
    // expectation the same way (dropping the `\\?\` verbatim prefix).
    #[cfg(windows)]
    let (pwd_command, expected_dir) = (
        "$(pwd -W)",
        canonical_dir
            .display()
            .to_string()
            .trim_start_matches(r"\\?\")
            .replace('\\', "/"),
    );
    #[cfg(unix)]
    let (pwd_command, expected_dir) = ("$(pwd)", canonical_dir.display().to_string());
    let path = std::env::var_os("PATH").expect("PATH should be available for PATH-based programs");

    let output = block_on(move || async move {
        let mut command = Command::new("sh");
        command
            .env_clear()
            .env("PATH", path)
            .env("RUNITE_PROCESS_VAR", "visible")
            .envs([("RUNITE_PROCESS_REMOVED", "removed")])
            .env_remove("RUNITE_PROCESS_REMOVED")
            .current_dir(work_dir)
            .args([
                "-c",
                &format!(
                    "printf '%s|%s|%s|%s|%s' \"$1\" \"$2\" \"${{RUNITE_PROCESS_VAR-unset}}\" \"${{RUNITE_PROCESS_REMOVED-unset}}\" \"{pwd_command}\""
                ),
                "runite-sh",
                "first",
                "second",
            ]);
        command.output().await
    })
    .expect("shell command should succeed");

    assert_eq!(
        String::from_utf8(output.stdout).expect("output should be UTF-8"),
        format!("first|second|visible|unset|{expected_dir}")
    );
}

#[test]
fn command_output_reports_success_bytes_and_nonzero_errors() {
    let (echo, true_status, false_status) = block_on(|| async {
        let echo = Command::new("echo")
            .arg("hello")
            .output()
            .await
            .expect("echo output should succeed");
        let true_status = Command::new("true")
            .status()
            .await
            .expect("true status should succeed");
        // A non-zero exit is reported via `output.status`, not as an error.
        let false_output = Command::new("false")
            .output()
            .await
            .expect("false output should not be an error");
        (
            echo,
            (true_status.success(), true_status.code()),
            (false_output.status.success(), false_output.status.code()),
        )
    });

    assert_eq!(echo.stdout, b"hello\n");
    assert!(echo.status.success());
    assert_eq!(true_status, (true, Some(0)));
    assert_eq!(false_status, (false, Some(1)));
}

#[test]
fn stdio_null_piped_and_inherit_configurations_are_observable() {
    let (null_stdin_output, null_output_status, inherited_handles_none) = block_on(|| async {
        let mut cat = Command::new("cat")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .expect("cat should spawn with null stdin");
        let mut null_stdin_output = Vec::new();
        cat.stdout
            .as_mut()
            .expect("stdout should be piped")
            .read_to_end(&mut null_stdin_output)
            .await
            .expect("cat stdout should read");
        assert!(cat.wait().await.expect("cat should wait").success());

        let null_output_status = Command::new("sh")
            .args(["-c", "printf hidden; printf diagnostic >&2"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .expect("shell with null output should run")
            .success();

        let mut inherited = Command::new("true")
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("true should spawn with inherited stdio");
        let inherited_handles_none = (
            inherited.stdin.is_none(),
            inherited.stdout.is_none(),
            inherited.stderr.is_none(),
        );
        assert!(
            inherited
                .wait()
                .await
                .expect("inherited true should wait")
                .success()
        );

        (
            null_stdin_output,
            null_output_status,
            inherited_handles_none,
        )
    });

    assert!(null_stdin_output.is_empty());
    assert!(null_output_status);
    assert_eq!(inherited_handles_none, (true, true, true));
}

#[test]
fn child_pipes_round_trip_stdout_and_stderr_after_stdin_close() {
    let (id_present, stdout, stderr, status) = block_on(|| async {
        let mut child = Command::new("sh")
            .args(["-c", "cat; printf 'err-bytes' >&2"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("shell should spawn with all pipes");

        let id_present = child.id().is_some();
        let mut stdin = child.stdin.take().expect("stdin should be piped");
        let mut stdout_pipe = child.stdout.take().expect("stdout should be piped");
        let mut stderr_pipe = child.stderr.take().expect("stderr should be piped");
        assert!(child.stdin.is_none());
        assert!(child.stdout.is_none());
        assert!(child.stderr.is_none());

        let first_write = stdin.write(b"alpha ").await.expect("first write");
        assert!(first_write > 0);
        stdin
            .write_all(b"beta")
            .await
            .expect("second write should complete");
        stdin.close().await.expect("stdin close should signal EOF");

        let mut stdout = Vec::new();
        stdout_pipe
            .read_to_end(&mut stdout)
            .await
            .expect("stdout should read to EOF");
        let mut stderr = Vec::new();
        stderr_pipe
            .read_to_end(&mut stderr)
            .await
            .expect("stderr should read to EOF");
        let status = child.wait().await.expect("child should wait");

        (
            id_present,
            stdout,
            stderr,
            (status.success(), status.code()),
        )
    });

    assert!(id_present);
    assert_eq!(stdout, b"alpha beta");
    assert_eq!(stderr, b"err-bytes");
    assert_eq!(status, (true, Some(0)));
}

#[test]
fn child_wait_kill_id_and_drop_paths_are_short_lived() {
    let (normal_id, normal_status, killed_status, dropped_id) = block_on(|| async {
        let mut normal = Command::new("true").spawn().expect("true should spawn");
        let normal_id = normal.id();
        let normal_status = normal.wait().await.expect("true should wait");

        let mut child = Command::new("cat")
            .stdin(Stdio::piped())
            .spawn()
            .expect("cat should spawn and wait for stdin");
        child.kill().expect("cat should be killed");
        let killed_status = child.wait().await.expect("killed cat should wait");

        let dropped = Command::new("true")
            .spawn()
            .expect("short-lived child should spawn");
        let dropped_id = dropped.id();
        drop(dropped);

        (
            normal_id,
            (normal_status.success(), normal_status.code()),
            (
                killed_status.success(),
                killed_status.code(),
                #[cfg(unix)]
                killed_status.signal(),
            ),
            dropped_id,
        )
    });

    assert!(normal_id.is_some());
    assert_eq!(normal_status, (true, Some(0)));
    assert!(!killed_status.0);
    #[cfg(unix)]
    assert_eq!(killed_status.2, Some(libc::SIGKILL));
    assert!(dropped_id.is_some());
}

#[test]
fn exit_status_accessors_report_success_failure_and_signal() {
    let statuses = block_on(|| async {
        let true_status = Command::new("true")
            .status()
            .await
            .expect("true should run");
        let false_status = Command::new("false")
            .status()
            .await
            .expect("false should run");
        let mut killed = Command::new("cat")
            .stdin(Stdio::piped())
            .spawn()
            .expect("cat should spawn");
        killed.kill().expect("cat should be killed");
        let killed_status = killed.wait().await.expect("killed cat should wait");

        (
            (true_status.success(), true_status.code()),
            (false_status.success(), false_status.code()),
            (
                killed_status.success(),
                killed_status.code(),
                #[cfg(unix)]
                killed_status.signal(),
            ),
        )
    });

    assert_eq!(statuses.0, (true, Some(0)));
    assert_eq!(statuses.1, (false, Some(1)));
    assert!(!statuses.2.0);
    // A killed child reports no exit code on Unix (it died to a signal);
    // Windows `TerminateProcess` sets exit code 1.
    #[cfg(unix)]
    assert_eq!(statuses.2.1, None);
    #[cfg(windows)]
    assert_eq!(statuses.2.1, Some(1));
    #[cfg(unix)]
    assert_eq!(statuses.2.2, Some(libc::SIGKILL));
}

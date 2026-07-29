//! End-to-end tests for the public `runite::process` API.

mod common;

use std::future::{Future, poll_fn};
use std::io::{BufRead, BufReader, Read as _, Write as _};
use std::path::PathBuf;
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::block_on;
use runite::io::{AsyncReadExt, AsyncWriteExt};
use runite::process::{Command, Stdio};
use runite::time;

const STDIN_HANDOFF_HELPER: &str = "RUNITE_STDIN_HANDOFF_HELPER";
const STDIN_HANDOFF_READY: &str = "RUNITE_STDIN_HANDOFF_READY";
const STDIN_HANDOFF_RESULT: &str = "RUNITE_STDIN_HANDOFF_RESULT:";

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
fn inherited_child_stdin_is_handed_off_without_reader_theft() {
    if let Ok(mode) = std::env::var(STDIN_HANDOFF_HELPER) {
        run_stdin_handoff_helper(&mode);
        return;
    }

    for mode in ["idle", "pending", "late-init"] {
        let executable = std::env::current_exe().expect("resolve process test executable");
        let mut helper = std::process::Command::new(executable)
            .args([
                "--exact",
                "inherited_child_stdin_is_handed_off_without_reader_theft",
                "--nocapture",
            ])
            .env(STDIN_HANDOFF_HELPER, mode)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("spawn stdin handoff helper");
        let stdout = helper.stdout.take().expect("helper stdout pipe");
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let (continue_sender, continue_receiver) = std::sync::mpsc::sync_channel(1);
        let output_reader = std::thread::spawn(move || {
            let mut stdout = BufReader::new(stdout);
            let mut output = String::new();
            loop {
                let mut line = String::new();
                let read = stdout.read_line(&mut line)?;
                if read == 0 {
                    let _ = ready_sender.send(Err(output.clone()));
                    return Ok::<_, std::io::Error>(output);
                }
                output.push_str(&line);
                if line.contains(STDIN_HANDOFF_READY) {
                    let _ = ready_sender.send(Ok(()));
                    break;
                }
            }
            let _ = continue_receiver.recv();
            stdout.read_to_string(&mut output)?;
            Ok(output)
        });

        match ready_receiver.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => {}
            Ok(Err(output)) => {
                let _ = helper.kill();
                drop(helper.stdin.take());
                let _ = continue_sender.send(());
                let _ = helper.wait();
                let _ = output_reader.join();
                panic!("helper exited before readiness in {mode} mode:\n{output}");
            }
            Err(error) => {
                let _ = helper.kill();
                drop(helper.stdin.take());
                let _ = continue_sender.send(());
                let _ = helper.wait();
                let _ = output_reader.join();
                panic!("helper readiness timed out in {mode} mode: {error}");
            }
        }

        let payload = format!("handoff-{mode}\n");
        let mut stdin = helper.stdin.take().expect("helper stdin pipe");
        stdin
            .write_all(payload.as_bytes())
            .expect("write inherited stdin payload");
        drop(stdin);
        continue_sender
            .send(())
            .expect("release helper output reader");
        let status = helper.wait().expect("wait for stdin handoff helper");
        let output = output_reader
            .join()
            .expect("join helper output reader")
            .expect("read helper output");
        assert!(status.success(), "helper failed in {mode} mode:\n{output}");
        assert!(
            output.contains(&format!("{STDIN_HANDOFF_RESULT}{payload}")),
            "inherited child did not receive exact stdin in {mode} mode:\n{output}"
        );
    }
}

fn run_stdin_handoff_helper(mode: &str) {
    let output = match mode {
        "idle" => block_on(|| async {
            let _stdin = runite::stdin().expect("initialize process stdin reader");
            read_inherited_child_stdin().await
        }),
        "pending" => block_on(|| async {
            let mut stdin = runite::stdin().expect("initialize process stdin reader");
            let mut byte = [0u8; 1];
            let mut pending = Box::pin(stdin.read(&mut byte));
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let output = read_inherited_child_stdin().await;
            drop(pending);
            output
        }),
        "late-init" => block_on(|| async {
            let mut child = spawn_inherited_stdin_child();
            let mut stdin = runite::stdin().expect("initialize reader during handoff");
            let mut byte = [0u8; 1];
            let mut pending = Box::pin(stdin.read(&mut byte));
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            announce_stdin_handoff_ready();
            let output =
                time::timeout(Duration::from_secs(10), collect_inherited_stdin(&mut child))
                    .await
                    .expect("late-init inherited child timed out");
            drop(pending);
            output
        }),
        other => panic!("unknown stdin handoff helper mode: {other}"),
    };

    print!("{STDIN_HANDOFF_RESULT}{}", String::from_utf8_lossy(&output));
    std::io::stdout()
        .flush()
        .expect("flush stdin handoff result");
}

async fn read_inherited_child_stdin() -> Vec<u8> {
    let mut child = spawn_inherited_stdin_child();
    announce_stdin_handoff_ready();
    time::timeout(Duration::from_secs(10), collect_inherited_stdin(&mut child))
        .await
        .expect("inherited child timed out")
}

fn spawn_inherited_stdin_child() -> runite::process::Child {
    Command::new("cat")
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn inherited-stdin child")
}

fn announce_stdin_handoff_ready() {
    println!("{STDIN_HANDOFF_READY}");
    std::io::stdout()
        .flush()
        .expect("flush stdin handoff readiness");
}

async fn collect_inherited_stdin(child: &mut runite::process::Child) -> Vec<u8> {
    let mut output = Vec::new();
    child
        .stdout
        .as_mut()
        .expect("child stdout pipe")
        .read_to_end(&mut output)
        .await
        .expect("read inherited child output");
    assert!(
        child
            .wait()
            .await
            .expect("wait inherited-stdin child")
            .success()
    );
    output
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

/// Dropping `ChildStdin` closes it immediately, abandoning any pending write.
///
/// This is the documented escape from `close().await`, which drains first and
/// therefore cannot complete while a child is waiting for end of input before
/// reading again. The child here reads nothing until EOF, so only a close that
/// does *not* drain lets it proceed.
#[test]
fn dropping_child_stdin_closes_without_draining() {
    let status = block_on(|| async {
        // `cat` with stdin closed exits; the point is that it observes EOF at
        // all, which a draining close would not deliver here.
        let mut child = Command::new("sh")
            .arg("-c")
            // Sleep first so the parent's write is still in flight, then read
            // to end: the child only finishes once it sees EOF.
            .arg("sleep 0.2; cat >/dev/null")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .expect("child should spawn");

        let mut stdin = child.stdin.take().expect("stdin should be piped");
        stdin
            .write_all(b"payload")
            .await
            .expect("write should land");

        // Drop rather than `close().await`.
        drop(stdin);

        time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("dropping stdin should deliver EOF and let the child exit")
            .expect("child should be waitable")
    });

    assert!(status.success(), "the child should exit cleanly after EOF");
}

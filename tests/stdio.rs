//! End-to-end tests for the public `runite::stdio` API.
//!
//! These drive a real child process with a real inherited stdin pipe, because
//! the properties under test are about the interaction between the process-wide
//! stdin reader and the event loop's idle-shutdown probe. Both are invisible to
//! an in-process test that uses `block_on`, which has no quiescence probe.

use std::io::{BufRead, BufReader, Write as _};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const STDIN_LIVENESS_HELPER: &str = "RUNITE_STDIN_LIVENESS_HELPER";
const READY: &str = "RUNITE_STDIN_LIVENESS_READY";
const RESULT: &str = "RUNITE_STDIN_LIVENESS_RESULT:";

/// How long the parent waits before feeding the child, to prove `run()` stayed
/// alive rather than happening to observe input that was already buffered.
const FEED_DELAY: Duration = Duration::from_millis(750);

fn helper_command(mode: &str) -> Command {
    let executable = std::env::current_exe().expect("resolve stdio test executable");
    let mut command = Command::new(executable);
    command
        .args(["--exact", "stdin_read_keeps_run_alive", "--nocapture"])
        .env(STDIN_LIVENESS_HELPER, mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    command
}

/// Reads one byte of stdin from a spawned task and drives the loop with `run()`
/// — the exact pattern the `runite::stdio` module documentation teaches.
fn run_await_helper() {
    let handle = runite::spawn(async {
        let mut input = runite::stdin().expect("stdin should open");
        let mut byte = [0u8; 1];
        let read = runite::io::AsyncReadExt::read(&mut input, &mut byte)
            .await
            .expect("stdin read should succeed");
        (read, byte[0])
    });

    println!("{READY}");
    std::io::stdout().flush().expect("flush readiness marker");

    let started = Instant::now();
    runite::run();
    let elapsed = started.elapsed();

    let outcome = match runite::block_on(handle) {
        Ok((read, byte)) => format!("ok:{read}:{}", byte as char),
        Err(error) if error.is_cancelled() => "cancelled".to_string(),
        Err(error) => format!("error:{error:?}"),
    };
    println!("{RESULT}{outcome}:{}", elapsed.as_millis());
    std::io::stdout().flush().expect("flush result marker");
}

/// A stdin read that is cancelled before any input arrives must release its
/// runtime-liveness reference, so `run()` can still reach quiescence. Without
/// that release the loop would park forever on a reader nobody is waiting for.
fn run_cancel_helper() {
    runite::spawn(async {
        let mut input = runite::stdin().expect("stdin should open");
        let mut byte = [0u8; 1];
        let read = runite::io::AsyncReadExt::read(&mut input, &mut byte);
        // Times out and drops the read future while it is still pending.
        let _ = runite::time::timeout(Duration::from_millis(50), read).await;
    });

    println!("{READY}");
    std::io::stdout().flush().expect("flush readiness marker");

    let started = Instant::now();
    runite::run();
    println!("{RESULT}returned:{}", started.elapsed().as_millis());
    std::io::stdout().flush().expect("flush result marker");
}

/// Reads the helper's readiness marker, then its result line.
fn drive_helper(mode: &str, feed: Option<&[u8]>) -> String {
    let mut helper = helper_command(mode).spawn().expect("spawn stdin helper");
    let mut stdin = helper.stdin.take().expect("helper stdin pipe");
    let mut stdout = BufReader::new(helper.stdout.take().expect("helper stdout pipe"));

    let mut line = String::new();
    loop {
        line.clear();
        let read = stdout.read_line(&mut line).expect("read helper output");
        assert!(read != 0, "helper exited before signalling readiness");
        if line.contains(READY) {
            break;
        }
    }

    if let Some(bytes) = feed {
        // The helper is inside `run()` with nothing buffered. Waiting here is
        // what makes this a real test: an early `run()` return cancels the task
        // well before these bytes are written.
        std::thread::sleep(FEED_DELAY);
        // A helper that already gave up has closed this pipe. Tolerate the
        // broken pipe so the assertion below reports the real outcome
        // ("cancelled") instead of failing here with an I/O error.
        let _ = stdin.write_all(bytes).and_then(|()| stdin.flush());
    }

    let mut result = None;
    loop {
        line.clear();
        let read = stdout.read_line(&mut line).expect("read helper output");
        if read == 0 {
            break;
        }
        if let Some(rest) = line.trim_end().strip_prefix(RESULT) {
            result = Some(rest.to_string());
            break;
        }
    }

    // Release the child's stdin so it can exit even if it is still reading.
    drop(stdin);
    let _ = helper.wait();
    result.expect("helper should report a result")
}

/// A task awaiting stdin must keep `run()` alive. The process-wide reader
/// thread is not a scheduler-visible wake source, so without explicit liveness
/// accounting the loop reaches quiescence immediately and terminalizes the task
/// with `JoinError::Cancelled` while input is still on its way.
#[test]
fn stdin_read_keeps_run_alive() {
    match std::env::var(STDIN_LIVENESS_HELPER).as_deref() {
        Ok("await") => return run_await_helper(),
        Ok("cancel") => return run_cancel_helper(),
        Ok(other) => panic!("unknown stdin liveness helper mode: {other}"),
        Err(_) => {}
    }

    let awaited = drive_helper("await", Some(b"hello\n"));
    let (outcome, elapsed) = awaited
        .rsplit_once(':')
        .expect("result should carry an elapsed measurement");
    assert_eq!(
        outcome, "ok:1:h",
        "the spawned task must observe stdin rather than being cancelled"
    );
    let elapsed: u128 = elapsed.parse().expect("elapsed should be numeric");
    assert!(
        elapsed >= FEED_DELAY.as_millis() / 2,
        "run() returned after {elapsed}ms, so it did not wait for stdin"
    );

    let cancelled = drive_helper("cancel", None);
    let (outcome, _) = cancelled
        .rsplit_once(':')
        .expect("result should carry an elapsed measurement");
    assert_eq!(
        outcome, "returned",
        "a cancelled stdin read must release liveness so run() can finish"
    );
}

//! End-to-end tests for the public `runite::process` API.

mod common;

use std::time::Duration;

use common::block_on;
use runite::process::{Command, Stdio};
use runite::time;

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

    let bytes = result
        .expect("output must not deadlock when stderr is piped")
        .expect("command should succeed");
    assert_eq!(bytes, b"done");
}

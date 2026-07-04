//! Async subprocess pipeline demo.
//!
//! This example starts `cat` (resolved from `PATH`; on Windows this expects a
//! Git-style `cat.exe`) with piped stdin/stdout, writes a few lines to the
//! child, closes stdin to signal EOF, then reads the echoed output line by
//! line through `runite::io::BufReader` before awaiting the child status.

use runite::io::{AsyncWriteExt, BufReader};
use runite::process::{Command, Stdio};

#[runite::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().expect("stdin should be piped");
    stdin.write_all(b"alpha\nbeta\ngamma\n").await?;
    stdin.close().await?;

    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut reader = BufReader::new(stdout);
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        print!("[child stdout] {line}");
        lines.push(line);
    }

    let status = child.wait().await?;
    assert!(status.success(), "cat should exit successfully");
    assert_eq!(lines, ["alpha\n", "beta\n", "gamma\n"]);
    println!("[parent] child exited with status {status:?}");
    Ok(())
}

//! Async subprocess management.
//!
//! This module provides a portable, runtime-aware equivalent of
//! [`std::process`]. Use [`Command`] to configure and spawn a subprocess,
//! [`Child`] to wait for or terminate it, [`Stdio`] to choose how standard
//! streams are wired, and the child pipe types to read and write those streams
//! without blocking the runtime thread.
//!
//! The API is designed to be platform-neutral even though each backend uses the
//! host operating system's process facilities. Examples that start external
//! programs are marked `no_run` because available commands and paths vary across
//! environments.
//!
//! # Examples
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use runite::io::{AsyncReadExt, AsyncWriteExt};
//! use runite::process::{Command, Stdio};
//!
//! let mut child = Command::new("/bin/cat")
//!     .stdin(Stdio::piped())
//!     .stdout(Stdio::piped())
//!     .spawn()?;
//!
//! let mut stdin = child.stdin.take().expect("stdin should be piped");
//! stdin.write_all(b"hello\n").await?;
//! stdin.close().await?;
//!
//! let mut output = Vec::new();
//! child
//!     .stdout
//!     .as_mut()
//!     .expect("stdout should be piped")
//!     .read_to_end(&mut output)
//!     .await?;
//! assert_eq!(output, b"hello\n");
//! assert!(child.wait().await?.success());
//! # Ok(())
//! # }
//! ```
//!
//! The same pattern can be driven by the event loop in a doctest or binary:
//!
//! ```no_run
//! use runite::process::Command;
//!
//! runite::queue_future(async {
//!     let status = Command::new("/bin/echo")
//!         .arg("ready")
//!         .status()
//!         .await
//!         .expect("subprocess should run");
//!     assert!(status.success());
//! });
//!
//! runite::run();
//! ```

mod child;
mod command;
pub(crate) mod pipe;
mod status;

pub use child::Child;
pub use command::{Command, Stdio};
pub use pipe::{ChildStderr, ChildStdin, ChildStdout};
pub use status::ExitStatus;

pub(crate) use command::{CommandSpec, EnvChange, StdioKind};

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::io::{AsyncReadExt, AsyncWriteExt};
    use crate::{queue_future, queue_task, run};

    use super::{Command, Stdio};

    #[test]
    fn true_and_false_report_exit_codes() {
        let observed = Arc::new(Mutex::new(None::<(Option<i32>, Option<i32>)>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            queue_future(async move {
                let true_status = Command::new("true")
                    .status()
                    .await
                    .expect("true should run");
                let false_status = Command::new("false")
                    .status()
                    .await
                    .expect("false should run");
                *observed_for_task.lock().unwrap() =
                    Some((true_status.code(), false_status.code()));
            });
        });

        run();
        assert_eq!(*observed.lock().unwrap(), Some((Some(0), Some(1))));
    }

    #[test]
    fn piped_stdout_reads_echo_output() {
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            queue_future(async move {
                let mut child = Command::new("/bin/echo")
                    .arg("hello")
                    .stdout(Stdio::piped())
                    .spawn()
                    .expect("echo should spawn");
                let mut output = Vec::new();
                child
                    .stdout
                    .as_mut()
                    .expect("stdout should be piped")
                    .read_to_end(&mut output)
                    .await
                    .expect("stdout should read");
                assert!(child.wait().await.expect("echo should wait").success());
                *observed_for_task.lock().unwrap() = Some(output);
            });
        });

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"hello\n".as_slice())
        );
    }

    #[test]
    fn piped_stdin_writes_to_cat_stdout() {
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            queue_future(async move {
                let mut child = Command::new("/bin/cat")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .expect("cat should spawn");
                let mut stdin = child.stdin.take().expect("stdin should be piped");
                stdin
                    .write_all(b"round trip bytes")
                    .await
                    .expect("stdin write should succeed");
                stdin.close().await.expect("stdin close should succeed");

                let mut output = Vec::new();
                child
                    .stdout
                    .as_mut()
                    .expect("stdout should be piped")
                    .read_to_end(&mut output)
                    .await
                    .expect("stdout should read");
                assert!(child.wait().await.expect("cat should wait").success());
                *observed_for_task.lock().unwrap() = Some(output);
            });
        });

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"round trip bytes".as_slice())
        );
    }

    #[test]
    fn kill_reports_signal_status() {
        let observed = Arc::new(Mutex::new(None::<Option<i32>>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            queue_future(async move {
                let mut child = Command::new("/bin/sleep")
                    .arg("100")
                    .spawn()
                    .expect("sleep should spawn");
                child.kill().expect("sleep should be killed");
                let status = child.wait().await.expect("killed sleep should wait");
                #[cfg(unix)]
                {
                    *observed_for_task.lock().unwrap() = Some(status.signal());
                }
            });
        });

        run();
        #[cfg(unix)]
        assert_eq!(*observed.lock().unwrap(), Some(Some(libc::SIGKILL)));
    }
}

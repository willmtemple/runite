//! Async pipe endpoints for child standard streams.
//!
//! These handle types are created when a [`Command`](super::Command) configures
//! a standard stream with [`Stdio::piped`](super::Stdio::piped). They implement
//! the runtime's async I/O traits so subprocess input and output can be composed
//! with other tasks without blocking the event loop.
//!
//! Child pipes are set nonblocking when the process is spawned. Reads and writes
//! retry the OS call after one-shot fd readiness (`io_uring` poll on Linux,
//! kqueue on macOS aarch64); they do not use the blocking thread pool.
//! Like other runite handles, pipe futures should be polled on their creating
//! runtime thread.
//!
//! # Examples
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use runite::io::{AsyncReadExt, AsyncWriteExt};
//! use runite::process::{Command, Stdio};
//!
//! let mut child = Command::new("cat")
//!     .stdin(Stdio::piped())
//!     .stdout(Stdio::piped())
//!     .spawn()?;
//!
//! let mut stdin = child.stdin.take().expect("stdin should be piped");
//! stdin.write_all(b"ping").await?;
//! stdin.close().await?;
//!
//! let mut stdout = Vec::new();
//! child
//!     .stdout
//!     .as_mut()
//!     .expect("stdout should be piped")
//!     .read_to_end(&mut stdout)
//!     .await?;
//! assert_eq!(stdout, b"ping");
//! # Ok(())
//! # }
//! ```
//!
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io::{self, IoSlice};

use crate::io::{AsyncRead, AsyncWrite, ReadState, WriteState};
use crate::sys::handle::{OwnedFile, RawFile, raw_file};

#[derive(Debug)]
pub(crate) struct Pipe {
    fd: Option<OwnedFile>,
}

impl Pipe {
    pub(crate) fn new(fd: OwnedFile) -> Self {
        Self { fd: Some(fd) }
    }

    fn raw_fd(&self) -> io::Result<RawFile> {
        self.fd
            .as_ref()
            .map(raw_file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "pipe is closed"))
    }

    fn close(&mut self) {
        self.fd = None;
    }
}

/// Async writer connected to a child process's standard input.
///
/// Created when [`Command::stdin`](super::Command::stdin) is configured with
/// [`Stdio::piped`](super::Stdio::piped). Closing or dropping this handle closes
/// the child's stdin pipe and can signal EOF to the child.
///
/// `poll_close` waits for an abandoned in-flight write to complete before
/// closing the descriptor, preserving operation ordering. Await
/// [`write_all`](crate::io::AsyncWriteExt::write_all) before `close` when the
/// child must receive the whole buffer.
///
/// # Closing can deadlock against a child that is waiting for EOF
///
/// Because `close` drains first, it cannot complete while a write cannot. If a
/// write was cancelled with the pipe buffer full, and the child will not read
/// again until it sees end of input, the three parties wait on each other: the
/// close waits for the write, the write waits for the child to drain the pipe,
/// and the child waits for the close.
///
/// This is inherent to "close flushes what you already wrote" and is not
/// resolvable by a timeout — a bounded drain would replace a visible hang with
/// silent truncation, and the caller could not tell whether the child received
/// the bytes.
///
/// **Dropping the handle is the escape.** Drop closes the descriptor
/// immediately and abandons the pending write, so the child observes end of
/// input and can proceed. Use it when the whole buffer reaching the child is
/// not required:
///
/// ```no_run
/// # async fn example(mut child: runite::process::Child) -> std::io::Result<()> {
/// use runite::io::AsyncWriteExt;
///
/// let mut stdin = child.stdin.take().expect("stdin should be piped");
/// stdin.write_all(b"input").await?;
/// // `close().await` drains first, and can block on a child waiting for EOF.
/// // Dropping closes now.
/// drop(stdin);
/// child.wait().await?;
/// # Ok(())
/// # }
/// ```
pub struct ChildStdin {
    // Pending writes must be dropped before the pipe descriptor.
    write_state: WriteState,
    pipe: Pipe,
}

/// Async reader connected to a child process's standard output.
///
/// Created when [`Command::stdout`](super::Command::stdout) is configured with
/// [`Stdio::piped`](super::Stdio::piped). It implements [`AsyncRead`] for
/// consuming bytes produced by the child using nonblocking fd readiness.
pub struct ChildStdout {
    // Pending reads must be dropped before the pipe descriptor.
    read_state: ReadState,
    pipe: Pipe,
}

/// Async reader connected to a child process's standard error.
///
/// Created when [`Command::stderr`](super::Command::stderr) is configured with
/// [`Stdio::piped`](super::Stdio::piped). It implements [`AsyncRead`] for
/// consuming diagnostic bytes produced by the child using nonblocking fd
/// readiness.
pub struct ChildStderr {
    // Pending reads must be dropped before the pipe descriptor.
    read_state: ReadState,
    pipe: Pipe,
}

impl ChildStdin {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            write_state: WriteState::default(),
            pipe,
        }
    }
}

impl ChildStdout {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            read_state: ReadState::default(),
            pipe,
        }
    }
}

impl ChildStderr {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            read_state: ReadState::default(),
            pipe,
        }
    }
}

impl AsyncWrite for ChildStdin {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_operation(cx, buf, 0)
    }

    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();
        let fd = match this.pipe.raw_fd() {
            Ok(fd) => fd,
            Err(error) => return Poll::Ready(Err(error)),
        };
        this.write_state
            .poll_write(cx, generation, buf, move |data| {
                crate::sys::current::process::write_pipe_future(fd, data)
            })
    }

    fn poll_write_vectored_operation(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        match bufs.iter().find(|buf| !buf.is_empty()) {
            Some(buf) => self.as_mut().poll_write_operation(cx, buf, generation),
            None => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // An abandoned write stays owned by the pipe, so returning `Ok`
        // unconditionally would report bytes as visible while they are still in
        // flight and would swallow that operation's error.
        self.get_mut().write_state.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_state.poll_drain(cx).is_pending() {
            return Poll::Pending;
        }
        this.pipe.close();
        Poll::Ready(Ok(()))
    }
}

macro_rules! impl_async_read {
    ($ty:ty) => {
        impl AsyncRead for $ty {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                if buf.is_empty() {
                    return Poll::Ready(Ok(0));
                }

                let this = self.get_mut();

                let fd = match this.pipe.raw_fd() {
                    Ok(fd) => fd,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                this.read_state.poll_slice(cx, buf, move |len| {
                    crate::sys::current::process::read_pipe_future(fd, len)
                })
            }
        }
    };
}

impl_async_read!(ChildStdout);
impl_async_read!(ChildStderr);

#[cfg(all(test, unix))]
mod tests {
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::future::poll_fn;
    use std::io;
    use std::sync::{Arc, Mutex};

    use super::{ChildStdin, Pipe};
    use crate::io::AsyncWrite;
    use crate::sys::handle::OwnedFile;

    struct PendingOnce<T> {
        pending: bool,
        result: Option<io::Result<T>>,
    }

    impl<T> PendingOnce<T> {
        fn new(result: io::Result<T>) -> Self {
            Self {
                pending: true,
                result: Some(result),
            }
        }
    }

    impl<T: Unpin> Future for PendingOnce<T> {
        type Output = io::Result<T>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.pending {
                self.pending = false;
                Poll::Pending
            } else {
                Poll::Ready(self.result.take().expect("polled after completion"))
            }
        }
    }

    #[test]
    fn child_stdin_does_not_reuse_an_abandoned_write_count() {
        let path = std::env::current_dir()
            .expect("current directory")
            .join("target")
            .join(format!("child-pipe-pending-write-{}", std::process::id()));
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            let path = path.clone();
            crate::spawn(async move {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("open fixture");
                let fd: OwnedFile = file.into();
                let mut stdin = ChildStdin::from_pipe(Pipe::new(fd));
                let old = b"old".to_vec();
                let old_generation = crate::io::next_operation_id();

                poll_fn(|cx| {
                    assert!(
                        stdin
                            .write_state
                            .poll_write(cx, old_generation, &old, |_| {
                                Box::pin(PendingOnce::new(Ok(old.len())))
                            })
                            .is_pending()
                    );
                    Poll::Ready(())
                })
                .await;

                let written = poll_fn(|cx| Pin::new(&mut stdin).poll_write(cx, b"new bytes"))
                    .await
                    .expect("new write");
                assert_eq!(written, 9);
                drop(stdin);
                *observed.lock().unwrap() = Some(std::fs::read(&path).expect("read fixture"));
                std::fs::remove_file(&path).expect("remove fixture");
            });
        }

        crate::run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"new bytes".as_slice())
        );
    }
}

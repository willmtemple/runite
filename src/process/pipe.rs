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
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use crate::io::{AsyncRead, AsyncWrite};
use crate::sys::handle::{OwnedFile, RawFile, raw_file};

type PendingRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;

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
/// `poll_close` drops any pending write state and closes the fd immediately; it
/// does not flush bytes beyond writes that have already completed. Await
/// [`write_all`](crate::io::AsyncWriteExt::write_all) before `close` when the
/// child must receive the whole buffer. EOF is visible to the child only after
/// completed writes and the close.
pub struct ChildStdin {
    pipe: Pipe,
    pending_write: Option<PendingWrite>,
}

/// Async reader connected to a child process's standard output.
///
/// Created when [`Command::stdout`](super::Command::stdout) is configured with
/// [`Stdio::piped`](super::Stdio::piped). It implements [`AsyncRead`] for
/// consuming bytes produced by the child using nonblocking fd readiness.
pub struct ChildStdout {
    pipe: Pipe,
    pending_read: Option<PendingRead>,
    read_overflow: Option<Box<crate::io::ReadOverflow>>,
}

/// Async reader connected to a child process's standard error.
///
/// Created when [`Command::stderr`](super::Command::stderr) is configured with
/// [`Stdio::piped`](super::Stdio::piped). It implements [`AsyncRead`] for
/// consuming diagnostic bytes produced by the child using nonblocking fd
/// readiness.
pub struct ChildStderr {
    pipe: Pipe,
    pending_read: Option<PendingRead>,
    read_overflow: Option<Box<crate::io::ReadOverflow>>,
}

impl ChildStdin {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            pipe,
            pending_write: None,
        }
    }
}

impl ChildStdout {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            pipe,
            pending_read: None,
            read_overflow: None,
        }
    }
}

impl ChildStderr {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            pipe,
            pending_read: None,
            read_overflow: None,
        }
    }
}

impl AsyncWrite for ChildStdin {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();
        if this.pending_write.is_none() {
            match this.pipe.raw_fd() {
                Ok(fd) => {
                    this.pending_write = Some(crate::sys::current::process::write_pipe_future(
                        fd,
                        buf.to_vec(),
                    ));
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }

        match this
            .pending_write
            .as_mut()
            .expect("pending child stdin write must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_write = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.pending_write = None;
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

                if let Some(overflow) = this.read_overflow.as_mut() {
                    let n = overflow.drain_into(buf);
                    if overflow.is_drained() {
                        this.read_overflow = None;
                    }
                    return Poll::Ready(Ok(n));
                }

                if this.pending_read.is_none() {
                    match this.pipe.raw_fd() {
                        Ok(fd) => {
                            this.pending_read = Some(
                                crate::sys::current::process::read_pipe_future(fd, buf.len()),
                            );
                        }
                        Err(error) => return Poll::Ready(Err(error)),
                    }
                }

                match this
                    .pending_read
                    .as_mut()
                    .expect("pending child pipe read must exist")
                    .as_mut()
                    .poll(cx)
                {
                    Poll::Ready(result) => {
                        this.pending_read = None;
                        let data = result?;
                        let n = data.len().min(buf.len());
                        buf[..n].copy_from_slice(&data[..n]);
                        // Retain any bytes that did not fit rather than discarding them.
                        if data.len() > n {
                            this.read_overflow =
                                Some(Box::new(crate::io::ReadOverflow::new(&data[n..])));
                        }
                        Poll::Ready(Ok(n))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    };
}

impl_async_read!(ChildStdout);
impl_async_read!(ChildStderr);

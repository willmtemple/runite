use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use crate::io::{AsyncRead, AsyncWrite};

type PendingRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;

#[derive(Debug)]
pub(crate) struct Pipe {
    fd: Option<OwnedFd>,
}

impl Pipe {
    pub(crate) fn new(fd: OwnedFd) -> Self {
        Self { fd: Some(fd) }
    }

    fn raw_fd(&self) -> io::Result<RawFd> {
        self.fd
            .as_ref()
            .map(AsRawFd::as_raw_fd)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "pipe is closed"))
    }

    fn close(&mut self) {
        self.fd = None;
    }
}

/// Async writer connected to a child process's stdin.
pub struct ChildStdin {
    pipe: Pipe,
    pending_write: Option<PendingWrite>,
}

/// Async reader connected to a child process's stdout.
pub struct ChildStdout {
    pipe: Pipe,
    pending_read: Option<PendingRead>,
}

/// Async reader connected to a child process's stderr.
pub struct ChildStderr {
    pipe: Pipe,
    pending_read: Option<PendingRead>,
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
        }
    }
}

impl ChildStderr {
    pub(crate) fn from_pipe(pipe: Pipe) -> Self {
        Self {
            pipe,
            pending_read: None,
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
                        let read = data.len();
                        if read > buf.len() {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "read completed with more bytes than destination buffer can hold",
                            )));
                        }
                        buf[..read].copy_from_slice(&data);
                        Poll::Ready(Ok(read))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    };
}

impl_async_read!(ChildStdout);
impl_async_read!(ChildStderr);

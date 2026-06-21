//! Hyper runtime trait implementations for [`TcpStream`].
//!
//! Gated behind the `hyper` Cargo feature so consumers that do not need the HTTP transport
//! integration are not forced to pull in the `hyper` dependency.

use core::pin::Pin;
use core::task::{Context, Poll};

use std::io;
use std::net::Shutdown;

use hyper::rt::{Read as HyperRead, ReadBufCursor, Write as HyperWrite};

use super::TcpStream;

impl HyperRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if this.pending_read.is_none() {
            this.pending_read = Some(match this.read_timeout_value() {
                Some(timeout) => Box::pin(crate::sys::current::net::recv_timeout(
                    this.raw_fd(),
                    buf.remaining(),
                    0,
                    timeout,
                )),
                None => crate::sys::current::net::recv_future(this.raw_fd(), buf.remaining()),
            });
        }

        let poll = this
            .pending_read
            .as_mut()
            .expect("pending read future should exist")
            .as_mut()
            .poll(cx);
        match poll {
            Poll::Ready(Ok(data)) => {
                this.pending_read = None;
                buf.put_slice(&data);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.pending_read = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl HyperWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if this.pending_write.is_none() {
            this.pending_write = Some(match this.write_timeout_value() {
                Some(timeout) => Box::pin(crate::sys::current::net::send_timeout(
                    this.raw_fd(),
                    buf.to_vec(),
                    0,
                    timeout,
                )),
                None => crate::sys::current::net::send_future(this.raw_fd(), buf.to_vec()),
            });
        }

        let poll = this
            .pending_write
            .as_mut()
            .expect("pending write future should exist")
            .as_mut()
            .poll(cx);
        match poll {
            Poll::Ready(Ok(written)) => {
                this.pending_write = None;
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => {
                this.pending_write = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.get_mut();
        if this.pending_shutdown.is_none() {
            this.pending_shutdown = Some(crate::sys::current::net::shutdown_future(
                this.raw_fd(),
                Shutdown::Write,
            ));
        }

        let poll = this
            .pending_shutdown
            .as_mut()
            .expect("pending shutdown future should exist")
            .as_mut()
            .poll(cx);
        match poll {
            Poll::Ready(Ok(())) => {
                this.pending_shutdown = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.pending_shutdown = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

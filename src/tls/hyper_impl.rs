//! Hyper transport impls for [`TlsStream`], gated on `hyper` + `rustls`.
//!
//! [`TcpStream`](crate::net::TcpStream) implements hyper's traits directly on
//! the runtime's pending-operation state. A TLS session cannot: what hyper
//! reads and writes is plaintext, which only exists on the other side of the
//! record layer. These impls therefore drive the stream's own record layer, so
//! hyper inherits the same buffering and cancellation behaviour as any other
//! caller.
//!
//! The write half is [`AsyncWrite`] verbatim. The read half is not: hyper's
//! cursor exposes uninitialized memory, which [`AsyncRead`] cannot be handed,
//! and staging through a scratch buffer would mean sizing that buffer for a TLS
//! record and paying for it on every poll of every connection. It reads out of
//! rustls's receive buffer instead.

use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::io::{self, BufRead as _};

use hyper::rt::{Read as HyperRead, ReadBufCursor, Write as HyperWrite};

use super::TlsStream;
use crate::io::{AsyncRead, AsyncWrite};

impl<S> HyperRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Reads plaintext into hyper's cursor.
    ///
    /// This does not go through [`AsyncRead`], which would need an initialized
    /// slice the cursor cannot supply. It drives the session and then copies
    /// out of rustls's own receive buffer, so the plaintext is copied once,
    /// into the caller's memory, with no scratch buffer between them.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let capacity = buf.remaining();
        if capacity == 0 {
            return Poll::Ready(Ok(()));
        }

        let this = self.get_mut();
        ready!(this.poll_fill(cx))?;

        // rustls hands its receive buffer over one chunk at a time, roughly one
        // per record, so this drains as many as the cursor holds rather than
        // costing hyper a poll per record.
        let mut remaining = capacity;
        while remaining > 0 {
            // `into_first_chunk` rather than `fill_buf`: it ties the borrow to
            // the connection instead of to the `Reader` temporary, which is
            // what lets the chunk outlive the statement that produced it.
            //
            // `poll_fill` has already resolved the first chunk, so an error can
            // only belong to a later one — a peer that truncated the stream
            // after the bytes now in the cursor. Deliver those and let the next
            // read report it, rather than discarding plaintext that arrived.
            let Ok(chunk) = this.conn.reader().into_first_chunk() else {
                break;
            };
            // An empty chunk is the peer's `close_notify`; leaving the cursor
            // untouched is how hyper is told the stream ended.
            let taken = chunk.len().min(remaining);
            if taken == 0 {
                break;
            }
            buf.put_slice(&chunk[..taken]);
            this.conn.reader().consume(taken);
            remaining -= taken;
        }
        Poll::Ready(Ok(()))
    }
}

impl<S> HyperWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        AsyncWrite::poll_write(self, cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        AsyncWrite::poll_write_vectored(self, cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        // rustls coalesces the slices into as few records as it can, so telling
        // hyper the truth here saves it a header and a tag per extra slice.
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_flush(self, cx)
    }

    /// Ends the TLS session, then the transport's write direction.
    ///
    /// Hyper calls this when a connection is finished; for TLS that has to mean
    /// `close_notify` first, so it maps onto
    /// [`AsyncWrite::poll_close`] rather than a bare transport shutdown.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_close(self, cx)
    }
}

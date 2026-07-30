//! Hyper transport impls for [`TlsStream`], gated on `hyper` + `rustls`.
//!
//! [`TcpStream`](crate::net::TcpStream) implements hyper's traits directly on
//! the runtime's pending-operation state. A TLS session cannot: what hyper
//! reads and writes is plaintext, which only exists on the other side of the
//! record layer. These impls therefore go through the stream's own
//! [`AsyncRead`]/[`AsyncWrite`], which is where the record layer lives, so
//! hyper inherits the same buffering and cancellation behaviour as any other
//! caller.

use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::io;

use hyper::rt::{Read as HyperRead, ReadBufCursor, Write as HyperWrite};

use super::{CIPHERTEXT_CHUNK, TlsStream};
use crate::io::{AsyncRead, AsyncWrite};

impl<S> HyperRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Reads plaintext into hyper's cursor.
    ///
    /// The cursor exposes uninitialized memory, and reaching runite's
    /// `AsyncRead` requires an initialized slice, so this stages the read
    /// through a scratch buffer rather than reaching for `unsafe`. The copy is
    /// bounded by one TLS record, which is also the most a single read of a
    /// decrypted stream can produce.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buf: ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let capacity = buf.remaining().min(CIPHERTEXT_CHUNK);
        if capacity == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut scratch = [0u8; CIPHERTEXT_CHUNK];
        let read = ready!(AsyncRead::poll_read(self, cx, &mut scratch[..capacity]))?;
        buf.put_slice(&scratch[..read]);
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

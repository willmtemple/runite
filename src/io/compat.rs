//! Compatibility adapters between runite's I/O traits and [`futures-io`].
//!
//! Available with the `futures-compat` feature. These adapters wrap a runite
//! [`AsyncRead`]/[`AsyncBufRead`]/[`AsyncWrite`]/[`AsyncSeek`] value so it can
//! be used where the corresponding `futures-io` traits are expected, and vice
//! versa, easing interop with the broader `futures` ecosystem.
//!
//! The adapters do not make an object `Send` or detach it from runite's
//! event-loop ownership. [`Compat`] retains at most one accepted write buffer
//! while the runite transport completes it, preventing a cancelled
//! `futures-io` write from aliasing the next caller's buffer. A consumer must
//! still poll a wrapped runite transport on the runtime thread that owns it.
//!
//! # Examples
//!
//! ```
//! use core::pin::Pin;
//! use core::task::{Context, Poll};
//! use std::io;
//!
//! use runite::io::compat::FuturesCompat;
//! use runite::io::AsyncReadExt;
//!
//! struct FuturesBytes(&'static [u8]);
//!
//! impl futures_io::AsyncRead for FuturesBytes {
//!     fn poll_read(
//!         mut self: Pin<&mut Self>,
//!         _cx: &mut Context<'_>,
//!         buf: &mut [u8],
//!     ) -> Poll<io::Result<usize>> {
//!         let read = buf.len().min(self.0.len());
//!         buf[..read].copy_from_slice(&self.0[..read]);
//!         self.0 = &self.0[read..];
//!         Poll::Ready(Ok(read))
//!     }
//! }
//!
//! runite::spawn(async {
//!     let mut reader = FuturesCompat::new(FuturesBytes(b"compat"));
//!     let mut out = Vec::new();
//!     reader.read_to_end(&mut out).await.unwrap();
//!     assert_eq!(&out, b"compat");
//! });
//! runite::run();
//! ```
//!
//! [`AsyncBufRead`]: super::AsyncBufRead
//! [`AsyncSeek`]: super::AsyncSeek
//! [`futures-io`]: https://docs.rs/futures-io

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io::{self, IoSlice, IoSliceMut, SeekFrom};

use super::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite};

/// Adapts a runite I/O value to the `futures-io` traits.
///
/// This type is available with the `futures-compat` feature. It wraps a value
/// that implements runite's asynchronous I/O traits and exposes the
/// corresponding `futures_io` traits for integration with libraries that
/// accept them. The adapter does not make the wrapped value executor-independent
/// or `Send`; it only exposes the alternate trait methods.
///
/// A successful write may be buffered while the wrapped runite writer finishes
/// it. Call `futures_io::AsyncWrite::poll_flush` or `poll_close` before dropping
/// or unwrapping the adapter when every accepted byte must be preserved. For
/// wrapped values that implement both writing and seeking, a seek first drains
/// accepted bytes before repositioning the cursor.
/// Returning [`Poll::Pending`] means the current caller's buffer has not been
/// accepted; once accepted, the adapter owns a copy, so cancelling that caller
/// cannot make its completion count satisfy a later write.
///
/// # Examples
///
/// This example is ignored by default because the module only exists when the
/// crate is built with `--features futures-compat`.
///
/// ```ignore
/// use runite::io::compat::Compat;
///
/// # let runite_reader = unimplemented!();
/// let futures_reader = Compat::new(runite_reader);
/// ```
pub struct Compat<T> {
    inner: T,
    write: CompatWriteState,
}

impl<T> std::fmt::Debug for Compat<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Compat").finish_non_exhaustive()
    }
}

impl<T> Compat<T> {
    /// Wraps `inner` for use through `futures_io` traits.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            write: CompatWriteState::default(),
        }
    }

    /// Consumes the adapter and returns the wrapped value.
    ///
    /// The unwritten remainder of any accepted adapter buffer is discarded; an
    /// already in-flight prefix may still complete on `inner`. Flush or close
    /// first when every accepted byte must be preserved.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[derive(Default)]
struct CompatWriteState {
    pending: Option<CompatPendingWrite>,
}

struct CompatPendingWrite {
    data: Vec<u8>,
    written: usize,
    operation: super::WriteOperation,
}

enum CompatDrainStep {
    Complete,
    Progress,
}

impl CompatWriteState {
    fn poll_write<T: AsyncWrite + Unpin>(
        &mut self,
        inner: &mut T,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.poll_drain(inner, cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        self.accept(inner, cx, buf.to_vec())
    }

    fn poll_write_vectored<T: AsyncWrite + Unpin>(
        &mut self,
        inner: &mut T,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let accepted = match bufs
            .iter()
            .try_fold(0usize, |total, buf| total.checked_add(buf.len()))
        {
            Some(accepted) => accepted,
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "vectored compatibility buffer length overflowed",
                )));
            }
        };
        if accepted == 0 {
            return Poll::Ready(Ok(0));
        }
        match self.poll_drain(inner, cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }

        let mut data = Vec::with_capacity(accepted);
        for buf in bufs {
            data.extend_from_slice(buf);
        }
        self.accept(inner, cx, data)
    }

    fn accept<T: AsyncWrite + Unpin>(
        &mut self,
        inner: &mut T,
        cx: &mut Context<'_>,
        data: Vec<u8>,
    ) -> Poll<io::Result<usize>> {
        let accepted = data.len();
        self.pending = Some(CompatPendingWrite {
            data,
            written: 0,
            operation: super::WriteOperation::new(),
        });
        match self.poll_step(inner, cx) {
            Poll::Ready(Ok(CompatDrainStep::Complete | CompatDrainStep::Progress))
            | Poll::Pending => Poll::Ready(Ok(accepted)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }

    fn poll_drain<T: AsyncWrite + Unpin>(
        &mut self,
        inner: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match self.poll_step(inner, cx) {
                Poll::Ready(Ok(CompatDrainStep::Progress)) => {}
                Poll::Ready(Ok(CompatDrainStep::Complete)) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_step<T: AsyncWrite + Unpin>(
        &mut self,
        inner: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<CompatDrainStep>> {
        let Some(pending) = self.pending.as_mut() else {
            return Poll::Ready(Ok(CompatDrainStep::Complete));
        };
        let remaining = &pending.data[pending.written..];
        let written = match Pin::new(&mut *inner).poll_write_operation(
            cx,
            remaining,
            pending.operation.generation(),
        ) {
            Poll::Ready(Ok(written)) => written,
            Poll::Ready(Err(error)) => {
                self.pending = None;
                return Poll::Ready(Err(error));
            }
            Poll::Pending => return Poll::Pending,
        };
        if written == 0 {
            self.pending = None;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write accepted compatibility buffer",
            )));
        }
        if written > remaining.len() {
            self.pending = None;
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "writer reported more bytes than the compatibility buffer",
            )));
        }
        pending.written += written;
        if pending.written == pending.data.len() {
            self.pending = None;
            Poll::Ready(Ok(CompatDrainStep::Complete))
        } else {
            Poll::Ready(Ok(CompatDrainStep::Progress))
        }
    }
}

impl<T> Compat<T> {
    /// Returns a shared reference to the wrapped value.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped value.
    ///
    /// Direct writes can be reordered with bytes already accepted into this
    /// adapter. Flush before using this method for direct I/O.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: AsyncRead + Unpin> futures_io::AsyncRead for Compat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf)
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        AsyncRead::poll_read_vectored(Pin::new(&mut self.inner), cx, bufs)
    }
}

impl<T: AsyncWrite + Unpin> futures_io::AsyncWrite for Compat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        this.write.poll_write(&mut this.inner, cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        this.write.poll_write_vectored(&mut this.inner, cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        match this.write.poll_drain(&mut this.inner, cx) {
            Poll::Ready(Ok(())) => AsyncWrite::poll_flush(Pin::new(&mut this.inner), cx),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        match this.write.poll_drain(&mut this.inner, cx) {
            Poll::Ready(Ok(())) => AsyncWrite::poll_close(Pin::new(&mut this.inner), cx),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncBufRead + Unpin> futures_io::AsyncBufRead for Compat<T> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        AsyncBufRead::poll_fill_buf(Pin::new(&mut self.get_mut().inner), cx)
    }

    fn consume(mut self: Pin<&mut Self>, amount: usize) {
        AsyncBufRead::consume(Pin::new(&mut self.inner), amount);
    }
}

impl<T: AsyncSeek + AsyncWrite + Unpin> futures_io::AsyncSeek for Compat<T> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.as_mut().get_mut();
        match this.write.poll_drain(&mut this.inner, cx) {
            Poll::Ready(Ok(())) => AsyncSeek::poll_seek(Pin::new(&mut this.inner), cx, position),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Adapts a `futures-io` value to runite's I/O traits.
///
/// This type is available with the `futures-compat` feature. It wraps a value
/// that implements `futures_io`'s asynchronous I/O traits and exposes the
/// corresponding runite traits. The wrapped value is still polled on the
/// current runite task and is not buffered or made `Send` by the adapter.
///
/// # Examples
///
/// This example is ignored by default because the module only exists when the
/// crate is built with `--features futures-compat`.
///
/// ```ignore
/// use runite::io::compat::FuturesCompat;
///
/// # let futures_reader = unimplemented!();
/// let runite_reader = FuturesCompat::new(futures_reader);
/// ```
pub struct FuturesCompat<T> {
    inner: T,
}

impl<T> std::fmt::Debug for FuturesCompat<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FuturesCompat")
            .finish_non_exhaustive()
    }
}

impl<T> FuturesCompat<T> {
    /// Wraps `inner` for use through runite's async I/O traits.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Consumes the adapter and returns the wrapped value.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Returns a shared reference to the wrapped value.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: futures_io::AsyncRead + Unpin> AsyncRead for FuturesCompat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        futures_io::AsyncRead::poll_read(Pin::new(&mut self.inner), cx, buf)
    }

    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        futures_io::AsyncRead::poll_read_vectored(Pin::new(&mut self.inner), cx, bufs)
    }
}

impl<T: futures_io::AsyncWrite + Unpin> AsyncWrite for FuturesCompat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        futures_io::AsyncWrite::poll_write(Pin::new(&mut self.inner), cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        futures_io::AsyncWrite::poll_write_vectored(Pin::new(&mut self.inner), cx, bufs)
    }

    fn poll_write_vectored_operation(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        _generation: u64,
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|buf| buf.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        futures_io::AsyncWrite::poll_write_vectored(Pin::new(&mut self.inner), cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_io::AsyncWrite::poll_flush(Pin::new(&mut self.inner), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        futures_io::AsyncWrite::poll_close(Pin::new(&mut self.inner), cx)
    }
}

impl<T: futures_io::AsyncBufRead + Unpin> AsyncBufRead for FuturesCompat<T> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        futures_io::AsyncBufRead::poll_fill_buf(Pin::new(&mut self.get_mut().inner), cx)
    }

    fn consume(mut self: Pin<&mut Self>, amount: usize) {
        futures_io::AsyncBufRead::consume(Pin::new(&mut self.inner), amount);
    }
}

impl<T: futures_io::AsyncSeek + Unpin> AsyncSeek for FuturesCompat<T> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        futures_io::AsyncSeek::poll_seek(Pin::new(&mut self.inner), cx, position)
    }
}

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

/// Asynchronous byte-oriented input.
///
/// `AsyncRead` is the polling primitive behind [`AsyncReadExt`](super::AsyncReadExt)
/// and buffered readers. Implementors attempt to copy bytes into `buf` without
/// blocking the current thread. If no bytes are currently available, return
/// [`Poll::Pending`] and arrange for `cx.waker()` to be woken when progress may
/// be possible.
///
/// Returning `Poll::Ready(Ok(0))` means either EOF or that `buf` was empty. A
/// successful nonzero return value is the number of bytes initialized in `buf`.
/// Futures built on this trait are thread-local in runite and need not be
/// [`Send`](core::marker::Send).
///
/// # Examples
///
/// ```
/// use core::pin::Pin;
/// use core::task::{Context, Poll};
/// use std::io;
///
/// use runite::io::{AsyncRead, AsyncReadExt};
///
/// struct Bytes(&'static [u8]);
///
/// impl AsyncRead for Bytes {
///     fn poll_read(
///         mut self: Pin<&mut Self>,
///         _cx: &mut Context<'_>,
///         buf: &mut [u8],
///     ) -> Poll<io::Result<usize>> {
///         let read = buf.len().min(self.0.len());
///         buf[..read].copy_from_slice(&self.0[..read]);
///         self.0 = &self.0[read..];
///         Poll::Ready(Ok(read))
///     }
/// }
///
/// runite::queue_future(async {
///     let mut reader = Bytes(b"runite");
///     let mut out = [0; 6];
///     reader.read_exact(&mut out).await.unwrap();
///     assert_eq!(&out, b"runite");
/// });
/// runite::run();
/// ```
pub trait AsyncRead {
    /// Attempts to read bytes into `buf`.
    ///
    /// Implementations must never block. Return [`Poll::Pending`] after storing
    /// the latest waker when the operation would block, `Poll::Ready(Ok(n))`
    /// after reading `n` bytes, or `Poll::Ready(Err(error))` for an I/O error.
    /// A return value of `Ok(0)` indicates EOF unless `buf` is empty.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>;
}

/// Asynchronous byte-oriented output.
///
/// `AsyncWrite` is the polling primitive behind [`AsyncWriteExt`](super::AsyncWriteExt)
/// and buffered writers. Implementors attempt to accept bytes from `buf` without
/// blocking the current thread. If no progress is currently possible, return
/// [`Poll::Pending`] and wake `cx.waker()` when the writer may be ready again.
///
/// `poll_write` may accept fewer bytes than were provided. Callers that require
/// the whole buffer to be written should use
/// [`write_all`](super::AsyncWriteExt::write_all). Futures built on this trait
/// are thread-local in runite and need not be [`Send`](core::marker::Send).
///
/// # Examples
///
/// ```
/// use core::pin::Pin;
/// use core::task::{Context, Poll};
/// use std::cell::RefCell;
/// use std::io;
/// use std::rc::Rc;
///
/// use runite::io::{AsyncWrite, AsyncWriteExt};
///
/// #[derive(Clone)]
/// struct Sink(Rc<RefCell<Vec<u8>>>);
///
/// impl AsyncWrite for Sink {
///     fn poll_write(
///         self: Pin<&mut Self>,
///         _cx: &mut Context<'_>,
///         buf: &[u8],
///     ) -> Poll<io::Result<usize>> {
///         self.0.borrow_mut().extend_from_slice(buf);
///         Poll::Ready(Ok(buf.len()))
///     }
///
///     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
///         Poll::Ready(Ok(()))
///     }
///
///     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
///         Poll::Ready(Ok(()))
///     }
/// }
///
/// let written = Rc::new(RefCell::new(Vec::new()));
/// let observed = Rc::clone(&written);
/// runite::queue_future(async move {
///     let mut writer = Sink(written);
///     writer.write_all(b"bytes").await.unwrap();
///     writer.flush().await.unwrap();
/// });
/// runite::run();
/// assert_eq!(&*observed.borrow(), b"bytes");
/// ```
pub trait AsyncWrite {
    /// Attempts to write bytes from `buf`.
    ///
    /// Implementations must never block. Return [`Poll::Pending`] after storing
    /// the latest waker when the operation would block, `Poll::Ready(Ok(n))`
    /// after accepting `n` bytes, or `Poll::Ready(Err(error))` for an I/O error.
    /// Returning `Ok(0)` for a non-empty buffer signals that no progress was made.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    /// Attempts to flush buffered output to the underlying destination.
    ///
    /// Returns [`Poll::Ready`] once all previously accepted bytes have been made
    /// visible to the next layer, or [`Poll::Pending`] if flushing would block.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    /// Attempts to flush and close the writer.
    ///
    /// After a successful close, further writes are implementation-defined and
    /// should generally be treated as errors by callers.
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

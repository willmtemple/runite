//! Poll-based asynchronous byte I/O traits.
//!
//! This module contains the core [`AsyncRead`], [`AsyncBufRead`],
//! [`AsyncWrite`], and [`AsyncSeek`] traits used by runite's files, sockets,
//! process pipes, and adapters. Implementations expose non-blocking poll
//! methods; extension traits such as
//! [`AsyncReadExt`](super::AsyncReadExt) turn those poll methods into futures for
//! everyday async code.
//!
//! # Examples
//!
//! ```
//! use core::pin::Pin;
//! use core::task::{Context, Poll};
//! use std::cell::RefCell;
//! use std::io;
//! use std::rc::Rc;
//!
//! use runite::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
//!
//! struct Bytes(&'static [u8]);
//!
//! impl AsyncRead for Bytes {
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
//! #[derive(Clone)]
//! struct Sink(Rc<RefCell<Vec<u8>>>);
//!
//! impl AsyncWrite for Sink {
//!     fn poll_write(
//!         self: Pin<&mut Self>,
//!         _cx: &mut Context<'_>,
//!         buf: &[u8],
//!     ) -> Poll<io::Result<usize>> {
//!         self.0.borrow_mut().extend_from_slice(buf);
//!         Poll::Ready(Ok(buf.len()))
//!     }
//!
//!     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
//!         Poll::Ready(Ok(()))
//!     }
//!
//!     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
//!         Poll::Ready(Ok(()))
//!     }
//! }
//!
//! let written = Rc::new(RefCell::new(Vec::new()));
//! let observed = Rc::clone(&written);
//! runite::spawn(async move {
//!     let mut reader = Bytes(b"ping");
//!     let mut buf = [0; 4];
//!     reader.read_exact(&mut buf).await.unwrap();
//!
//!     let mut writer = Sink(written);
//!     writer.write_all(&buf).await.unwrap();
//!     writer.flush().await.unwrap();
//! });
//! runite::run();
//! assert_eq!(&*observed.borrow(), b"ping");
//! ```

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io::{self, IoSlice, IoSliceMut, SeekFrom};

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
/// runite::spawn(async {
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

    /// Attempts to read into a sequence of byte slices.
    ///
    /// The default implementation reads into the first non-empty slice using
    /// [`poll_read`](Self::poll_read). Implementations may override this to use
    /// a platform vectored-I/O primitive. Returning `Ok(0)` means EOF only when
    /// at least one supplied slice was non-empty; an empty slice list (or a
    /// list containing only empty slices) completes with `Ok(0)` immediately.
    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        match bufs.iter_mut().find(|buf| !buf.is_empty()) {
            Some(buf) => self.as_mut().poll_read(cx, buf),
            None => Poll::Ready(Ok(0)),
        }
    }
}

/// Asynchronous buffered input.
///
/// `AsyncBufRead` exposes bytes already read from the underlying transport so
/// parsers can inspect them without copying. After a successful
/// [`poll_fill_buf`](Self::poll_fill_buf), call [`consume`](Self::consume) with
/// the number of bytes used before requesting more input.
///
/// Implementations must keep the returned slice valid until the reader is
/// polled or consumed again. An empty slice means EOF.
///
/// # Examples
///
/// ```
/// use core::pin::Pin;
/// use core::task::{Context, Poll, Waker};
/// use std::io;
///
/// use runite::io::{AsyncBufRead, AsyncRead, BufReader};
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
/// let mut reader = BufReader::with_capacity(4, Bytes(b"head:body"));
/// let mut cx = Context::from_waker(Waker::noop());
/// let Poll::Ready(Ok(bytes)) =
///     AsyncBufRead::poll_fill_buf(Pin::new(&mut reader), &mut cx)
/// else {
///     panic!("memory reader should be ready");
/// };
/// assert_eq!(bytes, b"head");
/// AsyncBufRead::consume(Pin::new(&mut reader), 4);
/// ```
pub trait AsyncBufRead: AsyncRead {
    /// Returns currently buffered bytes, refilling the buffer when necessary.
    ///
    /// Returns [`Poll::Pending`] when a refill is in progress. A successful
    /// empty slice indicates EOF.
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>>;

    /// Marks `amount` bytes from the most recent filled buffer as consumed.
    ///
    /// Implementations should clamp values larger than the available buffer
    /// rather than advancing beyond initialized data.
    fn consume(self: Pin<&mut Self>, amount: usize);
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
/// runite::spawn(async move {
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
    ///
    /// A direct caller must keep polling the same logical operation after
    /// `Pending`. Cancellation-capable adapters should use
    /// [`poll_write_operation`](Self::poll_write_operation) with a fresh
    /// generation for each future.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    /// Attempts to write from a sequence of byte slices.
    ///
    /// The default implementation writes from the first non-empty slice using
    /// [`poll_write`](Self::poll_write). Implementations may override this to
    /// use a platform vectored-I/O primitive. Empty input completes with
    /// `Ok(0)` without polling the scalar write path.
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match bufs.iter().find(|buf| !buf.is_empty()) {
            Some(buf) => self.as_mut().poll_write(cx, buf),
            None => Poll::Ready(Ok(0)),
        }
    }

    /// Polls a registered logical write operation.
    ///
    /// Runtime I/O implementations use `generation` to distinguish a re-poll
    /// from a later future after cancellation. Custom writers can rely on the
    /// default forwarding implementation.
    #[doc(hidden)]
    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        _generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.poll_write(cx, buf)
    }

    /// Polls a registered logical vectored write operation.
    ///
    /// This is the cancellation-aware counterpart to
    /// [`poll_write_vectored`](Self::poll_write_vectored). The default forwards
    /// to that method so custom vectored implementations remain effective.
    /// Runtime-backed writers that use `generation` override this hook.
    #[doc(hidden)]
    fn poll_write_vectored_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        _generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, bufs)
    }

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

/// Asynchronous cursor positioning.
///
/// Implementors reposition a logical stream cursor without blocking. If a seek
/// cannot complete immediately, return [`Poll::Pending`], arrange a wakeup, and
/// require the caller to continue polling the same `position` until completion.
/// A successful result is the new byte offset from the start of the stream.
pub trait AsyncSeek {
    /// Attempts to reposition the stream cursor.
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>>;
}

/// Forwards every method of the four async I/O traits through a pointer type.
///
/// Written as a macro over `&mut T` and `Box<T>` because both deref to `T` and
/// the bodies are identical. Every method is forwarded explicitly, including
/// the vectored methods and the hidden `*_operation` hooks: a defaulted method
/// here would silently discard an implementation's override, which for the
/// `_operation` hooks means losing the generation a runtime-backed writer uses
/// to tell a re-poll from a later future after cancellation. Wrapping a
/// `TcpStream` in `&mut` must not quietly make its writes cancellation-unsafe.
macro_rules! forward_async_io {
    ($pointer:ty) => {
        impl<T: AsyncRead + Unpin + ?Sized> AsyncRead for $pointer {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut **self).poll_read(cx, buf)
            }

            fn poll_read_vectored(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &mut [IoSliceMut<'_>],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut **self).poll_read_vectored(cx, bufs)
            }
        }

        impl<T: AsyncBufRead + Unpin + ?Sized> AsyncBufRead for $pointer {
            fn poll_fill_buf(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<&[u8]>> {
                Pin::new(&mut **self.get_mut()).poll_fill_buf(cx)
            }

            fn consume(mut self: Pin<&mut Self>, amount: usize) {
                Pin::new(&mut **self).consume(amount);
            }
        }

        impl<T: AsyncWrite + Unpin + ?Sized> AsyncWrite for $pointer {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut **self).poll_write(cx, buf)
            }

            fn poll_write_vectored(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut **self).poll_write_vectored(cx, bufs)
            }

            fn poll_write_operation(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
                generation: u64,
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut **self).poll_write_operation(cx, buf, generation)
            }

            fn poll_write_vectored_operation(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[IoSlice<'_>],
                generation: u64,
            ) -> Poll<io::Result<usize>> {
                Pin::new(&mut **self).poll_write_vectored_operation(cx, bufs, generation)
            }

            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut **self).poll_flush(cx)
            }

            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut **self).poll_close(cx)
            }
        }

        impl<T: AsyncSeek + Unpin + ?Sized> AsyncSeek for $pointer {
            fn poll_seek(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                position: SeekFrom,
            ) -> Poll<io::Result<u64>> {
                Pin::new(&mut **self).poll_seek(cx, position)
            }
        }
    };
}

forward_async_io!(&mut T);
forward_async_io!(Box<T>);

// `Pin<P>` is separate: it is already pinned, so the target needs no `Unpin`
// bound and forwarding goes through `as_mut` rather than a fresh `Pin::new`.
impl<P> AsyncRead for Pin<P>
where
    P: core::ops::DerefMut + Unpin,
    P::Target: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().as_mut().poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().as_mut().poll_read_vectored(cx, bufs)
    }
}

impl<P> AsyncBufRead for Pin<P>
where
    P: core::ops::DerefMut + Unpin,
    P::Target: AsyncBufRead,
{
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().as_mut().poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        self.get_mut().as_mut().consume(amount);
    }
}

impl<P> AsyncWrite for Pin<P>
where
    P: core::ops::DerefMut + Unpin,
    P::Target: AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().as_mut().poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().as_mut().poll_write_vectored(cx, bufs)
    }

    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .as_mut()
            .poll_write_operation(cx, buf, generation)
    }

    fn poll_write_vectored_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .as_mut()
            .poll_write_vectored_operation(cx, bufs, generation)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().as_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().as_mut().poll_close(cx)
    }
}

impl<P> AsyncSeek for Pin<P>
where
    P: core::ops::DerefMut + Unpin,
    P::Target: AsyncSeek,
{
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        position: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        self.get_mut().as_mut().poll_seek(cx, position)
    }
}

//! Future-returning extension traits for runite I/O.
//!
//! This module adapts the low-level poll methods from [`AsyncRead`] and
//! [`AsyncWrite`] into futures such as [`AsyncReadExt::read_exact`] and
//! [`AsyncWriteExt::write_all`]. It also provides [`Lines`], a stream adapter for
//! UTF-8 line-oriented readers.
//!
//! # Examples
//!
//! ```
//! use core::pin::Pin;
//! use core::task::{Context, Poll};
//! use std::io;
//!
//! use runite::io::{AsyncRead, AsyncReadExt, StreamExt};
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
//! runite::queue_future(async {
//!     let lines = Bytes(b"alpha\nbeta")
//!         .lines()
//!         .collect::<Vec<_>>()
//!         .await
//!         .into_iter()
//!         .collect::<Result<Vec<_>, _>>()
//!         .unwrap();
//!     assert_eq!(lines, ["alpha", "beta"]);
//! });
//! runite::run();
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use super::{AsyncRead, AsyncWrite, Stream};

const READ_TO_END_CHUNK: usize = 8192;
const COPY_BUF_SIZE: usize = 8192;

/// Convenience futures for values that implement [`AsyncRead`].
///
/// This trait is implemented for every async reader and provides async methods
/// analogous to the standard library's blocking `Read` helpers. The returned
/// futures borrow the reader and are intended to run on runite's current-thread
/// executor.
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
///     let mut reader = Bytes(b"alpha\nbeta");
///     let mut out = Vec::new();
///     let read = reader.read_to_end(&mut out).await.unwrap();
///     assert_eq!(read, 10);
///     assert_eq!(&out, b"alpha\nbeta");
/// });
/// runite::run();
/// ```
pub trait AsyncReadExt: AsyncRead {
    /// Reads some bytes into `buf`.
    ///
    /// The returned future completes with the number of bytes read. A value of
    /// `0` means EOF, unless `buf` is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::io;
    /// # use runite::io::{AsyncRead, AsyncReadExt};
    /// # struct Bytes(&'static [u8]);
    /// # impl AsyncRead for Bytes {
    /// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
    /// #         let read = buf.len().min(self.0.len());
    /// #         buf[..read].copy_from_slice(&self.0[..read]);
    /// #         self.0 = &self.0[read..];
    /// #         Poll::Ready(Ok(read))
    /// #     }
    /// # }
    /// runite::queue_future(async {
    ///     let mut reader = Bytes(b"abc");
    ///     let mut buf = [0; 2];
    ///     let read = reader.read(&mut buf).await.unwrap();
    ///     assert_eq!(read, 2);
    ///     assert_eq!(&buf, b"ab");
    /// });
    /// runite::run();
    /// ```
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Read<'a, Self>
    where
        Self: Unpin,
    {
        Read { reader: self, buf }
    }

    /// Reads exactly enough bytes to fill `buf`.
    ///
    /// The future keeps reading until `buf` is full. If EOF is reached first, it
    /// returns [`io::ErrorKind::UnexpectedEof`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::io;
    /// # use runite::io::{AsyncRead, AsyncReadExt};
    /// # struct Bytes(&'static [u8]);
    /// # impl AsyncRead for Bytes {
    /// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
    /// #         let read = buf.len().min(self.0.len());
    /// #         buf[..read].copy_from_slice(&self.0[..read]);
    /// #         self.0 = &self.0[read..];
    /// #         Poll::Ready(Ok(read))
    /// #     }
    /// # }
    /// runite::queue_future(async {
    ///     let mut reader = Bytes(b"abcd");
    ///     let mut buf = [0; 4];
    ///     reader.read_exact(&mut buf).await.unwrap();
    ///     assert_eq!(&buf, b"abcd");
    /// });
    /// runite::run();
    /// ```
    fn read_exact<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadExact<'a, Self>
    where
        Self: Unpin,
    {
        ReadExact {
            reader: self,
            buf,
            filled: 0,
        }
    }

    /// Reads all remaining bytes and appends them to `buf`.
    ///
    /// The returned count is the number of bytes appended by this call. Existing
    /// contents of `buf` are preserved.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::io;
    /// # use runite::io::{AsyncRead, AsyncReadExt};
    /// # struct Bytes(&'static [u8]);
    /// # impl AsyncRead for Bytes {
    /// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
    /// #         let read = buf.len().min(self.0.len());
    /// #         buf[..read].copy_from_slice(&self.0[..read]);
    /// #         self.0 = &self.0[read..];
    /// #         Poll::Ready(Ok(read))
    /// #     }
    /// # }
    /// runite::queue_future(async {
    ///     let mut reader = Bytes(b"tail");
    ///     let mut buf = b"head".to_vec();
    ///     let read = reader.read_to_end(&mut buf).await.unwrap();
    ///     assert_eq!(read, 4);
    ///     assert_eq!(&buf, b"headtail");
    /// });
    /// runite::run();
    /// ```
    fn read_to_end<'a>(&'a mut self, buf: &'a mut Vec<u8>) -> ReadToEnd<'a, Self>
    where
        Self: Unpin,
    {
        ReadToEnd {
            reader: self,
            buf,
            chunk: vec![0; READ_TO_END_CHUNK],
            start_len: 0,
            initialized: false,
        }
    }

    /// Splits this reader into a stream of UTF-8 lines.
    ///
    /// Newline bytes are not included in yielded strings. The final line is
    /// yielded even when the input does not end with a newline.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::io;
    /// # use runite::io::{AsyncRead, AsyncReadExt, StreamExt};
    /// # struct Bytes(&'static [u8]);
    /// # impl AsyncRead for Bytes {
    /// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
    /// #         let read = buf.len().min(self.0.len());
    /// #         buf[..read].copy_from_slice(&self.0[..read]);
    /// #         self.0 = &self.0[read..];
    /// #         Poll::Ready(Ok(read))
    /// #     }
    /// # }
    /// runite::queue_future(async {
    ///     let lines = Bytes(b"one\ntwo").lines().collect::<Vec<_>>().await;
    ///     let lines = lines.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    ///     assert_eq!(lines, ["one", "two"]);
    /// });
    /// runite::run();
    /// ```
    fn lines(self) -> Lines<Self>
    where
        Self: Sized,
    {
        Lines {
            reader: self,
            buf: Vec::new(),
            chunk: vec![0; READ_TO_END_CHUNK],
            eof: false,
        }
    }
}

impl<R: AsyncRead + ?Sized> AsyncReadExt for R {}

/// Convenience futures for values that implement [`AsyncWrite`].
///
/// The methods in this trait mirror the standard library's blocking `Write`
/// helpers, but return futures that poll the writer cooperatively on runite's
/// current-thread runtime.
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
///     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
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
///     writer.write_all(b"hello").await.unwrap();
///     writer.close().await.unwrap();
/// });
/// runite::run();
/// assert_eq!(&*observed.borrow(), b"hello");
/// ```
pub trait AsyncWriteExt: AsyncWrite {
    /// Writes some bytes from `buf`.
    ///
    /// The returned future completes after one successful `poll_write` call and
    /// may write fewer bytes than were provided.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::{cell::RefCell, io, rc::Rc};
    /// # use runite::io::{AsyncWrite, AsyncWriteExt};
    /// # #[derive(Clone)] struct Sink(Rc<RefCell<Vec<u8>>>);
    /// # impl AsyncWrite for Sink {
    /// #     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> { self.0.borrow_mut().extend_from_slice(buf); Poll::Ready(Ok(buf.len())) }
    /// #     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    /// #     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    /// # }
    /// let written = Rc::new(RefCell::new(Vec::new()));
    /// let observed = Rc::clone(&written);
    /// runite::queue_future(async move {
    ///     let mut writer = Sink(written);
    ///     assert_eq!(writer.write(b"hi").await.unwrap(), 2);
    /// });
    /// runite::run();
    /// assert_eq!(&*observed.borrow(), b"hi");
    /// ```
    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Write<'a, Self>
    where
        Self: Unpin,
    {
        Write { writer: self, buf }
    }

    /// Writes the entire contents of `buf`.
    ///
    /// The future repeatedly calls `poll_write` until all bytes are accepted. It
    /// returns [`io::ErrorKind::WriteZero`] if a non-empty remainder produces a
    /// zero-length write.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::{cell::RefCell, io, rc::Rc};
    /// # use runite::io::{AsyncWrite, AsyncWriteExt};
    /// # #[derive(Clone)] struct Sink(Rc<RefCell<Vec<u8>>>);
    /// # impl AsyncWrite for Sink {
    /// #     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> { self.0.borrow_mut().extend_from_slice(buf); Poll::Ready(Ok(buf.len())) }
    /// #     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    /// #     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
    /// # }
    /// let written = Rc::new(RefCell::new(Vec::new()));
    /// let observed = Rc::clone(&written);
    /// runite::queue_future(async move {
    ///     let mut writer = Sink(written);
    ///     writer.write_all(b"all").await.unwrap();
    /// });
    /// runite::run();
    /// assert_eq!(&*observed.borrow(), b"all");
    /// ```
    fn write_all<'a>(&'a mut self, buf: &'a [u8]) -> WriteAll<'a, Self>
    where
        Self: Unpin,
    {
        WriteAll {
            writer: self,
            buf,
            written: 0,
        }
    }

    /// Flushes buffered output.
    ///
    /// The future completes when the writer reports that all previously accepted
    /// bytes have been flushed to the next layer.
    fn flush(&mut self) -> Flush<'_, Self>
    where
        Self: Unpin,
    {
        Flush { writer: self }
    }

    /// Flushes and closes the writer.
    ///
    /// After this future completes successfully, callers should not attempt to
    /// write more bytes through the same writer.
    fn close(&mut self) -> Close<'_, Self>
    where
        Self: Unpin,
    {
        Close { writer: self }
    }
}

impl<W: AsyncWrite + ?Sized> AsyncWriteExt for W {}

/// Copies all bytes from `reader` into `writer`.
///
/// The returned future repeatedly reads from `reader`, writes every byte read to
/// `writer`, and flushes `writer` after `reader` reaches EOF. It resolves to the
/// number of bytes written.
///
/// # Examples
///
/// ```
/// # use core::pin::Pin;
/// # use core::task::{Context, Poll};
/// # use std::{cell::RefCell, io, rc::Rc};
/// # use runite::io::{AsyncRead, AsyncWrite};
/// # struct Bytes(&'static [u8]);
/// # impl AsyncRead for Bytes {
/// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
/// #         let read = buf.len().min(self.0.len());
/// #         buf[..read].copy_from_slice(&self.0[..read]);
/// #         self.0 = &self.0[read..];
/// #         Poll::Ready(Ok(read))
/// #     }
/// # }
/// # #[derive(Clone)] struct Sink(Rc<RefCell<Vec<u8>>>);
/// # impl AsyncWrite for Sink {
/// #     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
/// #         self.0.borrow_mut().extend_from_slice(buf);
/// #         Poll::Ready(Ok(buf.len()))
/// #     }
/// #     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// #     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// # }
/// let written = Rc::new(RefCell::new(Vec::new()));
/// let observed = Rc::clone(&written);
/// runite::queue_future(async move {
///     let mut reader = Bytes(b"copy me");
///     let mut writer = Sink(written);
///     let copied = runite::io::copy(&mut reader, &mut writer).await.unwrap();
///     assert_eq!(copied, 7);
/// });
/// runite::run();
/// assert_eq!(&*observed.borrow(), b"copy me");
/// ```
pub fn copy<'a, R, W>(reader: &'a mut R, writer: &'a mut W) -> Copy<'a, R, W>
where
    R: AsyncRead + ?Sized,
    W: AsyncWrite + ?Sized,
{
    Copy {
        reader,
        writer,
        buf: vec![0; COPY_BUF_SIZE],
        pos: 0,
        filled: 0,
        copied: 0,
        state: CopyState::Copying,
    }
}

/// Copies bytes in both directions between two streams until both reach EOF.
///
/// The returned future drives `a` to `b` and `b` to `a` in a single current-thread
/// task. When one direction reaches EOF, the write half of the opposite stream is
/// closed with [`AsyncWrite::poll_close`], signalling EOF to that peer while the
/// other direction continues.
///
/// # Examples
///
/// ```
/// # use core::pin::Pin;
/// # use core::task::{Context, Poll};
/// # use std::{cell::RefCell, io, rc::Rc};
/// # use runite::io::{AsyncRead, AsyncWrite};
/// # struct Endpoint {
/// #     read: &'static [u8],
/// #     written: Rc<RefCell<Vec<u8>>>,
/// # }
/// # impl AsyncRead for Endpoint {
/// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
/// #         let read = buf.len().min(self.read.len());
/// #         buf[..read].copy_from_slice(&self.read[..read]);
/// #         self.read = &self.read[read..];
/// #         Poll::Ready(Ok(read))
/// #     }
/// # }
/// # impl AsyncWrite for Endpoint {
/// #     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
/// #         self.written.borrow_mut().extend_from_slice(buf);
/// #         Poll::Ready(Ok(buf.len()))
/// #     }
/// #     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// #     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// # }
/// let left_written = Rc::new(RefCell::new(Vec::new()));
/// let right_written = Rc::new(RefCell::new(Vec::new()));
/// let left_observed = Rc::clone(&left_written);
/// let right_observed = Rc::clone(&right_written);
/// runite::queue_future(async move {
///     let mut left = Endpoint { read: b"left", written: left_written };
///     let mut right = Endpoint { read: b"right", written: right_written };
///     let copied = runite::io::copy_bidirectional(&mut left, &mut right).await.unwrap();
///     assert_eq!(copied, (4, 5));
/// });
/// runite::run();
/// assert_eq!(&*right_observed.borrow(), b"left");
/// assert_eq!(&*left_observed.borrow(), b"right");
/// ```
pub fn copy_bidirectional<'a, A, B>(a: &'a mut A, b: &'a mut B) -> CopyBidirectional<'a, A, B>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    CopyBidirectional {
        a,
        b,
        a_to_b: CopyHalf::new(),
        b_to_a: CopyHalf::new(),
    }
}

/// Future returned by [`AsyncReadExt::read`].
pub struct Read<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
}

impl<R: AsyncRead + Unpin + ?Sized> Future for Read<'_, R> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut *this.reader).poll_read(cx, this.buf)
    }
}

/// Future returned by [`AsyncReadExt::read_exact`].
pub struct ReadExact<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut [u8],
    filled: usize,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadExact<'_, R> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while this.filled < this.buf.len() {
            let read = match Pin::new(&mut *this.reader).poll_read(cx, &mut this.buf[this.filled..])
            {
                Poll::Ready(result) => result?,
                Poll::Pending => return Poll::Pending,
            };
            if read == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                )));
            }
            this.filled += read;
        }
        Poll::Ready(Ok(()))
    }
}

/// Future returned by [`AsyncReadExt::read_to_end`].
pub struct ReadToEnd<'a, R: ?Sized> {
    reader: &'a mut R,
    buf: &'a mut Vec<u8>,
    chunk: Vec<u8>,
    start_len: usize,
    initialized: bool,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadToEnd<'_, R> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if !this.initialized {
            this.start_len = this.buf.len();
            this.initialized = true;
        }

        loop {
            let read = match Pin::new(&mut *this.reader).poll_read(cx, &mut this.chunk) {
                Poll::Ready(result) => result?,
                Poll::Pending => return Poll::Pending,
            };
            if read == 0 {
                return Poll::Ready(Ok(this.buf.len() - this.start_len));
            }
            this.buf.extend_from_slice(&this.chunk[..read]);
        }
    }
}

/// Future returned by [`AsyncWriteExt::write`].
pub struct Write<'a, W: ?Sized> {
    writer: &'a mut W,
    buf: &'a [u8],
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Write<'_, W> {
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        Pin::new(&mut *this.writer).poll_write(cx, this.buf)
    }
}

/// Future returned by [`AsyncWriteExt::write_all`].
pub struct WriteAll<'a, W: ?Sized> {
    writer: &'a mut W,
    buf: &'a [u8],
    written: usize,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for WriteAll<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        while this.written < this.buf.len() {
            let written =
                match Pin::new(&mut *this.writer).poll_write(cx, &this.buf[this.written..]) {
                    Poll::Ready(result) => result?,
                    Poll::Pending => return Poll::Pending,
                };
            if written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                )));
            }
            this.written += written;
        }
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CopyState {
    Copying,
    Flushing,
}

/// Future returned by [`copy`].
///
/// # Examples
///
/// ```
/// # use core::pin::Pin;
/// # use core::task::{Context, Poll};
/// # use std::{cell::RefCell, io, rc::Rc};
/// # use runite::io::{AsyncRead, AsyncWrite};
/// # struct Bytes(&'static [u8]);
/// # impl AsyncRead for Bytes {
/// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
/// #         let read = buf.len().min(self.0.len());
/// #         buf[..read].copy_from_slice(&self.0[..read]);
/// #         self.0 = &self.0[read..];
/// #         Poll::Ready(Ok(read))
/// #     }
/// # }
/// # #[derive(Clone)] struct Sink(Rc<RefCell<Vec<u8>>>);
/// # impl AsyncWrite for Sink {
/// #     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
/// #         self.0.borrow_mut().extend_from_slice(buf);
/// #         Poll::Ready(Ok(buf.len()))
/// #     }
/// #     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// #     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// # }
/// runite::queue_future(async {
///     let mut reader = Bytes(b"bytes");
///     let out = Rc::new(RefCell::new(Vec::new()));
///     let mut writer = Sink(Rc::clone(&out));
///     let future: runite::io::Copy<'_, _, _> = runite::io::copy(&mut reader, &mut writer);
///     assert_eq!(future.await.unwrap(), 5);
///     assert_eq!(&*out.borrow(), b"bytes");
/// });
/// runite::run();
/// ```
pub struct Copy<'a, R: ?Sized, W: ?Sized> {
    reader: &'a mut R,
    writer: &'a mut W,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
    copied: u64,
    state: CopyState,
}

impl<R, W> Future for Copy<'_, R, W>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    type Output = io::Result<u64>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        loop {
            match this.state {
                CopyState::Copying if this.pos < this.filled => {
                    let written = match Pin::new(&mut *this.writer)
                        .poll_write(cx, &this.buf[this.pos..this.filled])
                    {
                        Poll::Ready(result) => result?,
                        Poll::Pending => return Poll::Pending,
                    };
                    if written == 0 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write copied data",
                        )));
                    }
                    this.pos += written;
                    this.copied += written as u64;
                }
                CopyState::Copying => {
                    this.pos = 0;
                    this.filled = match Pin::new(&mut *this.reader).poll_read(cx, &mut this.buf) {
                        Poll::Ready(result) => result?,
                        Poll::Pending => return Poll::Pending,
                    };
                    if this.filled == 0 {
                        this.state = CopyState::Flushing;
                    }
                }
                CopyState::Flushing => {
                    return match Pin::new(&mut *this.writer).poll_flush(cx) {
                        Poll::Ready(Ok(())) => Poll::Ready(Ok(this.copied)),
                        Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CopyHalfState {
    Copying,
    Closing,
    Done,
}

struct CopyHalf {
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
    copied: u64,
    state: CopyHalfState,
}

impl CopyHalf {
    fn new() -> Self {
        Self {
            buf: vec![0; COPY_BUF_SIZE],
            pos: 0,
            filled: 0,
            copied: 0,
            state: CopyHalfState::Copying,
        }
    }

    fn is_done(&self) -> bool {
        self.state == CopyHalfState::Done
    }
}

fn poll_copy_half<R, W>(
    reader: &mut R,
    writer: &mut W,
    half: &mut CopyHalf,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>>
where
    R: AsyncRead + Unpin + ?Sized,
    W: AsyncWrite + Unpin + ?Sized,
{
    loop {
        match half.state {
            CopyHalfState::Copying if half.pos < half.filled => {
                let written =
                    match Pin::new(&mut *writer).poll_write(cx, &half.buf[half.pos..half.filled]) {
                        Poll::Ready(result) => result?,
                        Poll::Pending => return Poll::Pending,
                    };
                if written == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write copied data",
                    )));
                }
                half.pos += written;
                half.copied += written as u64;
            }
            CopyHalfState::Copying => {
                half.pos = 0;
                half.filled = match Pin::new(&mut *reader).poll_read(cx, &mut half.buf) {
                    Poll::Ready(result) => result?,
                    Poll::Pending => return Poll::Pending,
                };
                if half.filled == 0 {
                    half.state = CopyHalfState::Closing;
                }
            }
            CopyHalfState::Closing => {
                match Pin::new(&mut *writer).poll_close(cx) {
                    Poll::Ready(result) => result?,
                    Poll::Pending => return Poll::Pending,
                }
                half.state = CopyHalfState::Done;
                return Poll::Ready(Ok(()));
            }
            CopyHalfState::Done => return Poll::Ready(Ok(())),
        }
    }
}

/// Future returned by [`copy_bidirectional`].
///
/// # Examples
///
/// ```
/// # use core::pin::Pin;
/// # use core::task::{Context, Poll};
/// # use std::{cell::RefCell, io, rc::Rc};
/// # use runite::io::{AsyncRead, AsyncWrite};
/// # struct Endpoint {
/// #     read: &'static [u8],
/// #     written: Rc<RefCell<Vec<u8>>>,
/// # }
/// # impl AsyncRead for Endpoint {
/// #     fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
/// #         let read = buf.len().min(self.read.len());
/// #         buf[..read].copy_from_slice(&self.read[..read]);
/// #         self.read = &self.read[read..];
/// #         Poll::Ready(Ok(read))
/// #     }
/// # }
/// # impl AsyncWrite for Endpoint {
/// #     fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
/// #         self.written.borrow_mut().extend_from_slice(buf);
/// #         Poll::Ready(Ok(buf.len()))
/// #     }
/// #     fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// #     fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
/// # }
/// runite::queue_future(async {
///     let left_out = Rc::new(RefCell::new(Vec::new()));
///     let right_out = Rc::new(RefCell::new(Vec::new()));
///     let mut left = Endpoint { read: b"abc", written: Rc::clone(&left_out) };
///     let mut right = Endpoint { read: b"de", written: Rc::clone(&right_out) };
///     let future: runite::io::CopyBidirectional<'_, _, _> =
///         runite::io::copy_bidirectional(&mut left, &mut right);
///     assert_eq!(future.await.unwrap(), (3, 2));
///     assert_eq!(&*right_out.borrow(), b"abc");
///     assert_eq!(&*left_out.borrow(), b"de");
/// });
/// runite::run();
/// ```
pub struct CopyBidirectional<'a, A: ?Sized, B: ?Sized> {
    a: &'a mut A,
    b: &'a mut B,
    a_to_b: CopyHalf,
    b_to_a: CopyHalf,
}

impl<A, B> Future for CopyBidirectional<'_, A, B>
where
    A: AsyncRead + AsyncWrite + Unpin + ?Sized,
    B: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    type Output = io::Result<(u64, u64)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if !this.a_to_b.is_done() {
            match poll_copy_half(&mut *this.a, &mut *this.b, &mut this.a_to_b, cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }

        if !this.b_to_a.is_done() {
            match poll_copy_half(&mut *this.b, &mut *this.a, &mut this.b_to_a, cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }

        if this.a_to_b.is_done() && this.b_to_a.is_done() {
            Poll::Ready(Ok((this.a_to_b.copied, this.b_to_a.copied)))
        } else {
            Poll::Pending
        }
    }
}

/// Future returned by [`AsyncWriteExt::flush`].
pub struct Flush<'a, W: ?Sized> {
    writer: &'a mut W,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Flush<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.writer).poll_flush(cx)
    }
}

/// Future returned by [`AsyncWriteExt::close`].
pub struct Close<'a, W: ?Sized> {
    writer: &'a mut W,
}

impl<W: AsyncWrite + Unpin + ?Sized> Future for Close<'_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.writer).poll_close(cx)
    }
}

/// Stream of UTF-8 lines returned by [`AsyncReadExt::lines`].
///
/// Each item is an [`io::Result<String>`]. Newline terminators are removed, and
/// invalid UTF-8 is reported as [`io::ErrorKind::InvalidData`].
pub struct Lines<R> {
    reader: R,
    buf: Vec<u8>,
    chunk: Vec<u8>,
    eof: bool,
}

impl<R> Unpin for Lines<R> {}

impl<R: AsyncRead + Unpin> Stream for Lines<R> {
    type Item = io::Result<String>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(newline) = this.buf.iter().position(|byte| *byte == b'\n') {
                let mut line = this.buf.drain(..=newline).collect::<Vec<_>>();
                let _ = line.pop();
                return Poll::Ready(Some(string_from_line(line)));
            }

            if this.eof {
                if this.buf.is_empty() {
                    return Poll::Ready(None);
                }
                let line = core::mem::take(&mut this.buf);
                return Poll::Ready(Some(string_from_line(line)));
            }

            match Pin::new(&mut this.reader).poll_read(cx, &mut this.chunk) {
                Poll::Ready(Ok(0)) => this.eof = true,
                Poll::Ready(Ok(read)) => this.buf.extend_from_slice(&this.chunk[..read]),
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn string_from_line(line: Vec<u8>) -> io::Result<String> {
    String::from_utf8(line).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::{AsyncRead, AsyncWrite, copy, copy_bidirectional};
    use crate::{queue_future, run};
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::cell::RefCell;
    use std::io;
    use std::rc::Rc;

    struct MemoryIo {
        read: Vec<u8>,
        read_pos: usize,
        written: Rc<RefCell<Vec<u8>>>,
        max_write: usize,
        flushed: Rc<RefCell<bool>>,
        closed: Rc<RefCell<bool>>,
    }

    impl MemoryIo {
        fn new(read: &[u8], max_write: usize) -> Self {
            Self {
                read: read.to_vec(),
                read_pos: 0,
                written: Rc::new(RefCell::new(Vec::new())),
                max_write,
                flushed: Rc::new(RefCell::new(false)),
                closed: Rc::new(RefCell::new(false)),
            }
        }

        fn written(&self) -> Rc<RefCell<Vec<u8>>> {
            Rc::clone(&self.written)
        }

        fn closed(&self) -> Rc<RefCell<bool>> {
            Rc::clone(&self.closed)
        }
    }

    impl AsyncRead for MemoryIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let available = self.read.len() - self.read_pos;
            let read = buf.len().min(available).min(3);
            let start = self.read_pos;
            let end = start + read;
            buf[..read].copy_from_slice(&self.read[start..end]);
            self.read_pos = end;
            Poll::Ready(Ok(read))
        }
    }

    impl AsyncWrite for MemoryIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if *self.closed.borrow() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "writer is closed",
                )));
            }

            let written = buf.len().min(self.max_write);
            self.written.borrow_mut().extend_from_slice(&buf[..written]);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            *self.flushed.borrow_mut() = true;
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            *self.closed.borrow_mut() = true;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn copy_writes_all_bytes_and_flushes() {
        let written = Rc::new(RefCell::new(Vec::new()));
        let flushed = Rc::new(RefCell::new(false));

        {
            let written = Rc::clone(&written);
            let flushed = Rc::clone(&flushed);
            queue_future(async move {
                let mut reader = MemoryIo::new(b"abcdefghi", 2);
                let mut writer = MemoryIo::new(b"", 2);
                writer.written = written;
                writer.flushed = flushed;

                let copied = copy(&mut reader, &mut writer).await.unwrap();
                assert_eq!(copied, 9);
            });
        }

        run();
        assert_eq!(&*written.borrow(), b"abcdefghi");
        assert!(*flushed.borrow());
    }

    #[test]
    fn copy_bidirectional_copies_both_ways_and_closes_write_halves() {
        let mut left = MemoryIo::new(b"left-to-right", 4);
        let mut right = MemoryIo::new(b"right-to-left", 5);
        let left_written = left.written();
        let right_written = right.written();
        let left_closed = left.closed();
        let right_closed = right.closed();

        queue_future(async move {
            let copied = copy_bidirectional(&mut left, &mut right).await.unwrap();
            assert_eq!(copied, (13, 13));
        });

        run();
        assert_eq!(&*right_written.borrow(), b"left-to-right");
        assert_eq!(&*left_written.borrow(), b"right-to-left");
        assert!(*left_closed.borrow());
        assert!(*right_closed.borrow());
    }
}

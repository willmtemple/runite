//! Buffered async I/O adapters.
//!
//! Small reads and writes can amplify system-call overhead. For example, repeatedly
//! reading one byte while searching for a newline can submit one runtime I/O
//! operation per byte. [`BufReader`] amortizes that cost by reading larger chunks
//! from the inner reader and serving small reads from memory. [`BufWriter`] does
//! the same for writes by collecting small writes and forwarding larger chunks to
//! the inner writer.
//!
//! These adapters inherit runite's current-thread I/O model: they do not make an
//! inner reader or writer `Send`, and they should be driven on the event loop
//! that owns the underlying object. [`BufWriter`] has no blocking or best-effort
//! drop flush; buffered bytes are only forwarded by explicit flush/close calls
//! or by later writes that force the buffer to drain.
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
//! use runite::io::{AsyncWrite, AsyncWriteExt, BufWriter};
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
//!     let mut writer = BufWriter::with_capacity(8, Sink(written));
//!     writer.write_all(b"buffered").await.unwrap();
//!     writer.flush().await.unwrap();
//! });
//! runite::run();
//! assert_eq!(&*observed.borrow(), b"buffered");
//! ```

use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use std::io;

use super::{AsyncRead, AsyncWrite};

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

/// Adds buffering to an async reader.
///
/// `BufReader` reads from the wrapped reader into an in-memory buffer and then
/// serves subsequent reads from that buffer. This reduces syscall amplification
/// for byte-wise reads or line-oriented parsing: instead of issuing one
/// underlying read for each small call, the adapter performs larger reads and
/// amortizes that cost across many callers.
///
/// The adapter delegates all real I/O to the wrapped [`AsyncRead`] value. When
/// the destination buffer is at least as large as the internal buffer and no
/// bytes are currently buffered, reads bypass the internal buffer.
///
/// # Examples
///
/// ```
/// use core::pin::Pin;
/// use core::task::{Context, Poll};
/// use std::io;
///
/// use runite::io::{AsyncRead, AsyncReadExt, BufReader};
///
/// struct Bytes {
///     data: &'static [u8],
/// }
///
/// impl AsyncRead for Bytes {
///     fn poll_read(
///         mut self: Pin<&mut Self>,
///         _cx: &mut Context<'_>,
///         buf: &mut [u8],
///     ) -> Poll<io::Result<usize>> {
///         let read = buf.len().min(self.data.len());
///         buf[..read].copy_from_slice(&self.data[..read]);
///         self.data = &self.data[read..];
///         Poll::Ready(Ok(read))
///     }
/// }
///
/// runite::spawn(async {
///     let mut reader = BufReader::with_capacity(4, Bytes { data: b"hello" });
///     let mut out = [0; 5];
///     reader.read_exact(&mut out).await.unwrap();
///     assert_eq!(&out, b"hello");
/// });
/// runite::run();
/// ```
pub struct BufReader<R> {
    inner: R,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
}

impl<R: AsyncRead> BufReader<R> {
    /// Creates a buffered reader with the default capacity.
    ///
    /// The default is currently 8 KiB.
    pub fn new(inner: R) -> Self {
        Self::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    /// Creates a buffered reader with the specified capacity.
    ///
    /// A capacity of zero disables internal buffering and delegates reads
    /// directly to the wrapped reader.
    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            inner,
            buf: vec![0; capacity],
            pos: 0,
            filled: 0,
        }
    }

    /// Returns a shared reference to the wrapped reader.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped reader.
    ///
    /// Reading from the inner reader directly may desynchronize it from bytes
    /// already held in this adapter's buffer.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Consumes this adapter and returns the wrapped reader.
    ///
    /// Any unread bytes currently held in the internal buffer are discarded.
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Returns the bytes currently held in the internal buffer.
    ///
    /// The slice contains bytes that can be read without polling the inner
    /// reader again.
    pub fn buffer(&self) -> &[u8] {
        &self.buf[self.pos..self.filled]
    }
}

impl<R: AsyncRead + Unpin> BufReader<R> {
    /// Returns the currently buffered bytes, refilling from the inner reader if empty.
    ///
    /// This is the buffered-read hook for line parsers and protocol decoders in
    /// runite's current trait shape. Call [`consume`](Self::consume) after using
    /// bytes from the returned slice.
    pub async fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.pos == self.filled {
            self.pos = 0;
            self.filled =
                poll_fn(|cx| Pin::new(&mut self.inner).poll_read(cx, &mut self.buf)).await?;
        }
        Ok(self.buffer())
    }

    /// Advances the internal cursor by `amount` bytes.
    ///
    /// Passing a value larger than the number of currently buffered bytes
    /// consumes the whole buffer.
    pub fn consume(&mut self, amount: usize) {
        self.pos = self.filled.min(self.pos.saturating_add(amount));
    }

    /// Reads a single UTF-8 line into `buf`.
    ///
    /// The trailing newline is included when present. Returns the number of
    /// bytes appended to `buf`, or `0` on EOF with no buffered bytes remaining.
    /// Invalid UTF-8 is returned as [`io::ErrorKind::InvalidData`].
    /// This method uses [`fill_buf`](Self::fill_buf) and
    /// [`consume`](Self::consume), so line-oriented reads benefit from this
    /// adapter's larger underlying reads.
    pub async fn read_line(&mut self, buf: &mut String) -> io::Result<usize> {
        let mut bytes = Vec::new();
        loop {
            let available = self.fill_buf().await?;
            if available.is_empty() {
                break;
            }

            let take = match available.iter().position(|byte| *byte == b'\n') {
                Some(newline) => newline + 1,
                None => available.len(),
            };
            let found_newline = available[..take].last() == Some(&b'\n');
            bytes.extend_from_slice(&available[..take]);
            self.consume(take);
            if found_newline {
                break;
            }
        }

        let read = bytes.len();
        let line = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        buf.push_str(&line);
        Ok(read)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BufReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();
        if this.pos < this.filled {
            let read = buf.len().min(this.filled - this.pos);
            buf[..read].copy_from_slice(&this.buf[this.pos..this.pos + read]);
            this.pos += read;
            return Poll::Ready(Ok(read));
        }

        if buf.len() >= this.buf.len() {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }

        this.pos = 0;
        this.filled = ready!(Pin::new(&mut this.inner).poll_read(cx, &mut this.buf))?;
        let read = buf.len().min(this.filled);
        buf[..read].copy_from_slice(&this.buf[..read]);
        this.pos = read;
        Poll::Ready(Ok(read))
    }
}

impl<R: AsyncRead + AsyncWrite + Unpin> AsyncWrite for BufReader<R> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

/// Adds buffering to an async writer.
///
/// `BufWriter` stores small writes in memory and flushes them to the wrapped
/// writer as larger chunks. This reduces syscall amplification for code that
/// writes many short byte slices, while preserving [`AsyncWrite`]'s poll-based
/// shape. Buffered bytes are flushed on explicit flush and close operations.
///
/// Drop does **not** flush. `BufWriter` has no `Drop` implementation, so any
/// bytes still in the internal buffer are silently discarded when the adapter is
/// dropped. Call [`AsyncWriteExt::flush`](super::AsyncWriteExt::flush) or
/// [`AsyncWriteExt::close`](super::AsyncWriteExt::close) before dropping the
/// adapter when buffered data must reach the inner writer.
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
/// use runite::io::{AsyncWrite, AsyncWriteExt, BufWriter};
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
///     let mut writer = BufWriter::with_capacity(8, Sink(written));
///     writer.write_all(b"hello").await.unwrap();
///     writer.flush().await.unwrap();
/// });
/// runite::run();
/// assert_eq!(&*observed.borrow(), b"hello");
/// ```
pub struct BufWriter<W> {
    inner: W,
    buf: Vec<u8>,
    written: usize,
}

impl<W: AsyncWrite> BufWriter<W> {
    /// Creates a buffered writer with the default capacity.
    ///
    /// The default is currently 8 KiB.
    pub fn new(inner: W) -> Self {
        Self::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    /// Creates a buffered writer with the specified capacity.
    ///
    /// A capacity of zero disables internal buffering and delegates writes
    /// directly to the wrapped writer.
    pub fn with_capacity(capacity: usize, inner: W) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(capacity),
            written: 0,
        }
    }

    /// Returns a shared reference to the wrapped writer.
    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    /// Returns a mutable reference to the wrapped writer.
    ///
    /// Writing to the inner writer directly can interleave with bytes already
    /// buffered by this adapter.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Consumes this adapter and returns the wrapped writer.
    ///
    /// Any bytes currently held in the internal buffer are discarded. Flush or
    /// close this adapter before calling `into_inner` when buffered data must be
    /// preserved.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite + Unpin> BufWriter<W> {
    fn poll_flush_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.written < self.buf.len() {
            let written =
                ready!(Pin::new(&mut self.inner).poll_write(cx, &self.buf[self.written..]))?;
            if written == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write buffered data",
                )));
            }
            self.written += written;
        }

        self.buf.clear();
        self.written = 0;
        Poll::Ready(Ok(()))
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BufWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();
        if this.buf.capacity() == 0 {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        }

        if this.written > 0 || buf.len() > this.buf.capacity() - this.buf.len() {
            ready!(this.poll_flush_buf(cx))?;
        }

        if buf.len() >= this.buf.capacity() {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        }

        this.buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_flush_buf(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_flush_buf(cx))?;
        Pin::new(&mut this.inner).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    use crate::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use crate::{run, spawn};

    use super::{BufReader, BufWriter};

    struct ChunkedReader {
        data: Vec<u8>,
        pos: usize,
        max_read: usize,
        reads: Rc<RefCell<usize>>,
    }

    impl AsyncRead for ChunkedReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            *self.reads.borrow_mut() += 1;
            let remaining = self.data.len() - self.pos;
            let read = remaining.min(buf.len()).min(self.max_read);
            let pos = self.pos;
            buf[..read].copy_from_slice(&self.data[pos..pos + read]);
            self.pos += read;
            Poll::Ready(Ok(read))
        }
    }

    #[derive(Clone)]
    struct RecordingWriter {
        data: Rc<RefCell<Vec<u8>>>,
        writes: Rc<RefCell<usize>>,
        closes: Rc<RefCell<usize>>,
    }

    impl AsyncWrite for RecordingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            *self.writes.borrow_mut() += 1;
            self.data.borrow_mut().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            *self.closes.borrow_mut() += 1;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn buf_reader_serves_small_reads_from_one_inner_read() {
        let reads = Rc::new(RefCell::new(0));
        let observed = Rc::new(RefCell::new(Vec::new()));
        let observed_for_task = Rc::clone(&observed);
        let reads_for_task = Rc::clone(&reads);

        spawn(async move {
            let inner = ChunkedReader {
                data: b"abcdef".to_vec(),
                pos: 0,
                max_read: 4,
                reads: reads_for_task,
            };
            let mut reader = BufReader::with_capacity(4, inner);
            let mut first = [0; 1];
            let mut second = [0; 1];
            reader.read_exact(&mut first).await.unwrap();
            reader.read_exact(&mut second).await.unwrap();
            observed_for_task.borrow_mut().extend_from_slice(&first);
            observed_for_task.borrow_mut().extend_from_slice(&second);
        });

        run();
        assert_eq!(&*observed.borrow(), b"ab");
        assert_eq!(*reads.borrow(), 1);
    }

    #[test]
    fn buf_reader_fill_buf_and_consume_expose_buffered_bytes() {
        let reads = Rc::new(RefCell::new(0));
        let observed = Rc::new(RefCell::new(Vec::new()));
        let observed_for_task = Rc::clone(&observed);

        spawn({
            let reads = Rc::clone(&reads);
            async move {
                let inner = ChunkedReader {
                    data: b"line\nrest".to_vec(),
                    pos: 0,
                    max_read: 8,
                    reads,
                };
                let mut reader = BufReader::with_capacity(8, inner);
                let filled = reader.fill_buf().await.unwrap();
                let newline = filled.iter().position(|byte| *byte == b'\n').unwrap();
                observed_for_task
                    .borrow_mut()
                    .extend_from_slice(&filled[..=newline]);
                reader.consume(newline + 1);
                observed_for_task
                    .borrow_mut()
                    .extend_from_slice(reader.buffer());
            }
        });

        run();
        assert_eq!(&*observed.borrow(), b"line\nres");
        assert_eq!(*reads.borrow(), 1);
    }

    #[test]
    fn buf_reader_read_line_uses_internal_buffer() {
        let reads = Rc::new(RefCell::new(0));
        let observed = Rc::new(RefCell::new(Vec::new()));
        let observed_for_task = Rc::clone(&observed);

        spawn({
            let reads = Rc::clone(&reads);
            async move {
                let inner = ChunkedReader {
                    data: b"alpha\nbeta\n".to_vec(),
                    pos: 0,
                    max_read: 11,
                    reads,
                };
                let mut reader = BufReader::with_capacity(11, inner);
                let mut first = String::new();
                let mut second = String::new();
                assert_eq!(reader.read_line(&mut first).await.unwrap(), 6);
                assert_eq!(reader.read_line(&mut second).await.unwrap(), 5);
                observed_for_task.borrow_mut().push(first);
                observed_for_task.borrow_mut().push(second);
            }
        });

        run();
        assert_eq!(
            *observed.borrow(),
            vec!["alpha\n".to_string(), "beta\n".to_string()]
        );
        assert_eq!(*reads.borrow(), 1);
    }

    #[test]
    fn buf_writer_batches_small_writes_until_flush() {
        let data = Rc::new(RefCell::new(Vec::new()));
        let writes = Rc::new(RefCell::new(0));

        spawn({
            let data = Rc::clone(&data);
            let writes = Rc::clone(&writes);
            async move {
                let inner = RecordingWriter {
                    data,
                    writes,
                    closes: Rc::new(RefCell::new(0)),
                };
                let mut writer = BufWriter::with_capacity(8, inner);
                writer.write_all(b"ab").await.unwrap();
                writer.write_all(b"cd").await.unwrap();
                writer.flush().await.unwrap();
            }
        });

        run();
        assert_eq!(&*data.borrow(), b"abcd");
        assert_eq!(*writes.borrow(), 1);
    }

    #[test]
    fn buf_writer_flushes_before_close() {
        let data = Rc::new(RefCell::new(Vec::new()));
        let writes = Rc::new(RefCell::new(0));
        let closes = Rc::new(RefCell::new(0));

        spawn({
            let data = Rc::clone(&data);
            let writes = Rc::clone(&writes);
            let closes = Rc::clone(&closes);
            async move {
                let inner = RecordingWriter {
                    data,
                    writes,
                    closes,
                };
                let mut writer = BufWriter::with_capacity(8, inner);
                writer.write_all(b"close me").await.unwrap();
                writer.close().await.unwrap();
            }
        });

        run();
        assert_eq!(&*data.borrow(), b"close me");
        assert_eq!(*writes.borrow(), 1);
        assert_eq!(*closes.borrow(), 1);
    }
}

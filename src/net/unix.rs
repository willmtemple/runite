//! Async Unix domain socket primitives.
//!
//! This module provides stream, listener, and datagram sockets for local
//! interprocess communication on Unix platforms. Paths are encoded exactly as
//! Unix socket path names and must not contain interior NUL bytes.
//!
//! # Examples
//!
//! Connected socket pairs are deterministic and do not require a filesystem
//! socket path:
//!
//! ```
//! runite::spawn(async {
//!     let (mut left, mut right) = runite::net::unix::UnixStream::pair().unwrap();
//!     left.write_all(b"x").await.unwrap();
//!
//!     let mut buf = [0; 1];
//!     let read = right.read(&mut buf).await.unwrap();
//!     assert_eq!(&buf[..read], b"x");
//! });
//! runite::run();
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::ffi::c_void;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::io::{AsyncRead, AsyncWrite, ReadOverflow, Stream};

type PendingRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;
type PendingShutdown = Pin<Box<dyn Future<Output = io::Result<()>> + 'static>>;
use crate::op::net::NetOp;

/// Async Unix domain stream socket.
///
/// `UnixStream` is a byte-oriented local socket similar to
/// [`std::os::unix::net::UnixStream`], but its read, write, and connect
/// operations integrate with the runite runtime.
///
/// It implements [`AsyncRead`] and
/// [`AsyncWrite`] (so it works with [`copy`](crate::io::copy),
/// [`BufReader`](crate::io::BufReader), and the `AsyncReadExt`/`AsyncWriteExt`
/// combinators), supports [`shutdown`](Self::shutdown), and can be
/// [`split`](Self::into_split) into owned read/write halves. Like the other
/// runite stream types it is effectively `!Send` and should be driven on its
/// owning runtime thread.
pub struct UnixStream {
    inner: Arc<UnixStreamInner>,
    pending_read: Option<PendingRead>,
    read_overflow: Option<Box<ReadOverflow>>,
    pending_write: Option<PendingWrite>,
    pending_write_ident: Option<(*const u8, usize)>,
    pending_shutdown: Option<PendingShutdown>,
}

#[derive(Debug)]
struct UnixStreamInner {
    fd: OwnedFd,
}

impl std::fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixStream")
            .field("fd", &self.inner.fd.as_raw_fd())
            .finish_non_exhaustive()
    }
}

/// Async Unix domain listening socket.
///
/// A listener accepts inbound [`UnixStream`] connections from a filesystem
/// socket path on Unix platforms.
#[derive(Debug)]
pub struct UnixListener {
    fd: OwnedFd,
}

/// Async Unix domain datagram socket.
///
/// `UnixDatagram` sends and receives message-oriented datagrams between Unix
/// domain socket paths or between connected datagram socket pairs.
#[derive(Debug)]
pub struct UnixDatagram {
    fd: OwnedFd,
}

impl UnixStream {
    /// Connects to a Unix domain stream socket at `path`.
    ///
    /// The path must name an existing Unix stream listener.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut stream = runite::net::unix::UnixStream::connect("service.sock")
    ///         .await
    ///         .unwrap();
    ///     stream.write_all(b"ping").await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let fd = socket(libc::SOCK_STREAM)?;
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        connect_async(fd.as_raw_fd(), &addr).await?;
        Ok(Self::from_owned_fd(fd))
    }

    /// Creates a pair of connected Unix domain stream sockets.
    ///
    /// The pair is already connected and does not create a filesystem entry.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (left, right) = std::os::unix::net::UnixStream::pair()?;
        left.set_nonblocking(true)?;
        right.set_nonblocking(true)?;
        Ok((
            // SAFETY: `into_raw_fd` transfers ownership of this fresh pair
            // endpoint, and `OwnedFd` takes it exactly once.
            Self::from_owned_fd(unsafe { OwnedFd::from_raw_fd(left.into_raw_fd()) }),
            // SAFETY: `into_raw_fd` transfers ownership of this fresh pair
            // endpoint, and `OwnedFd` takes it exactly once.
            Self::from_owned_fd(unsafe { OwnedFd::from_raw_fd(right.into_raw_fd()) }),
        ))
    }

    /// Reads bytes from the stream.
    ///
    /// Returns the number of bytes copied into `buf`. A return value of `0`
    /// indicates EOF when `buf` is not empty.
    ///
    /// Delegates to the [`AsyncRead`] path so the in-flight
    /// read is stashed on the stream and is cancel-safe: a dropped read future
    /// retains its bytes for the next read.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        core::future::poll_fn(|cx| Pin::new(&mut *self).poll_read(cx, buf)).await
    }

    /// Writes bytes to the stream.
    ///
    /// The operation may write fewer bytes than `buf.len()`; use
    /// [`write_all`](Self::write_all) to keep writing until the full buffer is
    /// sent.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        core::future::poll_fn(|cx| Pin::new(&mut *self).poll_write(cx, buf)).await
    }

    /// Writes the entire buffer to the stream.
    pub async fn write_all(&mut self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let written = self.write(buf).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            buf = &buf[written..];
        }
        Ok(())
    }

    /// Returns the local socket address of this stream.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: `self.raw_fd()` remains owned by `self`; `ManuallyDrop`
        // prevents the temporary std stream from closing it.
        let stream = ManuallyDrop::new(unsafe {
            std::os::unix::net::UnixStream::from_raw_fd(self.raw_fd())
        });
        stream.local_addr()
    }

    /// Returns the remote peer address of this stream.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: `self.raw_fd()` remains owned by `self`; `ManuallyDrop`
        // prevents the temporary std stream from closing it.
        let stream = ManuallyDrop::new(unsafe {
            std::os::unix::net::UnixStream::from_raw_fd(self.raw_fd())
        });
        stream.peer_addr()
    }

    /// Shuts down the read, write, or both halves of the connection.
    pub async fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        crate::sys::current::net::shutdown_future(self.raw_fd(), how).await
    }

    /// Splits this stream into independently owned read and write halves.
    ///
    /// The halves share the underlying socket via reference counting, so they
    /// can be moved into separate tasks on the same runite runtime thread to
    /// read and write concurrently. Recombine them with
    /// [`UnixStream::reunite`].
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let read = Self::from_shared(Arc::clone(&self.inner));
        let write = Self::from_shared(self.inner);
        (
            OwnedReadHalf { stream: read },
            OwnedWriteHalf { stream: write },
        )
    }

    /// Reassembles a [`UnixStream`] from the two halves produced by
    /// [`into_split`](Self::into_split).
    ///
    /// Returns [`ReuniteError`] if the halves came from different streams.
    #[allow(clippy::result_large_err)]
    pub fn reunite(read: OwnedReadHalf, write: OwnedWriteHalf) -> Result<Self, ReuniteError> {
        if Arc::ptr_eq(&read.stream.inner, &write.stream.inner) {
            drop(read);
            Ok(write.stream)
        } else {
            Err(ReuniteError(read, write))
        }
    }

    fn from_owned_fd(fd: OwnedFd) -> Self {
        Self::from_shared(Arc::new(UnixStreamInner { fd }))
    }

    fn from_shared(inner: Arc<UnixStreamInner>) -> Self {
        Self {
            inner,
            pending_read: None,
            read_overflow: None,
            pending_write: None,
            pending_write_ident: None,
            pending_shutdown: None,
        }
    }

    fn raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();

        // Serve bytes a previous read produced that overflowed a smaller caller
        // buffer before submitting or polling a new read.
        if let Some(overflow) = this.read_overflow.as_mut() {
            let n = overflow.drain_into(buf);
            if overflow.is_drained() {
                this.read_overflow = None;
            }
            return Poll::Ready(Ok(n));
        }

        if this.pending_read.is_none() {
            this.pending_read = Some(crate::sys::current::net::recv_future(
                this.raw_fd(),
                buf.len(),
            ));
        }

        match this
            .pending_read
            .as_mut()
            .expect("pending read must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_read = None;
                let data = result?;
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                if data.len() > n {
                    this.read_overflow = Some(Box::new(ReadOverflow::new(&data[n..])));
                }
                Poll::Ready(Ok(n))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        let ident = (buf.as_ptr(), buf.len());
        if this.pending_write.is_none() {
            this.pending_write = Some(crate::sys::current::net::send_future(
                this.raw_fd(),
                buf.to_vec(),
            ));
            this.pending_write_ident = Some(ident);
        } else if this.pending_write_ident != Some(ident) {
            // A write is in flight for a *different* buffer: the future that
            // submitted it was dropped mid-flight. Those bytes are already
            // committed to the kernel, so counting this op against the new
            // buffer would corrupt the stream. Reject instead.
            return Poll::Ready(Err(io::Error::other(
                "write buffer changed while a previous write was still in flight",
            )));
        }

        match this
            .pending_write
            .as_mut()
            .expect("pending write must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_write = None;
                this.pending_write_ident = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pending_shutdown.is_none() {
            this.pending_shutdown = Some(crate::sys::current::net::shutdown_future(
                this.raw_fd(),
                Shutdown::Write,
            ));
        }

        match this
            .pending_shutdown
            .as_mut()
            .expect("pending shutdown must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_shutdown = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Owned read half of a [`UnixStream`], created by
/// [`UnixStream::into_split`]. Implements [`AsyncRead`].
#[derive(Debug)]
pub struct OwnedReadHalf {
    stream: UnixStream,
}

/// Owned write half of a [`UnixStream`], created by
/// [`UnixStream::into_split`]. Implements [`AsyncWrite`];
/// use [`shutdown`](Self::shutdown) to half-close the write direction.
#[derive(Debug)]
pub struct OwnedWriteHalf {
    stream: UnixStream,
}

impl OwnedReadHalf {
    /// Reassembles the original [`UnixStream`] with the matching write half.
    #[allow(clippy::result_large_err)]
    pub fn reunite(self, write: OwnedWriteHalf) -> Result<UnixStream, ReuniteError> {
        UnixStream::reunite(self, write)
    }
}

impl OwnedWriteHalf {
    /// Half-closes the write direction of the connection with
    /// [`Shutdown::Write`].
    pub async fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Write).await
    }

    /// Reassembles the original [`UnixStream`] with the matching read half.
    #[allow(clippy::result_large_err)]
    pub fn reunite(self, read: OwnedReadHalf) -> Result<UnixStream, ReuniteError> {
        UnixStream::reunite(read, self)
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_close(cx)
    }
}

/// Error returned by [`UnixStream::reunite`] when the two halves did not
/// originate from the same [`UnixStream`]. Returns ownership of both halves.
pub struct ReuniteError(pub OwnedReadHalf, pub OwnedWriteHalf);

impl std::fmt::Debug for ReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReuniteError(..)")
    }
}

impl std::fmt::Display for ReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("tried to reunite halves from different UnixStreams")
    }
}

impl std::error::Error for ReuniteError {}

impl UnixListener {
    /// Binds a Unix domain stream listener to `path`.
    ///
    /// The path must not already exist. Remove a stale socket file before
    /// binding if your application owns that path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let listener = runite::net::unix::UnixListener::bind("runite.sock").unwrap();
    /// assert!(listener.local_addr().unwrap().as_pathname().is_some());
    /// ```
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let fd = socket(libc::SOCK_STREAM)?;
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        bind_sync(fd.as_raw_fd(), &addr)?;
        listen_sync(fd.as_raw_fd(), 1024)?;
        Ok(Self { fd })
    }

    /// Accepts an incoming connection.
    ///
    /// The returned address is the peer address reported by the operating
    /// system for the accepted stream.
    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        loop {
            match accept_sync(self.raw_fd()) {
                Ok((fd, addr)) => return Ok((UnixStream::from_owned_fd(fd), addr)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    crate::sys::current::fd::wait_readable(self.raw_fd()).await?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Returns a [`Stream`] that yields inbound connections as they arrive.
    ///
    /// The stream is infinite: it never yields `None`. Borrows the listener for
    /// the lifetime of the stream, so use [`accept`](Self::accept) directly when
    /// a borrowed stream adapter is not convenient.
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming {
            listener: self,
            pending: None,
        }
    }

    /// Returns the local socket address of this listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        // SAFETY: `self.raw_fd()` remains owned by `self`; `ManuallyDrop`
        // prevents the temporary std listener from closing it.
        let listener = ManuallyDrop::new(unsafe {
            std::os::unix::net::UnixListener::from_raw_fd(self.raw_fd())
        });
        listener.local_addr()
    }

    fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Stream of inbound Unix domain connections.
///
/// Created by [`UnixListener::incoming`], this borrowed stream repeatedly
/// accepts new connections from its listener. It yields `Some(Err(_))` for
/// accept errors and does not terminate on its own.
pub struct Incoming<'a> {
    listener: &'a UnixListener,
    pending: Option<Pin<Box<dyn Future<Output = io::Result<UnixStream>> + 'a>>>,
}

impl std::fmt::Debug for Incoming<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Incoming")
            .field("listener", self.listener)
            .finish_non_exhaustive()
    }
}

impl Stream for Incoming<'_> {
    type Item = io::Result<UnixStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.pending.is_none() {
            let fd = this.listener.raw_fd();
            this.pending = Some(Box::pin(async move {
                loop {
                    match accept_sync(fd) {
                        Ok((stream_fd, _addr)) => {
                            return Ok(UnixStream::from_owned_fd(stream_fd));
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            crate::sys::current::fd::wait_readable(fd).await?;
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                        Err(error) => return Err(error),
                    }
                }
            }));
        }

        let future = this
            .pending
            .as_mut()
            .expect("pending accept future present");
        match future.as_mut().poll(cx) {
            Poll::Ready(result) => {
                this.pending = None;
                Poll::Ready(Some(result))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl UnixDatagram {
    /// Binds a Unix domain datagram socket to `path`.
    ///
    /// The path must not already exist. Remove a stale socket file before
    /// binding if your application owns that path.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let fd = socket(libc::SOCK_DGRAM)?;
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        bind_sync(fd.as_raw_fd(), &addr)?;
        Ok(Self { fd })
    }

    /// Creates an unbound Unix domain datagram socket.
    ///
    /// An unbound socket can be connected to a peer path or used for sending to
    /// explicit paths.
    pub fn unbound() -> io::Result<Self> {
        socket(libc::SOCK_DGRAM).map(|fd| Self { fd })
    }

    /// Creates a pair of connected Unix domain datagram sockets.
    ///
    /// The pair is already connected and does not create filesystem entries.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let (left, right) = runite::net::unix::UnixDatagram::pair().unwrap();
    ///     left.send(b"x").await.unwrap();
    ///
    ///     let mut buf = [0; 1];
    ///     let read = right.recv(&mut buf).await.unwrap();
    ///     assert_eq!(&buf[..read], b"x");
    /// });
    /// runite::run();
    /// ```
    pub fn pair() -> io::Result<(Self, Self)> {
        let (left, right) = std::os::unix::net::UnixDatagram::pair()?;
        left.set_nonblocking(true)?;
        right.set_nonblocking(true)?;
        Ok((
            Self {
                // SAFETY: `into_raw_fd` transfers ownership of this fresh pair
                // endpoint, and `OwnedFd` takes it exactly once.
                fd: unsafe { OwnedFd::from_raw_fd(left.into_raw_fd()) },
            },
            Self {
                // SAFETY: `into_raw_fd` transfers ownership of this fresh pair
                // endpoint, and `OwnedFd` takes it exactly once.
                fd: unsafe { OwnedFd::from_raw_fd(right.into_raw_fd()) },
            },
        ))
    }

    /// Connects the socket to a default peer.
    ///
    /// Once connected, [`send`](Self::send) and [`recv`](Self::recv) operate
    /// relative to that peer.
    pub async fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        connect_async(self.raw_fd(), &addr).await
    }

    /// Receives a datagram from the connected peer.
    ///
    /// The socket must be connected first with [`connect`](Self::connect) or
    /// created by [`pair`](Self::pair). If `buf` is smaller than the datagram,
    /// the returned length is capped at `buf.len()` and the operating system
    /// discards the excess bytes.
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let data = crate::sys::current::net::recv(NetOp::Recv {
            fd: self.raw_fd(),
            len: buf.len(),
            flags: 0,
        })
        .await?;
        let read = data.len();
        buf[..read].copy_from_slice(&data);
        Ok(read)
    }

    /// Receives a datagram and returns the sender address.
    ///
    /// If `buf` is smaller than the datagram, the returned length is capped at
    /// `buf.len()` and the operating system discards the excess bytes.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            match recv_from_sync(self.raw_fd(), buf) {
                Ok(result) => return Ok(result),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    crate::sys::current::fd::wait_readable(self.raw_fd()).await?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Sends a datagram to the connected peer.
    ///
    /// The socket must be connected first with [`connect`](Self::connect) or
    /// created by [`pair`](Self::pair).
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        crate::sys::current::net::send(NetOp::Send {
            fd: self.raw_fd(),
            data: buf.to_vec(),
            flags: 0,
        })
        .await
    }

    /// Sends a datagram to `path`.
    ///
    /// This method does not change the socket's default peer.
    pub async fn send_to(&self, buf: &[u8], path: impl AsRef<Path>) -> io::Result<usize> {
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        loop {
            match send_to_sync(self.raw_fd(), buf, &addr) {
                Ok(sent) => return Ok(sent),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_writable(self.raw_fd()).await?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

struct RawUnixSocketAddr {
    addr: libc::sockaddr_un,
    len: libc::socklen_t,
}

impl RawUnixSocketAddr {
    fn from_path(path: &Path) -> io::Result<Self> {
        let bytes = path.as_os_str().as_bytes();
        if bytes.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unix socket path contains an interior NUL byte",
            ));
        }

        // SAFETY: sockaddr_un is a plain C address struct; an all-zero value is
        // valid before filling sun_family and sun_path.
        let mut addr = unsafe { MaybeUninit::<libc::sockaddr_un>::zeroed().assume_init() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
        {
            addr.sun_len = 0;
        }

        if bytes.len() >= addr.sun_path.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unix socket path is too long",
            ));
        }

        for (slot, byte) in addr.sun_path.iter_mut().zip(bytes.iter().copied()) {
            *slot = byte as libc::c_char;
        }

        let len = sockaddr_un_path_offset(&addr) + bytes.len() + 1;
        let len = libc::socklen_t::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unix socket address length exceeds socklen_t",
            )
        })?;
        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
        {
            addr.sun_len = u8::try_from(len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Unix socket address length exceeds sun_len",
                )
            })?;
        }

        Ok(Self { addr, len })
    }

    fn as_ptr(&self) -> *const libc::sockaddr {
        &self.addr as *const libc::sockaddr_un as *const libc::sockaddr
    }
}

fn socket(socket_type: i32) -> io::Result<OwnedFd> {
    // SAFETY: socket takes only integer arguments; no user pointers are passed.
    let fd = cvt(unsafe { libc::socket(libc::AF_UNIX, socket_type, 0) })?;
    if let Err(error) = set_cloexec(fd).and_then(|_| set_nonblocking(fd)) {
        // SAFETY: `fd` is the fresh descriptor returned by socket and has not
        // been wrapped, so closing it here releases it exactly once.
        let _ = unsafe { libc::close(fd) };
        return Err(error);
    }
    // SAFETY: `fd` is a fresh descriptor returned by successful socket and
    // ownership is transferred to `OwnedFd` exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

async fn connect_async(fd: RawFd, addr: &RawUnixSocketAddr) -> io::Result<()> {
    loop {
        // SAFETY: `fd` is valid for the duration of the call, and `addr`
        // points to `addr.len` initialized bytes describing a sockaddr_un.
        let result = unsafe { libc::connect(fd, addr.as_ptr(), addr.len) };
        if result == 0 {
            return Ok(());
        }

        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => {}
            Some(libc::EINPROGRESS) | Some(libc::EALREADY) => {
                wait_writable(fd).await?;
                return socket_error(fd);
            }
            Some(libc::EISCONN) => return Ok(()),
            _ => return Err(error),
        }
    }
}

fn bind_sync(fd: RawFd, addr: &RawUnixSocketAddr) -> io::Result<()> {
    // SAFETY: `fd` is valid for the duration of the call, and `addr` points to
    // `addr.len` initialized bytes describing a sockaddr_un.
    cvt(unsafe { libc::bind(fd, addr.as_ptr(), addr.len) }).map(|_| ())
}

fn listen_sync(fd: RawFd, backlog: i32) -> io::Result<()> {
    // SAFETY: `fd` is a valid socket descriptor for the duration of the call;
    // listen takes no user pointers.
    cvt(unsafe { libc::listen(fd, backlog) }).map(|_| ())
}

fn accept_sync(fd: RawFd) -> io::Result<(OwnedFd, SocketAddr)> {
    // SAFETY: `fd` is a valid listener descriptor; null address pointers are
    // allowed when the peer address is not requested from accept.
    let accepted = cvt(unsafe {
        libc::accept(
            fd,
            std::ptr::null_mut::<libc::sockaddr>(),
            std::ptr::null_mut::<libc::socklen_t>(),
        )
    })?;
    if let Err(error) = set_cloexec(accepted).and_then(|_| set_nonblocking(accepted)) {
        // SAFETY: `accepted` is a fresh descriptor not wrapped by OwnedFd yet,
        // so closing it here releases it exactly once.
        let _ = unsafe { libc::close(accepted) };
        return Err(error);
    }

    // SAFETY: `accepted` is a fresh descriptor returned by accept and ownership
    // is transferred to `OwnedFd` exactly once.
    let owned = unsafe { OwnedFd::from_raw_fd(accepted) };
    let addr = {
        // SAFETY: `owned` retains ownership of the descriptor; `ManuallyDrop`
        // prevents the temporary std stream from closing it.
        let stream = ManuallyDrop::new(unsafe {
            std::os::unix::net::UnixStream::from_raw_fd(owned.as_raw_fd())
        });
        stream.peer_addr()?
    };
    Ok((owned, addr))
}

fn recv_from_sync(fd: RawFd, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
    // SAFETY: `fd` remains owned by the caller; `ManuallyDrop` prevents the
    // temporary std datagram socket from closing it.
    let socket = ManuallyDrop::new(unsafe { std::os::unix::net::UnixDatagram::from_raw_fd(fd) });
    socket.recv_from(buf)
}

fn send_to_sync(fd: RawFd, buf: &[u8], addr: &RawUnixSocketAddr) -> io::Result<usize> {
    // SAFETY: `fd` is valid for the duration of the call; `buf` is readable for
    // `buf.len()` bytes and `addr` points to an initialized sockaddr_un.
    let sent = unsafe {
        libc::sendto(
            fd,
            buf.as_ptr().cast::<c_void>(),
            buf.len(),
            0,
            addr.as_ptr(),
            addr.len,
        )
    };
    cvt_long(sent).map(|sent| sent as usize)
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is valid for the duration of the fcntl call; F_GETFD uses no
    // user pointers.
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFD) })?;
    // SAFETY: `fd` is valid for the duration of the fcntl call; F_SETFD uses
    // the integer flags argument and no user pointers.
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) })?;
    Ok(())
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is valid for the duration of the fcntl call; F_GETFL uses no
    // user pointers.
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    // SAFETY: `fd` is valid for the duration of the fcntl call; F_SETFL uses
    // the integer flags argument and no user pointers.
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
    Ok(())
}

fn socket_error(fd: RawFd) -> io::Result<()> {
    let mut so_error: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` is valid for the duration of the call, and `so_error`/`len`
    // point to writable initialized storage for SO_ERROR.
    cvt(unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &mut so_error as *mut libc::c_int as *mut c_void,
            &mut len,
        )
    })?;
    if so_error == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(so_error))
    }
}

async fn wait_writable(fd: RawFd) -> io::Result<()> {
    crate::sys::current::fd::wait_writable(fd).await
}

fn sockaddr_un_path_offset(addr: &libc::sockaddr_un) -> usize {
    let base = addr as *const libc::sockaddr_un as usize;
    let path = addr.sun_path.as_ptr() as usize;
    path - base
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

fn cvt_long(value: libc::ssize_t) -> io::Result<libc::ssize_t> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

// -- File-descriptor interop -------------------------------------------------
//
// This module is already `#[cfg(unix)]`, so these fd-based impls need no further
// gating.

impl AsFd for UnixStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

impl From<OwnedFd> for UnixStream {
    /// Adopts an already-connected Unix-domain stream socket. Use
    /// [`UnixStream::from_std`] to adopt a [`std::os::unix::net::UnixStream`] and
    /// set the mode runite's backend expects.
    fn from(fd: OwnedFd) -> Self {
        Self::from_owned_fd(fd)
    }
}

impl UnixStream {
    /// Adopts a blocking [`std::os::unix::net::UnixStream`] and switches it to
    /// the non-blocking mode runite's driver expects.
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> io::Result<Self> {
        let fd = OwnedFd::from(stream);
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }
}

impl AsFd for UnixListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl From<OwnedFd> for UnixListener {
    /// Adopts an already-listening Unix-domain socket. Use
    /// [`UnixListener::from_std`] to adopt a std listener and set the mode.
    fn from(fd: OwnedFd) -> Self {
        Self { fd }
    }
}

impl UnixListener {
    /// Adopts a blocking [`std::os::unix::net::UnixListener`] and switches it to
    /// non-blocking mode.
    pub fn from_std(listener: std::os::unix::net::UnixListener) -> io::Result<Self> {
        let fd = OwnedFd::from(listener);
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self { fd })
    }
}

impl AsFd for UnixDatagram {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

impl AsRawFd for UnixDatagram {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl From<OwnedFd> for UnixDatagram {
    /// Adopts an existing Unix-domain datagram socket. Use
    /// [`UnixDatagram::from_std`] to adopt a std datagram socket and set the mode.
    fn from(fd: OwnedFd) -> Self {
        Self { fd }
    }
}

impl UnixDatagram {
    /// Adopts a blocking [`std::os::unix::net::UnixDatagram`] and switches it to
    /// non-blocking mode.
    pub fn from_std(socket: std::os::unix::net::UnixDatagram) -> io::Result<Self> {
        let fd = OwnedFd::from(socket);
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self { fd })
    }
}

// -- Hyper transport integration (feature = "hyper") --------------------------

/// `hyper::rt` transport impls for [`UnixStream`], mirroring the
/// [`TcpStream`](crate::net::TcpStream) integration so hyper can speak HTTP
/// over Unix-domain sockets (Docker socket, systemd services, local RPC).
/// Uses the same stashed pending-op + `ReadOverflow` machinery as the
/// `AsyncRead`/`AsyncWrite` impls, so reads stay cancel-safe.
#[cfg(feature = "hyper")]
mod hyper_impl {
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::io;
    use std::net::Shutdown;

    use hyper::rt::{Read as HyperRead, ReadBufCursor, Write as HyperWrite};

    use super::UnixStream;

    impl HyperRead for UnixStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: ReadBufCursor<'_>,
        ) -> Poll<Result<(), io::Error>> {
            let this = self.get_mut();
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            // Serve any surplus from a previous read before submitting a new one.
            if let Some(overflow) = this.read_overflow.as_mut() {
                let n = buf.remaining().min(overflow.remaining());
                buf.put_slice(overflow.front(n));
                overflow.advance(n);
                if overflow.is_drained() {
                    this.read_overflow = None;
                }
                return Poll::Ready(Ok(()));
            }

            if this.pending_read.is_none() {
                this.pending_read = Some(crate::sys::current::net::recv_future(
                    this.raw_fd(),
                    buf.remaining(),
                ));
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
                    // `put_slice` panics if given more than the cursor can hold,
                    // so copy only what fits and retain the surplus.
                    let n = data.len().min(buf.remaining());
                    buf.put_slice(&data[..n]);
                    if data.len() > n {
                        this.read_overflow =
                            Some(Box::new(crate::io::ReadOverflow::new(&data[n..])));
                    }
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

    impl HyperWrite for UnixStream {
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
                this.pending_write = Some(crate::sys::current::net::send_future(
                    this.raw_fd(),
                    buf.to_vec(),
                ));
            }

            let poll = this
                .pending_write
                .as_mut()
                .expect("pending write future should exist")
                .as_mut()
                .poll(cx);
            match poll {
                Poll::Ready(result) => {
                    this.pending_write = None;
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
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
                Poll::Ready(result) => {
                    this.pending_shutdown = None;
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use crate::{queue_macrotask, run, spawn};

    use super::{UnixDatagram, UnixListener, UnixStream};

    #[test]
    fn unix_stream_pair_round_trip() {
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);

        queue_macrotask(move || {
            let received_for_task = Arc::clone(&received_for_task);
            spawn(async move {
                let (mut left, mut right) = UnixStream::pair().expect("stream pair should open");
                left.write_all(b"ping")
                    .await
                    .expect("stream write should succeed");

                let mut buffer = [0; 16];
                let read = right
                    .read(&mut buffer)
                    .await
                    .expect("stream read should succeed");
                *received_for_task.lock().unwrap() = Some(buffer[..read].to_vec());
            });
        });
        run();

        assert_eq!(
            received.lock().unwrap().as_deref(),
            Some(b"ping".as_slice())
        );
    }

    #[test]
    fn unix_listener_accept_round_trip() {
        let path = test_socket_path("stream");
        remove_socket_file(&path);
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);
        let path_for_task = path.clone();

        queue_macrotask(move || {
            let received_for_task = Arc::clone(&received_for_task);
            spawn(async move {
                let listener = Arc::new(
                    UnixListener::bind(&path_for_task).expect("listener should bind to path"),
                );
                assert_eq!(
                    listener.local_addr().unwrap().as_pathname(),
                    Some(path_for_task.as_path())
                );

                let listener_for_accept = Arc::clone(&listener);
                let server = spawn(async move {
                    let (mut stream, _peer_addr) = listener_for_accept
                        .accept()
                        .await
                        .expect("listener should accept");
                    let mut buffer = [0; 16];
                    let read = stream
                        .read(&mut buffer)
                        .await
                        .expect("server read should succeed");
                    stream
                        .write_all(b"pong")
                        .await
                        .expect("server write should succeed");
                    buffer[..read].to_vec()
                });

                let mut client = UnixStream::connect(&path_for_task)
                    .await
                    .expect("client should connect");
                client
                    .write_all(b"ping")
                    .await
                    .expect("client write should succeed");
                let mut response = [0; 16];
                let read = client
                    .read(&mut response)
                    .await
                    .expect("client read should succeed");
                assert_eq!(&response[..read], b"pong");

                *received_for_task.lock().unwrap() =
                    Some(server.await.expect("server task should not be aborted"));
            });
        });
        run();

        assert_eq!(
            received.lock().unwrap().as_deref(),
            Some(b"ping".as_slice())
        );
        remove_socket_file(&path);
    }

    #[test]
    fn unix_datagram_send_recv() {
        let server_path = test_socket_path("dgram-server");
        let client_path = test_socket_path("dgram-client");
        remove_socket_file(&server_path);
        remove_socket_file(&client_path);
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);
        let server_path_for_task = server_path.clone();
        let client_path_for_task = client_path.clone();

        queue_macrotask(move || {
            let received_for_task = Arc::clone(&received_for_task);
            spawn(async move {
                let server = UnixDatagram::bind(&server_path_for_task).expect("server should bind");
                let client = UnixDatagram::bind(&client_path_for_task).expect("client should bind");

                client
                    .send_to(b"ping", &server_path_for_task)
                    .await
                    .expect("client send_to should succeed");

                let mut buffer = [0; 16];
                let (read, peer) = server
                    .recv_from(&mut buffer)
                    .await
                    .expect("server recv_from should succeed");
                assert_eq!(peer.as_pathname(), Some(client_path_for_task.as_path()));
                *received_for_task.lock().unwrap() = Some(buffer[..read].to_vec());
            });
        });
        run();

        assert_eq!(
            received.lock().unwrap().as_deref(),
            Some(b"ping".as_slice())
        );
        remove_socket_file(&server_path);
        remove_socket_file(&client_path);
    }

    fn test_socket_path(name: &str) -> PathBuf {
        let dir = PathBuf::from("target").join("runite-uds-tests");
        std::fs::create_dir_all(&dir).expect("test socket directory should be created");
        dir.join(format!(
            "{}-{}-{:?}.sock",
            name,
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn remove_socket_file(path: &Path) {
        let _ = std::fs::remove_file(path);
    }
}

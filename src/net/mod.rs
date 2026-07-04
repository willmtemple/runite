//! Portable async networking primitives.
//!
//! This module provides TCP and UDP sockets that integrate with runite's
//! event-loop-per-thread runtime. The public surface follows the general shape
//! of [`std::net`], but uses async methods for operations that would otherwise
//! block the caller. Unix domain sockets are available from the `unix`
//! submodule on Unix
//! platforms and are re-exported from this module.
//!
//! # Network model
//!
//! Networking operations are tied to the current runite event loop. `TcpStream`,
//! split halves, UDP sockets, and their pending I/O futures are effectively
//! current-thread values; drive them on the runtime thread that owns the
//! operation, and move work to another thread by sending a macrotask to that
//! thread rather than by migrating the socket future. Linux uses
//! completion-based `io_uring` operations, while macOS aarch64 waits for
//! readiness with `kqueue` and then performs nonblocking socket calls.
//!
//! This differs from Tokio's default scheduler and async-std: runite has no
//! work-stealing socket executor, and split TCP halves are intended for separate
//! runite tasks on the same runtime thread, not cross-thread ownership.
//!
//! # Examples
//!
//! A TCP listener and client can run on the same local runtime loop:
//!
//! ```
//! use runite::net::{TcpListener, TcpStream};
//!
//! runite::spawn(async {
//!     let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//!     let address = listener.local_addr().unwrap();
//!
//!     let server = runite::spawn(async move {
//!         let (mut stream, _) = listener.accept().await.unwrap();
//!         let mut byte = [0];
//!         stream.read_exact(&mut byte).await.unwrap();
//!         stream.write_all(&byte).await.unwrap();
//!     });
//!
//!     let mut client = TcpStream::connect(address).await.unwrap();
//!     client.write_all(b"x").await.unwrap();
//!     let mut echoed = [0];
//!     client.read_exact(&mut echoed).await.unwrap();
//!     assert_eq!(echoed, *b"x");
//!     server.await.unwrap();
//! });
//!
//! runite::run();
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use std::io;
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};

use crate::io::{AsyncRead, AsyncWrite, Stream};
use crate::op::net::NetOp;
use crate::sys::handle::{OwnedSock, RawSock, owned_sock_from_raw, raw_sock};

#[cfg(feature = "hyper")]
mod hyper_impl;
mod interop;
#[cfg(unix)]
pub mod unix;

#[cfg(unix)]
pub use unix::{UnixDatagram, UnixListener, UnixStream};

#[derive(Debug)]
struct TcpStreamInner {
    fd: OwnedSock,
    timeouts: Mutex<SocketTimeouts>,
}

#[derive(Debug)]
struct TcpListenerInner {
    fd: OwnedSock,
}

#[derive(Debug)]
struct UdpSocketInner {
    fd: OwnedSock,
    timeouts: Mutex<SocketTimeouts>,
}

#[derive(Clone, Copy, Debug, Default)]
struct SocketTimeouts {
    read: Option<Duration>,
    write: Option<Duration>,
}

type PendingRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;
type PendingShutdown = Pin<Box<dyn Future<Output = io::Result<()>> + 'static>>;

use crate::io::ReadOverflow;

/// Async TCP stream connected to a peer.
///
/// `TcpStream` owns a connected stream socket and provides async byte-oriented
/// reads, writes, shutdown, socket option access, and owned split halves. Reads
/// and writes may complete partially; use [`read_exact`](Self::read_exact) or
/// [`write_all`](Self::write_all) when a protocol needs a full buffer.
///
/// Pending operations are stored in the stream and are tied to the current
/// runite event loop. Dropping a pending read, write, timeout, or shutdown
/// future cancels interest in that operation but cannot roll back bytes already
/// transferred by the operating system. The type is effectively `!Send`, and
/// should be driven on its owning runtime thread.
///
/// With the `hyper` feature enabled, it also implements Hyper's runtime I/O
/// traits so it can be used directly as an HTTP transport.
pub struct TcpStream {
    inner: Arc<TcpStreamInner>,
    pending_read: Option<PendingRead>,
    /// Bytes a completed read produced that did not fit the caller's buffer
    /// (e.g. when a read future is dropped after being submitted with a large
    /// buffer and a later `poll_read` presents a smaller one). Served before any
    /// new read is submitted so no received bytes are ever lost. Boxed so the
    /// common no-overflow case is just a null pointer.
    read_overflow: Option<Box<ReadOverflow>>,
    pending_write: Option<PendingWrite>,
    /// Identity `(ptr, len)` of the buffer that `pending_write` was submitted
    /// for. A re-poll presenting a different buffer means the original write
    /// future was dropped mid-flight and an unrelated write started; reporting
    /// the in-flight op's byte count against the new buffer would corrupt the
    /// stream, so that is rejected instead.
    pending_write_ident: Option<(*const u8, usize)>,
    pending_shutdown: Option<PendingShutdown>,
}

impl std::fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpStream")
            .field("fd", &raw_sock(&self.inner.fd))
            .finish_non_exhaustive()
    }
}

/// Async TCP listening socket.
///
/// A listener accepts inbound [`TcpStream`] connections from a bound local
/// address. Use [`TcpListener::accept`] for one connection at a time or
/// [`TcpListener::incoming`] for a [`Stream`] adapter.
#[derive(Debug)]
pub struct TcpListener {
    inner: Arc<TcpListenerInner>,
}

/// Configurable TCP socket builder.
///
/// `TcpSocket` creates an unbound TCP socket so options such as `SO_REUSEADDR`
/// and `SO_REUSEPORT` can be configured before calling [`bind`](Self::bind).
/// Socket options that affect binding should be set before `bind`; changing
/// them after binding is platform-specific and may not affect the bound socket.
/// `SO_REUSEPORT` is platform support dependent: runite exposes the OS error if
/// the option is unavailable or invalid for the socket. To bind multiple
/// listeners to the same address, every participating socket must enable
/// `SO_REUSEPORT` before binding. This is useful for per-core accept loops or
/// other load-distribution designs where the kernel spreads incoming
/// connections across multiple listener sockets.
///
/// # Examples
///
/// ```
/// runite::spawn(async {
///     let socket = runite::net::TcpSocket::new_v4().unwrap();
///     socket.set_reuseaddr(true).unwrap();
///     socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
///     let listener = socket.listen(128).unwrap();
///     assert!(listener.local_addr().unwrap().port() != 0);
/// });
/// runite::run();
/// ```
#[derive(Debug)]
pub struct TcpSocket {
    fd: OwnedSock,
}

/// Async UDP socket.
///
/// `UdpSocket` sends and receives discrete datagrams. It can operate in
/// unconnected mode with [`send_to`](Self::send_to) and
/// [`recv_from`](Self::recv_from), or be connected to a default peer with
/// [`connect`](Self::connect).
#[derive(Debug)]
pub struct UdpSocket {
    inner: Arc<UdpSocketInner>,
}

impl TcpSocket {
    /// Creates a new unbound IPv4 TCP socket.
    ///
    /// The socket is created with close-on-exec and non-blocking behavior
    /// consistent with the runtime's TCP sockets.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    ///     let listener = socket.listen(128).unwrap();
    ///     assert!(listener.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub fn new_v4() -> io::Result<Self> {
        crate::sys::current::net::tcp_socket_v4().map(Self::from_owned_fd)
    }

    /// Creates a new unbound IPv6 TCP socket.
    ///
    /// The socket is created with close-on-exec and non-blocking behavior
    /// consistent with the runtime's TCP sockets.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v6().unwrap();
    ///     socket.bind("[::1]:0".parse().unwrap()).unwrap();
    ///     let listener = socket.listen(128).unwrap();
    ///     assert!(listener.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub fn new_v6() -> io::Result<Self> {
        crate::sys::current::net::tcp_socket_v6().map(Self::from_owned_fd)
    }

    /// Enables or disables `SO_REUSEADDR`.
    ///
    /// Set this option before [`bind`](Self::bind) when the bind behavior should
    /// be affected.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     socket.set_reuseaddr(true).unwrap();
    ///     assert!(socket.reuseaddr().unwrap());
    /// });
    /// runite::run();
    /// ```
    pub fn set_reuseaddr(&self, enabled: bool) -> io::Result<()> {
        crate::sys::current::net::set_reuse_addr(self.raw_fd(), enabled)
    }

    /// Reads the current `SO_REUSEADDR` setting.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     let _ = socket.reuseaddr().unwrap();
    /// });
    /// runite::run();
    /// ```
    pub fn reuseaddr(&self) -> io::Result<bool> {
        crate::sys::current::net::reuse_addr(self.raw_fd())
    }

    /// Enables or disables `SO_REUSEPORT`.
    ///
    /// Set this option before [`bind`](Self::bind) on every socket that should
    /// share the same local address. If the platform does not support the option
    /// for TCP sockets, the OS error is returned.
    ///
    /// # Examples
    ///
    /// Multiple listener sockets can share one local address when all of them opt
    /// in before binding. The kernel can then distribute accepts across
    /// per-thread or per-core listener loops.
    ///
    /// ```no_run
    /// use runite::net::TcpSocket;
    ///
    /// let first = TcpSocket::new_v4().unwrap();
    /// first.set_reuseaddr(true).unwrap();
    /// first.set_reuseport(true).unwrap();
    /// first.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    /// let addr = first.local_addr().unwrap();
    /// let first_listener = first.listen(128).unwrap();
    ///
    /// let second = TcpSocket::new_v4().unwrap();
    /// second.set_reuseaddr(true).unwrap();
    /// second.set_reuseport(true).unwrap();
    /// second.bind(addr).unwrap();
    /// let second_listener = second.listen(128).unwrap();
    ///
    /// assert_eq!(first_listener.local_addr().unwrap(), addr);
    /// assert_eq!(second_listener.local_addr().unwrap(), addr);
    /// ```
    pub fn set_reuseport(&self, enabled: bool) -> io::Result<()> {
        crate::sys::current::net::set_reuse_port(self.raw_fd(), enabled)
    }

    /// Reads the current `SO_REUSEPORT` setting.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     let _ = socket.reuseport().unwrap();
    /// });
    /// runite::run();
    /// ```
    pub fn reuseport(&self) -> io::Result<bool> {
        crate::sys::current::net::reuse_port(self.raw_fd())
    }

    /// Binds the socket to a local address.
    ///
    /// Configure binding-related options such as [`set_reuseaddr`](Self::set_reuseaddr)
    /// and [`set_reuseport`](Self::set_reuseport) before calling this method.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    ///     assert!(socket.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub fn bind(&self, addr: SocketAddr) -> io::Result<()> {
        crate::sys::current::net::bind_socket(self.raw_fd(), addr)
    }

    /// Marks the bound socket as a TCP listener.
    ///
    /// `backlog` is passed to the operating system's `listen(2)` after being
    /// checked to fit in an `i32`.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    ///     let listener = socket.listen(128).unwrap();
    ///     assert!(listener.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub fn listen(self, backlog: u32) -> io::Result<TcpListener> {
        let backlog = i32::try_from(backlog).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "backlog exceeds i32 range")
        })?;
        crate::sys::current::net::listen_socket(self.raw_fd(), backlog)?;
        Ok(TcpListener::from_owned_fd(self.fd))
    }

    /// Connects the socket to a remote address.
    ///
    /// This consumes the builder and returns the same [`TcpStream`] type as
    /// [`TcpStream::connect`].
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let listener = runite::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    ///     let addr = listener.local_addr().unwrap();
    ///     let server = runite::spawn(async move {
    ///         let (_stream, _peer) = listener.accept().await.unwrap();
    ///     });
    ///
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     let _stream = socket.connect(addr).await.unwrap();
    ///     server.await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn connect(self, addr: SocketAddr) -> io::Result<TcpStream> {
        crate::sys::current::net::connect(NetOp::Connect {
            fd: self.raw_fd(),
            addr,
        })
        .await?;
        Ok(TcpStream::from_owned_fd(self.fd))
    }

    /// Returns the local socket address.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    ///     assert!(socket.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        crate::sys::current::net::local_addr(self.raw_fd())
    }

    fn from_owned_fd(fd: OwnedSock) -> Self {
        Self { fd }
    }

    fn raw_fd(&self) -> RawSock {
        raw_sock(&self.fd)
    }
}

impl TcpStream {
    /// Connects to the first resolved address that succeeds.
    ///
    /// The address is resolved with [`ToSocketAddrs`]. If resolution returns
    /// more than one address, each endpoint is attempted until one connects.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut stream = runite::net::TcpStream::connect("127.0.0.1:8080")
    ///         .await
    ///         .unwrap();
    ///     stream.write_all(b"ping").await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn connect<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs + Send + 'static,
    {
        let addrs = crate::sys::current::net::resolve_addrs(addr).await?;
        let mut last_error = None;
        for addr in addrs {
            match crate::sys::current::net::connect_stream(addr).await {
                Ok(fd) => return Ok(Self::from_owned_fd(fd)),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "address resolution returned no usable TCP endpoints",
            )
        }))
    }

    /// Connects to `addr`, failing if the timeout elapses first.
    ///
    /// A zero timeout is rejected with [`io::ErrorKind::InvalidInput`]. If the
    /// timeout expires, the error kind is [`io::ErrorKind::TimedOut`].
    pub async fn connect_timeout(addr: &SocketAddr, timeout: Duration) -> io::Result<Self> {
        validate_timeout(timeout)?;
        crate::sys::current::net::connect_stream_timeout(*addr, timeout)
            .await
            .map(Self::from_owned_fd)
    }

    /// Reads bytes from the stream.
    ///
    /// Returns the number of bytes copied into `buf`. This may be fewer bytes
    /// than requested. A return value of `0` indicates EOF when `buf` is not
    /// empty. If a configured read timeout expires, the error kind is
    /// [`io::ErrorKind::TimedOut`].
    ///
    /// # Cancel safety
    ///
    /// This method is cancel-safe. If the returned future is dropped before it
    /// resolves, bytes that the in-flight read already received are retained on
    /// the stream and returned by the next read, so no data is lost — it is safe
    /// to use in a `select!`.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Delegate to the AsyncRead path so the in-flight recv is stashed on the
        // stream: dropping this future retains the operation (cancel-safe — a
        // completed-but-unclaimed read is served by the next read via the
        // overflow buffer) and it cannot race a concurrent trait-based read.
        core::future::poll_fn(|cx| Pin::new(&mut *self).poll_read(cx, buf)).await
    }

    /// Reads exactly `buf.len()` bytes from the stream.
    pub async fn read_exact(&mut self, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let read = self.read(buf).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            buf = &mut buf[read..];
        }
        Ok(())
    }

    /// Writes bytes to the stream.
    ///
    /// The operation may write fewer bytes than `buf.len()`. If a configured
    /// write timeout expires, the error kind is [`io::ErrorKind::TimedOut`]; use
    /// [`write_all`](Self::write_all) to keep writing until the full buffer is
    /// sent.
    ///
    /// # Cancel safety
    ///
    /// This method is **not** cancel-safe. Because the write is completion-based,
    /// a future dropped mid-flight may have already committed bytes to the
    /// kernel without reporting the count, and re-polling with a *different*
    /// buffer afterward is rejected. Drive a write to completion (or use the same
    /// buffer) rather than cancelling it in a `select!`.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Delegate to the AsyncWrite path so the in-flight send is stashed on the
        // stream and the buffer-identity guard applies uniformly across the
        // inherent and trait-based write APIs.
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

    /// Shuts down the read, write, or both halves of the connection.
    pub async fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        crate::sys::current::net::shutdown(NetOp::Shutdown {
            fd: self.raw_fd(),
            how,
        })
        .await
    }

    /// Splits this stream into independently owned read and write halves.
    ///
    /// The halves share the underlying socket via reference counting, so they
    /// can be moved into separate tasks on the same runite runtime thread to read
    /// and write concurrently. They are not a cross-thread split. Use
    /// [`OwnedReadHalf`]/[`OwnedWriteHalf`] and recombine them later with
    /// [`TcpStream::reunite`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use runite::io::{AsyncReadExt, AsyncWriteExt};
    /// use runite::net::TcpStream;
    ///
    /// # async fn example(stream: TcpStream) -> std::io::Result<()> {
    /// let (mut reader, mut writer) = stream.into_split();
    ///
    /// runite::spawn(async move {
    ///     let mut buf = [0; 1024];
    ///     let _ = reader.read(&mut buf).await;
    /// });
    ///
    /// writer.write_all(b"ping").await?;
    /// writer.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let read = Self {
            inner: Arc::clone(&self.inner),
            pending_read: None,
            read_overflow: None,
            pending_write: None,
            pending_write_ident: None,
            pending_shutdown: None,
        };
        let write = Self {
            inner: self.inner,
            pending_read: None,
            read_overflow: None,
            pending_write: None,
            pending_write_ident: None,
            pending_shutdown: None,
        };
        (
            OwnedReadHalf { stream: read },
            OwnedWriteHalf { stream: write },
        )
    }

    /// Reassembles a [`TcpStream`] from the two halves produced by
    /// [`into_split`](Self::into_split).
    ///
    /// Returns [`ReuniteError`] if the halves came from different streams.
    // ReuniteError intentionally carries both halves (each a full TcpStream)
    // so the caller gets them back, like tokio's ReuniteError; the large Err is
    // by design and only materializes on the rare mismatch path.
    #[allow(clippy::result_large_err)]
    pub fn reunite(read: OwnedReadHalf, write: OwnedWriteHalf) -> Result<Self, ReuniteError> {
        if Arc::ptr_eq(&read.stream.inner, &write.stream.inner) {
            drop(read);
            Ok(write.stream)
        } else {
            Err(ReuniteError(read, write))
        }
    }

    /// Duplicates the underlying stream socket.
    ///
    /// The returned stream refers to the same TCP connection but owns an
    /// independent file descriptor.
    pub async fn try_clone(&self) -> io::Result<Self> {
        crate::sys::current::net::duplicate(self.raw_fd())
            .await
            .map(Self::from_owned_fd)
    }

    /// Returns the local socket address of this stream.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        crate::sys::current::net::local_addr(self.raw_fd())
    }

    /// Returns the remote peer address of this stream.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        crate::sys::current::net::peer_addr(self.raw_fd())
    }

    /// Reads the current `TCP_NODELAY` setting.
    ///
    /// When enabled, small writes are sent without Nagle coalescing.
    pub fn nodelay(&self) -> io::Result<bool> {
        crate::sys::current::net::nodelay(self.raw_fd())
    }

    /// Enables or disables `TCP_NODELAY`.
    ///
    /// Enabling this option can reduce latency for protocols that exchange
    /// small messages.
    pub fn set_nodelay(&self, enabled: bool) -> io::Result<()> {
        crate::sys::current::net::set_nodelay(self.raw_fd(), enabled)
    }

    /// Reads the socket's IP time-to-live value.
    pub fn ttl(&self) -> io::Result<u32> {
        crate::sys::current::net::ttl(self.raw_fd())
    }

    /// Sets the socket's IP time-to-live value.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        crate::sys::current::net::set_ttl(self.raw_fd(), ttl)
    }

    /// Returns the read timeout used by async read operations on this stream.
    ///
    /// Split halves share this setting because they share the same stream state.
    pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.read_timeout_value())
    }

    /// Sets the read timeout used by async read operations on this stream.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected. If the timeout expires, read
    /// operations return [`io::ErrorKind::TimedOut`]. Split halves share this
    /// setting because they share the same stream state.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().read = timeout;
        Ok(())
    }

    /// Returns the write timeout used by async write operations on this stream.
    ///
    /// Split halves share this setting because they share the same stream state.
    pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.write_timeout_value())
    }

    /// Sets the write timeout used by async write operations on this stream.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected. If the timeout expires, write
    /// operations return [`io::ErrorKind::TimedOut`]. Split halves share this
    /// setting because they share the same stream state.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().write = timeout;
        Ok(())
    }

    fn from_owned_fd(fd: OwnedSock) -> Self {
        Self {
            inner: Arc::new(TcpStreamInner {
                fd,
                timeouts: Mutex::new(SocketTimeouts::default()),
            }),
            pending_read: None,
            read_overflow: None,
            pending_write: None,
            pending_write_ident: None,
            pending_shutdown: None,
        }
    }

    fn raw_fd(&self) -> RawSock {
        raw_sock(&self.inner.fd)
    }

    fn read_timeout_value(&self) -> Option<Duration> {
        self.inner.timeouts.lock().unwrap().read
    }

    fn write_timeout_value(&self) -> Option<Duration> {
        self.inner.timeouts.lock().unwrap().write
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();

        // Serve any bytes a previous read produced that overflowed a smaller
        // caller buffer before submitting (or polling) a new read.
        if let Some(overflow) = this.read_overflow.as_mut() {
            let n = overflow.drain_into(buf);
            if overflow.is_drained() {
                this.read_overflow = None;
            }
            return Poll::Ready(Ok(n));
        }

        if this.pending_read.is_none() {
            this.pending_read = Some(match this.read_timeout_value() {
                Some(timeout) => Box::pin(crate::sys::current::net::recv_timeout(
                    this.raw_fd(),
                    buf.len(),
                    0,
                    timeout,
                )),
                None => crate::sys::current::net::recv_future(this.raw_fd(), buf.len()),
            });
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
                // If the completed read is larger than the current buffer (the
                // submitted buffer shrank across polls), keep the surplus for the
                // next read rather than discarding it.
                if data.len() > n {
                    this.read_overflow = Some(Box::new(ReadOverflow::new(&data[n..])));
                }
                Poll::Ready(Ok(n))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for TcpStream {
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
            this.pending_write = Some(match this.write_timeout_value() {
                Some(timeout) => Box::pin(crate::sys::current::net::send_timeout(
                    this.raw_fd(),
                    buf.to_vec(),
                    0,
                    timeout,
                )),
                None => crate::sys::current::net::send_future(this.raw_fd(), buf.to_vec()),
            });
            this.pending_write_ident = Some(ident);
        } else if this.pending_write_ident != Some(ident) {
            // A write is in flight for a *different* buffer: the future that
            // submitted it was dropped mid-flight and an unrelated write began.
            // Those bytes are already committed to the kernel, so reporting this
            // op's count against the new buffer would corrupt the stream. Reject
            // instead. (The in-flight op keeps running; drive writes to
            // completion, or reuse the same buffer, to resume cleanly.)
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

/// Owned read half of a [`TcpStream`].
///
/// Created by [`TcpStream::into_split`], this half keeps the socket alive and
/// implements [`AsyncRead`] so it can be moved to another runite task on the
/// same runtime thread dedicated to receiving bytes. Recombine it with the
/// matching [`OwnedWriteHalf`] through
/// [`TcpStream::reunite`] when exclusive stream ownership is needed again.
#[derive(Debug)]
pub struct OwnedReadHalf {
    stream: TcpStream,
}

/// Owned write half of a [`TcpStream`].
///
/// Created by [`TcpStream::into_split`], this half keeps the socket alive and
/// implements [`AsyncWrite`] so it can be moved to another runite task on the
/// same runtime thread dedicated to sending bytes. Use [`shutdown`](Self::shutdown)
/// to half-close the write direction with [`std::net::Shutdown::Write`]
/// without dropping the matching [`OwnedReadHalf`].
#[derive(Debug)]
pub struct OwnedWriteHalf {
    stream: TcpStream,
}

impl OwnedReadHalf {
    /// Returns the local socket address of the underlying stream.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    /// Returns the remote peer address of the underlying stream.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Reassembles the original [`TcpStream`] with the matching write half.
    ///
    /// Returns [`ReuniteError`] and both halves if `write` originated from a
    /// different stream.
    #[allow(clippy::result_large_err)]
    pub fn reunite(self, write: OwnedWriteHalf) -> Result<TcpStream, ReuniteError> {
        TcpStream::reunite(self, write)
    }
}

impl OwnedWriteHalf {
    /// Returns the local socket address of the underlying stream.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    /// Returns the remote peer address of the underlying stream.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Shuts down the write half of the connection, signalling EOF to the peer
    /// while the read half remains usable.
    pub async fn shutdown(&self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Write).await
    }

    /// Reassembles the original [`TcpStream`] with the matching read half.
    ///
    /// Returns [`ReuniteError`] and both halves if `read` originated from a
    /// different stream.
    #[allow(clippy::result_large_err)]
    pub fn reunite(self, read: OwnedReadHalf) -> Result<TcpStream, ReuniteError> {
        TcpStream::reunite(read, self)
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

/// Error returned by [`TcpStream::reunite`] when the two halves did not
/// originate from the same [`TcpStream`]. Returns ownership of both halves.
pub struct ReuniteError(pub OwnedReadHalf, pub OwnedWriteHalf);

impl std::fmt::Debug for ReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReuniteError(..)")
    }
}

impl std::fmt::Display for ReuniteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the provided halves are not from the same TcpStream")
    }
}

impl std::error::Error for ReuniteError {}

impl TcpListener {
    /// Binds a TCP listener to the first resolved address that succeeds.
    ///
    /// Binding to port `0` asks the OS to assign an available port, which can be
    /// retrieved with [`local_addr`](Self::local_addr).
    ///
    /// Like [`std::net::TcpListener::bind`], this does **not** set `SO_REUSEADDR`.
    /// To reuse a recently-closed address (or bind while a previous socket is in
    /// `TIME_WAIT`), configure a [`TcpSocket`] and enable it before binding:
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::TcpSocket::new_v4().unwrap();
    ///     socket.set_reuseaddr(true).unwrap();
    ///     socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    ///     let listener = socket.listen(1024).unwrap();
    ///     assert!(listener.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let listener = runite::net::TcpListener::bind("127.0.0.1:0")
    ///         .await
    ///         .unwrap();
    ///     assert!(listener.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub async fn bind<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs + Send + 'static,
    {
        let addrs = crate::sys::current::net::resolve_addrs(addr).await?;
        let mut last_error = None;
        for addr in addrs {
            match crate::sys::current::net::bind_listener(addr, None).await {
                Ok(fd) => return Ok(Self::from_owned_fd(fd)),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "address resolution returned no usable listener endpoints",
            )
        }))
    }

    /// Accepts an incoming connection.
    ///
    /// The returned address is the peer address reported by the operating
    /// system for the accepted stream.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let accepted =
            crate::sys::current::net::accept(NetOp::Accept { fd: self.raw_fd() }).await?;

        // SAFETY: `accepted.fd` is the fresh descriptor returned by accept and
        // ownership is transferred to `OwnedFd` exactly once here.
        let stream = TcpStream::from_owned_fd(unsafe { owned_sock_from_raw(accepted.fd) });
        Ok((stream, accepted.peer_addr))
    }

    /// Returns a [`Stream`] that yields inbound connections as they arrive.
    ///
    /// The stream is infinite: it never yields `None`. Each item is the result
    /// of an accept, so transient errors surface as `Some(Err(_))` without
    /// ending iteration.
    pub fn incoming(&self) -> Incoming {
        Incoming {
            listener: self.share(),
            pending: None,
        }
    }

    /// Returns the local socket address of this listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        crate::sys::current::net::local_addr(self.raw_fd())
    }

    /// Reads the listener socket's IP time-to-live value.
    pub fn ttl(&self) -> io::Result<u32> {
        crate::sys::current::net::ttl(self.raw_fd())
    }

    /// Sets the listener socket's IP time-to-live value.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        crate::sys::current::net::set_ttl(self.raw_fd(), ttl)
    }

    fn from_owned_fd(fd: OwnedSock) -> Self {
        Self {
            inner: Arc::new(TcpListenerInner { fd }),
        }
    }

    /// Internal fd-sharing clone (reference-counts the same socket). Not public:
    /// callers who want an independent listener use [`try_clone`](Self::try_clone),
    /// which duplicates the descriptor, mirroring `std::net::TcpListener`.
    fn share(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    fn raw_fd(&self) -> RawSock {
        raw_sock(&self.inner.fd)
    }
}

/// Stream of inbound TCP connections.
///
/// Created by [`TcpListener::incoming`], this stream repeatedly accepts new
/// connections from its listener. It yields `Some(Err(_))` for accept errors and
/// does not terminate on its own.
pub struct Incoming {
    listener: TcpListener,
    pending: Option<Pin<Box<dyn Future<Output = io::Result<TcpStream>>>>>,
}

impl std::fmt::Debug for Incoming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Incoming")
            .field("listener", &self.listener)
            .finish_non_exhaustive()
    }
}

impl Stream for Incoming {
    type Item = io::Result<TcpStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.pending.is_none() {
            let fd = this.listener.raw_fd();
            this.pending = Some(Box::pin(async move {
                let accepted = crate::sys::current::net::accept(NetOp::Accept { fd }).await?;
                // SAFETY: `accepted.fd` is the fresh descriptor returned by
                // accept; ownership is transferred to `OwnedFd` exactly once.
                Ok(TcpStream::from_owned_fd(unsafe {
                    owned_sock_from_raw(accepted.fd)
                }))
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

impl UdpSocket {
    /// Binds a UDP socket to the first resolved address that succeeds.
    ///
    /// Binding to port `0` asks the OS to choose an available local port.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let socket = runite::net::UdpSocket::bind("127.0.0.1:0")
    ///         .await
    ///         .unwrap();
    ///     assert!(socket.local_addr().unwrap().port() != 0);
    /// });
    /// runite::run();
    /// ```
    pub async fn bind<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs + Send + 'static,
    {
        let addrs = crate::sys::current::net::resolve_addrs(addr).await?;
        let mut last_error = None;
        for addr in addrs {
            match crate::sys::current::net::bind_datagram(addr).await {
                Ok(fd) => return Ok(Self::from_owned_fd(fd)),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "address resolution returned no usable UDP endpoints",
            )
        }))
    }

    /// Connects the socket to a default peer.
    ///
    /// Once connected, [`send`](Self::send), [`recv`](Self::recv), and [`peer_addr`](Self::peer_addr)
    /// operate relative to that peer.
    pub async fn connect<A>(&self, addr: A) -> io::Result<()>
    where
        A: ToSocketAddrs + Send + 'static,
    {
        let addrs = crate::sys::current::net::resolve_addrs(addr).await?;
        let mut last_error = None;
        for addr in addrs {
            match crate::sys::current::net::connect(NetOp::Connect {
                fd: self.raw_fd(),
                addr,
            })
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "address resolution returned no usable UDP peers",
            )
        }))
    }

    /// Sends a datagram to the connected peer.
    ///
    /// The socket must first be connected with [`connect`](Self::connect).
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        match self.write_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::send_timeout(self.raw_fd(), buf.to_vec(), 0, timeout)
                    .await
            }
            None => {
                crate::sys::current::net::send(NetOp::Send {
                    fd: self.raw_fd(),
                    data: buf.to_vec(),
                    flags: 0,
                })
                .await
            }
        }
    }

    /// Receives a datagram from the connected peer.
    ///
    /// The socket must first be connected with [`connect`](Self::connect). If
    /// `buf` is smaller than the datagram, the returned length is capped at
    /// `buf.len()` and the operating system discards the excess bytes. If a
    /// configured read timeout expires, the error kind is
    /// [`io::ErrorKind::TimedOut`].
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let data = match self.read_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::recv_timeout(self.raw_fd(), buf.len(), 0, timeout).await?
            }
            None => {
                crate::sys::current::net::recv(NetOp::Recv {
                    fd: self.raw_fd(),
                    len: buf.len(),
                    flags: 0,
                })
                .await?
            }
        };
        let read = data.len();
        buf[..read].copy_from_slice(&data);
        Ok(read)
    }

    /// Peeks at the next datagram from the connected peer without consuming it.
    ///
    /// If `buf` is smaller than the datagram, the returned view is truncated to
    /// `buf.len()`. Because this is a peek, the datagram remains queued; a later
    /// non-peek receive with a too-small buffer consumes it and discards the
    /// excess bytes. If a configured read timeout expires, the error kind is
    /// [`io::ErrorKind::TimedOut`].
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let data = match self.read_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::recv_timeout(
                    self.raw_fd(),
                    buf.len(),
                    crate::sys::current::net::MSG_PEEK,
                    timeout,
                )
                .await?
            }
            None => {
                crate::sys::current::net::recv(NetOp::Recv {
                    fd: self.raw_fd(),
                    len: buf.len(),
                    flags: crate::sys::current::net::MSG_PEEK,
                })
                .await?
            }
        };
        let read = data.len();
        buf[..read].copy_from_slice(&data);
        Ok(read)
    }

    /// Sends a datagram to `addr`.
    ///
    /// The destination is resolved with [`ToSocketAddrs`]. If multiple
    /// destinations are returned, each is tried until a send succeeds.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
    ///     let receiver = runite::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    ///     let sender = runite::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    ///     let receiver_addr = receiver.local_addr().unwrap();
    ///
    ///     sender.send_to(b"x", receiver_addr).await.unwrap();
    ///     let mut buf = [0; 1];
    ///     let (read, _peer) = receiver.recv_from(&mut buf).await.unwrap();
    ///     assert_eq!(&buf[..read], b"x");
    /// });
    /// runite::run();
    /// ```
    pub async fn send_to<A>(&self, buf: &[u8], addr: A) -> io::Result<usize>
    where
        A: ToSocketAddrs + Send + 'static,
    {
        let addrs = crate::sys::current::net::resolve_addrs(addr).await?;
        let mut last_error = None;
        let timeout = self.write_timeout_value();
        for addr in addrs {
            let result = match timeout {
                Some(timeout) => {
                    crate::sys::current::net::send_to_timeout(
                        self.raw_fd(),
                        buf.to_vec(),
                        addr,
                        0,
                        timeout,
                    )
                    .await
                }
                None => {
                    crate::sys::current::net::send_to(NetOp::SendTo {
                        fd: self.raw_fd(),
                        target: addr,
                        data: buf.to_vec(),
                        flags: 0,
                    })
                    .await
                }
            };
            match result {
                Ok(sent) => return Ok(sent),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "address resolution returned no usable UDP destinations",
            )
        }))
    }

    /// Receives a datagram and returns the sender address.
    ///
    /// If `buf` is smaller than the datagram, the returned length is capped at
    /// `buf.len()` and the operating system discards the excess bytes. If a
    /// configured read timeout expires, the error kind is
    /// [`io::ErrorKind::TimedOut`].
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let datagram = match self.read_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::recv_from_timeout(self.raw_fd(), buf.len(), 0, timeout)
                    .await?
            }
            None => {
                crate::sys::current::net::recv_from(NetOp::RecvFrom {
                    fd: self.raw_fd(),
                    len: buf.len(),
                    flags: 0,
                })
                .await?
            }
        };
        let read = datagram.data.len();
        buf[..read].copy_from_slice(&datagram.data);
        Ok((read, datagram.peer_addr))
    }

    /// Peeks at the next datagram and returns the sender address without consuming it.
    ///
    /// If `buf` is smaller than the datagram, the returned view is truncated to
    /// `buf.len()`. Because this is a peek, the datagram remains queued; a later
    /// non-peek receive with a too-small buffer consumes it and discards the
    /// excess bytes. If a configured read timeout expires, the error kind is
    /// [`io::ErrorKind::TimedOut`].
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let datagram = match self.read_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::recv_from_timeout(
                    self.raw_fd(),
                    buf.len(),
                    crate::sys::current::net::MSG_PEEK,
                    timeout,
                )
                .await?
            }
            None => {
                crate::sys::current::net::recv_from(NetOp::RecvFrom {
                    fd: self.raw_fd(),
                    len: buf.len(),
                    flags: crate::sys::current::net::MSG_PEEK,
                })
                .await?
            }
        };
        let read = datagram.data.len();
        buf[..read].copy_from_slice(&datagram.data);
        Ok((read, datagram.peer_addr))
    }

    /// Duplicates the underlying UDP socket.
    pub async fn try_clone(&self) -> io::Result<Self> {
        crate::sys::current::net::duplicate(self.raw_fd())
            .await
            .map(Self::from_owned_fd)
    }

    /// Returns the local socket address of this socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        crate::sys::current::net::local_addr(self.raw_fd())
    }

    /// Returns the connected peer address, if the socket has been connected.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        crate::sys::current::net::peer_addr(self.raw_fd())
    }

    /// Reads the `SO_BROADCAST` setting.
    pub fn broadcast(&self) -> io::Result<bool> {
        crate::sys::current::net::broadcast(self.raw_fd())
    }

    /// Enables or disables `SO_BROADCAST`.
    pub fn set_broadcast(&self, enabled: bool) -> io::Result<()> {
        crate::sys::current::net::set_broadcast(self.raw_fd(), enabled)
    }

    /// Reads the socket's IP time-to-live value.
    pub fn ttl(&self) -> io::Result<u32> {
        crate::sys::current::net::ttl(self.raw_fd())
    }

    /// Sets the socket's IP time-to-live value.
    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        crate::sys::current::net::set_ttl(self.raw_fd(), ttl)
    }

    /// Returns the read timeout used by async receive operations on this handle.
    ///
    /// A socket created with [`try_clone`](Self::try_clone) has independent
    /// timeout settings.
    pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.read_timeout_value())
    }

    /// Sets the read timeout used by async receive operations on this handle.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected. If the timeout expires,
    /// receive operations return [`io::ErrorKind::TimedOut`]. A socket created
    /// with [`try_clone`](Self::try_clone) has independent timeout settings.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().read = timeout;
        Ok(())
    }

    /// Returns the write timeout used by async send operations on this handle.
    ///
    /// A socket created with [`try_clone`](Self::try_clone) has independent
    /// timeout settings.
    pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.write_timeout_value())
    }

    /// Sets the write timeout used by async send operations on this handle.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected. If the timeout expires, send
    /// operations return [`io::ErrorKind::TimedOut`]. A socket created with
    /// [`try_clone`](Self::try_clone) has independent timeout settings.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().write = timeout;
        Ok(())
    }

    fn from_owned_fd(fd: OwnedSock) -> Self {
        Self {
            inner: Arc::new(UdpSocketInner {
                fd,
                timeouts: Mutex::new(SocketTimeouts::default()),
            }),
        }
    }

    fn raw_fd(&self) -> RawSock {
        raw_sock(&self.inner.fd)
    }

    fn read_timeout_value(&self) -> Option<Duration> {
        self.inner.timeouts.lock().unwrap().read
    }

    fn write_timeout_value(&self) -> Option<Duration> {
        self.inner.timeouts.lock().unwrap().write
    }
}

fn validate_optional_timeout(timeout: Option<Duration>) -> io::Result<()> {
    if let Some(timeout) = timeout {
        validate_timeout(timeout)?;
    }
    Ok(())
}

fn validate_timeout(timeout: Duration) -> io::Result<()> {
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "zero-duration timeouts are not supported",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::{queue_macrotask, run, spawn};

    use super::{TcpListener, TcpStream, UdpSocket};
    use std::io::ErrorKind;
    use std::net::SocketAddr;

    #[test]
    fn tcp_listener_and_stream_round_trip() {
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);

        queue_macrotask(move || {
            let received_for_task = Arc::clone(&received_for_task);
            spawn(async move {
                let listener = Arc::new(
                    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .expect("listener should bind"),
                );
                let local_addr = listener
                    .local_addr()
                    .expect("listener should expose address");

                let listener_for_accept = Arc::clone(&listener);
                let server = spawn(async move {
                    let (mut stream, peer_addr) = listener_for_accept
                        .accept()
                        .await
                        .expect("listener should accept");
                    assert_eq!(peer_addr.ip().to_string(), "127.0.0.1");

                    let mut buffer = [0; 32];
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

                let mut client = TcpStream::connect(local_addr)
                    .await
                    .expect("client should connect");
                client
                    .set_nodelay(true)
                    .expect("setting TCP_NODELAY should succeed");
                assert!(
                    client
                        .nodelay()
                        .expect("reading TCP_NODELAY should succeed"),
                    "TCP_NODELAY should be enabled",
                );
                client
                    .write_all(b"ping")
                    .await
                    .expect("client write should succeed");
                let mut response = [0; 4];
                client
                    .read_exact(&mut response)
                    .await
                    .expect("client read should succeed");
                assert_eq!(&response, b"pong");

                let server_bytes = server.await.expect("server task should not be aborted");
                *received_for_task
                    .lock()
                    .expect("received buffer should not be poisoned") = Some(server_bytes);
            });
        });
        run();

        let received = received
            .lock()
            .expect("received buffer should not be poisoned");
        assert_eq!(received.as_deref(), Some(b"ping".as_slice()));
    }

    #[test]
    fn tcp_connect_resolves_localhost() {
        let peer = Arc::new(Mutex::new(None::<String>));
        let peer_for_task = Arc::clone(&peer);

        queue_macrotask(move || {
            let peer_for_task = Arc::clone(&peer_for_task);
            spawn(async move {
                let listener = Arc::new(
                    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .expect("listener should bind"),
                );
                let port = listener
                    .local_addr()
                    .expect("listener should expose address")
                    .port();

                let listener_for_accept = Arc::clone(&listener);
                let server = spawn(async move {
                    let (stream, peer_addr) = listener_for_accept
                        .accept()
                        .await
                        .expect("listener should accept");
                    drop(stream);
                    peer_addr
                });

                let _client = TcpStream::connect(format!("localhost:{port}"))
                    .await
                    .expect("localhost DNS connect should succeed");
                let peer_addr = server.await.expect("server task should not be aborted");
                *peer_for_task
                    .lock()
                    .expect("peer buffer should not be poisoned") =
                    Some(peer_addr.ip().to_string());
            });
        });
        run();

        let peer = peer.lock().expect("peer buffer should not be poisoned");
        assert_eq!(peer.as_deref(), Some("127.0.0.1"));
    }

    #[test]
    fn udp_send_to_and_recv_from_round_trip() {
        let server_received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let server_received_for_task = Arc::clone(&server_received);

        queue_macrotask(move || {
            let server_received_for_task = Arc::clone(&server_received_for_task);
            spawn(async move {
                let server = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .expect("server udp socket should bind");
                let client = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .expect("client udp socket should bind");

                server
                    .set_broadcast(true)
                    .expect("enabling broadcast should succeed");
                assert!(
                    server
                        .broadcast()
                        .expect("reading broadcast should succeed"),
                    "broadcast should be enabled",
                );
                client.set_ttl(42).expect("setting ttl should succeed");
                assert_eq!(client.ttl().expect("reading ttl should succeed"), 42);

                let server_addr = server.local_addr().expect("server should expose address");
                let client_addr = client.local_addr().expect("client should expose address");

                let server_task = spawn(async move {
                    let mut peek_buffer = [0; 32];
                    let (peeked, peek_peer) = server
                        .peek_from(&mut peek_buffer)
                        .await
                        .expect("server peek_from should succeed");
                    assert_eq!(&peek_buffer[..peeked], b"ping");
                    assert_eq!(peek_peer, client_addr);

                    let mut buffer = [0; 32];
                    let (read, peer) = server
                        .recv_from(&mut buffer)
                        .await
                        .expect("server recv_from should succeed");
                    assert_eq!(peer, client_addr);
                    server
                        .send_to(b"pong", peer)
                        .await
                        .expect("server send_to should succeed");
                    buffer[..read].to_vec()
                });

                client
                    .send_to(b"ping", server_addr)
                    .await
                    .expect("client send_to should succeed");
                let mut response = [0; 32];
                let (read, peer) = client
                    .recv_from(&mut response)
                    .await
                    .expect("client recv_from should succeed");
                assert_eq!(peer, server_addr);
                assert_eq!(&response[..read], b"pong");

                let received = server_task
                    .await
                    .expect("server task should not be aborted");
                *server_received_for_task.lock().unwrap() = Some(received);
            });
        });
        run();

        let server_received = server_received.lock().unwrap();
        assert_eq!(server_received.as_deref(), Some(b"ping".as_slice()));
    }

    #[test]
    fn udp_connected_sockets_and_timeouts_work() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_for_task = Arc::clone(&observed);

        queue_macrotask(move || {
            let observed_for_task = Arc::clone(&observed_for_task);
            spawn(async move {
                let server = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .expect("server udp socket should bind");
                let client = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .expect("client udp socket should bind");

                let server_addr = server.local_addr().expect("server should expose address");
                let client_addr = client.local_addr().expect("client should expose address");

                client
                    .connect(server_addr)
                    .await
                    .expect("client udp connect should succeed");
                server
                    .connect(client_addr)
                    .await
                    .expect("server udp connect should succeed");

                client
                    .set_read_timeout(Some(Duration::from_millis(5)))
                    .expect("setting read timeout should succeed");
                assert_eq!(
                    client
                        .read_timeout()
                        .expect("reading read timeout should succeed"),
                    Some(Duration::from_millis(5))
                );

                let mut buffer = [0; 16];
                let error = client
                    .recv(&mut buffer)
                    .await
                    .expect_err("recv should time out before any datagram arrives");
                assert_eq!(error.kind(), ErrorKind::TimedOut);
                observed_for_task
                    .lock()
                    .unwrap()
                    .push("timed out".to_string());

                server
                    .send(b"hello")
                    .await
                    .expect("server send should succeed");

                let peeked = client.peek(&mut buffer).await.expect("peek should succeed");
                assert_eq!(&buffer[..peeked], b"hello");

                let read = client.recv(&mut buffer).await.expect("recv should succeed");
                assert_eq!(&buffer[..read], b"hello");
                observed_for_task
                    .lock()
                    .unwrap()
                    .push("received".to_string());
            });
        });
        run();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.as_slice(), ["timed out", "received"]);
    }

    #[test]
    fn tcp_into_split_read_write_and_reunite() {
        use crate::io::{AsyncReadExt, AsyncWriteExt};

        let ok = Arc::new(Mutex::new(false));
        let ok_for_task = Arc::clone(&ok);

        queue_macrotask(move || {
            let ok_for_task = Arc::clone(&ok_for_task);
            spawn(async move {
                let listener = Arc::new(
                    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .expect("listener should bind"),
                );
                let local_addr = listener.local_addr().expect("address");

                let listener_for_accept = Arc::clone(&listener);
                let server = spawn(async move {
                    let (mut stream, _) = listener_for_accept.accept().await.expect("accept");
                    let mut buffer = [0; 4];
                    stream.read_exact(&mut buffer).await.expect("server read");
                    stream.write_all(b"pong").await.expect("server write");
                    buffer
                });

                let client = TcpStream::connect(local_addr).await.expect("connect");
                let (mut read_half, mut write_half) = client.into_split();

                // Write from one task while reading in this one.
                let writer = spawn(async move {
                    write_half.write_all(b"ping").await.expect("split write");
                    write_half
                });

                let mut response = [0; 4];
                read_half
                    .read_exact(&mut response)
                    .await
                    .expect("split read");
                assert_eq!(&response, b"pong");

                let write_half = writer.await.expect("writer task");
                let server_bytes = server.await.expect("server task");
                assert_eq!(&server_bytes, b"ping");

                // Halves came from the same stream, so reunite succeeds.
                TcpStream::reunite(read_half, write_half).expect("reunite");

                *ok_for_task.lock().unwrap() = true;
            });
        });
        run();

        assert!(*ok.lock().unwrap(), "split round-trip should complete");
    }

    #[test]
    fn tcp_incoming_yields_connections() {
        use crate::io::StreamExt;

        let count = Arc::new(Mutex::new(0usize));
        let count_for_task = Arc::clone(&count);

        queue_macrotask(move || {
            let count_for_task = Arc::clone(&count_for_task);
            spawn(async move {
                let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .await
                    .expect("listener should bind");
                let local_addr = listener.local_addr().expect("address");

                let server = spawn(async move {
                    let mut incoming = listener.incoming();
                    let mut accepted = 0;
                    while accepted < 3 {
                        let stream = incoming
                            .next()
                            .await
                            .expect("incoming is infinite")
                            .expect("accept should succeed");
                        drop(stream);
                        accepted += 1;
                    }
                    accepted
                });

                for _ in 0..3 {
                    let stream = TcpStream::connect(local_addr).await.expect("connect");
                    drop(stream);
                }

                *count_for_task.lock().unwrap() = server.await.expect("server task");
            });
        });
        run();

        assert_eq!(*count.lock().unwrap(), 3);
    }
}

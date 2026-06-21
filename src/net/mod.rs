//! Portable async networking API.
//!
//! The public surface follows the general shape of `std::net`, but uses async methods for socket
//! operations that would otherwise block the caller.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use std::io;
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::{Arc, Mutex};

use crate::io::{AsyncRead, AsyncWrite};
use crate::op::net::NetOp;

#[cfg(feature = "hyper")]
mod hyper_impl;
#[cfg(unix)]
pub mod unix;

#[cfg(unix)]
pub use unix::{UnixDatagram, UnixListener, UnixStream};

#[derive(Debug)]
struct TcpStreamInner {
    fd: OwnedFd,
    timeouts: Mutex<SocketTimeouts>,
}

#[derive(Debug)]
struct TcpListenerInner {
    fd: OwnedFd,
}

#[derive(Debug)]
struct UdpSocketInner {
    fd: OwnedFd,
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

/// Async TCP stream.
///
/// This type also implements Hyper's runtime I/O traits, allowing it to be used directly as an
/// HTTP transport.
pub struct TcpStream {
    inner: Arc<TcpStreamInner>,
    pending_read: Option<PendingRead>,
    pending_write: Option<PendingWrite>,
    pending_shutdown: Option<PendingShutdown>,
}

#[derive(Clone, Debug)]
/// Async TCP listening socket.
pub struct TcpListener {
    inner: Arc<TcpListenerInner>,
}

#[derive(Debug)]
/// Async UDP socket.
pub struct UdpSocket {
    inner: Arc<UdpSocketInner>,
}

impl TcpStream {
    /// Connects to the first resolved address that succeeds.
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

    /// Connects to `addr`, failing if the deadline elapses first.
    pub async fn connect_timeout(addr: &SocketAddr, timeout: Duration) -> io::Result<Self> {
        validate_timeout(timeout)?;
        crate::sys::current::net::connect_stream_timeout(*addr, timeout)
            .await
            .map(Self::from_owned_fd)
    }

    /// Reads bytes from the stream.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
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
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
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

    /// Duplicates the underlying stream socket.
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
    pub fn nodelay(&self) -> io::Result<bool> {
        crate::sys::current::net::nodelay(self.raw_fd())
    }

    /// Enables or disables `TCP_NODELAY`.
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

    /// Returns the read timeout used by async read operations on this handle.
    pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.read_timeout_value())
    }

    /// Sets the read timeout used by async read operations on this handle.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().read = timeout;
        Ok(())
    }

    /// Returns the write timeout used by async write operations on this handle.
    pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.write_timeout_value())
    }

    /// Sets the write timeout used by async write operations on this handle.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().write = timeout;
        Ok(())
    }

    fn from_owned_fd(fd: OwnedFd) -> Self {
        Self {
            inner: Arc::new(TcpStreamInner {
                fd,
                timeouts: Mutex::new(SocketTimeouts::default()),
            }),
            pending_read: None,
            pending_write: None,
            pending_shutdown: None,
        }
    }

    fn raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
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
                let read = data.len();
                if read > buf.len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "read completed with more bytes than destination buffer can hold",
                    )));
                }
                buf[..read].copy_from_slice(&data);
                Poll::Ready(Ok(read))
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

        match this
            .pending_write
            .as_mut()
            .expect("pending write must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_write = None;
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

impl TcpListener {
    /// Binds a TCP listener to the first resolved address that succeeds.
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
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let accepted =
            crate::sys::current::net::accept(NetOp::Accept { fd: self.raw_fd() }).await?;

        // SAFETY: `accepted.fd` is the fresh descriptor returned by accept and
        // ownership is transferred to `OwnedFd` exactly once here.
        let stream = TcpStream::from_owned_fd(unsafe { OwnedFd::from_raw_fd(accepted.fd) });
        Ok((stream, accepted.peer_addr))
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

    fn from_owned_fd(fd: OwnedFd) -> Self {
        Self {
            inner: Arc::new(TcpListenerInner { fd }),
        }
    }

    fn raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

impl UdpSocket {
    /// Binds a UDP socket to the first resolved address that succeeds.
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
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let data = match self.read_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::recv_timeout(
                    self.raw_fd(),
                    buf.len(),
                    libc::MSG_PEEK,
                    timeout,
                )
                .await?
            }
            None => {
                crate::sys::current::net::recv(NetOp::Recv {
                    fd: self.raw_fd(),
                    len: buf.len(),
                    flags: libc::MSG_PEEK,
                })
                .await?
            }
        };
        let read = data.len();
        buf[..read].copy_from_slice(&data);
        Ok(read)
    }

    /// Sends a datagram to `addr`.
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
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let datagram = match self.read_timeout_value() {
            Some(timeout) => {
                crate::sys::current::net::recv_from_timeout(
                    self.raw_fd(),
                    buf.len(),
                    libc::MSG_PEEK,
                    timeout,
                )
                .await?
            }
            None => {
                crate::sys::current::net::recv_from(NetOp::RecvFrom {
                    fd: self.raw_fd(),
                    len: buf.len(),
                    flags: libc::MSG_PEEK,
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
    pub fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.read_timeout_value())
    }

    /// Sets the read timeout used by async receive operations on this handle.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().read = timeout;
        Ok(())
    }

    /// Returns the write timeout used by async send operations on this handle.
    pub fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(self.write_timeout_value())
    }

    /// Sets the write timeout used by async send operations on this handle.
    ///
    /// Passing `Some(Duration::ZERO)` is rejected.
    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        validate_optional_timeout(timeout)?;
        self.inner.timeouts.lock().unwrap().write = timeout;
        Ok(())
    }

    fn from_owned_fd(fd: OwnedFd) -> Self {
        Self {
            inner: Arc::new(UdpSocketInner {
                fd,
                timeouts: Mutex::new(SocketTimeouts::default()),
            }),
        }
    }

    fn raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
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

    use crate::{queue_future, queue_task, run};

    use super::{TcpListener, TcpStream, UdpSocket};
    use std::io::ErrorKind;
    use std::net::SocketAddr;

    #[test]
    fn tcp_listener_and_stream_round_trip() {
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);

        queue_task(move || {
            let received_for_task = Arc::clone(&received_for_task);
            queue_future(async move {
                let listener = Arc::new(
                    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                        .await
                        .expect("listener should bind"),
                );
                let local_addr = listener
                    .local_addr()
                    .expect("listener should expose address");

                let listener_for_accept = Arc::clone(&listener);
                let server = queue_future(async move {
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

                let server_bytes = server.await;
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

        queue_task(move || {
            let peer_for_task = Arc::clone(&peer_for_task);
            queue_future(async move {
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
                let server = queue_future(async move {
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
                let peer_addr = server.await;
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

        queue_task(move || {
            let server_received_for_task = Arc::clone(&server_received_for_task);
            queue_future(async move {
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

                let server_task = queue_future(async move {
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

                let received = server_task.await;
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

        queue_task(move || {
            let observed_for_task = Arc::clone(&observed_for_task);
            queue_future(async move {
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
}

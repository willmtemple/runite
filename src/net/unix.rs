//! Async Unix domain socket API.

use std::ffi::c_void;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::SocketAddr;
use std::path::Path;

use crate::op::net::NetOp;

/// Async Unix domain stream socket.
#[derive(Debug)]
pub struct UnixStream {
    fd: OwnedFd,
}

/// Async Unix domain listening socket.
#[derive(Debug)]
pub struct UnixListener {
    fd: OwnedFd,
}

/// Async Unix domain datagram socket.
#[derive(Debug)]
pub struct UnixDatagram {
    fd: OwnedFd,
}

impl UnixStream {
    /// Connects to a Unix domain stream socket at `path`.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let fd = socket(libc::SOCK_STREAM)?;
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        connect_async(fd.as_raw_fd(), &addr).await?;
        Ok(Self { fd })
    }

    /// Creates a pair of connected Unix domain stream sockets.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (left, right) = std::os::unix::net::UnixStream::pair()?;
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

    /// Reads bytes from the stream.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
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

    /// Writes bytes to the stream.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        crate::sys::current::net::send(NetOp::Send {
            fd: self.raw_fd(),
            data: buf.to_vec(),
            flags: 0,
        })
        .await
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

    fn from_owned_fd(fd: OwnedFd) -> Self {
        Self { fd }
    }

    fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

impl UnixListener {
    /// Binds a Unix domain stream listener to `path`.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let fd = socket(libc::SOCK_STREAM)?;
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        bind_sync(fd.as_raw_fd(), &addr)?;
        listen_sync(fd.as_raw_fd(), 1024)?;
        Ok(Self { fd })
    }

    /// Accepts an incoming connection.
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

impl UnixDatagram {
    /// Binds a Unix domain datagram socket to `path`.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<Self> {
        let fd = socket(libc::SOCK_DGRAM)?;
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        bind_sync(fd.as_raw_fd(), &addr)?;
        Ok(Self { fd })
    }

    /// Creates an unbound Unix domain datagram socket.
    pub fn unbound() -> io::Result<Self> {
        socket(libc::SOCK_DGRAM).map(|fd| Self { fd })
    }

    /// Creates a pair of connected Unix domain datagram sockets.
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
    pub async fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let addr = RawUnixSocketAddr::from_path(path.as_ref())?;
        connect_async(self.raw_fd(), &addr).await
    }

    /// Receives a datagram from the connected peer.
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
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        crate::sys::current::net::send(NetOp::Send {
            fd: self.raw_fd(),
            data: buf.to_vec(),
            flags: 0,
        })
        .await
    }

    /// Sends a datagram to `path`.
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

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
async fn wait_writable(fd: RawFd) -> io::Result<()> {
    use crate::op::completion::completion_for_current_thread;
    use crate::platform::linux_x86_64::runtime::with_current_driver;
    use crate::platform::linux_x86_64::uring::{IORING_OP_POLL_ADD, IoUringCqe};

    let (future, handle) = completion_for_current_thread::<io::Result<()>>();
    let callback_handle = handle.clone();
    let token = with_current_driver(|driver| {
        driver.submit_operation(
            move |sqe| {
                sqe.opcode = IORING_OP_POLL_ADD;
                sqe.fd = fd;
                sqe.len = 0;
                sqe.op_flags = (libc::POLLOUT | libc::POLLERR | libc::POLLHUP) as u32;
            },
            move |cqe: IoUringCqe| {
                if cqe.res < 0 {
                    callback_handle.complete(Err(io::Error::from_raw_os_error(-cqe.res)));
                } else {
                    callback_handle.complete(Ok(()));
                }
            },
        )
    })?;

    handle.set_cancel(move || {
        let _ = with_current_driver(|driver| driver.cancel_operation(token));
    });

    future.await
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use crate::{queue_future, queue_task, run};

    use super::{UnixDatagram, UnixListener, UnixStream};

    #[test]
    fn unix_stream_pair_round_trip() {
        let received = Arc::new(Mutex::new(None::<Vec<u8>>));
        let received_for_task = Arc::clone(&received);

        queue_task(move || {
            let received_for_task = Arc::clone(&received_for_task);
            queue_future(async move {
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

        queue_task(move || {
            let received_for_task = Arc::clone(&received_for_task);
            queue_future(async move {
                let listener = Arc::new(
                    UnixListener::bind(&path_for_task).expect("listener should bind to path"),
                );
                assert_eq!(
                    listener.local_addr().unwrap().as_pathname(),
                    Some(path_for_task.as_path())
                );

                let listener_for_accept = Arc::clone(&listener);
                let server = queue_future(async move {
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

                *received_for_task.lock().unwrap() = Some(server.await);
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

        queue_task(move || {
            let received_for_task = Arc::clone(&received_for_task);
            queue_future(async move {
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
        let dir = std::env::current_dir()
            .expect("current directory should be available")
            .join("target")
            .join("runite-uds-tests");
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

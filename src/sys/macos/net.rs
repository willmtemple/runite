//! macOS networking backend.

use std::ffi::c_void;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::net::{
    Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs,
};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::time::Duration;

use crate::op::completion::completion_for_current_thread;
use crate::op::net::{AcceptedSocket, NetOp, ReceivedDatagram};
use crate::sys::blocking::spawn_blocking;

const DEFAULT_LISTENER_BACKLOG: i32 = 1024;

type RecvFuture = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type SendFuture = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;
type ShutdownFuture = Pin<Box<dyn Future<Output = io::Result<()>> + 'static>>;

pub async fn resolve_addrs<A>(addr: A) -> io::Result<Vec<SocketAddr>>
where
    A: ToSocketAddrs + Send + 'static,
{
    offload(move || {
        let addrs = addr.to_socket_addrs()?.collect::<Vec<_>>();
        if addrs.is_empty() {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "address resolved to no socket addresses",
            ))
        } else {
            Ok(addrs)
        }
    })
    .await
}

pub async fn socket(op: NetOp) -> io::Result<OwnedFd> {
    let NetOp::Socket {
        domain,
        socket_type,
        protocol,
        flags,
    } = op
    else {
        unreachable!("socket backend called with non-socket op");
    };

    socket_sync(domain, socket_type, protocol, flags)
}

pub async fn connect(op: NetOp) -> io::Result<()> {
    let NetOp::Connect { fd, addr } = op else {
        unreachable!("connect backend called with non-connect op");
    };

    connect_async(fd, RawSocketAddr::from_socket_addr(addr)).await
}

pub async fn bind(op: NetOp) -> io::Result<()> {
    let NetOp::Bind { fd, addr } = op else {
        unreachable!("bind backend called with non-bind op");
    };

    bind_sync(fd, RawSocketAddr::from_socket_addr(addr))
}

pub async fn listen(op: NetOp) -> io::Result<()> {
    let NetOp::Listen { fd, backlog } = op else {
        unreachable!("listen backend called with non-listen op");
    };

    listen_sync(fd, backlog)
}

pub async fn accept(op: NetOp) -> io::Result<AcceptedSocket> {
    let NetOp::Accept { fd } = op else {
        unreachable!("accept backend called with non-accept op");
    };

    accept_async(fd).await
}

pub async fn send(op: NetOp) -> io::Result<usize> {
    let NetOp::Send { fd, data, flags } = op else {
        unreachable!("send backend called with non-send op");
    };

    send_async(fd, data, flags).await
}

pub async fn send_to(op: NetOp) -> io::Result<usize> {
    let NetOp::SendTo {
        fd,
        target,
        data,
        flags,
    } = op
    else {
        unreachable!("send_to backend called with non-send_to op");
    };

    send_to_async(fd, target, data, flags).await
}

pub async fn recv(op: NetOp) -> io::Result<Vec<u8>> {
    let NetOp::Recv { fd, len, flags } = op else {
        unreachable!("recv backend called with non-recv op");
    };

    recv_async(fd, len, flags).await
}

pub async fn recv_from(op: NetOp) -> io::Result<ReceivedDatagram> {
    let NetOp::RecvFrom { fd, len, flags } = op else {
        unreachable!("recv_from backend called with non-recv_from op");
    };

    recv_from_async(fd, len, flags).await
}

pub async fn shutdown(op: NetOp) -> io::Result<()> {
    let NetOp::Shutdown { fd, how } = op else {
        unreachable!("shutdown backend called with non-shutdown op");
    };

    shutdown_sync(fd, how)
}

pub async fn connect_stream(addr: SocketAddr) -> io::Result<OwnedFd> {
    match connect_stream_inner(addr).await {
        Err(error) if should_try_ipv4_loopback(addr, &error) => {
            connect_stream_inner(localhost_v4(addr)).await
        }
        result => result,
    }
}

pub async fn bind_listener(addr: SocketAddr, backlog: Option<i32>) -> io::Result<OwnedFd> {
    match bind_listener_inner(addr, backlog).await {
        Err(error) if should_try_ipv4_loopback(addr, &error) => {
            bind_listener_inner(localhost_v4(addr), backlog).await
        }
        result => result,
    }
}

pub async fn bind_datagram(addr: SocketAddr) -> io::Result<OwnedFd> {
    match bind_datagram_inner(addr).await {
        Err(error) if should_try_ipv4_loopback(addr, &error) => {
            bind_datagram_inner(localhost_v4(addr)).await
        }
        result => result,
    }
}

async fn connect_stream_inner(addr: SocketAddr) -> io::Result<OwnedFd> {
    let stream = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_STREAM,
        protocol: 0,
        flags: 0,
    })
    .await?;

    connect(NetOp::Connect {
        fd: stream.as_raw_fd(),
        addr,
    })
    .await?;
    Ok(stream)
}

async fn bind_listener_inner(addr: SocketAddr, backlog: Option<i32>) -> io::Result<OwnedFd> {
    let listener = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_STREAM,
        protocol: 0,
        flags: 0,
    })
    .await?;

    // Do not set SO_REUSEADDR implicitly (matches std::net::TcpListener::bind).
    // Callers who want it opt in via `net::TcpSocket::set_reuseaddr` before bind.

    bind(NetOp::Bind {
        fd: listener.as_raw_fd(),
        addr,
    })
    .await?;
    listen(NetOp::Listen {
        fd: listener.as_raw_fd(),
        backlog: backlog.unwrap_or(DEFAULT_LISTENER_BACKLOG),
    })
    .await?;
    Ok(listener)
}

async fn bind_datagram_inner(addr: SocketAddr) -> io::Result<OwnedFd> {
    let socket = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_DGRAM,
        protocol: 0,
        flags: 0,
    })
    .await?;

    bind(NetOp::Bind {
        fd: socket.as_raw_fd(),
        addr,
    })
    .await?;
    Ok(socket)
}

pub fn tcp_socket_v4() -> io::Result<OwnedFd> {
    socket_sync(libc::AF_INET, libc::SOCK_STREAM, 0, 0)
}

pub fn tcp_socket_v6() -> io::Result<OwnedFd> {
    socket_sync(libc::AF_INET6, libc::SOCK_STREAM, 0, 0)
}

pub fn bind_socket(fd: RawFd, addr: SocketAddr) -> io::Result<()> {
    bind_sync(fd, RawSocketAddr::from_socket_addr(addr))
}

pub fn listen_socket(fd: RawFd, backlog: i32) -> io::Result<()> {
    listen_sync(fd, backlog)
}

pub async fn duplicate(fd: RawFd) -> io::Result<OwnedFd> {
    let duplicated = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
    set_nonblocking(duplicated)?;
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

pub async fn recv_timeout(
    fd: RawFd,
    len: usize,
    flags: i32,
    timeout: Duration,
) -> io::Result<Vec<u8>> {
    io_timeout(timeout, recv_async(fd, len, flags)).await
}

pub async fn send_timeout(
    fd: RawFd,
    data: Vec<u8>,
    flags: i32,
    timeout: Duration,
) -> io::Result<usize> {
    io_timeout(timeout, send_async(fd, data, flags)).await
}

pub async fn recv_from_timeout(
    fd: RawFd,
    len: usize,
    flags: i32,
    timeout: Duration,
) -> io::Result<ReceivedDatagram> {
    io_timeout(timeout, recv_from_async(fd, len, flags)).await
}

pub async fn send_to_timeout(
    fd: RawFd,
    data: Vec<u8>,
    target: SocketAddr,
    flags: i32,
    timeout: Duration,
) -> io::Result<usize> {
    io_timeout(timeout, send_to_async(fd, target, data, flags)).await
}

pub async fn connect_stream_timeout(addr: SocketAddr, timeout: Duration) -> io::Result<OwnedFd> {
    let fd = socket_sync(socket_domain(addr), libc::SOCK_STREAM, 0, 0)?;
    if let Err(error) = io_timeout(
        timeout,
        connect_async(fd.as_raw_fd(), RawSocketAddr::from_socket_addr(addr)),
    )
    .await
    {
        drop(fd);
        return Err(error);
    }
    Ok(fd)
}

pub fn local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    socket_addr_with(libc::getsockname, fd)
}

pub fn peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    socket_addr_with(libc::getpeername, fd)
}

pub fn nodelay(fd: RawFd) -> io::Result<bool> {
    let mut value = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    cvt(unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &mut value as *mut libc::c_int as *mut c_void,
            &mut len,
        )
    })?;
    Ok(value != 0)
}

pub fn broadcast(fd: RawFd) -> io::Result<bool> {
    getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_BROADCAST).map(|value| value != 0)
}

pub fn reuse_addr(fd: RawFd) -> io::Result<bool> {
    getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR).map(|value| value != 0)
}

pub fn set_reuse_addr(fd: RawFd, enabled: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEADDR, enabled.into())
}

pub fn reuse_port(fd: RawFd) -> io::Result<bool> {
    getsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT).map(|value| value != 0)
}

pub fn set_reuse_port(fd: RawFd, enabled: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_REUSEPORT, enabled.into())
}

pub fn set_broadcast(fd: RawFd, enabled: bool) -> io::Result<()> {
    setsockopt_int(fd, libc::SOL_SOCKET, libc::SO_BROADCAST, enabled.into())
}

pub fn ttl(fd: RawFd) -> io::Result<u32> {
    match socket_family(fd)? {
        libc::AF_INET => {
            getsockopt_int(fd, libc::IPPROTO_IP, libc::IP_TTL).map(|value| value as u32)
        }
        libc::AF_INET6 => getsockopt_int(fd, libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS)
            .map(|value| value as u32),
        family => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported socket family {family} for TTL"),
        )),
    }
}

pub fn set_ttl(fd: RawFd, ttl: u32) -> io::Result<()> {
    let ttl = i32::try_from(ttl)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "TTL exceeds i32 range"))?;
    match socket_family(fd)? {
        libc::AF_INET => setsockopt_int(fd, libc::IPPROTO_IP, libc::IP_TTL, ttl),
        libc::AF_INET6 => setsockopt_int(fd, libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS, ttl),
        family => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported socket family {family} for TTL"),
        )),
    }
}

pub fn set_nodelay(fd: RawFd, enabled: bool) -> io::Result<()> {
    let value: libc::c_int = enabled.into();
    cvt(unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &value as *const libc::c_int as *const c_void,
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    })
    .map(|_| ())
}

pub fn recv_future(fd: RawFd, len: usize) -> RecvFuture {
    Box::pin(recv(NetOp::Recv { fd, len, flags: 0 }))
}

pub fn send_future(fd: RawFd, data: Vec<u8>) -> SendFuture {
    Box::pin(send(NetOp::Send { fd, data, flags: 0 }))
}

pub fn shutdown_future(fd: RawFd, how: Shutdown) -> ShutdownFuture {
    Box::pin(shutdown(NetOp::Shutdown { fd, how }))
}

async fn offload<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let handle_for_task = handle.clone();
    if let Err(error) = spawn_blocking(move || handle_for_task.complete(work())) {
        handle.complete(Err(error));
    }
    future.await
}

async fn io_timeout<T>(
    timeout: Duration,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    crate::time::timeout(timeout, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "operation timed out"))?
}

fn socket_domain(addr: SocketAddr) -> i32 {
    match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

fn shutdown_how(how: Shutdown) -> i32 {
    match how {
        Shutdown::Read => libc::SHUT_RD,
        Shutdown::Write => libc::SHUT_WR,
        Shutdown::Both => libc::SHUT_RDWR,
    }
}

fn socket_addr_with(
    op: unsafe extern "C" fn(RawFd, *mut libc::sockaddr, *mut libc::socklen_t) -> libc::c_int,
    fd: RawFd,
) -> io::Result<SocketAddr> {
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    cvt(unsafe { op(fd, storage.as_mut_ptr().cast::<libc::sockaddr>(), &mut len) })?;
    let storage = unsafe { storage.assume_init() };
    socket_addr_from_storage(&storage, len)
}

fn socket_family(fd: RawFd) -> io::Result<i32> {
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    cvt(unsafe { libc::getsockname(fd, storage.as_mut_ptr().cast::<libc::sockaddr>(), &mut len) })?;
    let storage = unsafe { storage.assume_init() };
    Ok(storage.ss_family as i32)
}

fn getsockopt_int(fd: RawFd, level: i32, name: i32) -> io::Result<i32> {
    let mut value = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    cvt(unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            &mut value as *mut libc::c_int as *mut c_void,
            &mut len,
        )
    })?;
    Ok(value)
}

fn setsockopt_int(fd: RawFd, level: i32, name: i32, value: i32) -> io::Result<()> {
    cvt(unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &value as *const libc::c_int as *const c_void,
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    })
    .map(|_| ())
}

fn socket_addr_from_storage(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    match storage.ss_family as i32 {
        libc::AF_INET => {
            if len < std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sockaddr_in length is truncated",
                ));
            }
            let addr = unsafe { *(storage as *const _ as *const libc::sockaddr_in) };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
                u16::from_be(addr.sin_port),
            )))
        }
        libc::AF_INET6 => {
            if len < std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sockaddr_in6 length is truncated",
                ));
            }
            let addr = unsafe { *(storage as *const _ as *const libc::sockaddr_in6) };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(addr.sin6_addr.s6_addr),
                u16::from_be(addr.sin6_port),
                addr.sin6_flowinfo,
                addr.sin6_scope_id,
            )))
        }
        family => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported socket family {family}"),
        )),
    }
}

#[derive(Clone, Copy)]
struct RawSocketAddr {
    storage: libc::sockaddr_storage,
    len: libc::socklen_t,
}

impl RawSocketAddr {
    fn from_socket_addr(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) => {
                let sockaddr = libc::sockaddr_in {
                    sin_len: std::mem::size_of::<libc::sockaddr_in>() as u8,
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: addr.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_be_bytes(addr.ip().octets()).to_be(),
                    },
                    sin_zero: [0; 8],
                };
                let mut storage =
                    unsafe { MaybeUninit::<libc::sockaddr_storage>::zeroed().assume_init() };
                unsafe {
                    std::ptr::write(
                        &mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in,
                        sockaddr,
                    );
                }
                Self {
                    storage,
                    len: std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                }
            }
            SocketAddr::V6(addr) => {
                let sockaddr = libc::sockaddr_in6 {
                    sin6_len: std::mem::size_of::<libc::sockaddr_in6>() as u8,
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: addr.port().to_be(),
                    sin6_flowinfo: addr.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: addr.ip().octets(),
                    },
                    sin6_scope_id: addr.scope_id(),
                };
                let mut storage =
                    unsafe { MaybeUninit::<libc::sockaddr_storage>::zeroed().assume_init() };
                unsafe {
                    std::ptr::write(
                        &mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6,
                        sockaddr,
                    );
                }
                Self {
                    storage,
                    len: std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                }
            }
        }
    }

    fn as_ptr(&self) -> *const libc::sockaddr {
        &self.storage as *const libc::sockaddr_storage as *const libc::sockaddr
    }

    fn len(&self) -> libc::socklen_t {
        self.len
    }
}

fn socket_sync(domain: i32, socket_type: i32, protocol: i32, _flags: u32) -> io::Result<OwnedFd> {
    let fd = cvt(unsafe { libc::socket(domain, socket_type, protocol) })?;
    set_cloexec(fd)?;
    set_nonblocking(fd)?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

async fn connect_async(fd: RawFd, addr: RawSocketAddr) -> io::Result<()> {
    loop {
        let result = unsafe { libc::connect(fd, addr.as_ptr(), addr.len()) };
        if result == 0 {
            return Ok(());
        }

        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINPROGRESS) | Some(libc::EALREADY) => {
                crate::sys::current::fd::wait_writable(fd).await?;
                return socket_error(fd);
            }
            Some(libc::EISCONN) => return Ok(()),
            _ => return Err(error),
        }
    }
}

fn bind_sync(fd: RawFd, addr: RawSocketAddr) -> io::Result<()> {
    cvt(unsafe { libc::bind(fd, addr.as_ptr(), addr.len()) }).map(|_| ())
}

fn listen_sync(fd: RawFd, backlog: i32) -> io::Result<()> {
    cvt(unsafe { libc::listen(fd, backlog) }).map(|_| ())
}

fn accept_sync(fd: RawFd) -> io::Result<AcceptedSocket> {
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let accepted_fd =
        cvt(unsafe { libc::accept(fd, storage.as_mut_ptr().cast::<libc::sockaddr>(), &mut len) })?;
    set_cloexec(accepted_fd)?;
    let storage = unsafe { storage.assume_init() };
    let peer_addr = socket_addr_from_storage(&storage, len)?;
    Ok(AcceptedSocket {
        fd: accepted_fd,
        peer_addr,
    })
}

async fn accept_async(fd: RawFd) -> io::Result<AcceptedSocket> {
    loop {
        match accept_sync(fd) {
            Ok(socket) => {
                set_nonblocking(socket.fd)?;
                return Ok(socket);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                crate::sys::current::fd::wait_readable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

async fn send_async(fd: RawFd, data: Vec<u8>, flags: i32) -> io::Result<usize> {
    loop {
        match send_slice_sync(fd, &data, flags) {
            Ok(written) => return Ok(written),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                crate::sys::current::fd::wait_writable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn send_slice_sync(fd: RawFd, data: &[u8], flags: i32) -> io::Result<usize> {
    let written = unsafe { libc::send(fd, data.as_ptr().cast::<c_void>(), data.len(), flags) };
    cvt_long(written).map(|written| written as usize)
}

async fn send_to_async(
    fd: RawFd,
    target: SocketAddr,
    data: Vec<u8>,
    flags: i32,
) -> io::Result<usize> {
    loop {
        match send_to_slice_sync(fd, target, &data, flags) {
            Ok(written) => return Ok(written),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                crate::sys::current::fd::wait_writable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn send_to_slice_sync(fd: RawFd, target: SocketAddr, data: &[u8], flags: i32) -> io::Result<usize> {
    let addr = RawSocketAddr::from_socket_addr(target);
    let written = unsafe {
        libc::sendto(
            fd,
            data.as_ptr().cast::<c_void>(),
            data.len(),
            flags,
            addr.as_ptr(),
            addr.len(),
        )
    };
    cvt_long(written).map(|written| written as usize)
}

async fn recv_async(fd: RawFd, len: usize, flags: i32) -> io::Result<Vec<u8>> {
    loop {
        match recv_sync(fd, len, flags) {
            Ok(data) => return Ok(data),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                crate::sys::current::fd::wait_readable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn recv_sync(fd: RawFd, len: usize, flags: i32) -> io::Result<Vec<u8>> {
    let mut data = vec![0u8; len];
    let read = unsafe { libc::recv(fd, data.as_mut_ptr().cast::<c_void>(), len, flags) };
    let read = cvt_long(read)? as usize;
    data.truncate(read);
    Ok(data)
}

async fn recv_from_async(fd: RawFd, len: usize, flags: i32) -> io::Result<ReceivedDatagram> {
    loop {
        match recv_from_sync(fd, len, flags) {
            Ok(datagram) => return Ok(datagram),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                crate::sys::current::fd::wait_readable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn recv_from_sync(fd: RawFd, len: usize, flags: i32) -> io::Result<ReceivedDatagram> {
    let mut data = vec![0u8; len];
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let read = unsafe {
        libc::recvfrom(
            fd,
            data.as_mut_ptr().cast::<c_void>(),
            len,
            flags,
            storage.as_mut_ptr().cast::<libc::sockaddr>(),
            &mut addr_len,
        )
    };
    let read = cvt_long(read)? as usize;
    data.truncate(read);
    let storage = unsafe { storage.assume_init() };
    let peer_addr = socket_addr_from_storage(&storage, addr_len)?;
    Ok(ReceivedDatagram { data, peer_addr })
}

fn shutdown_sync(fd: RawFd, how: Shutdown) -> io::Result<()> {
    cvt(unsafe { libc::shutdown(fd, shutdown_how(how)) }).map(|_| ())
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFD) })?;
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) })?;
    Ok(())
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
    Ok(())
}

fn should_try_ipv4_loopback(addr: SocketAddr, error: &io::Error) -> bool {
    matches!(addr, SocketAddr::V6(v6) if v6.ip().is_loopback())
        && matches!(
            error.raw_os_error(),
            Some(libc::EADDRNOTAVAIL | libc::EAFNOSUPPORT | libc::ENETUNREACH)
        )
}

fn socket_error(fd: RawFd) -> io::Result<()> {
    let mut so_error: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
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

fn localhost_v4(addr: SocketAddr) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, addr.port()))
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

fn cvt_long(value: libc::ssize_t) -> io::Result<libc::ssize_t> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

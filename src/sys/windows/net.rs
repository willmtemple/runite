//! Windows networking backend.
//!
//! Sockets are created overlapped (`WSA_FLAG_OVERLAPPED`), associated with the
//! runtime thread's completion port, and driven by completion-based Winsock
//! calls: `ConnectEx`/`AcceptEx` for the connection lifecycle and
//! `WSASend`/`WSARecv`/`WSASendTo`/`WSARecvFrom` for data, all with
//! runtime-owned staging buffers pinned in the packet context. Non-blocking
//! control operations (`bind`, `listen`, `shutdown`, socket options) run
//! inline; DNS resolution offloads `to_socket_addrs` to the blocking pool.
//!
//! Sockets stay in blocking mode: overlapped operations never block the
//! submitting thread, and mixing `FIONBIO` with overlapped I/O is discouraged.

use std::ffi::c_void;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::net::{
    Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs,
};
use std::os::windows::io::IntoRawSocket;
use std::pin::Pin;
use std::sync::Once;
use std::time::Duration;

use windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER;
use windows_sys::Win32::Networking::WinSock::{
    ADDRESS_FAMILY, AF_INET, AF_INET6, AcceptEx, IN_ADDR, IN_ADDR_0, IN6_ADDR, IN6_ADDR_0,
    INVALID_SOCKET, IP_TTL, IPPROTO_IP, IPPROTO_IPV6, IPPROTO_TCP, IPV6_UNICAST_HOPS,
    LPFN_CONNECTEX, SD_BOTH, SD_RECEIVE, SD_SEND, SIO_GET_EXTENSION_FUNCTION_POINTER, SO_BROADCAST,
    SO_REUSEADDR, SO_TYPE, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCK_DGRAM,
    SOCK_STREAM, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_IN6_0, SOCKADDR_STORAGE,
    SOCKET_ERROR, SOL_SOCKET, TCP_NODELAY, WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED,
    WSA_IO_PENDING, WSABUF, WSADATA, WSADuplicateSocketW, WSAEADDRNOTAVAIL, WSAEAFNOSUPPORT,
    WSAECONNABORTED, WSAECONNREFUSED, WSAECONNRESET, WSAEHOSTUNREACH, WSAEINVAL, WSAENETUNREACH,
    WSAETIMEDOUT, WSAGetLastError, WSAID_CONNECTEX, WSAIoctl, WSAPROTOCOL_INFOW, WSARecv,
    WSARecvFrom, WSASend, WSASendTo, WSASocketW, WSAStartup, bind as wsa_bind,
    connect as wsa_connect, getpeername, getsockname, getsockopt, listen as wsa_listen, setsockopt,
    shutdown as wsa_shutdown,
};
use windows_sys::Win32::System::IO::OVERLAPPED;
use windows_sys::Win32::System::Threading::GetCurrentProcessId;

use crate::op::completion::completion_for_current_thread;
use crate::op::net::{AcceptedSocket, NetOp, ReceivedDatagram};
use crate::platform::current::runtime::with_current_driver;
use crate::platform::windows::driver::OverlappedResult;
use crate::sys::blocking::spawn_blocking;
use crate::sys::handle::{OwnedSock, RawFile, RawSock, owned_sock_from_raw, raw_sock};
use crate::sys::windows::overlapped::submit;

const DEFAULT_LISTENER_BACKLOG: i32 = 1024;

/// Peek flag for `recv`-family operations, re-exported for the public layer.
pub const MSG_PEEK: i32 = windows_sys::Win32::Networking::WinSock::MSG_PEEK;

// Completion packets report NT statuses translated to Win32 error codes,
// which are not the classic `WSAE*` codes `io::ErrorKind` classification
// understands; `normalize_socket_error` remaps them.
/// How an aborted/reset TCP connection surfaces from a completion packet.
const ERROR_NETNAME_DELETED: i32 = 64;
/// A semaphore/connect wait timeout (`STATUS_IO_TIMEOUT`).
const ERROR_SEM_TIMEOUT: i32 = 121;
/// A refused connection (`STATUS_CONNECTION_REFUSED`).
const ERROR_CONNECTION_REFUSED: i32 = 1225;
/// An unreachable network (`STATUS_NETWORK_UNREACHABLE`).
const ERROR_NETWORK_UNREACHABLE: i32 = 1231;
/// An unreachable host (`STATUS_HOST_UNREACHABLE`).
const ERROR_HOST_UNREACHABLE: i32 = 1232;
/// ICMP port-unreachable surfaced on a UDP receive.
const ERROR_PORT_UNREACHABLE: i32 = 1234;
/// A locally aborted connection (`STATUS_CONNECTION_ABORTED`).
const ERROR_CONNECTION_ABORTED: i32 = 1236;
/// A receive whose buffer was smaller than the datagram
/// (`STATUS_BUFFER_OVERFLOW`); the transferred count is still valid.
const ERROR_MORE_DATA: i32 = 234;

/// `WSASocketW` "create from protocol info" sentinel for all three of
/// af/type/protocol (winsock2.h `FROM_PROTOCOL_INFO`, not re-exported by
/// `windows-sys`).
const FROM_PROTOCOL_INFO: i32 = -1;

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

pub async fn connect(op: NetOp) -> io::Result<()> {
    let NetOp::Connect { fd, addr } = op else {
        unreachable!("connect backend called with non-connect op");
    };

    connect_async(fd, addr).await
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

pub async fn connect_stream(addr: SocketAddr) -> io::Result<OwnedSock> {
    match connect_stream_inner(addr).await {
        Err(error) if should_try_ipv4_loopback(addr, &error) => {
            connect_stream_inner(localhost_v4(addr)).await
        }
        result => result,
    }
}

pub async fn bind_listener(addr: SocketAddr, backlog: Option<i32>) -> io::Result<OwnedSock> {
    match bind_listener_inner(addr, backlog).await {
        Err(error) if should_try_ipv4_loopback(addr, &error) => {
            bind_listener_inner(localhost_v4(addr), backlog).await
        }
        result => result,
    }
}

pub async fn bind_datagram(addr: SocketAddr) -> io::Result<OwnedSock> {
    match bind_datagram_inner(addr).await {
        Err(error) if should_try_ipv4_loopback(addr, &error) => {
            bind_datagram_inner(localhost_v4(addr)).await
        }
        result => result,
    }
}

async fn connect_stream_inner(addr: SocketAddr) -> io::Result<OwnedSock> {
    let stream = socket_sync(socket_domain(addr), SOCK_STREAM, 0, 0)?;
    connect_async(raw_sock(&stream), addr).await?;
    Ok(stream)
}

async fn bind_listener_inner(addr: SocketAddr, backlog: Option<i32>) -> io::Result<OwnedSock> {
    let listener = socket_sync(socket_domain(addr), SOCK_STREAM, 0, 0)?;

    // Do not set SO_REUSEADDR implicitly (matches std::net::TcpListener::bind).
    // Callers who want it opt in via `net::TcpSocket::set_reuseaddr` before bind.

    bind_sync(raw_sock(&listener), RawSocketAddr::from_socket_addr(addr))?;
    listen_sync(
        raw_sock(&listener),
        backlog.unwrap_or(DEFAULT_LISTENER_BACKLOG),
    )?;
    Ok(listener)
}

async fn bind_datagram_inner(addr: SocketAddr) -> io::Result<OwnedSock> {
    let socket = socket_sync(socket_domain(addr), SOCK_DGRAM, 0, 0)?;
    bind_sync(raw_sock(&socket), RawSocketAddr::from_socket_addr(addr))?;
    Ok(socket)
}

pub fn tcp_socket_v4() -> io::Result<OwnedSock> {
    socket_sync(AF_INET as i32, SOCK_STREAM, 0, 0)
}

pub fn tcp_socket_v6() -> io::Result<OwnedSock> {
    socket_sync(AF_INET6 as i32, SOCK_STREAM, 0, 0)
}

pub fn bind_socket(fd: RawSock, addr: SocketAddr) -> io::Result<()> {
    bind_sync(fd, RawSocketAddr::from_socket_addr(addr))
}

pub fn listen_socket(fd: RawSock, backlog: i32) -> io::Result<()> {
    listen_sync(fd, backlog)
}

pub async fn duplicate(fd: RawSock) -> io::Result<OwnedSock> {
    wsa_init()?;

    // SAFETY: `WSAPROTOCOL_INFOW` is a plain C struct; zeroes are a valid
    // out-buffer state.
    let mut info = unsafe { std::mem::zeroed::<WSAPROTOCOL_INFOW>() };
    // SAFETY: `fd` is an open socket and `info` is a valid out-pointer;
    // duplicating into the current process is the documented same-process
    // clone idiom.
    cvt(unsafe { WSADuplicateSocketW(fd as usize, GetCurrentProcessId(), &mut info) })?;

    // SAFETY: `info` was just produced by `WSADuplicateSocketW`.
    let duplicated = unsafe {
        WSASocketW(
            FROM_PROTOCOL_INFO,
            FROM_PROTOCOL_INFO,
            FROM_PROTOCOL_INFO,
            &info,
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if duplicated == INVALID_SOCKET {
        return Err(last_wsa_error());
    }
    // SAFETY: `duplicated` is a fresh socket exclusively owned here.
    let socket = unsafe { owned_sock_from_raw(duplicated as RawSock) };

    // The duplicate shares the original's file object, which is already bound
    // to a completion port; completions route to the original's port.
    associate_socket_reused(raw_sock(&socket))?;
    Ok(socket)
}

pub async fn recv_timeout(
    fd: RawSock,
    len: usize,
    flags: i32,
    timeout: Duration,
) -> io::Result<Vec<u8>> {
    io_timeout(timeout, recv_async(fd, len, flags)).await
}

pub async fn send_timeout(
    fd: RawSock,
    data: Vec<u8>,
    flags: i32,
    timeout: Duration,
) -> io::Result<usize> {
    io_timeout(timeout, send_async(fd, data, flags)).await
}

pub async fn recv_from_timeout(
    fd: RawSock,
    len: usize,
    flags: i32,
    timeout: Duration,
) -> io::Result<ReceivedDatagram> {
    io_timeout(timeout, recv_from_async(fd, len, flags)).await
}

pub async fn send_to_timeout(
    fd: RawSock,
    data: Vec<u8>,
    target: SocketAddr,
    flags: i32,
    timeout: Duration,
) -> io::Result<usize> {
    io_timeout(timeout, send_to_async(fd, target, data, flags)).await
}

pub async fn connect_stream_timeout(addr: SocketAddr, timeout: Duration) -> io::Result<OwnedSock> {
    let socket = socket_sync(socket_domain(addr), SOCK_STREAM, 0, 0)?;
    if let Err(error) = io_timeout(timeout, connect_async(raw_sock(&socket), addr)).await {
        drop(socket);
        return Err(error);
    }
    Ok(socket)
}

pub fn local_addr(fd: RawSock) -> io::Result<SocketAddr> {
    socket_addr_with(getsockname, fd)
}

pub fn peer_addr(fd: RawSock) -> io::Result<SocketAddr> {
    socket_addr_with(getpeername, fd)
}

pub fn nodelay(fd: RawSock) -> io::Result<bool> {
    getsockopt_int(fd, IPPROTO_TCP, TCP_NODELAY).map(|value| value != 0)
}

pub fn set_nodelay(fd: RawSock, enabled: bool) -> io::Result<()> {
    setsockopt_int(fd, IPPROTO_TCP, TCP_NODELAY, enabled.into())
}

pub fn broadcast(fd: RawSock) -> io::Result<bool> {
    getsockopt_int(fd, SOL_SOCKET, SO_BROADCAST).map(|value| value != 0)
}

pub fn set_broadcast(fd: RawSock, enabled: bool) -> io::Result<()> {
    setsockopt_int(fd, SOL_SOCKET, SO_BROADCAST, enabled.into())
}

pub fn reuse_addr(fd: RawSock) -> io::Result<bool> {
    getsockopt_int(fd, SOL_SOCKET, SO_REUSEADDR).map(|value| value != 0)
}

pub fn set_reuse_addr(fd: RawSock, enabled: bool) -> io::Result<()> {
    setsockopt_int(fd, SOL_SOCKET, SO_REUSEADDR, enabled.into())
}

/// `SO_REUSEPORT` does not exist on Windows; per-core listener sharding uses
/// different primitives (`SO_REUSEADDR` semantics differ and are unsafe to
/// conflate). Both accessors report [`io::ErrorKind::Unsupported`].
pub fn reuse_port(_fd: RawSock) -> io::Result<bool> {
    Err(reuse_port_unsupported())
}

pub fn set_reuse_port(_fd: RawSock, _enabled: bool) -> io::Result<()> {
    Err(reuse_port_unsupported())
}

fn reuse_port_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_REUSEPORT is not supported on Windows",
    )
}

pub fn ttl(fd: RawSock) -> io::Result<u32> {
    match socket_family(fd)? {
        family if family == AF_INET => {
            getsockopt_int(fd, IPPROTO_IP, IP_TTL).map(|value| value as u32)
        }
        family if family == AF_INET6 => {
            getsockopt_int(fd, IPPROTO_IPV6, IPV6_UNICAST_HOPS).map(|value| value as u32)
        }
        family => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported socket family {family} for TTL"),
        )),
    }
}

pub fn set_ttl(fd: RawSock, ttl: u32) -> io::Result<()> {
    let ttl = i32::try_from(ttl)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "TTL exceeds i32 range"))?;
    match socket_family(fd)? {
        family if family == AF_INET => setsockopt_int(fd, IPPROTO_IP, IP_TTL, ttl),
        family if family == AF_INET6 => setsockopt_int(fd, IPPROTO_IPV6, IPV6_UNICAST_HOPS, ttl),
        family => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported socket family {family} for TTL"),
        )),
    }
}

/// Binds an adopted (`from_std`) socket to the current thread's completion
/// port so overlapped operations submitted on it can complete.
pub(crate) fn associate_adopted(fd: RawSock) -> io::Result<()> {
    associate_socket_reused(fd)
}

pub fn recv_future(fd: RawSock, len: usize) -> RecvFuture {
    Box::pin(recv(NetOp::Recv { fd, len, flags: 0 }))
}

pub fn send_future(fd: RawSock, data: Vec<u8>) -> SendFuture {
    Box::pin(send(NetOp::Send { fd, data, flags: 0 }))
}

pub fn shutdown_future(fd: RawSock, how: Shutdown) -> ShutdownFuture {
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

// -- Socket creation and control ---------------------------------------------

/// Initializes Winsock once per process (`WSACleanup` is never called; the
/// runtime keeps Winsock alive for the process lifetime, matching std).
fn wsa_init() -> io::Result<()> {
    static INIT: Once = Once::new();
    static mut RESULT: i32 = 0;

    // SAFETY: the static is written only inside `call_once` and read only
    // after it returns, which synchronizes.
    unsafe {
        INIT.call_once(|| {
            let mut data = std::mem::zeroed::<WSADATA>();
            RESULT = WSAStartup(0x0202, &mut data);
        });
        if RESULT != 0 {
            return Err(io::Error::from_raw_os_error(RESULT));
        }
    }
    Ok(())
}

fn socket_sync(domain: i32, socket_type: i32, protocol: i32, _flags: u32) -> io::Result<OwnedSock> {
    wsa_init()?;

    // SAFETY: no pointers besides the null protocol info; failure is reported
    // through `INVALID_SOCKET`.
    let raw = unsafe {
        WSASocketW(
            domain,
            socket_type,
            protocol,
            std::ptr::null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if raw == INVALID_SOCKET {
        return Err(last_wsa_error());
    }
    // SAFETY: `raw` is a fresh socket exclusively owned here.
    let socket = unsafe { owned_sock_from_raw(raw as RawSock) };

    associate_socket(raw_sock(&socket))?;
    Ok(socket)
}

fn associate_socket(fd: RawSock) -> io::Result<()> {
    with_current_driver(|driver| driver.associate_handle(fd as usize as *mut c_void))
}

fn associate_socket_reused(fd: RawSock) -> io::Result<()> {
    match associate_socket(fd) {
        Err(error) if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) => Ok(()),
        result => result,
    }
}

fn bind_sync(fd: RawSock, addr: RawSocketAddr) -> io::Result<()> {
    // SAFETY: `addr` owns a fully initialized sockaddr of `addr.len()` bytes.
    cvt(unsafe { wsa_bind(fd as usize, addr.as_ptr(), addr.len()) }).map(|_| ())
}

fn listen_sync(fd: RawSock, backlog: i32) -> io::Result<()> {
    // SAFETY: no pointer arguments.
    cvt(unsafe { wsa_listen(fd as usize, backlog) }).map(|_| ())
}

fn shutdown_sync(fd: RawSock, how: Shutdown) -> io::Result<()> {
    let how = match how {
        Shutdown::Read => SD_RECEIVE,
        Shutdown::Write => SD_SEND,
        Shutdown::Both => SD_BOTH,
    };
    // SAFETY: no pointer arguments.
    cvt(unsafe { wsa_shutdown(fd as usize, how) }).map(|_| ())
}

// -- Overlapped data path ------------------------------------------------------

/// Maps a `WSASend`-family return into the submission protocol: `Ok(())` when
/// a completion packet will arrive (success or `WSA_IO_PENDING`), `Err`
/// otherwise.
fn check_wsa_submission(result: i32) -> io::Result<()> {
    if result == 0 {
        // Synchronous success still posts a completion packet.
        return Ok(());
    }
    // SAFETY: read immediately after the failing Winsock call.
    let error = unsafe { WSAGetLastError() };
    if error == WSA_IO_PENDING {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(error))
    }
}

/// Normalizes NT-status-derived completion errors to the classic Winsock
/// codes so `io::ErrorKind` classification matches other backends.
fn normalize_socket_error(error: io::Error) -> io::Error {
    let remapped = match error.raw_os_error() {
        Some(ERROR_NETNAME_DELETED) | Some(ERROR_PORT_UNREACHABLE) => WSAECONNRESET,
        Some(ERROR_CONNECTION_REFUSED) => WSAECONNREFUSED,
        Some(ERROR_NETWORK_UNREACHABLE) => WSAENETUNREACH,
        Some(ERROR_HOST_UNREACHABLE) => WSAEHOSTUNREACH,
        Some(ERROR_CONNECTION_ABORTED) => WSAECONNABORTED,
        Some(ERROR_SEM_TIMEOUT) => WSAETIMEDOUT,
        _ => return error,
    };
    io::Error::from_raw_os_error(remapped)
}

fn socket_result(result: OverlappedResult) -> io::Result<usize> {
    result.into_result().map_err(normalize_socket_error)
}

/// Like [`socket_result`], but for datagram receives: a buffer smaller than
/// the datagram completes with `ERROR_MORE_DATA` while still transferring the
/// truncated prefix, which Unix `recv` reports as a plain short read.
fn datagram_recv_result(result: OverlappedResult) -> io::Result<usize> {
    match result.into_result() {
        Err(error) if error.raw_os_error() == Some(ERROR_MORE_DATA) => Ok(result.bytes),
        other => other.map_err(normalize_socket_error),
    }
}

fn cancel_handle(fd: RawSock) -> RawFile {
    RawFile::from_handle(fd as usize as *mut c_void)
}

struct BufferedIo {
    buffer: Vec<u8>,
    wsabuf: WSABUF,
    flags: u32,
}

impl BufferedIo {
    fn new(buffer: Vec<u8>, flags: i32) -> Self {
        Self {
            buffer,
            wsabuf: WSABUF {
                len: 0,
                buf: std::ptr::null_mut(),
            },
            flags: flags as u32,
        }
    }

    /// Points the `WSABUF` at the owned buffer. Called inside `start`, when
    /// the payload has reached its final heap address.
    fn fill_wsabuf(&mut self) {
        self.wsabuf = WSABUF {
            len: u32::try_from(self.buffer.len()).unwrap_or(u32::MAX),
            buf: self.buffer.as_mut_ptr(),
        };
    }
}

async fn recv_async(fd: RawSock, len: usize, flags: i32) -> io::Result<Vec<u8>> {
    submit(
        cancel_handle(fd),
        BufferedIo::new(vec![0u8; len.max(1)], flags),
        |data, overlapped| {
            data.fill_wsabuf();
            data.wsabuf.len = u32::try_from(len).unwrap_or(u32::MAX);
            // SAFETY: `wsabuf`/`flags` live in the packet context, stable for
            // the life of the operation; the buffer it points at does too.
            let result = unsafe {
                WSARecv(
                    fd as usize,
                    &data.wsabuf,
                    1,
                    std::ptr::null_mut(),
                    &mut data.flags,
                    overlapped,
                    None,
                )
            };
            check_wsa_submission(if result == SOCKET_ERROR { result } else { 0 })
        },
        move |data, result| {
            let read = datagram_recv_result(result)?;
            let mut buffer = data.buffer;
            buffer.truncate(read);
            Ok(buffer)
        },
    )
    .await
}

async fn send_async(fd: RawSock, data: Vec<u8>, flags: i32) -> io::Result<usize> {
    let flags = flags as u32;
    submit(
        cancel_handle(fd),
        BufferedIo::new(data, 0),
        move |data, overlapped| {
            data.fill_wsabuf();
            // SAFETY: as in `recv_async`.
            let result = unsafe {
                WSASend(
                    fd as usize,
                    &data.wsabuf,
                    1,
                    std::ptr::null_mut(),
                    flags,
                    overlapped,
                    None,
                )
            };
            check_wsa_submission(if result == SOCKET_ERROR { result } else { 0 })
        },
        |_data, result| socket_result(result),
    )
    .await
}

struct RecvFromPayload {
    io: BufferedIo,
    from: SOCKADDR_STORAGE,
    from_len: i32,
}

async fn recv_from_async(fd: RawSock, len: usize, flags: i32) -> io::Result<ReceivedDatagram> {
    // SAFETY: `SOCKADDR_STORAGE` is a plain C struct; zeroed is valid.
    let payload = RecvFromPayload {
        io: BufferedIo::new(vec![0u8; len.max(1)], flags),
        from: unsafe { std::mem::zeroed() },
        from_len: std::mem::size_of::<SOCKADDR_STORAGE>() as i32,
    };
    submit(
        cancel_handle(fd),
        payload,
        |payload, overlapped| {
            payload.io.fill_wsabuf();
            // SAFETY: every out-pointer (`wsabuf`, `flags`, `from`,
            // `from_len`) lives in the packet context, stable until the
            // completion packet reclaims it.
            let result = unsafe {
                WSARecvFrom(
                    fd as usize,
                    &payload.io.wsabuf,
                    1,
                    std::ptr::null_mut(),
                    &mut payload.io.flags,
                    (&raw mut payload.from).cast::<SOCKADDR>(),
                    &mut payload.from_len,
                    overlapped,
                    None,
                )
            };
            check_wsa_submission(if result == SOCKET_ERROR { result } else { 0 })
        },
        |payload, result| {
            let read = datagram_recv_result(result)?;
            let mut data = payload.io.buffer;
            data.truncate(read);
            let peer_addr = socket_addr_from_storage(&payload.from, payload.from_len)?;
            Ok(ReceivedDatagram { data, peer_addr })
        },
    )
    .await
}

struct SendToPayload {
    io: BufferedIo,
    to: RawSocketAddr,
}

async fn send_to_async(
    fd: RawSock,
    target: SocketAddr,
    data: Vec<u8>,
    flags: i32,
) -> io::Result<usize> {
    let flags = flags as u32;
    let payload = SendToPayload {
        io: BufferedIo::new(data, 0),
        to: RawSocketAddr::from_socket_addr(target),
    };
    submit(
        cancel_handle(fd),
        payload,
        move |payload, overlapped| {
            payload.io.fill_wsabuf();
            // SAFETY: as in `recv_from_async`; the destination address also
            // lives in the packet context.
            let result = unsafe {
                WSASendTo(
                    fd as usize,
                    &payload.io.wsabuf,
                    1,
                    std::ptr::null_mut(),
                    flags,
                    payload.to.as_ptr(),
                    payload.to.len(),
                    overlapped,
                    None,
                )
            };
            check_wsa_submission(if result == SOCKET_ERROR { result } else { 0 })
        },
        |_payload, result| socket_result(result),
    )
    .await
}

// -- ConnectEx / AcceptEx ------------------------------------------------------

/// Resolves the `ConnectEx` extension-function pointer for `fd`'s provider.
fn connect_ex_fn(
    fd: RawSock,
) -> io::Result<
    unsafe extern "system" fn(
        usize,
        *const SOCKADDR,
        i32,
        *const c_void,
        u32,
        *mut u32,
        *mut OVERLAPPED,
    ) -> i32,
> {
    let guid = WSAID_CONNECTEX;
    let mut function: LPFN_CONNECTEX = None;
    let mut bytes = 0u32;
    // SAFETY: in/out buffers are valid locals of the documented sizes; a null
    // overlapped makes this a synchronous control call.
    let result = unsafe {
        WSAIoctl(
            fd as usize,
            SIO_GET_EXTENSION_FUNCTION_POINTER,
            (&raw const guid).cast::<c_void>(),
            std::mem::size_of_val(&guid) as u32,
            (&raw mut function).cast::<c_void>(),
            std::mem::size_of_val(&function) as u32,
            &mut bytes,
            std::ptr::null_mut(),
            None,
        )
    };
    cvt(result)?;
    function.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "socket provider does not expose ConnectEx",
        )
    })
}

async fn connect_async(fd: RawSock, addr: SocketAddr) -> io::Result<()> {
    // `ConnectEx` only exists for connection-oriented sockets. A datagram
    // connect just records the default peer and never blocks, so it uses the
    // plain synchronous call.
    if getsockopt_int(fd, SOL_SOCKET, SO_TYPE)? == SOCK_DGRAM {
        let target = RawSocketAddr::from_socket_addr(addr);
        // SAFETY: `target` owns a fully initialized sockaddr of the given
        // length.
        return cvt(unsafe { wsa_connect(fd as usize, target.as_ptr(), target.len()) }).map(|_| ());
    }

    // `ConnectEx` requires a bound socket; bind to the wildcard address first.
    // An already-bound socket reports `WSAEINVAL`, which is fine.
    let wildcard = match addr {
        SocketAddr::V4(_) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)),
    };
    match bind_sync(fd, RawSocketAddr::from_socket_addr(wildcard)) {
        Ok(()) => {}
        Err(error) if error.raw_os_error() == Some(WSAEINVAL) => {}
        Err(error) => return Err(error),
    }

    let connect_ex = connect_ex_fn(fd)?;

    submit(
        cancel_handle(fd),
        RawSocketAddr::from_socket_addr(addr),
        |target, overlapped| {
            // SAFETY: the target address lives in the packet context; no send
            // buffer is supplied.
            let ok = unsafe {
                connect_ex(
                    fd as usize,
                    target.as_ptr(),
                    target.len(),
                    std::ptr::null(),
                    0,
                    std::ptr::null_mut(),
                    overlapped,
                )
            };
            check_wsa_submission(if ok == 0 { SOCKET_ERROR } else { 0 })
        },
        move |_target, result| {
            socket_result(result)?;
            // Finalize the socket state so getpeername/shutdown work.
            // SAFETY: no option buffer is required for this setsockopt.
            cvt(unsafe {
                setsockopt(
                    fd as usize,
                    SOL_SOCKET,
                    SO_UPDATE_CONNECT_CONTEXT,
                    std::ptr::null(),
                    0,
                )
            })
            .map(|_| ())
        },
    )
    .await
}

struct AcceptPayload {
    accept_socket: Option<OwnedSock>,
    /// Address out-buffer: local + remote, each `SOCKADDR_STORAGE` + 16 bytes,
    /// as `AcceptEx` requires.
    addresses: Box<[u8; 2 * ACCEPTEX_ADDR_LEN]>,
    received: u32,
}

const ACCEPTEX_ADDR_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;

async fn accept_async(fd: RawSock) -> io::Result<AcceptedSocket> {
    // The accept socket must match the listener's family and type.
    let family = socket_family(fd)?;
    let socket_type = getsockopt_int(fd, SOL_SOCKET, SO_TYPE)?;
    let accept_socket = socket_sync(i32::from(family), socket_type, 0, 0)?;

    let payload = AcceptPayload {
        accept_socket: Some(accept_socket),
        addresses: Box::new([0u8; 2 * ACCEPTEX_ADDR_LEN]),
        received: 0,
    };

    submit(
        cancel_handle(fd),
        payload,
        |payload, overlapped| {
            let accept_socket = payload
                .accept_socket
                .as_ref()
                .expect("accept socket is set until completion");
            // SAFETY: the address buffer and byte counter live in the packet
            // context; a zero receive length means no data is read into it.
            let ok = unsafe {
                AcceptEx(
                    fd as usize,
                    raw_sock(accept_socket) as usize,
                    payload.addresses.as_mut_ptr().cast::<c_void>(),
                    0,
                    ACCEPTEX_ADDR_LEN as u32,
                    ACCEPTEX_ADDR_LEN as u32,
                    &mut payload.received,
                    overlapped,
                )
            };
            check_wsa_submission(if ok == 0 { SOCKET_ERROR } else { 0 })
        },
        move |mut payload, result| {
            socket_result(result)?;
            let accept_socket = payload
                .accept_socket
                .take()
                .expect("accept socket is set until completion");
            let raw = raw_sock(&accept_socket);

            // Finalize the accepted socket so getpeername/shutdown work.
            let listener: usize = fd as usize;
            // SAFETY: `listener` is passed by value through the option buffer,
            // as `SO_UPDATE_ACCEPT_CONTEXT` requires.
            cvt(unsafe {
                setsockopt(
                    raw as usize,
                    SOL_SOCKET,
                    SO_UPDATE_ACCEPT_CONTEXT,
                    (&raw const listener).cast::<u8>(),
                    std::mem::size_of::<usize>() as i32,
                )
            })?;

            let peer_addr = peer_addr(raw)?;
            Ok(AcceptedSocket {
                fd: accept_socket.into_raw_socket(),
                peer_addr,
            })
        },
    )
    .await
}

// -- Address helpers -----------------------------------------------------------

fn socket_domain(addr: SocketAddr) -> i32 {
    match addr {
        SocketAddr::V4(_) => AF_INET as i32,
        SocketAddr::V6(_) => AF_INET6 as i32,
    }
}

type SockAddrFn = unsafe extern "system" fn(usize, *mut SOCKADDR, *mut i32) -> i32;

fn socket_addr_with(op: SockAddrFn, fd: RawSock) -> io::Result<SocketAddr> {
    let mut storage = MaybeUninit::<SOCKADDR_STORAGE>::zeroed();
    let mut len = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;
    // SAFETY: `storage`/`len` are valid out-pointers of the declared size.
    cvt(unsafe {
        op(
            fd as usize,
            storage.as_mut_ptr().cast::<SOCKADDR>(),
            &mut len,
        )
    })?;
    // SAFETY: the call succeeded, so the storage prefix is initialized.
    let storage = unsafe { storage.assume_init() };
    socket_addr_from_storage(&storage, len)
}

fn socket_family(fd: RawSock) -> io::Result<ADDRESS_FAMILY> {
    let mut storage = MaybeUninit::<SOCKADDR_STORAGE>::zeroed();
    let mut len = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;
    // SAFETY: as in `socket_addr_with`.
    cvt(unsafe {
        getsockname(
            fd as usize,
            storage.as_mut_ptr().cast::<SOCKADDR>(),
            &mut len,
        )
    })?;
    // SAFETY: the call succeeded, so at least the family field is initialized.
    let storage = unsafe { storage.assume_init() };
    Ok(storage.ss_family)
}

fn getsockopt_int(fd: RawSock, level: i32, name: i32) -> io::Result<i32> {
    let mut value = 0i32;
    let mut len = std::mem::size_of::<i32>() as i32;
    // SAFETY: `value`/`len` are valid out-pointers of the declared size.
    cvt(unsafe {
        getsockopt(
            fd as usize,
            level,
            name,
            (&raw mut value).cast::<u8>(),
            &mut len,
        )
    })?;
    Ok(value)
}

fn setsockopt_int(fd: RawSock, level: i32, name: i32, value: i32) -> io::Result<()> {
    // SAFETY: `value` is passed by pointer with its exact size.
    cvt(unsafe {
        setsockopt(
            fd as usize,
            level,
            name,
            (&raw const value).cast::<u8>(),
            std::mem::size_of::<i32>() as i32,
        )
    })
    .map(|_| ())
}

fn socket_addr_from_storage(storage: &SOCKADDR_STORAGE, len: i32) -> io::Result<SocketAddr> {
    match storage.ss_family {
        family if family == AF_INET => {
            if (len as usize) < std::mem::size_of::<SOCKADDR_IN>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sockaddr_in length is truncated",
                ));
            }
            // SAFETY: the family says this storage holds a `SOCKADDR_IN`, and
            // the length was just validated.
            let addr = unsafe { *(storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR_IN>() };
            // SAFETY: reading the `S_addr` view of the address union; all
            // views are plain integers/bytes.
            let ip = Ipv4Addr::from(u32::from_be(unsafe { addr.sin_addr.S_un.S_addr }));
            Ok(SocketAddr::V4(SocketAddrV4::new(
                ip,
                u16::from_be(addr.sin_port),
            )))
        }
        family if family == AF_INET6 => {
            if (len as usize) < std::mem::size_of::<SOCKADDR_IN6>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sockaddr_in6 length is truncated",
                ));
            }
            // SAFETY: the family says this storage holds a `SOCKADDR_IN6`, and
            // the length was just validated.
            let addr = unsafe { *(storage as *const SOCKADDR_STORAGE).cast::<SOCKADDR_IN6>() };
            // SAFETY: reading plain byte/integer views of the unions.
            let (octets, scope_id) =
                unsafe { (addr.sin6_addr.u.Byte, addr.Anonymous.sin6_scope_id) };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(octets),
                u16::from_be(addr.sin6_port),
                addr.sin6_flowinfo,
                scope_id,
            )))
        }
        family => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported socket family {family}"),
        )),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RawSocketAddr {
    storage: SOCKADDR_STORAGE,
    len: i32,
}

impl RawSocketAddr {
    fn from_socket_addr(addr: SocketAddr) -> Self {
        // SAFETY: `SOCKADDR_STORAGE` is a plain C struct; zeroed is valid.
        let mut storage = unsafe { std::mem::zeroed::<SOCKADDR_STORAGE>() };
        match addr {
            SocketAddr::V4(addr) => {
                let sockaddr = SOCKADDR_IN {
                    sin_family: AF_INET,
                    sin_port: addr.port().to_be(),
                    sin_addr: IN_ADDR {
                        S_un: IN_ADDR_0 {
                            S_addr: u32::from_ne_bytes(addr.ip().octets()),
                        },
                    },
                    sin_zero: [0; 8],
                };
                // SAFETY: `SOCKADDR_IN` fits inside `SOCKADDR_STORAGE`.
                unsafe {
                    std::ptr::write((&raw mut storage).cast::<SOCKADDR_IN>(), sockaddr);
                }
                Self {
                    storage,
                    len: std::mem::size_of::<SOCKADDR_IN>() as i32,
                }
            }
            SocketAddr::V6(addr) => {
                let sockaddr = SOCKADDR_IN6 {
                    sin6_family: AF_INET6,
                    sin6_port: addr.port().to_be(),
                    sin6_flowinfo: addr.flowinfo(),
                    sin6_addr: IN6_ADDR {
                        u: IN6_ADDR_0 {
                            Byte: addr.ip().octets(),
                        },
                    },
                    Anonymous: SOCKADDR_IN6_0 {
                        sin6_scope_id: addr.scope_id(),
                    },
                };
                // SAFETY: `SOCKADDR_IN6` fits inside `SOCKADDR_STORAGE`.
                unsafe {
                    std::ptr::write((&raw mut storage).cast::<SOCKADDR_IN6>(), sockaddr);
                }
                Self {
                    storage,
                    len: std::mem::size_of::<SOCKADDR_IN6>() as i32,
                }
            }
        }
    }

    fn as_ptr(&self) -> *const SOCKADDR {
        (&raw const self.storage).cast::<SOCKADDR>()
    }

    fn len(&self) -> i32 {
        self.len
    }
}

fn should_try_ipv4_loopback(addr: SocketAddr, error: &io::Error) -> bool {
    matches!(addr, SocketAddr::V6(v6) if v6.ip().is_loopback())
        && matches!(
            error.raw_os_error(),
            Some(code) if code == WSAEADDRNOTAVAIL || code == WSAEAFNOSUPPORT || code == WSAENETUNREACH
        )
}

fn localhost_v4(addr: SocketAddr) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, addr.port()))
}

fn last_wsa_error() -> io::Error {
    // SAFETY: no pointer arguments.
    io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
}

fn cvt(result: i32) -> io::Result<i32> {
    if result == SOCKET_ERROR {
        Err(last_wsa_error())
    } else {
        Ok(result)
    }
}

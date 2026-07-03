//! Linux networking backend.

use std::cell::Cell;
use std::ffi::c_void;
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::net::{
    Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs,
};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

thread_local! {
    // None = untested, Some(true) = io_uring works, Some(false) = use offload.
    // After the first successful IORING_OP_SEND, the clone in send() is skipped
    // for all subsequent calls on the same thread.
    static SEND_URING_SUPPORTED: Cell<Option<bool>> = const { Cell::new(None) };
}

use crate::op::completion::completion_for_current_thread;
use crate::op::net::{AcceptedSocket, NetOp, ReceivedDatagram};
use crate::platform::linux::runtime::with_current_driver;
use crate::platform::linux::uring::{
    IORING_OP_ACCEPT, IORING_OP_BIND, IORING_OP_CLOSE, IORING_OP_CONNECT, IORING_OP_LISTEN,
    IORING_OP_RECV, IORING_OP_RECVMSG, IORING_OP_SEND, IORING_OP_SENDMSG, IORING_OP_SHUTDOWN,
    IORING_OP_SOCKET, IoUringCqe, IoUringSqe,
};
use crate::sys::current::fd::{wait_readable, wait_writable};

const DEFAULT_LISTENER_BACKLOG: i32 = 1024;

// TODO(roadmap): unwired io_uring net-close / op-classifier scaffolding; a Linux
// agent will wire or remove it before release. See ROADMAP.md.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPath {
    IoUring,
    Offload,
}

#[allow(dead_code)]
pub fn execution_path(op: &NetOp) -> ExecutionPath {
    match op {
        NetOp::Socket { .. }
        | NetOp::Connect { .. }
        | NetOp::Bind { .. }
        | NetOp::Listen { .. }
        | NetOp::Accept { .. }
        | NetOp::Send { .. }
        | NetOp::Recv { .. }
        | NetOp::Shutdown { .. }
        | NetOp::Close { .. } => ExecutionPath::IoUring,
        NetOp::SendTo { .. } | NetOp::RecvFrom { .. } => ExecutionPath::IoUring,
    }
}

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

    match submit_uring::<OwnedFd, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_SOCKET;
            sqe.fd = domain;
            // IORING_OP_SOCKET reads the socket type (including the
            // SOCK_CLOEXEC / SOCK_NONBLOCK bits) from `sqe.off`, exactly like
            // the `type` argument of `socket(2)`; it does NOT consume socket
            // creation flags from `rw_flags` (`op_flags`), which must be 0.
            // Placing the flags in `op_flags` silently dropped SOCK_CLOEXEC,
            // leaking uring-created sockets across `exec`. OR them into the
            // type instead, mirroring `socket_sync`.
            sqe.off = u64::from(socket_type as u32 | flags);
            sqe.len = protocol as u32;
            sqe.op_flags = 0;
        },
        // SAFETY: `fd` is the non-negative descriptor returned by a successful
        // socket CQE and ownership is transferred to `OwnedFd` exactly once.
        move |cqe| cqe_to_result(cqe).map(|fd| unsafe { OwnedFd::from_raw_fd(fd as RawFd) }),
    )
    .await
    {
        // `socket(2)` never blocks, so when io_uring lacks IORING_OP_SOCKET we run
        // it inline on the event-loop thread rather than paying for a blocking-pool
        // thread hop.
        Err(error) if should_fallback_to_offload(&error) => {
            socket_sync(domain, socket_type, protocol, flags)
        }
        result => result,
    }
}

pub async fn connect(op: NetOp) -> io::Result<()> {
    let NetOp::Connect { fd, addr } = op else {
        unreachable!("connect backend called with non-connect op");
    };

    let raw_addr = RawSocketAddr::from_socket_addr(addr);
    let fallback_addr = raw_addr;
    let addr_ptr = raw_addr.as_ptr();
    let addr_len = raw_addr.len();
    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_CONNECT;
            sqe.fd = fd;
            sqe.addr = addr_ptr as u64;
            sqe.off = addr_len as u64;
        },
        move |cqe| {
            let _raw_addr = raw_addr;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
    {
        Err(error) if should_fallback_to_offload(&error) => connect_ready(fd, fallback_addr).await,
        result => result,
    }
}

pub async fn bind(op: NetOp) -> io::Result<()> {
    let NetOp::Bind { fd, addr } = op else {
        unreachable!("bind backend called with non-bind op");
    };

    let raw_addr = RawSocketAddr::from_socket_addr(addr);
    let fallback_addr = raw_addr;
    let addr_ptr = raw_addr.as_ptr();
    let addr_len = raw_addr.len();
    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_BIND;
            sqe.fd = fd;
            sqe.addr = addr_ptr as u64;
            sqe.off = addr_len as u64;
        },
        move |cqe| {
            let _raw_addr = raw_addr;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
    {
        // `bind(2)` never blocks; run inline instead of bouncing to the blocking pool.
        Err(error) if should_fallback_to_offload(&error) => bind_sync(fd, fallback_addr),
        result => result,
    }
}

pub async fn listen(op: NetOp) -> io::Result<()> {
    let NetOp::Listen { fd, backlog } = op else {
        unreachable!("listen backend called with non-listen op");
    };

    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_LISTEN;
            sqe.fd = fd;
            sqe.len = backlog as u32;
        },
        move |cqe| cqe_to_result(cqe).map(|_| ()),
    )
    .await
    {
        // `listen(2)` never blocks; run inline instead of bouncing to the blocking pool.
        Err(error) if should_fallback_to_offload(&error) => listen_sync(fd, backlog),
        result => result,
    }
}

pub async fn accept(op: NetOp) -> io::Result<AcceptedSocket> {
    let NetOp::Accept { fd } = op else {
        unreachable!("accept backend called with non-accept op");
    };

    let mut storage = Box::new(MaybeUninit::<libc::sockaddr_storage>::zeroed());
    let mut addr_len = Box::new(std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t);
    let storage_ptr = storage.as_mut_ptr();
    let addr_len_ptr = addr_len.as_mut() as *mut libc::socklen_t;

    match submit_uring::<AcceptedSocket, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_ACCEPT;
            sqe.fd = fd;
            sqe.addr = storage_ptr as u64;
            sqe.off = addr_len_ptr as u64;
            // For IORING_OP_ACCEPT, op_flags carries the accept4(2) flags. Set
            // SOCK_CLOEXEC so accepted connections are not leaked into child
            // processes (matching the accept_sync fallback and std/tokio).
            sqe.op_flags = libc::SOCK_CLOEXEC as u32;
        },
        move |cqe| {
            let accepted_fd = cqe_to_result(cqe)? as RawFd;
            // Take ownership of the accepted fd immediately so that an error
            // parsing the peer address below closes it via `OwnedFd`'s drop
            // rather than leaking a live connection.
            // SAFETY: `accepted_fd` is a fresh descriptor from a successful
            // accept CQE, owned here exactly once.
            let accepted = unsafe { OwnedFd::from_raw_fd(accepted_fd) };
            // SAFETY: a successful accept CQE means the kernel initialized
            // `*addr_len` bytes of the zeroed sockaddr_storage buffer.
            let storage = unsafe { storage.assume_init() };
            let peer_addr = socket_addr_from_storage(&storage, *addr_len)?;
            Ok(AcceptedSocket {
                fd: accepted.into_raw_fd(),
                peer_addr,
            })
        },
    )
    .await
    {
        Err(error) if should_fallback_to_offload(&error) => accept_ready(fd).await,
        result => result,
    }
}

pub async fn send(op: NetOp) -> io::Result<usize> {
    let NetOp::Send { fd, data, flags } = op else {
        unreachable!("send backend called with non-send op");
    };

    // Capability known: io_uring SEND is not supported — use the readiness
    // fallback directly instead of probing the ring again.
    if SEND_URING_SUPPORTED.with(|c| c.get()) == Some(false) {
        return send_ready(fd, data, flags).await;
    }

    // Capability known: io_uring SEND works — submit without a fallback clone.
    if SEND_URING_SUPPORTED.with(|c| c.get()) == Some(true) {
        let data = Arc::new(data.into_boxed_slice());
        let data_ptr = data.as_ptr();
        let data_len = data.len();
        return submit_uring_guarded::<usize, _>(
            move |sqe| {
                sqe.opcode = IORING_OP_SEND;
                sqe.fd = fd;
                sqe.addr = data_ptr as u64;
                sqe.len = data_len as u32;
                sqe.op_flags = flags as u32;
            },
            Box::new(Arc::clone(&data)),
            move |cqe| {
                let _data = data;
                cqe_to_result(cqe).map(|written| written as usize)
            },
        )
        .await;
    }

    // Capability unknown: probe with a one-time clone. Cache the result.
    let fallback_data = data.clone();
    let data = Arc::new(data.into_boxed_slice());
    let data_ptr = data.as_ptr();
    let data_len = data.len();
    match submit_uring_guarded::<usize, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_SEND;
            sqe.fd = fd;
            sqe.addr = data_ptr as u64;
            sqe.len = data_len as u32;
            sqe.op_flags = flags as u32;
        },
        Box::new(Arc::clone(&data)),
        move |cqe| {
            let _data = data;
            cqe_to_result(cqe).map(|written| written as usize)
        },
    )
    .await
    {
        Err(error) if should_fallback_to_offload(&error) => {
            SEND_URING_SUPPORTED.with(|c| c.set(Some(false)));
            send_ready(fd, fallback_data, flags).await
        }
        result => {
            if result.is_ok() {
                SEND_URING_SUPPORTED.with(|c| c.set(Some(true)));
            }
            result
        }
    }
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

    let raw_addr = Box::new(RawSocketAddr::from_socket_addr(target));
    let mut iov = Box::new(libc::iovec {
        iov_base: data.as_ptr() as *mut c_void,
        iov_len: data.len(),
    });
    // SAFETY: `msghdr` is a plain C struct where all-zero is a valid empty
    // state; the fields used by sendmsg are filled before submission.
    let mut msg = Box::new(unsafe { std::mem::zeroed::<libc::msghdr>() });
    msg.msg_name = raw_addr.as_ptr() as *mut c_void;
    msg.msg_namelen = raw_addr.len();
    msg.msg_iov = iov.as_mut() as *mut libc::iovec;
    msg.msg_iovlen = 1;
    let msg_ptr = msg.as_mut() as *mut libc::msghdr as u64;
    let iov = SendIovec(iov);
    let msg = SendMsghdr(msg);

    submit_uring::<usize, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_SENDMSG;
            sqe.fd = fd;
            sqe.addr = msg_ptr;
            sqe.op_flags = flags as u32;
        },
        move |cqe| {
            let _raw_addr = raw_addr;
            let _iov = iov;
            let _msg = msg;
            let _data = data;
            cqe_to_result(cqe).map(|written| written as usize)
        },
    )
    .await
}

pub async fn recv(op: NetOp) -> io::Result<Vec<u8>> {
    let NetOp::Recv { fd, len, flags } = op else {
        unreachable!("recv backend called with non-recv op");
    };

    let buffer = Arc::new(Mutex::new(vec![0; len].into_boxed_slice()));
    let buffer_ptr = buffer.lock().unwrap().as_mut_ptr();
    let buffer_len = len;
    match submit_uring_guarded::<Vec<u8>, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_RECV;
            sqe.fd = fd;
            sqe.addr = buffer_ptr as u64;
            sqe.len = buffer_len as u32;
            sqe.op_flags = flags as u32;
        },
        Box::new(Arc::clone(&buffer)),
        move |cqe| {
            let read = cqe_to_result(cqe)? as usize;
            let buffer = buffer.lock().unwrap();
            Ok(buffer[..read].to_vec())
        },
    )
    .await
    {
        Err(error) if should_fallback_to_offload(&error) => recv_ready(fd, len, flags).await,
        result => result,
    }
}

pub async fn recv_from(op: NetOp) -> io::Result<ReceivedDatagram> {
    let NetOp::RecvFrom { fd, len, flags } = op else {
        unreachable!("recv_from backend called with non-recv_from op");
    };

    let mut data = vec![0u8; len];
    let mut storage = Box::new(MaybeUninit::<libc::sockaddr_storage>::zeroed());
    let mut iov = Box::new(libc::iovec {
        iov_base: data.as_mut_ptr() as *mut c_void,
        iov_len: data.len(),
    });
    // SAFETY: `msghdr` is a plain C struct where all-zero is a valid empty
    // state; the fields used by recvmsg are filled before submission.
    let mut msg = Box::new(unsafe { std::mem::zeroed::<libc::msghdr>() });
    msg.msg_name = storage.as_mut_ptr() as *mut c_void;
    msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    msg.msg_iov = iov.as_mut() as *mut libc::iovec;
    msg.msg_iovlen = 1;
    let msg_ptr = msg.as_mut() as *mut libc::msghdr as u64;
    let iov = SendIovec(iov);
    let msg = SendMsghdr(msg);

    match submit_uring::<ReceivedDatagram, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_RECVMSG;
            sqe.fd = fd;
            sqe.addr = msg_ptr;
            sqe.op_flags = flags as u32;
        },
        move |cqe| {
            let _iov = iov;
            let addr_len = msg.0.msg_namelen;
            drop(msg);
            let read = cqe_to_result(cqe)? as usize;
            data.truncate(read);
            // SAFETY: a successful recvmsg CQE means the kernel initialized
            // `addr_len` bytes of the zeroed sockaddr_storage buffer.
            let storage = unsafe { storage.assume_init() };
            let peer_addr = socket_addr_from_storage(&storage, addr_len)?;
            Ok(ReceivedDatagram { data, peer_addr })
        },
    )
    .await
    {
        Err(error) if should_fallback_to_offload(&error) => recv_from_ready(fd, len, flags).await,
        result => result,
    }
}

pub async fn shutdown(op: NetOp) -> io::Result<()> {
    let NetOp::Shutdown { fd, how } = op else {
        unreachable!("shutdown backend called with non-shutdown op");
    };

    let fallback_how = how;
    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_SHUTDOWN;
            sqe.fd = fd;
            sqe.len = shutdown_how(how) as u32;
        },
        move |cqe| cqe_to_result(cqe).map(|_| ()),
    )
    .await
    {
        // `shutdown(2)` never blocks; run inline instead of bouncing to the blocking pool.
        Err(error) if should_fallback_to_offload(&error) => shutdown_sync(fd, fallback_how),
        result => result,
    }
}

#[allow(dead_code)]
pub async fn close(op: NetOp) -> io::Result<()> {
    let NetOp::Close { fd } = op else {
        unreachable!("close backend called with non-close op");
    };

    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_CLOSE;
            sqe.fd = fd;
        },
        move |cqe| cqe_to_result(cqe).map(|_| ()),
    )
    .await
    {
        // `close(2)` never blocks; run inline instead of bouncing to the blocking pool.
        Err(error) if should_fallback_to_offload(&error) => close_sync(fd),
        result => result,
    }
}

pub async fn connect_stream(addr: SocketAddr) -> io::Result<OwnedFd> {
    let socket = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_STREAM,
        protocol: 0,
        flags: libc::SOCK_CLOEXEC as u32,
    })
    .await?;

    let connect_result = connect(NetOp::Connect {
        fd: socket.as_raw_fd(),
        addr,
    })
    .await;
    match connect_result {
        Ok(()) => Ok(socket),
        Err(error) => Err(error),
    }
}

pub async fn bind_listener(addr: SocketAddr, backlog: Option<i32>) -> io::Result<OwnedFd> {
    let listener = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_STREAM,
        protocol: 0,
        flags: libc::SOCK_CLOEXEC as u32,
    })
    .await?;

    set_reuse_addr(listener.as_raw_fd(), true)?;

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

pub async fn bind_datagram(addr: SocketAddr) -> io::Result<OwnedFd> {
    let socket = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_DGRAM,
        protocol: 0,
        flags: libc::SOCK_CLOEXEC as u32,
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
    socket_sync(
        libc::AF_INET,
        libc::SOCK_STREAM,
        0,
        (libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK) as u32,
    )
}

pub fn tcp_socket_v6() -> io::Result<OwnedFd> {
    socket_sync(
        libc::AF_INET6,
        libc::SOCK_STREAM,
        0,
        (libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK) as u32,
    )
}

pub fn bind_socket(fd: RawFd, addr: SocketAddr) -> io::Result<()> {
    bind_sync(fd, RawSocketAddr::from_socket_addr(addr))
}

pub fn listen_socket(fd: RawFd, backlog: i32) -> io::Result<()> {
    listen_sync(fd, backlog)
}

pub async fn duplicate(fd: RawFd) -> io::Result<OwnedFd> {
    // `fcntl(F_DUPFD_CLOEXEC)` never blocks, so run it inline rather than on the
    // blocking pool.
    // SAFETY: `fd` is a valid descriptor for the duration of the fcntl call;
    // F_DUPFD_CLOEXEC does not access user pointers.
    let duplicated = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
    // SAFETY: `duplicated` is a fresh descriptor returned by successful fcntl
    // and ownership is transferred to `OwnedFd` exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

pub async fn recv_timeout(
    fd: RawFd,
    len: usize,
    flags: i32,
    timeout: Duration,
) -> io::Result<Vec<u8>> {
    let buffer = Arc::new(Mutex::new(vec![0; len].into_boxed_slice()));
    let buffer_ptr = buffer.lock().unwrap().as_mut_ptr();
    let buffer_len = len;
    submit_uring_with_linked_timeout_guarded::<Vec<u8>, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_RECV;
            sqe.fd = fd;
            sqe.addr = buffer_ptr as u64;
            sqe.len = buffer_len as u32;
            sqe.op_flags = flags as u32;
        },
        timeout,
        Box::new(Arc::clone(&buffer)),
        move |cqe| {
            let read = cqe_to_timed_result(cqe)? as usize;
            let buffer = buffer.lock().unwrap();
            Ok(buffer[..read].to_vec())
        },
    )
    .await
}

pub async fn send_timeout(
    fd: RawFd,
    data: Vec<u8>,
    flags: i32,
    timeout: Duration,
) -> io::Result<usize> {
    let data = Arc::new(data.into_boxed_slice());
    let data_ptr = data.as_ptr();
    let data_len = data.len();
    submit_uring_with_linked_timeout_guarded::<usize, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_SEND;
            sqe.fd = fd;
            sqe.addr = data_ptr as u64;
            sqe.len = data_len as u32;
            sqe.op_flags = flags as u32;
        },
        timeout,
        Box::new(Arc::clone(&data)),
        move |cqe| {
            let _data = data;
            cqe_to_timed_result(cqe).map(|written| written as usize)
        },
    )
    .await
}

pub async fn recv_from_timeout(
    fd: RawFd,
    len: usize,
    flags: i32,
    timeout: Duration,
) -> io::Result<ReceivedDatagram> {
    let mut data = vec![0u8; len];
    let mut storage = Box::new(MaybeUninit::<libc::sockaddr_storage>::zeroed());
    let mut iov = Box::new(libc::iovec {
        iov_base: data.as_mut_ptr() as *mut c_void,
        iov_len: data.len(),
    });
    // SAFETY: `msghdr` is a plain C struct where all-zero is a valid empty
    // state; the fields used by recvmsg are filled before submission.
    let mut msg = Box::new(unsafe { std::mem::zeroed::<libc::msghdr>() });
    msg.msg_name = storage.as_mut_ptr() as *mut c_void;
    msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    msg.msg_iov = iov.as_mut() as *mut libc::iovec;
    msg.msg_iovlen = 1;
    let msg_ptr = msg.as_mut() as *mut libc::msghdr as u64;
    let iov = SendIovec(iov);
    let msg = SendMsghdr(msg);

    submit_uring_with_linked_timeout::<ReceivedDatagram, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_RECVMSG;
            sqe.fd = fd;
            sqe.addr = msg_ptr;
            sqe.op_flags = flags as u32;
        },
        timeout,
        move |cqe| {
            let _iov = iov;
            let addr_len = msg.0.msg_namelen;
            drop(msg);
            let read = cqe_to_timed_result(cqe)? as usize;
            data.truncate(read);
            // SAFETY: a successful recvmsg CQE means the kernel initialized
            // `addr_len` bytes of the zeroed sockaddr_storage buffer.
            let storage = unsafe { storage.assume_init() };
            let peer_addr = socket_addr_from_storage(&storage, addr_len)?;
            Ok(ReceivedDatagram { data, peer_addr })
        },
    )
    .await
}

pub async fn send_to_timeout(
    fd: RawFd,
    data: Vec<u8>,
    target: SocketAddr,
    flags: i32,
    timeout: Duration,
) -> io::Result<usize> {
    let raw_addr = Box::new(RawSocketAddr::from_socket_addr(target));
    let mut iov = Box::new(libc::iovec {
        iov_base: data.as_ptr() as *mut c_void,
        iov_len: data.len(),
    });
    // SAFETY: `msghdr` is a plain C struct where all-zero is a valid empty
    // state; the fields used by sendmsg are filled before submission.
    let mut msg = Box::new(unsafe { std::mem::zeroed::<libc::msghdr>() });
    msg.msg_name = raw_addr.as_ptr() as *mut c_void;
    msg.msg_namelen = raw_addr.len();
    msg.msg_iov = iov.as_mut() as *mut libc::iovec;
    msg.msg_iovlen = 1;
    let msg_ptr = msg.as_mut() as *mut libc::msghdr as u64;
    let iov = SendIovec(iov);
    let msg = SendMsghdr(msg);

    submit_uring_with_linked_timeout::<usize, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_SENDMSG;
            sqe.fd = fd;
            sqe.addr = msg_ptr;
            sqe.op_flags = flags as u32;
        },
        timeout,
        move |cqe| {
            let _raw_addr = raw_addr;
            let _iov = iov;
            let _msg = msg;
            let _data = data;
            cqe_to_timed_result(cqe).map(|written| written as usize)
        },
    )
    .await
}

pub async fn connect_stream_timeout(addr: SocketAddr, timeout: Duration) -> io::Result<OwnedFd> {
    let socket = socket(NetOp::Socket {
        domain: socket_domain(addr),
        socket_type: libc::SOCK_STREAM,
        protocol: 0,
        flags: libc::SOCK_CLOEXEC as u32,
    })
    .await?;

    let fd = socket.as_raw_fd();
    let raw_addr = RawSocketAddr::from_socket_addr(addr);
    let addr_ptr = raw_addr.as_ptr();
    let addr_len = raw_addr.len();

    submit_uring_with_linked_timeout::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_CONNECT;
            sqe.fd = fd;
            sqe.addr = addr_ptr as u64;
            sqe.off = addr_len as u64;
        },
        timeout,
        move |cqe| {
            let _raw_addr = raw_addr;
            cqe_to_timed_result(cqe).map(|_| ())
        },
    )
    .await?;

    Ok(socket)
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
    // SAFETY: `fd` is a valid TCP socket descriptor; `value` and `len` point
    // to writable initialized storage for the duration of getsockopt.
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
    // SAFETY: `fd` is a valid TCP socket descriptor; `value` points to an
    // initialized c_int with the exact length passed to setsockopt.
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

pub type RecvFuture = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
pub type SendFuture = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;
pub type ShutdownFuture = Pin<Box<dyn Future<Output = io::Result<()>> + 'static>>;

pub fn recv_future(fd: RawFd, len: usize) -> RecvFuture {
    Box::pin(recv(NetOp::Recv { fd, len, flags: 0 }))
}

pub fn send_future(fd: RawFd, data: Vec<u8>) -> SendFuture {
    Box::pin(send(NetOp::Send { fd, data, flags: 0 }))
}

pub fn shutdown_future(fd: RawFd, how: Shutdown) -> ShutdownFuture {
    Box::pin(shutdown(NetOp::Shutdown { fd, how }))
}

async fn submit_uring<T: Send + 'static, M>(
    fill: impl FnOnce(&mut IoUringSqe),
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    submit_uring_guarded(fill, Box::new(()), map).await
}

async fn submit_uring_guarded<T: Send + 'static, M>(
    fill: impl FnOnce(&mut IoUringSqe),
    guard: Box<dyn std::any::Any + Send + 'static>,
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let callback_handle = handle.clone();
    let token = with_current_driver(|driver| {
        driver.submit_operation(fill, move |cqe| {
            callback_handle.complete(map(cqe));
        })
    })?;

    handle.set_cancel(move || {
        let _ =
            with_current_driver(|driver| driver.cancel_operation_with_guard(token, Some(guard)));
    });

    future.await
}

/// Like [`submit_uring`] but pairs the main SQE with an `IORING_OP_LINK_TIMEOUT`.
///
/// If the timeout elapses before the main op completes, the completion callback
/// receives a CQE with `res = -ECANCELED`.  Callers should map `-ECANCELED` to
/// `io::ErrorKind::TimedOut`.
async fn submit_uring_with_linked_timeout<T: Send + 'static, M>(
    fill: impl FnOnce(&mut IoUringSqe),
    timeout: Duration,
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    submit_uring_with_linked_timeout_guarded(fill, timeout, Box::new(()), map).await
}

async fn submit_uring_with_linked_timeout_guarded<T: Send + 'static, M>(
    fill: impl FnOnce(&mut IoUringSqe),
    timeout: Duration,
    guard: Box<dyn std::any::Any + Send + 'static>,
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let callback_handle = handle.clone();
    let token = with_current_driver(|driver| {
        driver.submit_operation_with_linked_timeout(fill, timeout, move |cqe| {
            callback_handle.complete(map(cqe));
        })
    })?;

    handle.set_cancel(move || {
        let _ =
            with_current_driver(|driver| driver.cancel_operation_with_guard(token, Some(guard)));
    });

    future.await
}

async fn offload<T: Send + 'static>(
    task: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let handle_for_task = handle.clone();
    if let Err(error) =
        crate::sys::blocking::spawn_blocking(move || handle_for_task.complete(task()))
    {
        handle.complete(Err(error));
    }
    future.await
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
    // SAFETY: `fd` is valid for the duration of the call, and `storage`/`len`
    // point to writable storage sized for a sockaddr_storage result.
    cvt(unsafe { op(fd, storage.as_mut_ptr().cast::<libc::sockaddr>(), &mut len) })?;
    // SAFETY: a successful socket address query initializes `len` bytes of the
    // zeroed sockaddr_storage buffer.
    let storage = unsafe { storage.assume_init() };
    socket_addr_from_storage(&storage, len)
}

fn socket_family(fd: RawFd) -> io::Result<i32> {
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `fd` is valid for the duration of the call, and `storage`/`len`
    // point to writable storage sized for a sockaddr_storage result.
    cvt(unsafe { libc::getsockname(fd, storage.as_mut_ptr().cast::<libc::sockaddr>(), &mut len) })?;
    // SAFETY: successful getsockname initialized the zeroed sockaddr_storage
    // buffer with at least the family field read below.
    let storage = unsafe { storage.assume_init() };
    Ok(storage.ss_family as i32)
}

fn getsockopt_int(fd: RawFd, level: i32, name: i32) -> io::Result<i32> {
    let mut value = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` is valid for the duration of the call, and `value`/`len`
    // point to writable initialized storage for an integer socket option.
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
    // SAFETY: `fd` is valid for the duration of the call, and `value` points
    // to an initialized c_int with the exact length passed to setsockopt.
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
                    "short IPv4 socket address from kernel",
                ));
            }

            // SAFETY: the family is AF_INET and `len` was checked large enough
            // for a sockaddr_in before casting the sockaddr_storage bytes.
            let addr = unsafe { *(storage as *const _ as *const libc::sockaddr_in) };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(addr.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(addr.sin_port),
            )))
        }
        libc::AF_INET6 => {
            if len < std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "short IPv6 socket address from kernel",
                ));
            }

            // SAFETY: the family is AF_INET6 and `len` was checked large enough
            // for a sockaddr_in6 before casting the sockaddr_storage bytes.
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
            format!("unsupported socket address family {family}"),
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
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: addr.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(addr.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                let mut storage =
                    // SAFETY: sockaddr_storage is a plain storage buffer; an
                    // all-zero value is valid before writing a sockaddr_in into it.
                    unsafe { MaybeUninit::<libc::sockaddr_storage>::zeroed().assume_init() };
                // SAFETY: `storage` is properly aligned and large enough for
                // sockaddr_in; the written bytes are tracked by `len`.
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
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: addr.port().to_be(),
                    sin6_flowinfo: addr.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: addr.ip().octets(),
                    },
                    sin6_scope_id: addr.scope_id(),
                };
                let mut storage =
                    // SAFETY: sockaddr_storage is a plain storage buffer; an
                    // all-zero value is valid before writing a sockaddr_in6 into it.
                    unsafe { MaybeUninit::<libc::sockaddr_storage>::zeroed().assume_init() };
                // SAFETY: `storage` is properly aligned and large enough for
                // sockaddr_in6; the written bytes are tracked by `len`.
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

fn cqe_to_result(cqe: IoUringCqe) -> io::Result<i32> {
    if cqe.res < 0 {
        Err(io::Error::from_raw_os_error(-cqe.res))
    } else {
        Ok(cqe.res)
    }
}

/// Like [`cqe_to_result`] but maps `-ECANCELED` (timeout fired) to `TimedOut`.
fn cqe_to_timed_result(cqe: IoUringCqe) -> io::Result<i32> {
    if cqe.res == -libc::ECANCELED {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "socket operation timed out",
        ));
    }
    cqe_to_result(cqe)
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

fn should_fallback_to_offload(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
    )
}

fn socket_sync(domain: i32, socket_type: i32, protocol: i32, flags: u32) -> io::Result<OwnedFd> {
    // SAFETY: socket takes only integer arguments; no user pointers are passed.
    let fd = cvt(unsafe { libc::socket(domain, socket_type | flags as i32, protocol) })?;
    // SAFETY: `fd` is a fresh descriptor returned by successful socket and
    // ownership is transferred to `OwnedFd` exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn bind_sync(fd: RawFd, addr: RawSocketAddr) -> io::Result<()> {
    // SAFETY: `fd` is valid for the duration of the call, and `addr` points to
    // `addr.len()` initialized bytes describing a sockaddr.
    cvt(unsafe { libc::bind(fd, addr.as_ptr(), addr.len()) }).map(|_| ())
}

fn listen_sync(fd: RawFd, backlog: i32) -> io::Result<()> {
    // SAFETY: `fd` is a valid socket descriptor for the duration of the call;
    // listen takes no user pointers.
    cvt(unsafe { libc::listen(fd, backlog) }).map(|_| ())
}

fn accept_sync(fd: RawFd) -> io::Result<AcceptedSocket> {
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `fd` is a valid listener descriptor; `storage` and `len` point to
    // writable storage sized for the peer sockaddr result.
    let accepted_fd = cvt(unsafe {
        libc::accept4(
            fd,
            storage.as_mut_ptr().cast::<libc::sockaddr>(),
            &mut len,
            libc::SOCK_CLOEXEC,
        )
    })?;
    // SAFETY: successful accept4 initialized `len` bytes of the zeroed
    // sockaddr_storage buffer.
    let storage = unsafe { storage.assume_init() };
    let peer_addr = socket_addr_from_storage(&storage, len)?;
    Ok(AcceptedSocket {
        fd: accepted_fd,
        peer_addr,
    })
}

fn send_slice_sync(fd: RawFd, data: &[u8], flags: i32) -> io::Result<usize> {
    // SAFETY: `fd` is valid for the duration of the call, and `data` is an
    // immutable buffer valid for `data.len()` bytes.
    let written = unsafe { libc::send(fd, data.as_ptr().cast::<c_void>(), data.len(), flags) };
    cvt_long(written).map(|written| written as usize)
}

fn recv_sync(fd: RawFd, len: usize, flags: i32) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0; len];
    // SAFETY: `fd` is valid for the duration of the call, and `buffer` is
    // exclusively owned and writable for `buffer.len()` bytes.
    let read = unsafe {
        libc::recv(
            fd,
            buffer.as_mut_ptr().cast::<c_void>(),
            buffer.len(),
            flags,
        )
    };
    let read = cvt_long(read)? as usize;
    buffer.truncate(read);
    Ok(buffer)
}

fn recv_from_sync(fd: RawFd, len: usize, flags: i32) -> io::Result<ReceivedDatagram> {
    let mut buffer = vec![0; len];
    let mut storage = MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: `fd` is valid for the duration of the call; `buffer` is writable
    // for `buffer.len()` bytes and `storage`/`addr_len` describe writable
    // sockaddr_storage for the sender address.
    let read = unsafe {
        libc::recvfrom(
            fd,
            buffer.as_mut_ptr().cast::<c_void>(),
            buffer.len(),
            flags,
            storage.as_mut_ptr().cast::<libc::sockaddr>(),
            &mut addr_len,
        )
    };
    let read = cvt_long(read)? as usize;
    buffer.truncate(read);
    // SAFETY: successful recvfrom initialized `addr_len` bytes of the zeroed
    // sockaddr_storage buffer.
    let storage = unsafe { storage.assume_init() };
    let peer_addr = socket_addr_from_storage(&storage, addr_len)?;
    Ok(ReceivedDatagram {
        data: buffer,
        peer_addr,
    })
}

fn shutdown_sync(fd: RawFd, how: Shutdown) -> io::Result<()> {
    // SAFETY: `fd` is a valid socket descriptor for the duration of the call;
    // shutdown takes no user pointers.
    cvt(unsafe { libc::shutdown(fd, shutdown_how(how)) }).map(|_| ())
}

/// Marks `fd` non-blocking so the readiness-based fallback helpers can poll it
/// without ever stalling the event loop. Idempotent.
fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `F_GETFL`/`F_SETFL` take a valid fd and an integer flag set; no
    // user pointers are involved.
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
    Ok(())
}

/// Reads and clears the pending socket error via `SO_ERROR`, used to surface the
/// result of a non-blocking `connect` once the socket reports writable.
fn socket_error(fd: RawFd) -> io::Result<()> {
    let mut err: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `getsockopt` writes a single `c_int` into `err`; `len` is
    // initialized to that value's size and updated in place.
    cvt(unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&mut err as *mut libc::c_int).cast::<c_void>(),
            &mut len,
        )
    })?;
    if err == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(err))
    }
}

// Readiness-based fallbacks used when an io_uring socket opcode is unsupported
// by the running kernel. They mirror the macOS backend: mark the fd
// non-blocking, attempt the syscall inline, and park on `IORING_OP_POLL_ADD`
// readiness (never the blocking thread pool) when it would block.

async fn connect_ready(fd: RawFd, addr: RawSocketAddr) -> io::Result<()> {
    set_nonblocking(fd)?;
    loop {
        // SAFETY: `fd` is valid for the duration of the call, and `addr` points
        // to `addr.len()` initialized bytes describing a sockaddr.
        let result = unsafe { libc::connect(fd, addr.as_ptr(), addr.len()) };
        if result == 0 {
            return Ok(());
        }

        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EINPROGRESS) | Some(libc::EALREADY) => {
                wait_writable(fd).await?;
                return socket_error(fd);
            }
            Some(libc::EISCONN) => return Ok(()),
            _ => return Err(error),
        }
    }
}

async fn accept_ready(fd: RawFd) -> io::Result<AcceptedSocket> {
    set_nonblocking(fd)?;
    loop {
        match accept_sync(fd) {
            Ok(socket) => return Ok(socket),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                wait_readable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

async fn send_ready(fd: RawFd, data: Vec<u8>, flags: i32) -> io::Result<usize> {
    set_nonblocking(fd)?;
    loop {
        match send_slice_sync(fd, &data, flags) {
            Ok(written) => return Ok(written),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                wait_writable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

async fn recv_ready(fd: RawFd, len: usize, flags: i32) -> io::Result<Vec<u8>> {
    set_nonblocking(fd)?;
    loop {
        match recv_sync(fd, len, flags) {
            Ok(data) => return Ok(data),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                wait_readable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

async fn recv_from_ready(fd: RawFd, len: usize, flags: i32) -> io::Result<ReceivedDatagram> {
    set_nonblocking(fd)?;
    loop {
        match recv_from_sync(fd, len, flags) {
            Ok(datagram) => return Ok(datagram),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                wait_readable(fd).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

#[allow(dead_code)]
fn close_sync(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is an owned raw descriptor being closed exactly once by the
    // networking backend.
    cvt(unsafe { libc::close(fd) }).map(|_| ())
}

/// Wrapper making `Box<libc::iovec>` sendable across the async CQE boundary.
///
/// Safety: `iov_base` points into a `Vec<u8>` that is owned by the same
/// closure, so the pointer is valid until the CQE fires and the closure drops.
struct SendIovec(#[allow(dead_code)] Box<libc::iovec>);
// SAFETY: the iovec pointer targets storage owned by the same completion
// closure, which keeps that storage alive until the kernel is done with it.
unsafe impl Send for SendIovec {}

/// Wrapper making `Box<libc::msghdr>` sendable across the async CQE boundary.
///
/// Safety: all raw pointers inside the `msghdr` (`msg_name`, `msg_iov`) point
/// into heap storage owned by the same closure, so they are valid until the
/// CQE fires and the closure drops.
struct SendMsghdr(Box<libc::msghdr>);
// SAFETY: all pointers inside the msghdr target storage owned by the same
// completion closure, which keeps that storage alive until the CQE is handled.
unsafe impl Send for SendMsghdr {}

fn cvt_long(value: libc::ssize_t) -> io::Result<libc::ssize_t> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    use crate::{run, spawn};

    /// Exercises the readiness-based fallback helpers (`connect_ready`,
    /// `accept_ready`, `send_ready`, `recv_ready`) directly, bypassing io_uring.
    /// On a modern kernel the production code always takes the io_uring path, so
    /// this test drives the fallback explicitly to prove it performs a correct,
    /// non-offloaded TCP echo using only `IORING_OP_POLL_ADD` readiness waits.
    #[test]
    fn readiness_fallback_round_trips_tcp_without_offload() {
        let done = Arc::new(Mutex::new(None));
        let done_for_task = Arc::clone(&done);

        spawn(async move {
            let listener = socket_sync(libc::AF_INET, libc::SOCK_STREAM, 0, 0)
                .expect("listener socket should be created");
            let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
            bind_sync(
                listener.as_raw_fd(),
                RawSocketAddr::from_socket_addr(loopback),
            )
            .expect("bind should succeed");
            listen_sync(listener.as_raw_fd(), DEFAULT_LISTENER_BACKLOG)
                .expect("listen should succeed");
            let bound = local_addr(listener.as_raw_fd()).expect("local_addr should resolve");

            let client = socket_sync(libc::AF_INET, libc::SOCK_STREAM, 0, 0)
                .expect("client socket should be created");

            // Connect + accept entirely through the readiness fallback.
            connect_ready(client.as_raw_fd(), RawSocketAddr::from_socket_addr(bound))
                .await
                .expect("readiness connect should succeed");
            let server = accept_ready(listener.as_raw_fd())
                .await
                .expect("readiness accept should succeed");

            let sent = send_ready(client.as_raw_fd(), b"ping".to_vec(), 0)
                .await
                .expect("readiness send should succeed");
            assert_eq!(sent, 4);

            let received = recv_ready(server.fd, 16, 0)
                .await
                .expect("readiness recv should succeed");
            assert_eq!(&received, b"ping");

            send_ready(server.fd, b"pong".to_vec(), 0)
                .await
                .expect("readiness send back should succeed");
            let echoed = recv_ready(client.as_raw_fd(), 16, 0)
                .await
                .expect("readiness recv back should succeed");
            assert_eq!(&echoed, b"pong");

            let _ = close_sync(server.fd);
            *done_for_task.lock().expect("result mutex poisoned") = Some(echoed);
        });

        run();

        let echoed = done.lock().expect("result mutex poisoned").take();
        assert_eq!(echoed.as_deref(), Some(&b"pong"[..]));
    }

    /// A socket accepted through the production io_uring `accept` path must have
    /// `FD_CLOEXEC` set, so live connections are never leaked into child
    /// processes. Regression test for the missing `SOCK_CLOEXEC` accept flag.
    #[test]
    fn accepted_socket_is_cloexec() {
        let cloexec = Arc::new(Mutex::new(None));
        let cloexec_task = Arc::clone(&cloexec);

        spawn(async move {
            let listener = socket_sync(libc::AF_INET, libc::SOCK_STREAM, 0, 0)
                .expect("listener socket should be created");
            let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
            bind_sync(
                listener.as_raw_fd(),
                RawSocketAddr::from_socket_addr(loopback),
            )
            .expect("bind should succeed");
            listen_sync(listener.as_raw_fd(), DEFAULT_LISTENER_BACKLOG)
                .expect("listen should succeed");
            let bound = local_addr(listener.as_raw_fd()).expect("local_addr should resolve");

            let client = socket_sync(libc::AF_INET, libc::SOCK_STREAM, 0, 0)
                .expect("client socket should be created");
            connect_ready(client.as_raw_fd(), RawSocketAddr::from_socket_addr(bound))
                .await
                .expect("connect should succeed");

            // Drive the production io_uring accept path (not the fallback).
            let server = accept(NetOp::Accept {
                fd: listener.as_raw_fd(),
            })
            .await
            .expect("accept should succeed");

            // SAFETY: F_GETFD only reads descriptor flags; no user pointers.
            let flags = unsafe { libc::fcntl(server.fd, libc::F_GETFD) };
            let is_cloexec = flags >= 0 && (flags & libc::FD_CLOEXEC) != 0;
            let _ = close_sync(server.fd);
            *cloexec_task.lock().expect("result mutex poisoned") = Some(is_cloexec);
        });

        run();

        assert_eq!(
            *cloexec.lock().expect("result mutex poisoned"),
            Some(true),
            "accepted socket must have FD_CLOEXEC set"
        );
    }

    /// A socket created through the production io_uring `IORING_OP_SOCKET` path
    /// must honour the requested `SOCK_CLOEXEC` flag. The flag rides in the
    /// socket *type* field (`sqe.off`), not `op_flags`. Placing it in `op_flags`
    /// (`rw_flags`) made kernels ≥5.19 reject the op with `EINVAL` — silently
    /// falling back to `socket_sync` so the uring fast path was always dead —
    /// and would drop `SOCK_CLOEXEC` outright on kernels that do not validate
    /// `rw_flags`. This test locks the observable property across both paths.
    #[test]
    fn uring_socket_is_cloexec() {
        let cloexec = Arc::new(Mutex::new(None));
        let cloexec_task = Arc::clone(&cloexec);

        spawn(async move {
            let sock = socket(NetOp::Socket {
                domain: libc::AF_INET,
                socket_type: libc::SOCK_STREAM,
                protocol: 0,
                flags: libc::SOCK_CLOEXEC as u32,
            })
            .await
            .expect("socket should be created");

            // SAFETY: F_GETFD only reads descriptor flags; no user pointers.
            let flags = unsafe { libc::fcntl(sock.as_raw_fd(), libc::F_GETFD) };
            *cloexec_task.lock().expect("result mutex poisoned") =
                Some(flags >= 0 && (flags & libc::FD_CLOEXEC) != 0);
        });

        run();

        assert_eq!(
            *cloexec.lock().expect("result mutex poisoned"),
            Some(true),
            "uring-created socket must have FD_CLOEXEC set"
        );
    }
}

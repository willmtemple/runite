//! Logical networking operations shared between the public API and Linux backend.

use std::net::{Shutdown, SocketAddr};
use std::os::fd::RawFd;

#[derive(Debug)]
pub enum NetOp {
    Socket {
        domain: i32,
        socket_type: i32,
        protocol: i32,
        flags: u32,
    },
    Connect {
        fd: RawFd,
        addr: SocketAddr,
    },
    Bind {
        fd: RawFd,
        addr: SocketAddr,
    },
    Listen {
        fd: RawFd,
        backlog: i32,
    },
    Accept {
        fd: RawFd,
    },
    Send {
        fd: RawFd,
        data: Vec<u8>,
        flags: i32,
    },
    SendTo {
        fd: RawFd,
        target: SocketAddr,
        data: Vec<u8>,
        flags: i32,
    },
    Recv {
        fd: RawFd,
        len: usize,
        flags: i32,
    },
    RecvFrom {
        fd: RawFd,
        len: usize,
        flags: i32,
    },
    Shutdown {
        fd: RawFd,
        how: Shutdown,
    },
    /// Explicit asynchronous close of a socket file descriptor.
    ///
    /// Reserved for a future explicit async-close API. Today both backends rely
    /// on synchronous `close(2)` via `OwnedFd`'s `Drop`, so this variant is
    /// never constructed; see `docs/ROADMAP.md` ("Explicit async close").
    #[allow(dead_code)]
    Close {
        fd: RawFd,
    },
}

#[derive(Clone, Debug)]
pub struct AcceptedSocket {
    pub fd: RawFd,
    pub peer_addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct ReceivedDatagram {
    pub data: Vec<u8>,
    pub peer_addr: SocketAddr,
}

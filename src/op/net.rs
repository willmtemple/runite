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

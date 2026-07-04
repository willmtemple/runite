//! Logical networking operations shared between the public API and Linux backend.

use crate::sys::handle::RawSock;
use std::net::{Shutdown, SocketAddr};

// `Socket`/`Bind`/`Listen` are constructed only by the Unix backends and the
// Unix-domain-socket layer; the Windows backend reaches its socket/bind/listen
// helpers directly.
#[cfg_attr(windows, allow(dead_code))]
#[derive(Debug)]
pub enum NetOp {
    Socket {
        domain: i32,
        socket_type: i32,
        protocol: i32,
        flags: u32,
    },
    Connect {
        fd: RawSock,
        addr: SocketAddr,
    },
    Bind {
        fd: RawSock,
        addr: SocketAddr,
    },
    Listen {
        fd: RawSock,
        backlog: i32,
    },
    Accept {
        fd: RawSock,
    },
    Send {
        fd: RawSock,
        data: Vec<u8>,
        flags: i32,
    },
    SendTo {
        fd: RawSock,
        target: SocketAddr,
        data: Vec<u8>,
        flags: i32,
    },
    Recv {
        fd: RawSock,
        len: usize,
        flags: i32,
    },
    RecvFrom {
        fd: RawSock,
        len: usize,
        flags: i32,
    },
    Shutdown {
        fd: RawSock,
        how: Shutdown,
    },
}

#[derive(Clone, Debug)]
pub struct AcceptedSocket {
    pub fd: RawSock,
    pub peer_addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct ReceivedDatagram {
    pub data: Vec<u8>,
    pub peer_addr: SocketAddr,
}

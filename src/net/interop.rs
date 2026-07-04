//! OS handle interop for the socket wrappers.
//!
//! On Unix the socket types expose and adopt file descriptors
//! (`AsFd`/`AsRawFd`/`From<OwnedFd>`/`from_std`); on Windows the equivalent
//! surface is socket-handle based (`AsSocket`/`AsRawSocket`/`From<OwnedSocket>`),
//! and adoption additionally binds the socket to the current runtime thread's
//! I/O completion port so overlapped operations can complete.

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

#[cfg(unix)]
use super::{TcpListener, TcpSocket, TcpStream, UdpSocket};
#[cfg(unix)]
use std::io;

// -- File-descriptor interop (Unix only) -------------------------------------
//
// These impls are `#[cfg(unix)]` because they expose raw/owned file
// descriptors, a Unix concept with no equivalent on the completion-based
// Windows backend (which would instead expose `AsSocket`/`AsRawSocket`).

#[cfg(unix)]
impl AsFd for TcpStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for TcpStream {
    /// Adopts an already-connected TCP socket. The caller is responsible for the
    /// descriptor being a valid, appropriately-configured stream socket; use
    /// [`TcpStream::from_std`] to adopt a [`std::net::TcpStream`] and set the
    /// mode runite's backend expects.
    fn from(fd: OwnedFd) -> Self {
        Self::from_owned_fd(fd)
    }
}

#[cfg(unix)]
impl AsFd for TcpListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for TcpListener {
    /// Adopts an already-listening TCP socket. Use [`TcpListener::from_std`] to
    /// adopt a [`std::net::TcpListener`] and set the expected mode.
    fn from(fd: OwnedFd) -> Self {
        Self::from_owned_fd(fd)
    }
}

#[cfg(unix)]
impl AsFd for UdpSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for UdpSocket {
    /// Adopts an existing UDP socket. Use [`UdpSocket::from_std`] to adopt a
    /// [`std::net::UdpSocket`] and set the expected mode.
    fn from(fd: OwnedFd) -> Self {
        Self::from_owned_fd(fd)
    }
}

#[cfg(unix)]
impl AsFd for TcpSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for TcpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for TcpSocket {
    /// Adopts an existing (typically unbound) TCP socket for configuration.
    fn from(fd: OwnedFd) -> Self {
        Self::from_owned_fd(fd)
    }
}

#[cfg(unix)]
impl TcpStream {
    /// Adopts a blocking [`std::net::TcpStream`], returning an async
    /// [`TcpStream`].
    ///
    /// The socket is switched to the non-blocking mode runite's driver expects.
    /// Ownership of the descriptor transfers to the returned stream.
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
        let fd = OwnedFd::from(stream);
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }
}

#[cfg(unix)]
impl TcpListener {
    /// Adopts a blocking [`std::net::TcpListener`], returning an async
    /// [`TcpListener`].
    ///
    /// The socket is switched to non-blocking mode. Ownership of the descriptor
    /// transfers to the returned listener.
    pub fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
        let fd = OwnedFd::from(listener);
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }
}

#[cfg(unix)]
impl UdpSocket {
    /// Adopts a blocking [`std::net::UdpSocket`], returning an async
    /// [`UdpSocket`].
    ///
    /// The socket is switched to non-blocking mode. Ownership of the descriptor
    /// transfers to the returned socket.
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<Self> {
        let fd = OwnedFd::from(socket);
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }
}

// -- Socket interop (Windows only) --------------------------------------------
//
// The Windows analogs of the Unix fd-interop impls above: sockets are exposed
// through `AsSocket`/`AsRawSocket`, and adoption binds the socket to the
// current runtime thread's I/O completion port so overlapped operations can
// complete. Adopted sockets must be overlapped-capable (sockets created by
// std and by Winsock default to `WSA_FLAG_OVERLAPPED`).

#[cfg(windows)]
mod windows_interop {
    use std::io;
    use std::os::windows::io::{AsSocket, BorrowedSocket, OwnedSocket, RawSocket};

    use crate::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
    use crate::sys::handle::raw_sock;

    impl AsSocket for TcpStream {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for TcpStream {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.inner.fd)
        }
    }

    impl From<OwnedSocket> for TcpStream {
        /// Adopts an already-connected TCP socket and associates it with the
        /// current runtime thread's completion port (best-effort; a socket
        /// whose file object is already bound to a port keeps its original
        /// binding). Use [`TcpStream::from_std`] for a fallible adoption.
        fn from(socket: OwnedSocket) -> Self {
            let _ = crate::sys::current::net::associate_adopted(raw_sock(&socket));
            Self::from_owned_fd(socket)
        }
    }

    impl AsSocket for TcpListener {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for TcpListener {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.inner.fd)
        }
    }

    impl From<OwnedSocket> for TcpListener {
        /// Adopts an already-listening TCP socket, associating it with the
        /// current runtime thread's completion port (best-effort, as for
        /// `TcpStream`).
        fn from(socket: OwnedSocket) -> Self {
            let _ = crate::sys::current::net::associate_adopted(raw_sock(&socket));
            Self::from_owned_fd(socket)
        }
    }

    impl AsSocket for UdpSocket {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for UdpSocket {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.inner.fd)
        }
    }

    impl From<OwnedSocket> for UdpSocket {
        /// Adopts an existing UDP socket, associating it with the current
        /// runtime thread's completion port (best-effort, as for `TcpStream`).
        fn from(socket: OwnedSocket) -> Self {
            let _ = crate::sys::current::net::associate_adopted(raw_sock(&socket));
            Self::from_owned_fd(socket)
        }
    }

    impl AsSocket for TcpSocket {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for TcpSocket {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.fd)
        }
    }

    impl From<OwnedSocket> for TcpSocket {
        /// Adopts an existing (typically unbound) TCP socket for configuration.
        fn from(socket: OwnedSocket) -> Self {
            let _ = crate::sys::current::net::associate_adopted(raw_sock(&socket));
            Self::from_owned_fd(socket)
        }
    }

    impl TcpStream {
        /// Adopts a [`std::net::TcpStream`], returning an async [`TcpStream`].
        ///
        /// The socket is associated with the current runtime thread's I/O
        /// completion port; ownership transfers to the returned stream.
        pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
            let socket = OwnedSocket::from(stream);
            crate::sys::current::net::associate_adopted(raw_sock(&socket))?;
            Ok(Self::from_owned_fd(socket))
        }
    }

    impl TcpListener {
        /// Adopts a [`std::net::TcpListener`], returning an async
        /// [`TcpListener`].
        ///
        /// The socket is associated with the current runtime thread's I/O
        /// completion port; ownership transfers to the returned listener.
        pub fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
            let socket = OwnedSocket::from(listener);
            crate::sys::current::net::associate_adopted(raw_sock(&socket))?;
            Ok(Self::from_owned_fd(socket))
        }
    }

    impl UdpSocket {
        /// Adopts a [`std::net::UdpSocket`], returning an async [`UdpSocket`].
        ///
        /// The socket is associated with the current runtime thread's I/O
        /// completion port; ownership transfers to the returned socket.
        pub fn from_std(socket: std::net::UdpSocket) -> io::Result<Self> {
            let socket = OwnedSocket::from(socket);
            crate::sys::current::net::associate_adopted(raw_sock(&socket))?;
            Ok(Self::from_owned_fd(socket))
        }
    }
}

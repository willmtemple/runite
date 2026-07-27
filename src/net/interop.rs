//! OS handle interop for the socket wrappers.
//!
//! On Unix the socket types expose and adopt file descriptors
//! (`AsFd`/`AsRawFd`/`TryFrom<OwnedFd>`/`from_owned`/`from_std`); on Windows
//! the equivalent surface is socket-handle based
//! (`AsSocket`/`AsRawSocket`/`TryFrom<OwnedSocket>`). Adoption is uniformly
//! fallible and additionally binds Windows sockets to the current runtime
//! thread's I/O completion port.

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
impl TcpStream {
    /// Fallibly adopts an already-connected owned socket descriptor.
    ///
    /// # Errors
    ///
    /// Returns the error from configuring the resource for this runtime.
    /// **The descriptor is closed on failure** — it is consumed either way
    /// and is not handed back to the caller.
    pub fn from_owned(fd: OwnedFd) -> io::Result<Self> {
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }

    /// Adopts a blocking [`std::net::TcpStream`], returning an async
    /// [`TcpStream`].
    ///
    /// The socket is switched to the non-blocking mode runite's driver expects.
    /// Ownership of the descriptor transfers to the returned stream.
    pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
        Self::from_owned(OwnedFd::from(stream))
    }
}

#[cfg(unix)]
impl TcpListener {
    /// Fallibly adopts an already-listening owned socket descriptor.
    ///
    /// # Errors
    ///
    /// Returns the error from configuring the resource for this runtime.
    /// **The descriptor is closed on failure** — it is consumed either way
    /// and is not handed back to the caller.
    pub fn from_owned(fd: OwnedFd) -> io::Result<Self> {
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }

    /// Adopts a blocking [`std::net::TcpListener`], returning an async
    /// [`TcpListener`].
    ///
    /// The socket is switched to non-blocking mode. Ownership of the descriptor
    /// transfers to the returned listener.
    pub fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
        Self::from_owned(OwnedFd::from(listener))
    }
}

#[cfg(unix)]
impl UdpSocket {
    /// Fallibly adopts an owned UDP socket descriptor.
    ///
    /// # Errors
    ///
    /// Returns the error from configuring the resource for this runtime.
    /// **The descriptor is closed on failure** — it is consumed either way
    /// and is not handed back to the caller.
    pub fn from_owned(fd: OwnedFd) -> io::Result<Self> {
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }

    /// Adopts a blocking [`std::net::UdpSocket`], returning an async
    /// [`UdpSocket`].
    ///
    /// The socket is switched to non-blocking mode. Ownership of the descriptor
    /// transfers to the returned socket.
    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<Self> {
        Self::from_owned(OwnedFd::from(socket))
    }
}

#[cfg(unix)]
impl TcpSocket {
    /// Fallibly adopts an owned TCP socket descriptor.
    ///
    /// # Errors
    ///
    /// Returns the error from configuring the resource for this runtime.
    /// **The descriptor is closed on failure** — it is consumed either way
    /// and is not handed back to the caller.
    pub fn from_owned(fd: OwnedFd) -> io::Result<Self> {
        crate::sys::current::net::set_nonblocking(fd.as_raw_fd())?;
        Ok(Self::from_owned_fd(fd))
    }
}

#[cfg(unix)]
macro_rules! impl_try_from_owned_fd {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl TryFrom<OwnedFd> for $ty {
                type Error = io::Error;

                fn try_from(fd: OwnedFd) -> io::Result<Self> {
                    Self::from_owned(fd)
                }
            }
        )+
    };
}

#[cfg(unix)]
impl_try_from_owned_fd!(TcpStream, TcpListener, UdpSocket, TcpSocket);

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
            raw_sock(&self.inner.fd).as_socket()
        }
    }

    impl AsSocket for TcpListener {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for TcpListener {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.inner.fd).as_socket()
        }
    }

    impl AsSocket for UdpSocket {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for UdpSocket {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.inner.fd).as_socket()
        }
    }

    impl AsSocket for TcpSocket {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.fd.as_socket()
        }
    }

    impl std::os::windows::io::AsRawSocket for TcpSocket {
        fn as_raw_socket(&self) -> RawSocket {
            raw_sock(&self.fd).as_socket()
        }
    }

    impl TcpStream {
        /// Strictly adopts an already-connected owned socket.
        ///
        /// # Errors
        ///
        /// Returns the error from configuring the resource for this runtime.
        /// **The descriptor is closed on failure** — it is consumed either way
        /// and is not handed back to the caller.
        pub fn from_owned(socket: OwnedSocket) -> io::Result<Self> {
            crate::sys::current::net::adopt_socket(socket).map(Self::from_owned_fd)
        }

        /// Adopts a [`std::net::TcpStream`], returning an async [`TcpStream`].
        ///
        /// The socket is associated with the current runtime thread's I/O
        /// completion port; ownership transfers to the returned stream.
        pub fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
            Self::from_owned(OwnedSocket::from(stream))
        }
    }

    impl TcpListener {
        /// Strictly adopts an already-listening owned socket.
        ///
        /// # Errors
        ///
        /// Returns the error from configuring the resource for this runtime.
        /// **The descriptor is closed on failure** — it is consumed either way
        /// and is not handed back to the caller.
        pub fn from_owned(socket: OwnedSocket) -> io::Result<Self> {
            crate::sys::current::net::adopt_socket(socket).map(Self::from_owned_fd)
        }

        /// Adopts a [`std::net::TcpListener`], returning an async
        /// [`TcpListener`].
        ///
        /// The socket is associated with the current runtime thread's I/O
        /// completion port; ownership transfers to the returned listener.
        pub fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
            Self::from_owned(OwnedSocket::from(listener))
        }
    }

    impl UdpSocket {
        /// Strictly adopts an owned UDP socket.
        ///
        /// # Errors
        ///
        /// Returns the error from configuring the resource for this runtime.
        /// **The descriptor is closed on failure** — it is consumed either way
        /// and is not handed back to the caller.
        pub fn from_owned(socket: OwnedSocket) -> io::Result<Self> {
            crate::sys::current::net::adopt_socket(socket).map(Self::from_owned_fd)
        }

        /// Adopts a [`std::net::UdpSocket`], returning an async [`UdpSocket`].
        ///
        /// The socket is associated with the current runtime thread's I/O
        /// completion port; ownership transfers to the returned socket.
        pub fn from_std(socket: std::net::UdpSocket) -> io::Result<Self> {
            Self::from_owned(OwnedSocket::from(socket))
        }
    }

    impl TcpSocket {
        /// Strictly adopts an owned TCP socket.
        ///
        /// # Errors
        ///
        /// Returns the error from configuring the resource for this runtime.
        /// **The descriptor is closed on failure** — it is consumed either way
        /// and is not handed back to the caller.
        pub fn from_owned(socket: OwnedSocket) -> io::Result<Self> {
            crate::sys::current::net::adopt_socket(socket).map(Self::from_owned_fd)
        }
    }

    macro_rules! impl_try_from_owned_socket {
        ($($ty:ty),+ $(,)?) => {
            $(
                impl TryFrom<OwnedSocket> for $ty {
                    type Error = io::Error;

                    fn try_from(socket: OwnedSocket) -> io::Result<Self> {
                        Self::from_owned(socket)
                    }
                }
            )+
        };
    }

    impl_try_from_owned_socket!(TcpStream, TcpListener, UdpSocket, TcpSocket);
}

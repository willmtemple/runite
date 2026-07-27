//! Platform I/O handle façade.
//!
//! POSIX backends address every kernel I/O object with a file descriptor, while
//! Windows separates file/pipe **handles** from **sockets** (distinct types with
//! distinct close functions, and `RawHandle` is a non-`Send` pointer). This
//! module defines the crate's I/O handle vocabulary exactly once so the op
//! descriptors and public wrappers can be written platform-neutrally; it is the
//! only place that chooses a representation.
//!
//! - [`RawFile`]/[`OwnedFile`] name file, pipe, and standard-stream handles.
//! - [`RawSock`]/[`OwnedSock`] name sockets.
//!
//! On Unix all four collapse to `RawFd`/`OwnedFd`.

#[cfg(unix)]
mod imp {
    use std::os::fd::{AsRawFd, FromRawFd};

    /// Platform-specific metadata payload carried by `op::fs::RawMetadata`.
    ///
    /// Unix expresses everything through the POSIX `mode`, so this is empty.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct PlatformMetadata;

    /// Platform-specific open-options payload carried by
    /// `op::fs::OpenOptions`. Unix has no extra open parameters today.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct PlatformOpenOptions;

    pub(crate) type RawFile = std::os::fd::RawFd;
    pub(crate) type OwnedFile = std::os::fd::OwnedFd;
    pub(crate) type RawSock = std::os::fd::RawFd;
    pub(crate) type OwnedSock = std::os::fd::OwnedFd;

    pub(crate) fn raw_file(file: &OwnedFile) -> RawFile {
        file.as_raw_fd()
    }

    pub(crate) fn raw_sock(sock: &OwnedSock) -> RawSock {
        sock.as_raw_fd()
    }

    #[cfg(feature = "hyper")]
    pub(crate) fn clone_raw_sock(sock: &RawSock) -> RawSock {
        *sock
    }

    /// Assumes ownership of a raw socket value.
    ///
    /// # Safety
    ///
    /// `sock` must be an open socket that nothing else owns; ownership
    /// transfers to the returned value exactly once.
    pub(crate) unsafe fn owned_sock_from_raw(sock: RawSock) -> OwnedSock {
        // SAFETY: forwarded contract — `sock` is open and uniquely owned.
        unsafe { OwnedSock::from_raw_fd(sock) }
    }
}

#[cfg(windows)]
mod imp {
    use std::fmt;
    use std::marker::PhantomData;
    use std::os::windows::io::{
        AsHandle, AsRawHandle, AsRawSocket, AsSocket, BorrowedHandle, BorrowedSocket, OwnedHandle,
        OwnedSocket, RawHandle, RawSocket,
    };
    use std::rc::Rc;
    use std::sync::Arc;

    use crate::platform::windows::driver::DriverId;
    use crate::platform::windows::runtime::ensure_current_driver;

    /// Platform-specific metadata payload carried by `op::fs::RawMetadata`.
    ///
    /// Windows file metadata is attribute-based; the raw attribute bits back
    /// `runite::os::windows::fs::MetadataExt::file_attributes`.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct PlatformMetadata {
        pub(crate) file_attributes: u32,
    }

    /// Platform-specific open-options payload carried by
    /// `op::fs::OpenOptions`; set through
    /// `runite::os::windows::fs::OpenOptionsExt` and applied by the Windows
    /// open backend. Field meanings mirror
    /// [`std::os::windows::fs::OpenOptionsExt`].
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub(crate) struct PlatformOpenOptions {
        pub(crate) access_mode: Option<u32>,
        pub(crate) share_mode: Option<u32>,
        pub(crate) custom_flags: u32,
        pub(crate) attributes: u32,
        pub(crate) security_qos_flags: u32,
    }

    /// Owned file/pipe handle plus its optional IOCP affinity.
    ///
    /// The handle itself is reference-counted so every accepted blocking or
    /// overlapped operation can retain ownership until its terminal result.
    pub(crate) struct OwnedFile {
        handle: Arc<OwnedHandle>,
        affinity: Option<DriverId>,
    }

    impl OwnedFile {
        pub(crate) fn bound(handle: OwnedHandle, affinity: DriverId) -> Self {
            Self {
                handle: Arc::new(handle),
                affinity: Some(affinity),
            }
        }

        pub(crate) fn unbound(handle: OwnedHandle) -> Self {
            Self {
                handle: Arc::new(handle),
                affinity: None,
            }
        }
    }

    impl fmt::Debug for OwnedFile {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("OwnedFile")
                .field("handle", &(self.handle.as_ref().as_raw_handle() as usize))
                .field("affinity", &self.affinity)
                .finish()
        }
    }

    impl AsHandle for OwnedFile {
        fn as_handle(&self) -> BorrowedHandle<'_> {
            self.handle.as_ref().as_handle()
        }
    }

    impl AsRawHandle for OwnedFile {
        fn as_raw_handle(&self) -> RawHandle {
            self.handle.as_ref().as_raw_handle()
        }
    }

    /// Cloneable operation reference that keeps a file/pipe handle live.
    #[derive(Clone)]
    pub(crate) struct RawFile {
        handle: Arc<OwnedHandle>,
        affinity: Option<DriverId>,
    }

    impl RawFile {
        pub(crate) fn as_handle(&self) -> RawHandle {
            self.handle.as_ref().as_raw_handle()
        }

        pub(crate) fn ensure_current(&self) -> std::io::Result<()> {
            match self.affinity {
                Some(affinity) => ensure_current_driver(affinity),
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Windows handle is not associated with a runite IOCP",
                )),
            }
        }

        pub(crate) fn affinity(&self) -> std::io::Result<DriverId> {
            self.ensure_current()?;
            Ok(self
                .affinity
                .expect("current IOCP handle must carry an affinity"))
        }
    }

    impl fmt::Debug for RawFile {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RawFile")
                .field("handle", &(self.as_handle() as usize))
                .field("affinity", &self.affinity)
                .finish()
        }
    }

    impl PartialEq for RawFile {
        fn eq(&self, other: &Self) -> bool {
            self.as_handle() == other.as_handle() && self.affinity == other.affinity
        }
    }

    impl Eq for RawFile {}

    /// Owned socket plus the IOCP it is permanently associated with.
    ///
    /// The marker intentionally makes public socket wrappers `!Send` on
    /// Windows. Operation references remain `Send` so completion callbacks can
    /// retain the kernel object safely.
    pub(crate) struct OwnedSock {
        socket: Arc<OwnedSocket>,
        affinity: DriverId,
        not_send: PhantomData<Rc<()>>,
    }

    impl OwnedSock {
        pub(crate) fn bound(socket: OwnedSocket, affinity: DriverId) -> Self {
            Self {
                socket: Arc::new(socket),
                affinity,
                not_send: PhantomData,
            }
        }
    }

    impl fmt::Debug for OwnedSock {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("OwnedSock")
                .field("socket", &self.socket.as_ref().as_raw_socket())
                .field("affinity", &self.affinity)
                .finish()
        }
    }

    impl AsSocket for OwnedSock {
        fn as_socket(&self) -> BorrowedSocket<'_> {
            self.socket.as_ref().as_socket()
        }
    }

    impl AsRawSocket for OwnedSock {
        fn as_raw_socket(&self) -> RawSocket {
            self.socket.as_ref().as_raw_socket()
        }
    }

    /// Cloneable operation reference that keeps a socket live through its
    /// terminal completion packet.
    #[derive(Clone)]
    pub(crate) struct RawSock {
        socket: Arc<OwnedSocket>,
        affinity: DriverId,
    }

    impl RawSock {
        pub(crate) fn as_socket(&self) -> RawSocket {
            self.socket.as_ref().as_raw_socket()
        }

        pub(crate) fn ensure_current(&self) -> std::io::Result<()> {
            ensure_current_driver(self.affinity)
        }

        pub(crate) fn affinity(&self) -> std::io::Result<DriverId> {
            self.ensure_current()?;
            Ok(self.affinity)
        }
    }

    impl fmt::Debug for RawSock {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RawSock")
                .field("socket", &self.as_socket())
                .field("affinity", &self.affinity)
                .finish()
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) enum OverlappedOwner {
        File(RawFile),
        Socket(RawSock),
    }

    impl OverlappedOwner {
        pub(crate) fn as_handle(&self) -> RawHandle {
            match self {
                Self::File(file) => file.as_handle(),
                Self::Socket(socket) => socket.as_socket() as usize as RawHandle,
            }
        }

        pub(crate) fn ensure_current(&self) -> std::io::Result<()> {
            match self {
                Self::File(file) => file.ensure_current(),
                Self::Socket(socket) => socket.ensure_current(),
            }
        }
    }

    impl From<RawFile> for OverlappedOwner {
        fn from(file: RawFile) -> Self {
            Self::File(file)
        }
    }

    impl From<RawSock> for OverlappedOwner {
        fn from(socket: RawSock) -> Self {
            Self::Socket(socket)
        }
    }

    pub(crate) fn raw_file(file: &OwnedFile) -> RawFile {
        RawFile {
            handle: Arc::clone(&file.handle),
            affinity: file.affinity,
        }
    }

    pub(crate) fn raw_sock(sock: &OwnedSock) -> RawSock {
        RawSock {
            socket: Arc::clone(&sock.socket),
            affinity: sock.affinity,
        }
    }

    #[cfg(feature = "hyper")]
    pub(crate) fn clone_raw_sock(sock: &RawSock) -> RawSock {
        sock.clone()
    }

    /// Converts an accepted operation reference into the public socket owner.
    ///
    /// # Safety
    ///
    /// `sock` must be the owning reference returned for a freshly accepted
    /// socket, and this must be its only conversion into an `OwnedSock`.
    pub(crate) unsafe fn owned_sock_from_raw(sock: RawSock) -> OwnedSock {
        OwnedSock {
            socket: sock.socket,
            affinity: sock.affinity,
            not_send: PhantomData,
        }
    }
}

pub(crate) use imp::*;

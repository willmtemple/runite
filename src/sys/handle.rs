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
    use std::os::windows::io::{AsRawHandle, AsRawSocket, FromRawSocket, RawHandle};

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

    /// A raw file/pipe handle value.
    ///
    /// This is `HANDLE` stored as an integer rather than
    /// [`std::os::windows::io::RawHandle`] so it is `Send`: op descriptors and
    /// blocking-pool closures carry handle *values* across threads, which is
    /// safe — only using the value performs I/O.
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub(crate) struct RawFile(isize);

    impl RawFile {
        pub(crate) fn from_handle(handle: RawHandle) -> Self {
            Self(handle as isize)
        }

        pub(crate) fn as_handle(self) -> RawHandle {
            self.0 as RawHandle
        }
    }

    pub(crate) type OwnedFile = std::os::windows::io::OwnedHandle;
    pub(crate) type RawSock = std::os::windows::io::RawSocket;
    pub(crate) type OwnedSock = std::os::windows::io::OwnedSocket;

    pub(crate) fn raw_file(file: &OwnedFile) -> RawFile {
        RawFile::from_handle(file.as_raw_handle())
    }

    pub(crate) fn raw_sock(sock: &OwnedSock) -> RawSock {
        sock.as_raw_socket()
    }

    /// Assumes ownership of a raw socket value.
    ///
    /// # Safety
    ///
    /// `sock` must be an open socket that nothing else owns; ownership
    /// transfers to the returned value exactly once.
    pub(crate) unsafe fn owned_sock_from_raw(sock: RawSock) -> OwnedSock {
        // SAFETY: forwarded contract — `sock` is open and uniquely owned.
        unsafe { OwnedSock::from_raw_socket(sock) }
    }
}

pub(crate) use imp::*;

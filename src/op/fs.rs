//! Logical filesystem operations.
//!
//! This layer owns request data so the public API can keep borrowed buffers while platform
//! backends pin, stage, or offload as needed.

use std::ffi::OsString;
use std::os::fd::RawFd;
use std::path::PathBuf;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
    pub create: bool,
    pub create_new: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataTarget {
    Path(PathBuf),
    File(RawFd),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
    BlockDevice,
    CharacterDevice,
    Fifo,
    Socket,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawMetadata {
    pub file_type: FileType,
    pub mode: u16,
    pub len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawDirEntry {
    pub path: PathBuf,
    pub file_name: OsString,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FsOp {
    Open {
        path: PathBuf,
        options: OpenOptions,
    },
    Read {
        fd: RawFd,
        offset: Option<u64>,
        len: usize,
    },
    Write {
        fd: RawFd,
        offset: Option<u64>,
        data: Vec<u8>,
    },
    Metadata {
        target: MetadataTarget,
        follow_symlinks: bool,
    },
    SetLen {
        fd: RawFd,
        len: u64,
    },
    SyncAll {
        fd: RawFd,
    },
    SyncData {
        fd: RawFd,
    },
    Duplicate {
        fd: RawFd,
    },
    CreateDir {
        path: PathBuf,
        mode: u32,
    },
    RemoveFile {
        path: PathBuf,
    },
    RemoveDir {
        path: PathBuf,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    ReadDir {
        path: PathBuf,
    },
    /// Explicit asynchronous close of a file descriptor.
    ///
    /// Reserved for a future explicit async-close API. Today both backends rely
    /// on synchronous `close(2)` via `OwnedFd`'s `Drop`, so this variant is
    /// never constructed; see `docs/ROADMAP.md` ("Explicit async close").
    #[allow(dead_code)]
    Close {
        fd: RawFd,
    },
}

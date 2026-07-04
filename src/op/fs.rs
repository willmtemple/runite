//! Logical filesystem operations.
//!
//! This layer owns request data so the public API can keep borrowed buffers while platform
//! backends pin, stage, or offload as needed.

use std::ffi::OsString;
use std::path::PathBuf;

use crate::sys::handle::RawFile;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
    pub create: bool,
    pub create_new: bool,
    /// Extra platform-specific open parameters (e.g. Windows access/share
    /// modes and flags).
    pub platform: crate::sys::handle::PlatformOpenOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataTarget {
    Path(PathBuf),
    File(RawFile),
}

// The POSIX-only variants (block/char device, fifo, socket) are never
// constructed by the Windows backend but remain part of the shared vocabulary.
#[cfg_attr(windows, allow(dead_code))]
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
    /// Full POSIX `st_mode` (file-type bits and permission bits), matching
    /// `std::os::unix::fs::MetadataExt::mode`. `u32` for parity with std even
    /// though the current backends fit it in 16 bits. The Windows backend
    /// synthesizes an equivalent value from the file attributes.
    pub mode: u32,
    pub len: u64,
    /// Extra platform-specific metadata (e.g. Windows file attributes).
    pub platform: crate::sys::handle::PlatformMetadata,
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
        fd: RawFile,
        offset: Option<u64>,
        len: usize,
    },
    Write {
        fd: RawFile,
        offset: Option<u64>,
        data: Vec<u8>,
    },
    Metadata {
        target: MetadataTarget,
        follow_symlinks: bool,
    },
    SetLen {
        fd: RawFile,
        len: u64,
    },
    SyncAll {
        fd: RawFile,
    },
    SyncData {
        fd: RawFile,
    },
    Duplicate {
        fd: RawFile,
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
}

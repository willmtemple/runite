//! Portable async filesystem primitives.
//!
//! This module provides file, directory, and metadata operations that integrate
//! with runite's event-loop-per-thread runtime. The public surface intentionally
//! mirrors [`std::fs`] where that shape makes sense, while using async methods
//! for operations that may block the caller.
//!
//! runite futures and handles are thread-affine: create and poll filesystem
//! futures on the runtime thread that owns them. Tasks do not migrate between
//! runtime threads, and there is no work-stealing scheduler. Same-thread wakeups
//! resume as microtasks, while backend completions and blocking-pool callbacks
//! re-enter the runtime as macrotasks.
//!
//! # Backend model
//!
//! On Linux, regular filesystem operations use the runtime's `io_uring`
//! completion backend where an opcode exists. On macOS aarch64, filesystem work
//! is offloaded to runite's blocking thread pool so slow disk or metadata calls
//! do not block the event-loop thread. On Windows, file reads and writes run as
//! overlapped operations on the thread's I/O completion port, while open,
//! metadata, directory, flush, and truncation work is offloaded to the blocking
//! pool. Directory iteration is also blocking-pool-backed on Linux because
//! `std::fs::read_dir`/directory scanning can block and is not modeled as an
//! `io_uring` operation here.
//!
//! This differs from Tokio's default multi-threaded scheduler and async-std:
//! runite keeps tasks on a JavaScript-style, thread-local event loop and mixes
//! completion-based filesystem I/O with blocking-pool offload only where the
//! platform requires it.
//!
//! Cancellation semantics:
//! - Dropping an I/O future cancels interest in the result.
//! - The runtime issues best-effort kernel cancellation where supported.
//! - The underlying OS operation may still complete after the future is dropped.
//! - Dropping a [`ReadDir`] releases the runtime's pending operation state, but
//!   it does not stop an already-running `std::fs::read_dir` producer.
//!
//! # Examples
//!
//! File examples perform real filesystem I/O, so this example is compile-tested
//! but not run by doctests:
//!
//! ```no_run
//! runite::spawn(async {
//!     runite::fs::write("runite-example.txt", b"hello").await.unwrap();
//!     let contents = runite::fs::read_to_string("runite-example.txt").await.unwrap();
//!     assert_eq!(contents, "hello");
//!     runite::fs::remove_file("runite-example.txt").await.unwrap();
//! });
//! runite::run();
//! ```

use alloc::sync::Arc;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::ffi::OsStr;
use std::io;
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};

use crate::io::{AsyncRead, AsyncWrite, Stream};
use crate::op::fs::{
    FileType as RawFileType, FsOp, MetadataTarget, OpenOptions as OpOpenOptions,
    RawDirEntry as OpDirEntry, RawMetadata,
};
use crate::sys::current::fs as sys_fs;
use crate::sys::handle::{OwnedFile, RawFile, raw_file};

struct FileInner {
    fd: OwnedFile,
}

type PendingFileRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingFileWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;

/// Async file handle.
///
/// `File` supports both cursor-based sequential I/O and offset-based positioned
/// I/O. It is not [`Clone`]; use [`try_clone`](Self::try_clone) to duplicate the
/// underlying file descriptor asynchronously. As with [`std::fs::File::try_clone`],
/// duplicated handles share the kernel-managed file cursor.
///
/// Use [`File::open`] and [`File::create`] for common cases or [`OpenOptions`]
/// for detailed access-mode control.
pub struct File {
    inner: Arc<FileInner>,
    pending_read: Option<PendingFileRead>,
    /// Bytes a completed read produced that overflowed a smaller caller buffer;
    /// served before any new read so no bytes are lost. See
    /// [`ReadOverflow`](crate::io::ReadOverflow).
    read_overflow: Option<Box<crate::io::ReadOverflow>>,
    pending_write: Option<PendingFileWrite>,
}

/// Builder used to configure how a [`File`] is opened.
///
/// Options mirror [`std::fs::OpenOptions`]: callers opt in to read, write,
/// append, truncation, and creation behavior before calling
/// [`open`](Self::open).
pub struct OpenOptions {
    inner: OpOpenOptions,
}

/// File metadata returned by [`metadata`] or [`File::metadata`].
///
/// Metadata exposes the file type, byte length, and platform mode bits reported
/// by the active backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Metadata {
    inner: RawMetadata,
}

/// Async directory-entry stream returned by [`read_dir`].
///
/// Call [`next_entry`](Self::next_entry) to pull entries one at a time, or use
/// the [`Stream`] implementation with the runtime's stream extension traits.
///
/// The stream starts an eager blocking-pool producer when it is created. Entries
/// are queued back to the creating runtime thread as macrotasks. Dropping the
/// stream releases runite's pending operation state, but it does not cancel an
/// already-running `std::fs::read_dir` call in the producer.
pub struct ReadDir {
    inner: sys_fs::ReadDirStream,
}

/// Directory entry yielded by [`ReadDir::next_entry`].
///
/// Each entry carries its path and file name and can resolve fresh metadata on
/// demand with [`metadata`](Self::metadata).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirEntry {
    inner: OpDirEntry,
}

impl File {
    /// Opens an existing file for reading.
    ///
    /// This is a convenience wrapper for `OpenOptions::new().read(true)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut file = runite::fs::File::open("input.txt").await.unwrap();
    ///     let mut contents = String::new();
    ///     file.read_to_string(&mut contents).await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens a file for writing, creating or truncating it first.
    ///
    /// This is a convenience wrapper for write-only create-and-truncate access.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Reads bytes from the file's current cursor position.
    ///
    /// Returns the number of bytes copied into `buf`. A return value of `0`
    /// indicates EOF when `buf` is not empty.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: bytes an in-flight read already received are retained on the
    /// file and returned by the next read if this future is dropped.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Delegate to the AsyncRead path so the in-flight read is stashed on the
        // file: dropping this future retains the operation (cancel-safe — a
        // completed-but-unclaimed read is served next via the overflow buffer)
        // and it cannot race a concurrent trait-based read. Positional
        // [`read_at`](Self::read_at) keeps using `read_impl`.
        core::future::poll_fn(|cx| Pin::new(&mut *self).poll_read(cx, buf)).await
    }

    /// Reads exactly `buf.len()` bytes from the current cursor position.
    pub async fn read_exact(&mut self, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let read = self.read(buf).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            buf = &mut buf[read..];
        }
        Ok(())
    }

    /// Reads all remaining bytes from the current cursor position and appends
    /// them to `buf`.
    pub async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let start_len = buf.len();
        let mut chunk = vec![0; 8192];
        loop {
            let read = self.read(&mut chunk).await?;
            if read == 0 {
                return Ok(buf.len() - start_len);
            }
            buf.extend_from_slice(&chunk[..read]);
        }
    }

    /// Reads all remaining UTF-8 bytes and appends them to `buf`.
    ///
    /// Returns [`io::ErrorKind::InvalidData`] if the remaining bytes are not
    /// valid UTF-8.
    pub async fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        let mut bytes = Vec::new();
        let read = self.read_to_end(&mut bytes).await?;
        let text = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        buf.push_str(&text);
        Ok(read)
    }

    /// Writes bytes at the file's current cursor position.
    ///
    /// The operation may write fewer bytes than `buf.len()`; use
    /// [`write_all`](Self::write_all) to keep writing until the full buffer is
    /// sent.
    ///
    /// # Cancel safety
    ///
    /// **Not** cancel-safe: a completion-based write dropped mid-flight may have
    /// already committed bytes without reporting the count. Drive writes to
    /// completion rather than cancelling them.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Delegate to the AsyncWrite path so the in-flight write is stashed on
        // the file. Positional [`write_at`](Self::write_at) keeps using
        // `write_impl`.
        core::future::poll_fn(|cx| Pin::new(&mut *self).poll_write(cx, buf)).await
    }

    /// Writes the entire buffer at the file's current cursor position.
    pub async fn write_all(&mut self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let written = self.write(buf).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            buf = &buf[written..];
        }
        Ok(())
    }

    /// Flushes any userspace buffering associated with this handle.
    ///
    /// runite does not add userspace buffering for [`File`], so this is a
    /// no-op. It does not call `fsync` or make data durable; use
    /// [`sync_all`](Self::sync_all) or [`sync_data`](Self::sync_data) for that.
    pub async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Synchronizes file contents and metadata to stable storage.
    pub async fn sync_all(&self) -> io::Result<()> {
        sys_fs::sync_all(FsOp::SyncAll { fd: self.raw_fd() }).await
    }

    /// Synchronizes file contents to stable storage.
    ///
    /// Metadata that is not needed to retrieve the file contents may be omitted,
    /// matching the behavior of [`std::fs::File::sync_data`].
    pub async fn sync_data(&self) -> io::Result<()> {
        sys_fs::sync_data(FsOp::SyncData { fd: self.raw_fd() }).await
    }

    /// Reads bytes starting at `offset` without using the shared file cursor.
    ///
    /// Positioned reads are useful when multiple tasks share a cloned handle and
    /// must avoid racing on the kernel-managed cursor.
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_impl(Some(offset), buf).await
    }

    /// Reads exactly `buf.len()` bytes starting at `offset`.
    pub async fn read_exact_at(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let read = self.read_at(offset, buf).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            offset = offset.saturating_add(read as u64);
            buf = &mut buf[read..];
        }
        Ok(())
    }

    /// Writes bytes starting at `offset` without using the shared file cursor.
    ///
    /// Positioned writes are useful when multiple tasks share a cloned handle and
    /// must avoid racing on the kernel-managed cursor.
    pub async fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.write_impl(Some(offset), buf).await
    }

    /// Writes the entire buffer starting at `offset`.
    pub async fn write_all_at(&self, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let written = self.write_at(offset, buf).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            offset = offset.saturating_add(written as u64);
            buf = &buf[written..];
        }
        Ok(())
    }

    /// Returns metadata for this file handle.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        sys_fs::metadata(FsOp::Metadata {
            target: MetadataTarget::File(self.raw_fd()),
            follow_symlinks: true,
        })
        .await
        .map(Metadata::from_raw)
    }

    /// Truncates or extends the underlying file to `len` bytes.
    pub async fn set_len(&self, len: u64) -> io::Result<()> {
        sys_fs::set_len(FsOp::SetLen {
            fd: self.raw_fd(),
            len,
        })
        .await
    }

    /// Seeks the file's cursor and returns the new position from the start.
    ///
    /// This repositions the kernel file cursor shared by this handle (and any
    /// [`try_clone`](Self::try_clone)d handles), so it affects the sequential
    /// [`read`](Self::read)/[`write`](Self::write) methods, not the positioned
    /// [`read_at`](Self::read_at)/[`write_at`](Self::write_at) methods. Mirrors
    /// [`std::io::Seek::seek`].
    pub async fn seek(&mut self, pos: std::io::SeekFrom) -> io::Result<u64> {
        sys_fs::seek(self.raw_fd(), pos)
    }

    /// Duplicates the underlying file description.
    ///
    /// As with [`std::fs::File::try_clone`], the cloned handle shares
    /// kernel-managed cursor state with this handle. Positioned I/O methods such
    /// as [`read_at`](Self::read_at) avoid that shared cursor.
    pub async fn try_clone(&self) -> io::Result<Self> {
        sys_fs::try_clone(FsOp::Duplicate { fd: self.raw_fd() })
            .await
            .map(File::from_owned_fd)
    }

    fn from_owned_fd(fd: OwnedFile) -> Self {
        Self {
            inner: Arc::new(FileInner { fd }),
            pending_read: None,
            read_overflow: None,
            pending_write: None,
        }
    }

    fn raw_fd(&self) -> RawFile {
        raw_file(&self.inner.fd)
    }

    async fn read_impl(&self, offset: Option<u64>, buf: &mut [u8]) -> io::Result<usize> {
        let data = sys_fs::read(FsOp::Read {
            fd: self.raw_fd(),
            offset,
            len: buf.len(),
        })
        .await?;

        let read = data.len();
        buf[..read].copy_from_slice(&data);
        Ok(read)
    }

    async fn write_impl(&self, offset: Option<u64>, buf: &[u8]) -> io::Result<usize> {
        sys_fs::write(FsOp::Write {
            fd: self.raw_fd(),
            offset,
            data: buf.to_vec(),
        })
        .await
    }
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();

        // Serve any surplus from a previous read before submitting a new one.
        if let Some(overflow) = this.read_overflow.as_mut() {
            let n = overflow.drain_into(buf);
            if overflow.is_drained() {
                this.read_overflow = None;
            }
            return Poll::Ready(Ok(n));
        }

        if this.pending_read.is_none() {
            let op = FsOp::Read {
                fd: this.raw_fd(),
                offset: None,
                len: buf.len(),
            };
            this.pending_read = Some(Box::pin(sys_fs::read(op)));
        }

        match this
            .pending_read
            .as_mut()
            .expect("pending read must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_read = None;
                let data = result?;
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                // Retain any bytes that did not fit rather than discarding them.
                if data.len() > n {
                    this.read_overflow = Some(Box::new(crate::io::ReadOverflow::new(&data[n..])));
                }
                Poll::Ready(Ok(n))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for File {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();
        if this.pending_write.is_none() {
            let op = FsOp::Write {
                fd: this.raw_fd(),
                offset: None,
                data: buf.to_vec(),
            };
            this.pending_write = Some(Box::pin(sys_fs::write(op)));
        }

        match this
            .pending_write
            .as_mut()
            .expect("pending write must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_write = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl OpenOptions {
    /// Creates a blank set of open options.
    ///
    /// No access mode is enabled by default; call methods such as
    /// [`read`](Self::read) or [`write`](Self::write) before opening.
    pub fn new() -> Self {
        Self {
            inner: OpOpenOptions::default(),
        }
    }

    /// Controls read access.
    pub fn read(&mut self, value: bool) -> &mut Self {
        self.inner.read = value;
        self
    }

    /// Controls write access.
    pub fn write(&mut self, value: bool) -> &mut Self {
        self.inner.write = value;
        self
    }

    /// Controls append mode.
    ///
    /// When append is enabled, writes are placed at the end of the file by the
    /// operating system.
    pub fn append(&mut self, value: bool) -> &mut Self {
        self.inner.append = value;
        self
    }

    /// Controls whether the file is truncated after opening.
    pub fn truncate(&mut self, value: bool) -> &mut Self {
        self.inner.truncate = value;
        self
    }

    /// Controls whether the file is created if it does not already exist.
    pub fn create(&mut self, value: bool) -> &mut Self {
        self.inner.create = value;
        self
    }

    /// Controls whether opening must create a brand-new file.
    ///
    /// When enabled, opening fails if the path already exists.
    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.inner.create_new = value;
        self
    }

    /// Mutable access to the platform open-options payload, for the
    /// OS-specific extension traits in [`crate::os`].
    #[cfg(windows)]
    pub(crate) fn platform_options_mut(&mut self) -> &mut crate::sys::handle::PlatformOpenOptions {
        &mut self.inner.platform
    }

    /// Opens a file with the configured options.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut options = runite::fs::OpenOptions::new();
    ///     let file = options
    ///         .read(true)
    ///         .write(true)
    ///         .open("example.txt")
    ///         .await
    ///         .unwrap();
    ///     let _ = file.metadata().await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        sys_fs::open(FsOp::Open {
            path: path.as_ref().to_path_buf(),
            options: self.inner.clone(),
        })
        .await
        .map(File::from_owned_fd)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl Metadata {
    fn from_raw(inner: RawMetadata) -> Self {
        Self { inner }
    }

    /// Returns the file length in bytes.
    pub fn len(&self) -> u64 {
        self.inner.len
    }

    /// Returns `true` if the file length is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this metadata describes a regular file.
    pub fn is_file(&self) -> bool {
        self.inner.file_type == RawFileType::File
    }

    /// Returns `true` if this metadata describes a directory.
    pub fn is_dir(&self) -> bool {
        self.inner.file_type == RawFileType::Directory
    }

    /// Returns `true` if this metadata describes a symbolic link.
    pub fn is_symlink(&self) -> bool {
        self.inner.file_type == RawFileType::Symlink
    }

    /// Returns the full POSIX `st_mode` — file-type bits *and* permission bits —
    /// matching `std::os::unix::fs::MetadataExt::mode`.
    ///
    /// This is consistent across the Linux and macOS backends. On Windows the
    /// value is synthesized: the file-type bits are the `S_IFMT` equivalents
    /// and the write permission bits reflect `FILE_ATTRIBUTE_READONLY`. Use
    /// `runite::os::windows::fs::MetadataExt::file_attributes` for the native
    /// attribute bits. To extract just the permission bits, mask with
    /// `0o7777`.
    pub fn mode(&self) -> u32 {
        self.inner.mode
    }

    /// The platform metadata payload, for the OS-specific extension traits in
    /// [`crate::os`].
    #[cfg(windows)]
    pub(crate) fn platform_metadata(&self) -> &crate::sys::handle::PlatformMetadata {
        &self.inner.platform
    }
}

impl ReadDir {
    /// Returns the next directory entry, or `None` once the stream is exhausted.
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        self.inner
            .next_entry()
            .await
            .map(|entry| entry.map(|inner| DirEntry { inner }))
    }
}

impl Stream for ReadDir {
    type Item = io::Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut next = core::pin::pin!(this.inner.next_entry());
        match next.as_mut().poll(cx) {
            Poll::Ready(Ok(Some(entry))) => Poll::Ready(Some(Ok(DirEntry { inner: entry }))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl DirEntry {
    /// Returns the full path to this directory entry.
    ///
    /// The path is the directory path passed to [`read_dir`] joined with this
    /// entry's file name.
    pub fn path(&self) -> PathBuf {
        self.inner.path.clone()
    }

    /// Returns the file name portion of this directory entry.
    pub fn file_name(&self) -> &OsStr {
        self.inner.file_name.as_os_str()
    }

    /// Resolves metadata for this entry.
    ///
    /// Metadata is fetched when this method is called, not cached when the entry
    /// is yielded.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        metadata(self.path()).await
    }
}

/// Reads the entire contents of a file into memory.
///
/// # Examples
///
/// ```no_run
/// runite::spawn(async {
///     let bytes = runite::fs::read("input.bin").await.unwrap();
///     assert!(!bytes.is_empty());
/// });
/// runite::run();
/// ```
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let mut file = File::open(path.as_ref()).await?;
    let mut output = Vec::new();
    file.read_to_end(&mut output).await?;
    Ok(output)
}

/// Reads the entire contents of a UTF-8 file into a [`String`].
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let bytes = read(path).await?;
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Replaces the contents of a file with `data`, creating it if needed.
///
/// Existing contents are truncated before the new bytes are written.
pub async fn write(path: impl AsRef<Path>, data: impl AsRef<[u8]>) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(data.as_ref()).await
}

/// Returns metadata for a filesystem path.
///
/// Symbolic links are followed.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sys_fs::metadata(FsOp::Metadata {
        target: MetadataTarget::Path(path.as_ref().to_path_buf()),
        follow_symlinks: true,
    })
    .await
    .map(Metadata::from_raw)
}

/// Returns metadata for a filesystem path **without** following symbolic links.
///
/// Unlike [`metadata`], if `path` is a symlink this reports the link itself, so
/// [`Metadata::is_symlink`] can be `true`. Mirrors [`std::fs::symlink_metadata`].
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sys_fs::metadata(FsOp::Metadata {
        target: MetadataTarget::Path(path.as_ref().to_path_buf()),
        follow_symlinks: false,
    })
    .await
    .map(Metadata::from_raw)
}

/// Creates a single directory.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::create_dir(FsOp::CreateDir {
        path: path.as_ref().to_path_buf(),
        mode: 0o777,
    })
    .await
}

/// Creates a directory and any missing parent directories.
///
/// Existing **directories** along the path are accepted and treated as success.
/// If any component already exists as a non-directory (for example, the final
/// component is a regular file), this returns an [`io::ErrorKind::AlreadyExists`]
/// error, matching [`std::fs::create_dir_all`].
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let mut current = PathBuf::new();

    for component in path.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() {
            continue;
        }
        // Drive prefixes (`C:`) and the root directory always exist and are
        // not creatable; attempting them reports access errors on Windows
        // rather than `AlreadyExists`.
        if matches!(
            component,
            std::path::Component::Prefix(_) | std::path::Component::RootDir
        ) {
            continue;
        }

        match create_dir(&current).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // An existing component is only acceptable if it is itself a
                // directory. Matching `std::fs::create_dir_all`, a path whose
                // final component is an existing file (or other non-directory)
                // must surface an error rather than silently succeed.
                match metadata(&current).await {
                    Ok(existing) if existing.is_dir() => {}
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("{} exists and is not a directory", current.display()),
                        ));
                    }
                    Err(metadata_error) => return Err(metadata_error),
                }
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// Removes a file.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::remove_file(FsOp::RemoveFile {
        path: path.as_ref().to_path_buf(),
    })
    .await
}

/// Removes an empty directory.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::remove_dir(FsOp::RemoveDir {
        path: path.as_ref().to_path_buf(),
    })
    .await
}

/// Renames or moves a filesystem entry.
///
/// Replacement behavior is platform-specific and matches the underlying
/// operating system operation.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::rename(FsOp::Rename {
        from: from.as_ref().to_path_buf(),
        to: to.as_ref().to_path_buf(),
    })
    .await
}

/// Opens an async directory-entry stream.
///
/// # Examples
///
/// ```
/// runite::spawn(async {
///     let mut entries = runite::fs::read_dir(".").await.unwrap();
///     let _ = entries.next_entry().await.unwrap();
/// });
/// runite::run();
/// ```
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    sys_fs::read_dir(FsOp::ReadDir {
        path: path.as_ref().to_path_buf(),
    })
    .map(|inner| ReadDir { inner })
}

// -- File-descriptor interop (Unix only) -------------------------------------
//
// `#[cfg(unix)]` because these expose raw/owned file descriptors, which the
// Windows backend would replace with `AsHandle`/`AsRawHandle`.

#[cfg(unix)]
impl AsFd for File {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

#[cfg(unix)]
impl From<OwnedFd> for File {
    /// Adopts an open file descriptor as an async [`File`].
    ///
    /// The descriptor must refer to a regular file opened for the access the
    /// caller intends to use; use [`File::from_std`] to adopt a
    /// [`std::fs::File`].
    fn from(fd: OwnedFd) -> Self {
        Self::from_owned_fd(fd)
    }
}

#[cfg(unix)]
impl File {
    /// Adopts a [`std::fs::File`], returning an async [`File`] that shares the
    /// same open file description.
    ///
    /// Files do not need non-blocking mode (the driver handles them via
    /// `io_uring` on Linux and the blocking pool on macOS), so this simply
    /// transfers ownership of the descriptor.
    pub fn from_std(file: std::fs::File) -> Self {
        Self::from_owned_fd(OwnedFd::from(file))
    }
}

// -- Handle interop (Windows only) --------------------------------------------
//
// The Windows analogs of the Unix fd-interop impls above: files are exposed
// through `AsHandle`/`AsRawHandle`, and adoption binds the handle to the
// current runtime thread's I/O completion port so overlapped reads and writes
// can complete. Handles opened without `FILE_FLAG_OVERLAPPED` still work, but
// each operation then completes synchronously on the event-loop thread.

#[cfg(windows)]
mod windows_interop {
    use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, OwnedHandle, RawHandle};

    use super::File;

    impl AsHandle for File {
        fn as_handle(&self) -> BorrowedHandle<'_> {
            self.inner.fd.as_handle()
        }
    }

    impl AsRawHandle for File {
        fn as_raw_handle(&self) -> RawHandle {
            self.inner.fd.as_raw_handle()
        }
    }

    impl From<OwnedHandle> for File {
        /// Adopts an open file handle as an async [`File`], associating it
        /// with the current runtime thread's completion port (best-effort; a
        /// handle whose file object is already bound to a port keeps its
        /// original binding). Use [`File::from_std`] for a fallible adoption.
        fn from(handle: OwnedHandle) -> Self {
            let _ = crate::sys::windows::overlapped::associate_file_reused(
                crate::sys::handle::raw_file(&handle),
            );
            Self::from_owned_fd(handle)
        }
    }

    impl File {
        /// Adopts a [`std::fs::File`], returning an async [`File`] that shares
        /// the same open file object.
        ///
        /// The handle is associated with the current runtime thread's I/O
        /// completion port. For fully asynchronous reads and writes, open the
        /// file with the `FILE_FLAG_OVERLAPPED` custom flag (e.g. via
        /// [`OpenOptionsExt::custom_flags`](std::os::windows::fs::OpenOptionsExt::custom_flags)
        /// or runite's own [`OpenOptions`](super::OpenOptions), which sets it
        /// automatically); a synchronous handle still completes every
        /// operation correctly but blocks the event-loop thread while it runs.
        pub fn from_std(file: std::fs::File) -> Self {
            Self::from(OwnedHandle::from(file))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OpenOptions, create_dir_all, metadata, read, read_dir, read_to_string, remove_dir,
        remove_file, rename, write,
    };
    use crate::io::StreamExt;
    use crate::spawn;
    use crate::{queue_macrotask, run};
    use std::collections::BTreeSet;
    use std::ffi::OsString;
    use std::future::{Future, poll_fn};
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::process;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::task::Poll;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("runite-{label}-{}-{nanos}", process::id()))
    }

    #[test]
    fn async_fs_round_trip() {
        let _guard = test_lock().lock().unwrap();
        let root = unique_path("fs-round-trip");
        let nested = root.join("nested");
        let file_path = nested.join("hello.txt");
        let renamed_path = nested.join("renamed.txt");
        let output = Arc::new(Mutex::new(None::<String>));

        {
            let output = Arc::clone(&output);
            queue_macrotask(move || {
                spawn(async move {
                    create_dir_all(&nested)
                        .await
                        .expect("dir creation should succeed");
                    write(&file_path, b"hello world")
                        .await
                        .expect("initial write should succeed");

                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&file_path)
                        .await
                        .expect("open should succeed");
                    file.write_at(6, b"runtime")
                        .await
                        .expect("positioned write should succeed");
                    file.sync_all().await.expect("sync should succeed");

                    let mut prefix = [0u8; 5];
                    file.read_exact_at(0, &mut prefix)
                        .await
                        .expect("positioned read should succeed");
                    assert_eq!(&prefix, b"hello");

                    let meta = file.metadata().await.expect("metadata should succeed");
                    assert!(meta.is_file());
                    assert!(meta.len() >= 13);

                    let cloned = file.try_clone().await.expect("clone should succeed");
                    cloned.set_len(13).await.expect("truncate should succeed");

                    rename(&file_path, &renamed_path)
                        .await
                        .expect("rename should succeed");
                    let text = read_to_string(&renamed_path)
                        .await
                        .expect("read_to_string should succeed");
                    assert_eq!(text, "hello runtime");

                    let bytes = read(&renamed_path).await.expect("read should succeed");
                    assert_eq!(bytes, b"hello runtime");

                    let path_meta = metadata(&renamed_path)
                        .await
                        .expect("path metadata should work");
                    assert!(path_meta.is_file());

                    *output.lock().unwrap() = Some(text);

                    remove_file(&renamed_path)
                        .await
                        .expect("remove_file should succeed");
                    remove_dir(&nested)
                        .await
                        .expect("remove nested dir should succeed");
                    remove_dir(&root)
                        .await
                        .expect("remove root dir should succeed");
                });
            });
        }

        run();

        assert_eq!(output.lock().unwrap().as_deref(), Some("hello runtime"));
    }

    #[test]
    fn async_read_dir_streams_entries() {
        let _guard = test_lock().lock().unwrap();
        let root = unique_path("fs-read-dir");
        let one = root.join("one.txt");
        let two = root.join("two.txt");
        let seen: Arc<Mutex<BTreeSet<OsString>>> = Arc::new(Mutex::new(BTreeSet::new()));

        {
            let seen = Arc::clone(&seen);
            queue_macrotask(move || {
                spawn(async move {
                    create_dir_all(&root)
                        .await
                        .expect("dir creation should succeed");
                    write(&one, b"1").await.expect("write one should succeed");
                    write(&two, b"2").await.expect("write two should succeed");

                    let mut dir = read_dir(&root).await.expect("read_dir should succeed");
                    while let Some(entry) = dir.next_entry().await.expect("stream should succeed") {
                        seen.lock()
                            .unwrap()
                            .insert(entry.file_name().to_os_string());
                    }

                    remove_file(&one).await.expect("remove one should succeed");
                    remove_file(&two).await.expect("remove two should succeed");
                    remove_dir(&root).await.expect("remove root should succeed");
                });
            });
        }

        run();

        let seen = seen.lock().unwrap();
        assert!(seen.contains(&OsString::from("one.txt")));
        assert!(seen.contains(&OsString::from("two.txt")));
    }

    #[test]
    fn read_dir_stream_yields_entries() {
        let _guard = test_lock().lock().unwrap();
        let root = unique_path("fs-read-dir-stream");
        let files = ["alpha.txt", "beta.txt", "gamma.txt"];
        let seen: Arc<Mutex<Option<BTreeSet<OsString>>>> = Arc::new(Mutex::new(None));

        {
            let seen = Arc::clone(&seen);
            queue_macrotask(move || {
                spawn(async move {
                    create_dir_all(&root)
                        .await
                        .expect("dir creation should succeed");
                    for file in files {
                        write(root.join(file), file.as_bytes())
                            .await
                            .expect("file write should succeed");
                    }

                    let dir = read_dir(&root).await.expect("read_dir should succeed");
                    let entries = dir
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .expect("stream should succeed");
                    let names = entries
                        .into_iter()
                        .map(|entry| entry.file_name().to_os_string())
                        .collect::<BTreeSet<_>>();
                    *seen.lock().unwrap() = Some(names);

                    for file in files {
                        remove_file(root.join(file))
                            .await
                            .expect("remove file should succeed");
                    }
                    remove_dir(&root).await.expect("remove root should succeed");
                });
            });
        }

        run();

        let seen = seen.lock().unwrap();
        let seen = seen.as_ref().expect("task should record entries");
        for file in files {
            assert!(seen.contains(&OsString::from(file)));
        }
    }

    #[test]
    fn read_borrows_user_buffer() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("borrowed-read");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                spawn(async move {
                    write(&path, b"borrowed buffer")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("open should succeed");
                    let mut buf = [0u8; 8];
                    let read = file.read(&mut buf).await.expect("read should succeed");
                    assert_eq!(read, 8);
                    *observed.lock().unwrap() = Some(buf.to_vec());
                    remove_file(&path).await.expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"borrowed".as_slice())
        );
    }

    #[test]
    fn read_to_end_collects_full_file() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("read-to-end");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                spawn(async move {
                    write(&path, b"full file contents")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("open should succeed");
                    let mut out = b"prefix:".to_vec();
                    let read = file
                        .read_to_end(&mut out)
                        .await
                        .expect("read_to_end should succeed");
                    assert_eq!(read, b"full file contents".len());
                    *observed.lock().unwrap() = Some(out);
                    remove_file(&path).await.expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"prefix:full file contents".as_slice())
        );
    }

    #[test]
    fn read_drop_during_inflight_does_not_uaf() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("drop-inflight-read");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                spawn(async move {
                    write(&path, b"cancel smoke test")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("open should succeed");

                    let mut dropped_buf = [0xAAu8; 64];
                    {
                        let mut read = Box::pin(file.read(&mut dropped_buf));
                        let _ = poll_fn(|cx| Poll::Ready(Pin::as_mut(&mut read).poll(cx))).await;
                    }

                    for _ in 0..32 {
                        crate::yield_now().await;
                    }

                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("reopen should succeed");
                    let mut buf = [0u8; 17];
                    let read = file
                        .read(&mut buf)
                        .await
                        .expect("second read should succeed");
                    *observed.lock().unwrap() = Some(buf[..read].to_vec());
                    remove_file(&path).await.expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"cancel smoke test".as_slice())
        );
    }
}

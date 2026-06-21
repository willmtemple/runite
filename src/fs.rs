//! Portable async filesystem API.
//!
//! Cancellation semantics:
//! - Dropping an I/O future cancels interest in the result.
//! - The runtime issues best-effort kernel cancellation where supported.
//! - The underlying OS operation may still complete after the future is dropped.
//!
//! The public surface intentionally mirrors `std::fs` where that shape makes sense, while using
//! async methods for operations that may block the caller.

use alloc::sync::Arc;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};

use crate::io::{AsyncRead, AsyncWrite, Stream};
use crate::op::fs::{
    FileType as RawFileType, FsOp, MetadataTarget, OpenOptions as OpOpenOptions,
    RawDirEntry as OpDirEntry, RawMetadata,
};
use crate::sys::current::fs as sys_fs;

struct FileInner {
    fd: OwnedFd,
}

type PendingFileRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingFileWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;

/// Async file handle.
///
/// `File` is cheap to clone internally and supports both cursor-based sequential I/O and
/// offset-based positioned I/O.
pub struct File {
    inner: Arc<FileInner>,
    pending_read: Option<PendingFileRead>,
    pending_write: Option<PendingFileWrite>,
}

/// Builder used to configure how a [`File`] is opened.
pub struct OpenOptions {
    inner: OpOpenOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// File metadata returned by [`metadata`] or [`File::metadata`].
pub struct Metadata {
    inner: RawMetadata,
}

/// Async directory-entry stream returned by [`read_dir`].
pub struct ReadDir {
    inner: sys_fs::ReadDirStream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Directory entry yielded by [`ReadDir::next_entry`].
pub struct DirEntry {
    inner: OpDirEntry,
}

impl File {
    /// Opens an existing file for reading.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens a file for writing, creating or truncating it first.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Reads bytes from the file's current cursor position.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_impl(None, buf).await
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
    pub async fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        let mut bytes = Vec::new();
        let read = self.read_to_end(&mut bytes).await?;
        let text = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        buf.push_str(&text);
        Ok(read)
    }

    /// Writes bytes at the file's current cursor position.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_impl(None, buf).await
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
    /// The current implementation does not add additional buffering beyond the kernel file
    /// description, so this is effectively a no-op.
    pub async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Synchronizes file contents and metadata to stable storage.
    pub async fn sync_all(&self) -> io::Result<()> {
        sys_fs::sync_all(FsOp::SyncAll { fd: self.raw_fd() }).await
    }

    /// Synchronizes file contents to stable storage.
    pub async fn sync_data(&self) -> io::Result<()> {
        sys_fs::sync_data(FsOp::SyncData { fd: self.raw_fd() }).await
    }

    /// Reads bytes starting at `offset` without using the shared file cursor.
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

    /// Duplicates the underlying file description.
    ///
    /// As with `std::fs::File::try_clone`, the cloned handle shares kernel-managed cursor state.
    pub async fn try_clone(&self) -> io::Result<Self> {
        sys_fs::try_clone(FsOp::Duplicate { fd: self.raw_fd() })
            .await
            .map(File::from_owned_fd)
    }

    fn from_owned_fd(fd: OwnedFd) -> Self {
        Self {
            inner: Arc::new(FileInner { fd }),
            pending_read: None,
            pending_write: None,
        }
    }

    fn raw_fd(&self) -> i32 {
        self.inner.fd.as_raw_fd()
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
                let read = data.len();
                if read > buf.len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "read completed with more bytes than destination buffer can hold",
                    )));
                }
                buf[..read].copy_from_slice(&data);
                Poll::Ready(Ok(read))
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
    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.inner.create_new = value;
        self
    }

    /// Opens a file with the configured options.
    ///
    /// # Examples
    ///
    /// ```
    /// # let _ = || async {
    /// let file = ruin_runtime::fs::OpenOptions::new()
    ///     .read(true)
    ///     .write(true)
    ///     .open("example.txt")
    ///     .await;
    /// # let _ = file;
    /// # };
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

    /// Returns the raw POSIX mode bits reported by the platform backend.
    pub fn mode(&self) -> u16 {
        self.inner.mode
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
    pub fn path(&self) -> PathBuf {
        self.inner.path.clone()
    }

    /// Returns the file name portion of this directory entry.
    pub fn file_name(&self) -> &OsStr {
        self.inner.file_name.as_os_str()
    }

    /// Resolves metadata for this entry.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        metadata(self.path()).await
    }
}

/// Reads the entire contents of a file into memory.
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
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sys_fs::metadata(FsOp::Metadata {
        target: MetadataTarget::Path(path.as_ref().to_path_buf()),
        follow_symlinks: true,
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
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let mut current = PathBuf::new();

    for component in path.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() {
            continue;
        }

        match create_dir(&current).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
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
/// # let _ = || async {
/// let mut entries = ruin_runtime::fs::read_dir(".").await.unwrap();
/// let _ = entries.next_entry().await;
/// # };
/// ```
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    sys_fs::read_dir(FsOp::ReadDir {
        path: path.as_ref().to_path_buf(),
    })
    .map(|inner| ReadDir { inner })
}

#[cfg(test)]
mod tests {
    use super::{
        OpenOptions, create_dir_all, metadata, read, read_dir, read_to_string, remove_dir,
        remove_file, rename, write,
    };
    use crate::io::StreamExt;
    use crate::queue_future;
    use crate::{queue_task, run};
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
        std::env::temp_dir().join(format!("ruin-runtime-{label}-{}-{nanos}", process::id()))
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
            queue_task(move || {
                queue_future(async move {
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
            queue_task(move || {
                queue_future(async move {
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
            queue_task(move || {
                queue_future(async move {
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
            queue_task(move || {
                queue_future(async move {
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
            queue_task(move || {
                queue_future(async move {
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
            queue_task(move || {
                queue_future(async move {
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

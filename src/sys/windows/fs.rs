//! Windows filesystem backend.
//!
//! File reads and writes are true completion-based I/O: handles are opened
//! with `FILE_FLAG_OVERLAPPED`, associated with the runtime thread's
//! completion port, and driven by overlapped `ReadFile`/`WriteFile` (see
//! `docs/WINDOWS.md`). Operations with no overlapped form — open, metadata,
//! directory scans, flush, truncation — are offloaded to the blocking pool,
//! mirroring the macOS backend.
//!
//! Cursor semantics: overlapped I/O always takes an explicit offset, so the
//! "current position" reads and writes bracket each operation with the file
//! object's shared pointer (`SetFilePointerEx`). Duplicated handles share the
//! file object and therefore the cursor, matching Unix `dup`.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::io;
use std::mem::ManuallyDrop;
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{FromRawHandle, OwnedHandle};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use windows_sys::Win32::Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_BEGIN,
    FILE_CURRENT, FILE_END, FILE_FLAG_OVERLAPPED, SetFilePointerEx,
};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

use crate::op::completion::completion_for_current_thread;
use crate::op::fs::{FileType, FsOp, MetadataTarget, RawDirEntry, RawMetadata};
use crate::platform::current::runtime::{QueueError, ThreadHandle, current_thread_handle};
use crate::sys::blocking::spawn_blocking;
use crate::sys::handle::{OwnedFile, PlatformMetadata, RawFile, raw_file};
use crate::sys::windows::overlapped;

pub async fn open(op: FsOp) -> io::Result<OwnedFile> {
    let FsOp::Open { path, options } = op else {
        unreachable!("open backend called with non-open op");
    };

    let file = offload(move || {
        let mut open = std::fs::OpenOptions::new();
        open.read(options.read)
            .write(options.write)
            .append(options.append)
            .truncate(options.truncate)
            .create(options.create)
            .create_new(options.create_new)
            .custom_flags(options.platform.custom_flags | FILE_FLAG_OVERLAPPED);
        if let Some(access) = options.platform.access_mode {
            open.access_mode(access);
        }
        if let Some(share) = options.platform.share_mode {
            open.share_mode(share);
        }
        if options.platform.attributes != 0 {
            open.attributes(options.platform.attributes);
        }
        if options.platform.security_qos_flags != 0 {
            open.security_qos_flags(options.platform.security_qos_flags);
        }
        let file = open.open(path)?;
        Ok(OwnedHandle::from(file))
    })
    .await?;

    // Bind the fresh handle to this runtime thread's completion port so
    // overlapped reads and writes post their packets here. Runs after the
    // offload so it executes on the runtime thread that owns the driver.
    overlapped::associate_file(raw_file(&file))?;
    Ok(file)
}

pub async fn read(op: FsOp) -> io::Result<Vec<u8>> {
    let FsOp::Read { fd, offset, len } = op else {
        unreachable!("read backend called with non-read op");
    };

    match offset {
        Some(offset) => overlapped::read_at(fd, len, offset).await,
        None => {
            let position = seek(fd, std::io::SeekFrom::Current(0))?;
            let data = overlapped::read_at(fd, len, position).await?;
            advance_cursor(fd, position, data.len() as u64);
            Ok(data)
        }
    }
}

pub async fn write(op: FsOp) -> io::Result<usize> {
    let FsOp::Write { fd, offset, data } = op else {
        unreachable!("write backend called with non-write op");
    };

    match offset {
        Some(offset) => overlapped::write_at(fd, data, offset).await,
        None => {
            let position = seek(fd, std::io::SeekFrom::Current(0))?;
            let written = overlapped::write_at(fd, data, position).await?;
            advance_cursor(fd, position, written as u64);
            Ok(written)
        }
    }
}

/// Moves the shared file cursor past a completed cursor-based operation.
///
/// Best-effort: append-mode handles ignore write offsets entirely (the kernel
/// appends atomically), and a failed pointer update only affects subsequent
/// cursor-based operations on the same handle.
fn advance_cursor(fd: RawFile, position: u64, transferred: u64) {
    let _ = seek(
        fd,
        std::io::SeekFrom::Start(position.saturating_add(transferred)),
    );
}

pub async fn metadata(op: FsOp) -> io::Result<RawMetadata> {
    let FsOp::Metadata {
        target,
        follow_symlinks,
    } = op
    else {
        unreachable!("metadata backend called with non-metadata op");
    };

    offload(move || {
        let metadata = match target {
            MetadataTarget::Path(path) => {
                if follow_symlinks {
                    std::fs::metadata(path)
                } else {
                    std::fs::symlink_metadata(path)
                }
            }
            MetadataTarget::File(fd) => {
                let file = borrow_file(fd);
                file.metadata()
            }
        }?;
        Ok(raw_metadata_from_std(&metadata))
    })
    .await
}

pub async fn sync_all(op: FsOp) -> io::Result<()> {
    let FsOp::SyncAll { fd } = op else {
        unreachable!("sync_all backend called with non-sync_all op");
    };

    offload(move || borrow_file(fd).sync_all()).await
}

pub async fn sync_data(op: FsOp) -> io::Result<()> {
    let FsOp::SyncData { fd } = op else {
        unreachable!("sync_data backend called with non-sync_data op");
    };

    offload(move || borrow_file(fd).sync_data()).await
}

pub async fn set_len(op: FsOp) -> io::Result<()> {
    let FsOp::SetLen { fd, len } = op else {
        unreachable!("set_len backend called with non-set_len op");
    };

    offload(move || borrow_file(fd).set_len(len)).await
}

/// Repositions the file's kernel cursor. `SetFilePointerEx` is a fast
/// metadata operation that does not block, so it runs inline on the event
/// loop.
pub fn seek(fd: RawFile, pos: std::io::SeekFrom) -> io::Result<u64> {
    let (method, offset) = match pos {
        std::io::SeekFrom::Start(n) => (
            FILE_BEGIN,
            i64::try_from(n).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "seek offset exceeds i64 range")
            })?,
        ),
        std::io::SeekFrom::End(n) => (FILE_END, n),
        std::io::SeekFrom::Current(n) => (FILE_CURRENT, n),
    };

    let mut new_position = 0i64;
    // SAFETY: `fd` names an open file handle and `new_position` is a valid
    // out-pointer.
    let ok = unsafe { SetFilePointerEx(fd.as_handle(), offset, &mut new_position, method) };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(new_position as u64)
    }
}

pub async fn try_clone(op: FsOp) -> io::Result<OwnedFile> {
    let FsOp::Duplicate { fd } = op else {
        unreachable!("try_clone backend called with non-duplicate op");
    };

    // `DuplicateHandle` within one process never blocks; run it inline like
    // the Linux backend's `F_DUPFD_CLOEXEC`.
    let mut duplicated = std::ptr::null_mut();
    // SAFETY: `fd` is an open handle owned by the caller; both process handles
    // are the current-process pseudo handle; `duplicated` is a valid
    // out-pointer.
    let ok = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            fd.as_handle(),
            GetCurrentProcess(),
            &mut duplicated,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `duplicated` is a fresh handle exclusively owned here.
    let file = unsafe { OwnedHandle::from_raw_handle(duplicated) };

    // The duplicate shares the original's file object, which is already bound
    // to a completion port; tolerate the re-association failure.
    overlapped::associate_file_reused(raw_file(&file))?;
    Ok(file)
}

pub async fn create_dir(op: FsOp) -> io::Result<()> {
    let FsOp::CreateDir { path, mode: _ } = op else {
        unreachable!("create_dir backend called with non-create_dir op");
    };

    offload(move || std::fs::create_dir(path)).await
}

pub async fn remove_file(op: FsOp) -> io::Result<()> {
    let FsOp::RemoveFile { path } = op else {
        unreachable!("remove_file backend called with non-remove_file op");
    };

    offload(move || std::fs::remove_file(path)).await
}

pub async fn remove_dir(op: FsOp) -> io::Result<()> {
    let FsOp::RemoveDir { path } = op else {
        unreachable!("remove_dir backend called with non-remove_dir op");
    };

    offload(move || std::fs::remove_dir(path)).await
}

pub async fn rename(op: FsOp) -> io::Result<()> {
    let FsOp::Rename { from, to } = op else {
        unreachable!("rename backend called with non-rename op");
    };

    offload(move || std::fs::rename(from, to)).await
}

pub fn read_dir(op: FsOp) -> io::Result<ReadDirStream> {
    let FsOp::ReadDir { path } = op else {
        unreachable!("read_dir backend called with non-read_dir op");
    };

    ReadDirStream::new(path)
}

pub struct ReadDirStream {
    state: Arc<ReadDirState>,
}

impl ReadDirStream {
    fn new(path: PathBuf) -> io::Result<Self> {
        let state = Arc::new(ReadDirState::new(current_thread_handle()));
        let producer = Arc::clone(&state);

        if let Err(error) = spawn_blocking(move || produce_dir_entries(path, producer)) {
            state.release_pending();
            return Err(error);
        }

        Ok(Self { state })
    }

    pub async fn next_entry(&mut self) -> io::Result<Option<RawDirEntry>> {
        poll_fn(|cx| self.state.poll_next(cx)).await
    }
}

struct ReadDirState {
    owner: ThreadHandle,
    queue: Mutex<VecDeque<io::Result<RawDirEntry>>>,
    done: AtomicBool,
    pending: AtomicBool,
    wake_queued: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl ReadDirState {
    fn new(owner: ThreadHandle) -> Self {
        owner.begin_async_operation();
        Self {
            owner,
            queue: Mutex::new(VecDeque::new()),
            done: AtomicBool::new(false),
            pending: AtomicBool::new(true),
            wake_queued: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }

    fn push(self: &Arc<Self>, entry: io::Result<RawDirEntry>) {
        self.queue.lock().unwrap().push_back(entry);
        self.notify();
    }

    fn finish(self: &Arc<Self>) {
        self.done.store(true, Ordering::Release);
        self.release_pending();
        self.notify();
    }

    fn release_pending(&self) {
        if self.pending.swap(false, Ordering::AcqRel) {
            self.owner.finish_async_operation();
        }
    }

    fn notify(self: &Arc<Self>) {
        if self.wake_queued.swap(true, Ordering::AcqRel) {
            return;
        }

        let state = Arc::clone(self);
        match self.owner.queue_macrotask(move || {
            state.wake_queued.store(false, Ordering::Release);
            if let Some(waker) = state.waker.lock().unwrap().take() {
                waker.wake();
            }
        }) {
            Ok(()) => {}
            Err(QueueError::Closed) => {
                self.wake_queued.store(false, Ordering::Release);
            }
            Err(QueueError::Full) => {
                // Directory stream wakeups originate from blocking completion
                // context; do not block or retry on overflow.
                tracing::error!(
                    target: crate::trace_targets::SCHEDULER,
                    event = "dir_stream_wake_dropped",
                    "dropping directory stream wake because the remote queue is full"
                );
                self.wake_queued.store(false, Ordering::Release);
            }
        }
    }

    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<io::Result<Option<RawDirEntry>>> {
        if let Some(entry) = self.queue.lock().unwrap().pop_front() {
            return Poll::Ready(entry.map(Some));
        }

        if self.done.load(Ordering::Acquire) {
            return Poll::Ready(Ok(None));
        }

        *self.waker.lock().unwrap() = Some(cx.waker().clone());

        if let Some(entry) = self.queue.lock().unwrap().pop_front() {
            let _ = self.waker.lock().unwrap().take();
            return Poll::Ready(entry.map(Some));
        }

        if self.done.load(Ordering::Acquire) {
            let _ = self.waker.lock().unwrap().take();
            return Poll::Ready(Ok(None));
        }

        Poll::Pending
    }
}

impl Drop for ReadDirStream {
    fn drop(&mut self) {
        self.state.release_pending();
    }
}

fn produce_dir_entries(path: PathBuf, state: Arc<ReadDirState>) {
    match std::fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let file_name = entry.file_name();
                        state.push(Ok(RawDirEntry {
                            path: entry.path(),
                            file_name,
                        }));
                    }
                    Err(error) => state.push(Err(error)),
                }
            }
            state.finish();
        }
        Err(error) => {
            state.push(Err(error));
            state.finish();
        }
    }
}

async fn offload<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let handle_for_task = handle.clone();
    if let Err(error) = spawn_blocking(move || handle_for_task.complete(work())) {
        handle.complete(Err(error));
    }
    future.await
}

/// Borrows a raw handle as a [`std::fs::File`] without taking ownership, so
/// std's handle-based metadata/flush/truncate wrappers can be reused. The
/// `ManuallyDrop` prevents the borrowed handle from being closed.
fn borrow_file(fd: RawFile) -> ManuallyDrop<std::fs::File> {
    // SAFETY: `fd` names a handle the caller keeps open for the duration of
    // the blocking operation; `ManuallyDrop` ensures ownership never
    // transfers.
    ManuallyDrop::new(unsafe { std::fs::File::from_raw_handle(fd.as_handle()) })
}

fn raw_metadata_from_std(metadata: &std::fs::Metadata) -> RawMetadata {
    let file_type = metadata.file_type();
    let kind = if file_type.is_symlink() {
        FileType::Symlink
    } else if file_type.is_dir() {
        FileType::Directory
    } else if file_type.is_file() {
        FileType::File
    } else {
        FileType::Unknown
    };

    let attributes = metadata.file_attributes();

    RawMetadata {
        file_type: kind,
        mode: synthesize_mode(attributes, kind),
        len: metadata.len(),
        platform: PlatformMetadata {
            file_attributes: attributes,
        },
    }
}

/// Synthesizes a POSIX-style `st_mode` from Windows file attributes so
/// `Metadata::mode()` has consistent cross-platform shape: the file-type bits
/// match `S_IFMT` values, and the permission bits reflect
/// `FILE_ATTRIBUTE_READONLY` (write bits cleared when set). This mirrors the
/// mapping used by MSYS/Cygwin-style environments.
fn synthesize_mode(attributes: u32, kind: FileType) -> u32 {
    const S_IFDIR: u32 = 0o040000;
    const S_IFREG: u32 = 0o100000;
    const S_IFLNK: u32 = 0o120000;

    let (type_bits, base_permissions) = match kind {
        FileType::Directory => (S_IFDIR, 0o777),
        FileType::Symlink => (S_IFLNK, 0o777),
        _ => (S_IFREG, 0o666),
    };

    let permissions = if attributes & FILE_ATTRIBUTE_READONLY != 0
        && attributes & FILE_ATTRIBUTE_DIRECTORY == 0
        && attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
    {
        base_permissions & !0o222
    } else {
        base_permissions
    };

    type_bits | permissions
}

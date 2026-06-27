//! macOS filesystem backend.

use std::collections::VecDeque;
use std::ffi::CString;
use std::future::poll_fn;
use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::op::completion::completion_for_current_thread;
use crate::op::fs::{FileType, FsOp, MetadataTarget, RawDirEntry, RawMetadata};
use crate::platform::current::runtime::{QueueError, ThreadHandle, current_thread_handle};
use crate::sys::blocking::spawn_blocking;

pub async fn open(op: FsOp) -> io::Result<OwnedFd> {
    let FsOp::Open { path, options } = op else {
        unreachable!("open backend called with non-open op");
    };

    offload(move || {
        let mut open = std::fs::OpenOptions::new();
        open.read(options.read)
            .write(options.write)
            .append(options.append)
            .truncate(options.truncate)
            .create(options.create)
            .create_new(options.create_new)
            .mode(0o666);
        let file = open.open(path)?;
        Ok(unsafe { OwnedFd::from_raw_fd(std::os::fd::IntoRawFd::into_raw_fd(file)) })
    })
    .await
}

pub async fn read(op: FsOp) -> io::Result<Vec<u8>> {
    let FsOp::Read { fd, offset, len } = op else {
        unreachable!("read backend called with non-read op");
    };

    offload(move || {
        let mut buffer = vec![0; len];
        let read = match offset {
            Some(offset) => unsafe {
                libc::pread(
                    fd,
                    buffer.as_mut_ptr().cast::<libc::c_void>(),
                    len,
                    offset as libc::off_t,
                )
            },
            None => unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), len) },
        };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        buffer.truncate(read as usize);
        Ok(buffer)
    })
    .await
}

pub async fn write(op: FsOp) -> io::Result<usize> {
    let FsOp::Write { fd, offset, data } = op else {
        unreachable!("write backend called with non-write op");
    };

    offload(move || {
        let written = match offset {
            Some(offset) => unsafe {
                libc::pwrite(
                    fd,
                    data.as_ptr().cast::<libc::c_void>(),
                    data.len(),
                    offset as libc::off_t,
                )
            },
            None => unsafe { libc::write(fd, data.as_ptr().cast::<libc::c_void>(), data.len()) },
        };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(written as usize)
    })
    .await
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
                let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
                let result = unsafe { libc::fstat(fd, &mut stat) };
                if result < 0 {
                    return Err(io::Error::last_os_error());
                }
                return Ok(raw_metadata_from_stat(&stat));
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

    offload(move || cvt(unsafe { libc::fsync(fd) }).map(|_| ())).await
}

pub async fn sync_data(op: FsOp) -> io::Result<()> {
    let FsOp::SyncData { fd } = op else {
        unreachable!("sync_data backend called with non-sync_data op");
    };

    offload(move || cvt(unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) }).map(|_| ())).await
}

pub async fn set_len(op: FsOp) -> io::Result<()> {
    let FsOp::SetLen { fd, len } = op else {
        unreachable!("set_len backend called with non-set_len op");
    };

    offload(move || cvt(unsafe { libc::ftruncate(fd, len as libc::off_t) }).map(|_| ())).await
}

pub async fn try_clone(op: FsOp) -> io::Result<OwnedFd> {
    let FsOp::Duplicate { fd } = op else {
        unreachable!("try_clone backend called with non-duplicate op");
    };

    offload(move || {
        let duplicated = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
        Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
    })
    .await
}

pub async fn create_dir(op: FsOp) -> io::Result<()> {
    let FsOp::CreateDir { path, mode } = op else {
        unreachable!("create_dir backend called with non-create_dir op");
    };

    offload(move || {
        let c_path = path_to_c_string(path)?;
        cvt(unsafe { libc::mkdir(c_path.as_ptr(), mode as libc::mode_t) }).map(|_| ())
    })
    .await
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
        match self.owner.queue_task(move || {
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

fn path_to_c_string(path: PathBuf) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains interior NUL bytes",
        )
    })
}

fn raw_metadata_from_std(metadata: &std::fs::Metadata) -> RawMetadata {
    let file_type = metadata.file_type();
    let kind = if file_type.is_file() {
        FileType::File
    } else if file_type.is_dir() {
        FileType::Directory
    } else if file_type.is_symlink() {
        FileType::Symlink
    } else if file_type.is_block_device() {
        FileType::BlockDevice
    } else if file_type.is_char_device() {
        FileType::CharacterDevice
    } else if file_type.is_fifo() {
        FileType::Fifo
    } else if file_type.is_socket() {
        FileType::Socket
    } else {
        FileType::Unknown
    };

    RawMetadata {
        file_type: kind,
        mode: (metadata.mode() & 0o7777) as u16,
        len: metadata.len(),
    }
}

fn raw_metadata_from_stat(stat: &libc::stat) -> RawMetadata {
    let kind = match stat.st_mode & libc::S_IFMT {
        libc::S_IFREG => FileType::File,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharacterDevice,
        libc::S_IFIFO => FileType::Fifo,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::Unknown,
    };

    RawMetadata {
        file_type: kind,
        mode: stat.st_mode & 0o7777,
        len: stat.st_size as u64,
    }
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

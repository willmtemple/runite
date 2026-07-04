//! Linux filesystem backend.

use std::collections::VecDeque;
use std::ffi::CString;
use std::future::poll_fn;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::op::completion::completion_for_current_thread;
use crate::op::fs::{FileType, FsOp, MetadataTarget, OpenOptions, RawDirEntry, RawMetadata};
use crate::platform::linux::runtime::{
    QueueError, ThreadHandle, current_thread_handle, with_current_driver,
};
use crate::platform::linux::uring::{
    IORING_FSYNC_DATASYNC, IORING_OP_CLOSE, IORING_OP_FSYNC, IORING_OP_FTRUNCATE,
    IORING_OP_MKDIRAT, IORING_OP_OPENAT, IORING_OP_READ, IORING_OP_RENAMEAT, IORING_OP_STATX,
    IORING_OP_UNLINKAT, IORING_OP_WRITE, IoUringCqe,
};

const STATX_BASIC_MASK: u32 =
    libc::STATX_TYPE | libc::STATX_MODE | libc::STATX_SIZE | libc::STATX_NLINK;
const FILE_CURSOR: u64 = u64::MAX;

// TODO(roadmap): unwired io_uring fs-close / op-classifier scaffolding; a Linux
// agent will wire or remove it before release. See ROADMAP.md.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPath {
    IoUring,
    Offload,
    Inline,
}

#[allow(dead_code)]
pub fn execution_path(op: &FsOp) -> ExecutionPath {
    match op {
        // `getdents`/`std::fs::read_dir` can block on disk and has no io_uring
        // opcode, so it streams on the blocking pool.
        FsOp::ReadDir { .. } => ExecutionPath::Offload,
        // `fcntl(F_DUPFD_CLOEXEC)` never blocks, so it runs inline.
        FsOp::Duplicate { .. } => ExecutionPath::Inline,
        FsOp::Open { .. }
        | FsOp::Read { .. }
        | FsOp::Write { .. }
        | FsOp::Metadata { .. }
        | FsOp::SetLen { .. }
        | FsOp::SyncAll { .. }
        | FsOp::SyncData { .. }
        | FsOp::CreateDir { .. }
        | FsOp::RemoveFile { .. }
        | FsOp::RemoveDir { .. }
        | FsOp::Rename { .. }
        | FsOp::Close { .. } => ExecutionPath::IoUring,
    }
}

pub async fn open(op: FsOp) -> io::Result<OwnedFd> {
    let FsOp::Open { path, options } = op else {
        unreachable!("open backend called with non-open op");
    };

    let path = path_to_c_string(&path)?;
    let path_ptr = path.as_ptr();
    let (flags, mode) = open_flags(&options)?;
    submit_uring::<OwnedFd, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_OPENAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = path_ptr as u64;
            sqe.len = mode;
            sqe.op_flags = flags as u32;
        },
        move |cqe| {
            let _path = path;
            // SAFETY: `fd` is the non-negative descriptor returned by a
            // successful openat CQE and ownership is transferred exactly once.
            cqe_to_result(cqe).map(|fd| unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
        },
    )
    .await
}

pub async fn read(op: FsOp) -> io::Result<Vec<u8>> {
    let FsOp::Read { fd, offset, len } = op else {
        unreachable!("read backend called with non-read op");
    };

    let buffer = Arc::new(Mutex::new(vec![0; len].into_boxed_slice()));
    let buffer_ptr = buffer.lock().unwrap().as_mut_ptr();
    let buffer_len = len;
    submit_uring_guarded::<Vec<u8>, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_READ;
            sqe.fd = fd;
            sqe.addr = buffer_ptr as u64;
            sqe.len = buffer_len as u32;
            sqe.off = offset.unwrap_or(FILE_CURSOR);
        },
        Box::new(Arc::clone(&buffer)),
        move |cqe| {
            let read = cqe_to_result(cqe)? as usize;
            let buffer = buffer.lock().unwrap();
            Ok(buffer[..read].to_vec())
        },
    )
    .await
}

pub async fn write(op: FsOp) -> io::Result<usize> {
    let FsOp::Write { fd, offset, data } = op else {
        unreachable!("write backend called with non-write op");
    };
    let data = Arc::new(data.into_boxed_slice());
    let data_ptr = data.as_ptr();
    let data_len = data.len();

    submit_uring_guarded::<usize, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_WRITE;
            sqe.fd = fd;
            sqe.addr = data_ptr as u64;
            sqe.len = data_len as u32;
            sqe.off = offset.unwrap_or(FILE_CURSOR);
        },
        Box::new(Arc::clone(&data)),
        move |cqe| {
            let _data = data;
            cqe_to_result(cqe).map(|written| written as usize)
        },
    )
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

    let mut statx = Box::new(MaybeUninit::<libc::statx>::zeroed());
    let statx_ptr = statx.as_mut_ptr();
    let (fd, path, flags) = match target {
        MetadataTarget::Path(path) => (
            libc::AT_FDCWD,
            path_to_c_string(&path)?,
            metadata_flags(follow_symlinks),
        ),
        MetadataTarget::File(fd) => (
            fd,
            CString::new(Vec::<u8>::new()).expect("empty statx path should be valid"),
            libc::AT_EMPTY_PATH,
        ),
    };
    let path_ptr = path.as_ptr();

    submit_uring::<RawMetadata, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_STATX;
            sqe.fd = fd;
            sqe.addr = path_ptr as u64;
            sqe.len = STATX_BASIC_MASK;
            sqe.off = statx_ptr as u64;
            sqe.op_flags = flags as u32;
        },
        move |cqe| {
            let _path = path;
            cqe_to_result(cqe)?;
            // SAFETY: a successful statx CQE means the kernel initialized the
            // `statx` buffer supplied in the SQE before completion.
            let statx = unsafe { statx.assume_init() };
            Ok(raw_metadata_from_statx(&statx))
        },
    )
    .await
}

pub async fn sync_all(op: FsOp) -> io::Result<()> {
    let FsOp::SyncAll { fd } = op else {
        unreachable!("sync_all backend called with non-sync_all op");
    };

    submit_sync(fd, 0).await
}

pub async fn sync_data(op: FsOp) -> io::Result<()> {
    let FsOp::SyncData { fd } = op else {
        unreachable!("sync_data backend called with non-sync_data op");
    };

    submit_sync(fd, IORING_FSYNC_DATASYNC).await
}

pub async fn set_len(op: FsOp) -> io::Result<()> {
    let FsOp::SetLen { fd, len } = op else {
        unreachable!("set_len backend called with non-set_len op");
    };

    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_FTRUNCATE;
            sqe.fd = fd;
            sqe.off = len;
        },
        move |cqe| cqe_to_result(cqe).map(|_| ()),
    )
    .await
    {
        // IORING_OP_FTRUNCATE requires Linux 6.9. On older kernels fall back to
        // a synchronous ftruncate(2): on a regular file it is a fast metadata
        // update, so running it inline (like the socket lifecycle fallbacks) is
        // acceptable rather than paying a blocking-pool hop.
        Err(error) if fs_should_fallback(&error) => set_len_sync(fd, len),
        result => result,
    }
}

fn set_len_sync(fd: RawFd, len: u64) -> io::Result<()> {
    // SAFETY: ftruncate takes only a descriptor and a length; no user pointers.
    cvt(unsafe { libc::ftruncate(fd, len as libc::off_t) }).map(|_| ())
}

/// Whether an io_uring op error indicates the opcode is unavailable on this
/// kernel and a synchronous-syscall fallback should be attempted.
fn fs_should_fallback(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
    )
}

pub async fn try_clone(op: FsOp) -> io::Result<OwnedFd> {
    let FsOp::Duplicate { fd } = op else {
        unreachable!("try_clone backend called with non-duplicate op");
    };

    // `fcntl(F_DUPFD_CLOEXEC)` never blocks, so run it inline rather than on the
    // blocking pool.
    // SAFETY: `fd` is a valid descriptor for the duration of the fcntl call;
    // F_DUPFD_CLOEXEC does not access user pointers.
    let duplicated = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
    // SAFETY: `duplicated` is a fresh descriptor returned by successful fcntl
    // and ownership is transferred to `OwnedFd` exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

pub async fn create_dir(op: FsOp) -> io::Result<()> {
    let FsOp::CreateDir { path, mode } = op else {
        unreachable!("create_dir backend called with non-create_dir op");
    };

    let path = path_to_c_string(&path)?;
    let path_ptr = path.as_ptr();
    submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_MKDIRAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = path_ptr as u64;
            sqe.len = mode;
        },
        move |cqe| {
            let _path = path;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
}

pub async fn remove_file(op: FsOp) -> io::Result<()> {
    let FsOp::RemoveFile { path } = op else {
        unreachable!("remove_file backend called with non-remove_file op");
    };

    submit_unlink(path, 0).await
}

pub async fn remove_dir(op: FsOp) -> io::Result<()> {
    let FsOp::RemoveDir { path } = op else {
        unreachable!("remove_dir backend called with non-remove_dir op");
    };

    submit_unlink(path, libc::AT_REMOVEDIR).await
}

pub async fn rename(op: FsOp) -> io::Result<()> {
    let FsOp::Rename { from, to } = op else {
        unreachable!("rename backend called with non-rename op");
    };

    let from = path_to_c_string(&from)?;
    let to = path_to_c_string(&to)?;
    let from_ptr = from.as_ptr();
    let to_ptr = to.as_ptr();
    submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_RENAMEAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = from_ptr as u64;
            sqe.len = libc::AT_FDCWD as u32;
            sqe.off = to_ptr as u64;
            sqe.op_flags = 0;
        },
        move |cqe| {
            let _from = from;
            let _to = to;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
}

#[allow(dead_code)]
pub async fn close(op: FsOp) -> io::Result<()> {
    let FsOp::Close { fd } = op else {
        unreachable!("close backend called with non-close op");
    };

    submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_CLOSE;
            sqe.fd = fd;
        },
        move |cqe| cqe_to_result(cqe).map(|_| ()),
    )
    .await
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

        if let Err(error) =
            crate::sys::blocking::spawn_blocking(move || produce_dir_entries(path, producer))
        {
            // Spawn failed: the producer thread will never call finish(), so
            // pending_ops would leak without an explicit decrement here.
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
                // Directory stream wakeups originate from completion context; do
                // not block or retry on overflow.
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
        }
        Err(error) => state.push(Err(error)),
    }

    state.finish();
}

async fn submit_sync(fd: RawFd, flags: u32) -> io::Result<()> {
    submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_FSYNC;
            sqe.fd = fd;
            sqe.op_flags = flags;
        },
        move |cqe| cqe_to_result(cqe).map(|_| ()),
    )
    .await
}

async fn submit_unlink(path: PathBuf, flags: i32) -> io::Result<()> {
    let path = path_to_c_string(&path)?;
    let path_ptr = path.as_ptr();
    submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_UNLINKAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = path_ptr as u64;
            sqe.op_flags = flags as u32;
        },
        move |cqe| {
            let _path = path;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
}

async fn submit_uring<T: Send + 'static, M>(
    fill: impl FnOnce(&mut crate::platform::linux::uring::IoUringSqe),
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    submit_uring_guarded(fill, Box::new(()), map).await
}

async fn submit_uring_guarded<T: Send + 'static, M>(
    fill: impl FnOnce(&mut crate::platform::linux::uring::IoUringSqe),
    guard: Box<dyn std::any::Any + Send + 'static>,
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let callback_handle = handle.clone();
    let token = with_current_driver(|driver| {
        driver.submit_operation(fill, move |cqe| {
            callback_handle.complete(map(cqe));
        })
    })?;

    handle.set_cancel(move || {
        let _ =
            with_current_driver(|driver| driver.cancel_operation_with_guard(token, Some(guard)));
    });

    future.await
}

fn path_to_c_string(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "paths containing NUL bytes are not supported",
        )
    })
}

fn open_flags(options: &OpenOptions) -> io::Result<(i32, u32)> {
    let access = access_mode(options)?;
    let creation = creation_mode(options)?;
    Ok((access | creation | libc::O_CLOEXEC, 0o666))
}

/// Resolves the access-mode flags, mirroring `std::fs::OpenOptions` so that
/// invalid access combinations fail with `EINVAL` on Linux exactly as they do
/// on the std-backed macOS path.
fn access_mode(options: &OpenOptions) -> io::Result<i32> {
    match (options.read, options.write, options.append) {
        (true, false, false) => Ok(libc::O_RDONLY),
        (false, true, false) => Ok(libc::O_WRONLY),
        (true, true, false) => Ok(libc::O_RDWR),
        (false, _, true) => Ok(libc::O_WRONLY | libc::O_APPEND),
        (true, _, true) => Ok(libc::O_RDWR | libc::O_APPEND),
        (false, false, false) => Err(io::Error::from_raw_os_error(libc::EINVAL)),
    }
}

/// Resolves the creation-mode flags, mirroring `std::fs::OpenOptions`. In
/// particular `truncate`/`create`/`create_new` without write access — e.g.
/// `read(true).truncate(true)` — is rejected with `EINVAL` rather than being
/// silently turned into `O_RDONLY | O_TRUNC`, which would truncate the file.
fn creation_mode(options: &OpenOptions) -> io::Result<i32> {
    match (options.write, options.append) {
        (true, false) => {}
        (false, false) => {
            if options.truncate || options.create || options.create_new {
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            }
        }
        (_, true) => {
            if options.truncate && !options.create_new {
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            }
        }
    }

    Ok(match (options.create, options.truncate, options.create_new) {
        (false, false, false) => 0,
        (true, false, false) => libc::O_CREAT,
        (false, true, false) => libc::O_TRUNC,
        (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
        (_, _, true) => libc::O_CREAT | libc::O_EXCL,
    })
}

fn metadata_flags(follow_symlinks: bool) -> i32 {
    let mut flags = libc::AT_NO_AUTOMOUNT;
    if !follow_symlinks {
        flags |= libc::AT_SYMLINK_NOFOLLOW;
    }
    flags
}

fn raw_metadata_from_statx(statx: &libc::statx) -> RawMetadata {
    RawMetadata {
        file_type: file_type_from_mode(statx.stx_mode),
        mode: statx.stx_mode,
        len: statx.stx_size,
    }
}

fn file_type_from_mode(mode: u16) -> FileType {
    match mode & libc::S_IFMT as u16 {
        value if value == libc::S_IFREG as u16 => FileType::File,
        value if value == libc::S_IFDIR as u16 => FileType::Directory,
        value if value == libc::S_IFLNK as u16 => FileType::Symlink,
        value if value == libc::S_IFBLK as u16 => FileType::BlockDevice,
        value if value == libc::S_IFCHR as u16 => FileType::CharacterDevice,
        value if value == libc::S_IFIFO as u16 => FileType::Fifo,
        value if value == libc::S_IFSOCK as u16 => FileType::Socket,
        _ => FileType::Unknown,
    }
}

fn cqe_to_result(cqe: IoUringCqe) -> io::Result<i32> {
    if cqe.res < 0 {
        Err(io::Error::from_raw_os_error(-cqe.res))
    } else {
        Ok(cqe.res)
    }
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

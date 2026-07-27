//! Linux filesystem backend.

use std::ffi::{CStr, CString};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

pub(crate) use crate::fs::ReadDirStream;
use crate::op::completion::local_completion_for_current_thread;
use crate::op::fs::{FileType, FsOp, MetadataTarget, OpenOptions, RawMetadata};
use crate::platform::linux::runtime::{
    cancel_operation_on_owner, current_thread_handle, with_current_driver,
};
use crate::platform::linux::uring::{
    IORING_FSYNC_DATASYNC, IORING_OP_FSYNC, IORING_OP_FTRUNCATE, IORING_OP_MKDIRAT,
    IORING_OP_OPENAT, IORING_OP_READ, IORING_OP_RENAMEAT, IORING_OP_STATX, IORING_OP_UNLINKAT,
    IORING_OP_WRITE, IoUringCqe, is_unsupported_operation,
};

const STATX_BASIC_MASK: u32 =
    libc::STATX_TYPE | libc::STATX_MODE | libc::STATX_SIZE | libc::STATX_NLINK;
const FILE_CURSOR: u64 = u64::MAX;

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

    let mut buffer = Vec::with_capacity(len);
    let buffer_ptr = buffer.as_mut_ptr();
    let buffer_len = buffer.capacity();
    submit_uring::<Vec<u8>, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_READ;
            sqe.fd = fd;
            sqe.addr = buffer_ptr as u64;
            sqe.len = buffer_len as u32;
            sqe.off = offset.unwrap_or(FILE_CURSOR);
        },
        move |cqe| {
            let read = cqe_to_result(cqe)? as usize;
            if read > buffer.capacity() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "io_uring read exceeded the submitted buffer length",
                ));
            }
            // SAFETY: a successful read CQE initialized exactly `read` bytes in
            // the vector's spare capacity, and the bound above keeps them
            // within the allocation.
            unsafe {
                buffer.set_len(read);
            }
            Ok(buffer)
        },
    )
    .await
}

pub async fn write(op: FsOp) -> io::Result<usize> {
    let FsOp::Write { fd, offset, data } = op else {
        unreachable!("write backend called with non-write op");
    };
    let data_ptr = data.as_ptr();
    let data_len = data.len();

    submit_uring::<usize, _>(
        move |sqe| {
            sqe.opcode = IORING_OP_WRITE;
            sqe.fd = fd;
            sqe.addr = data_ptr as u64;
            sqe.len = data_len as u32;
            sqe.off = offset.unwrap_or(FILE_CURSOR);
        },
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
        // update on ordinary local files, but it can block on network/FUSE
        // filesystems. Retain an owned duplicate across the blocking job so a
        // canceled caller cannot redirect the syscall through fd reuse.
        Err(error) if fs_should_fallback(&error) => {
            let file = duplicate_fd(fd)?;
            crate::task::spawn_blocking(move || set_len_sync(file.as_raw_fd(), len))?
                .await
                .map_err(|error| io::Error::other(error.to_string()))?
        }
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
    is_unsupported_operation(error)
}

/// Repositions the file's kernel cursor. `lseek(2)` on a regular file is a fast
/// metadata operation that does not block, so it runs inline on the event loop.
pub fn seek(fd: RawFd, pos: std::io::SeekFrom) -> io::Result<u64> {
    let (whence, offset) = match pos {
        std::io::SeekFrom::Start(n) => (libc::SEEK_SET, n as libc::off_t),
        std::io::SeekFrom::End(n) => (libc::SEEK_END, n as libc::off_t),
        std::io::SeekFrom::Current(n) => (libc::SEEK_CUR, n as libc::off_t),
    };
    // SAFETY: `lseek` takes only a descriptor and integer arguments.
    let result = unsafe { libc::lseek(fd, offset, whence) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as u64)
    }
}

pub async fn try_clone(op: FsOp) -> io::Result<OwnedFd> {
    let FsOp::Duplicate { fd } = op else {
        unreachable!("try_clone backend called with non-duplicate op");
    };

    duplicate_fd(fd)
}

fn duplicate_fd(fd: RawFd) -> io::Result<OwnedFd> {
    // `fcntl(F_DUPFD_CLOEXEC)` never blocks, so run it inline.
    let duplicated = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
    // SAFETY: `duplicated` is fresh and ownership transfers exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

pub async fn create_dir(op: FsOp) -> io::Result<()> {
    let FsOp::CreateDir { path, mode } = op else {
        unreachable!("create_dir backend called with non-create_dir op");
    };

    let c_path = path_to_c_string(&path)?;
    let path_ptr = c_path.as_ptr();
    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_MKDIRAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = path_ptr as u64;
            sqe.len = mode;
        },
        move |cqe| {
            let _path = c_path;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
    {
        // IORING_OP_MKDIRAT requires Linux 5.15. Older kernels fall back to
        // mkdirat(2) on the blocking pool; the path is owned by the job, so a
        // cancelled caller cannot invalidate it.
        Err(error) if fs_should_fallback(&error) => {
            let c_path = path_to_c_string(&path)?;
            crate::task::spawn_blocking(move || create_dir_sync(&c_path, mode))?
                .await
                .map_err(|error| io::Error::other(error.to_string()))?
        }
        result => result,
    }
}

fn create_dir_sync(path: &CStr, mode: u32) -> io::Result<()> {
    // SAFETY: `path` is a valid NUL-terminated C string for the call's duration.
    cvt(unsafe { libc::mkdirat(libc::AT_FDCWD, path.as_ptr(), mode as libc::mode_t) }).map(|_| ())
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

    let c_from = path_to_c_string(&from)?;
    let c_to = path_to_c_string(&to)?;
    let from_ptr = c_from.as_ptr();
    let to_ptr = c_to.as_ptr();
    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_RENAMEAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = from_ptr as u64;
            sqe.len = libc::AT_FDCWD as u32;
            sqe.off = to_ptr as u64;
            sqe.op_flags = 0;
        },
        move |cqe| {
            let _from = c_from;
            let _to = c_to;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
    {
        // IORING_OP_RENAMEAT requires Linux 5.11. Older kernels fall back to
        // renameat(2) on the blocking pool.
        Err(error) if fs_should_fallback(&error) => {
            let c_from = path_to_c_string(&from)?;
            let c_to = path_to_c_string(&to)?;
            crate::task::spawn_blocking(move || rename_sync(&c_from, &c_to))?
                .await
                .map_err(|error| io::Error::other(error.to_string()))?
        }
        result => result,
    }
}

fn rename_sync(from: &CStr, to: &CStr) -> io::Result<()> {
    // SAFETY: both paths are valid NUL-terminated C strings for the call.
    cvt(unsafe { libc::renameat(libc::AT_FDCWD, from.as_ptr(), libc::AT_FDCWD, to.as_ptr()) })
        .map(|_| ())
}

pub(crate) fn read_dir(op: FsOp) -> io::Result<ReadDirStream> {
    let FsOp::ReadDir { path } = op else {
        unreachable!("read_dir backend called with non-read_dir op");
    };

    ReadDirStream::new(path)
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
    let c_path = path_to_c_string(&path)?;
    let path_ptr = c_path.as_ptr();
    match submit_uring::<(), _>(
        move |sqe| {
            sqe.opcode = IORING_OP_UNLINKAT;
            sqe.fd = libc::AT_FDCWD;
            sqe.addr = path_ptr as u64;
            sqe.op_flags = flags as u32;
        },
        move |cqe| {
            let _path = c_path;
            cqe_to_result(cqe).map(|_| ())
        },
    )
    .await
    {
        // IORING_OP_UNLINKAT requires Linux 5.11. Older kernels fall back to
        // unlinkat(2) on the blocking pool.
        Err(error) if fs_should_fallback(&error) => {
            let c_path = path_to_c_string(&path)?;
            crate::task::spawn_blocking(move || unlink_sync(&c_path, flags))?
                .await
                .map_err(|error| io::Error::other(error.to_string()))?
        }
        result => result,
    }
}

fn unlink_sync(path: &CStr, flags: i32) -> io::Result<()> {
    // SAFETY: `path` is a valid NUL-terminated C string for the call's duration.
    cvt(unsafe { libc::unlinkat(libc::AT_FDCWD, path.as_ptr(), flags) }).map(|_| ())
}

async fn submit_uring<T: Send + 'static, M>(
    fill: impl FnOnce(&mut crate::platform::linux::uring::IoUringSqe),
    map: M,
) -> io::Result<T>
where
    M: FnOnce(IoUringCqe) -> io::Result<T> + Send + 'static,
{
    let owner = current_thread_handle();
    let (future, handle) = local_completion_for_current_thread::<io::Result<T>>();
    let callback_handle = handle.clone();
    let token = with_current_driver(|driver| {
        driver.submit_operation(fill, move |cqe| {
            callback_handle.complete(map(cqe));
        })
    })?;

    handle.set_cancel(move || {
        cancel_operation_on_owner(owner, token, None);
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

    Ok(
        match (options.create, options.truncate, options.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        },
    )
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
        mode: u32::from(statx.stx_mode),
        len: statx.stx_size,
        platform: Default::default(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::linux::uring::{
        IORING_OP_FTRUNCATE, SupportedOps, override_supported_ops,
    };
    use crate::platform::runtime_shared::test_support::ExecutionGate;
    use crate::sys::blocking::install_task_hook;
    use crate::{run, spawn};
    use std::fs::File;
    use std::os::fd::AsRawFd;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[test]
    fn capability_matrix_set_len_falls_back_without_ftruncate() {
        let _override = override_supported_ops(SupportedOps::all_except([IORING_OP_FTRUNCATE]));
        let name = CString::new("runite-set-len-test").expect("name should be valid");
        // SAFETY: memfd_create reads the NUL-terminated name and returns a new
        // descriptor on success.
        let raw =
            unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), libc::MFD_CLOEXEC) };
        assert!(
            raw >= 0,
            "memfd_create failed: {}",
            io::Error::last_os_error()
        );
        // SAFETY: `raw` is the fresh descriptor returned by memfd_create.
        let file = unsafe { OwnedFd::from_raw_fd(raw as RawFd) };
        let completed = Arc::new(AtomicBool::new(false));
        let completed_task = Arc::clone(&completed);

        spawn(async move {
            set_len(FsOp::SetLen {
                fd: file.as_raw_fd(),
                len: 4096,
            })
            .await
            .expect("ftruncate syscall fallback should succeed");

            let mut stat = MaybeUninit::<libc::stat>::uninit();
            // SAFETY: fstat writes one initialized `stat` for the live memfd.
            assert_eq!(
                unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) },
                0
            );
            // SAFETY: successful fstat initialized `stat`.
            assert_eq!(unsafe { stat.assume_init() }.st_size, 4096);
            completed_task.store(true, Ordering::Release);
        });
        run();

        assert!(completed.load(Ordering::Acquire));
    }

    /// The documented hard floor is Linux 5.6, but `MKDIRAT` (5.15),
    /// `RENAMEAT` (5.11), and `UNLINKAT` (5.11) are all newer. Without a
    /// syscall fallback these directory operations fail outright on a kernel
    /// that meets the documented floor.
    #[test]
    fn capability_matrix_directory_ops_fall_back_without_newer_opcodes() {
        let _override = override_supported_ops(SupportedOps::all_except([
            IORING_OP_MKDIRAT,
            IORING_OP_RENAMEAT,
            IORING_OP_UNLINKAT,
        ]));

        let root = std::env::temp_dir().join(format!(
            "runite-dirops-fallback-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let created = root.join("created");
        let renamed = root.join("renamed");
        std::fs::create_dir(&root).expect("test root should be creatable");

        let completed = Arc::new(AtomicBool::new(false));
        let completed_task = Arc::clone(&completed);
        let (created_path, renamed_path) = (created.clone(), renamed.clone());

        spawn(async move {
            create_dir(FsOp::CreateDir {
                path: created_path.clone(),
                mode: 0o777,
            })
            .await
            .expect("mkdirat syscall fallback should succeed");
            assert!(created_path.is_dir());

            rename(FsOp::Rename {
                from: created_path.clone(),
                to: renamed_path.clone(),
            })
            .await
            .expect("renameat syscall fallback should succeed");
            assert!(!created_path.exists() && renamed_path.is_dir());

            remove_dir(FsOp::RemoveDir {
                path: renamed_path.clone(),
            })
            .await
            .expect("unlinkat syscall fallback should succeed");
            assert!(!renamed_path.exists());

            completed_task.store(true, Ordering::Release);
        });
        run();

        assert!(completed.load(Ordering::Acquire));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn set_len_fallback_owns_fd_across_blocking_queue_delay() {
        let name = CString::new("runite-set-len-fd-race").expect("name should be valid");
        // SAFETY: memfd_create reads the NUL-terminated name and returns a new
        // descriptor on success.
        let raw =
            unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), libc::MFD_CLOEXEC) };
        assert!(
            raw >= 0,
            "memfd_create failed: {}",
            io::Error::last_os_error()
        );
        // SAFETY: `raw` is the fresh descriptor returned by memfd_create.
        let original = unsafe { OwnedFd::from_raw_fd(raw as RawFd) };
        let original_raw = original.as_raw_fd();
        let verifier = duplicate_fd(original_raw).expect("verification duplicate should open");
        let gate = ExecutionGate::default();
        let gate_runtime = gate.clone();
        let (result_tx, result_rx) = std::sync::mpsc::sync_channel(1);

        let runtime = std::thread::spawn(move || {
            let _ops = override_supported_ops(SupportedOps::all_except([IORING_OP_FTRUNCATE]));
            spawn(async move {
                let _hook = install_task_hook(Arc::new(gate_runtime));
                let result = set_len(FsOp::SetLen {
                    fd: original_raw,
                    len: 8192,
                })
                .await;
                result_tx.send(result).expect("test should receive result");
            });
            run();
        });

        let release = gate.release_on_drop();
        assert!(
            gate.wait_until_arrived(Duration::from_secs(2)),
            "blocking fallback should reach the execution gate"
        );
        let replacement = File::open("/dev/null").expect("replacement should open");
        // SAFETY: dup2 atomically replaces the descriptor while `original`
        // remains its sole Rust owner.
        assert_eq!(
            unsafe { libc::dup2(replacement.as_raw_fd(), original_raw) },
            original_raw
        );
        let reused = original;
        release.release();

        result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("fallback should finish")
            .expect("ftruncate should target the retained memfd");
        runtime.join().expect("runtime thread should exit");

        let mut stat = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fstat writes one stat for the live verifier.
        assert_eq!(
            unsafe { libc::fstat(verifier.as_raw_fd(), stat.as_mut_ptr()) },
            0
        );
        // SAFETY: successful fstat initialized stat.
        assert_eq!(unsafe { stat.assume_init() }.st_size, 8192);
        drop(reused);
    }
}

//! macOS filesystem backend.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;

pub(crate) use crate::fs::ReadDirStream;
use crate::op::completion::completion_for_current_thread;
use crate::op::fs::{FileType, FsOp, MetadataTarget, RawMetadata};
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

    offload(read_job(fd, offset, len)?).await
}

fn read_job(
    fd: RawFd,
    offset: Option<u64>,
    len: usize,
) -> io::Result<impl FnOnce() -> io::Result<Vec<u8>> + Send + 'static> {
    let fd = duplicate_fd(fd)?;
    Ok(move || read_owned(fd, offset, len))
}

fn read_owned(fd: OwnedFd, offset: Option<u64>, len: usize) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0; len];
    let read = match offset {
        Some(offset) => unsafe {
            libc::pread(
                fd.as_raw_fd(),
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                len,
                offset as libc::off_t,
            )
        },
        None => unsafe {
            libc::read(
                fd.as_raw_fd(),
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                len,
            )
        },
    };
    if read < 0 {
        return Err(io::Error::last_os_error());
    }
    buffer.truncate(read as usize);
    Ok(buffer)
}

pub async fn write(op: FsOp) -> io::Result<usize> {
    let FsOp::Write { fd, offset, data } = op else {
        unreachable!("write backend called with non-write op");
    };
    let fd = duplicate_fd(fd)?;

    offload(move || {
        let written = match offset {
            Some(offset) => unsafe {
                libc::pwrite(
                    fd.as_raw_fd(),
                    data.as_ptr().cast::<libc::c_void>(),
                    data.len(),
                    offset as libc::off_t,
                )
            },
            None => unsafe {
                libc::write(
                    fd.as_raw_fd(),
                    data.as_ptr().cast::<libc::c_void>(),
                    data.len(),
                )
            },
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

    match target {
        MetadataTarget::Path(path) => {
            offload(move || {
                if follow_symlinks {
                    std::fs::metadata(path)
                } else {
                    std::fs::symlink_metadata(path)
                }
                .map(|metadata| raw_metadata_from_std(&metadata))
            })
            .await
        }
        MetadataTarget::File(fd) => {
            let fd = duplicate_fd(fd)?;
            offload(move || {
                let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
                let result = unsafe { libc::fstat(fd.as_raw_fd(), &mut stat) };
                if result < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(raw_metadata_from_stat(&stat))
            })
            .await
        }
    }
}

/// Durably flushes a file to disk on macOS.
///
/// Plain `fsync(2)` on macOS does **not** flush the drive's write cache, so it
/// is not a real durability barrier; `fcntl(F_FULLFSYNC)` is. `std::fs` uses
/// `F_FULLFSYNC` for both `sync_all` and `sync_data` on Apple targets for this
/// reason, falling back to `fsync` on filesystems (e.g. some network mounts)
/// that do not support it. We match that behavior.
fn full_fsync(fd: RawFd) -> io::Result<()> {
    match cvt(unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) }) {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOTSUP) | Some(libc::EINVAL) | Some(libc::ENOTTY)
            ) =>
        {
            cvt(unsafe { libc::fsync(fd) }).map(|_| ())
        }
        Err(error) => Err(error),
    }
}

pub async fn sync_all(op: FsOp) -> io::Result<()> {
    let FsOp::SyncAll { fd } = op else {
        unreachable!("sync_all backend called with non-sync_all op");
    };
    let fd = duplicate_fd(fd)?;

    offload(move || full_fsync(fd.as_raw_fd())).await
}

pub async fn sync_data(op: FsOp) -> io::Result<()> {
    let FsOp::SyncData { fd } = op else {
        unreachable!("sync_data backend called with non-sync_data op");
    };
    let fd = duplicate_fd(fd)?;

    offload(move || full_fsync(fd.as_raw_fd())).await
}

pub async fn set_len(op: FsOp) -> io::Result<()> {
    let FsOp::SetLen { fd, len } = op else {
        unreachable!("set_len backend called with non-set_len op");
    };
    let fd = duplicate_fd(fd)?;

    offload(move || cvt(unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) }).map(|_| ()))
        .await
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

pub(crate) fn read_dir(op: FsOp) -> io::Result<ReadDirStream> {
    let FsOp::ReadDir { path } = op else {
        unreachable!("read_dir backend called with non-read_dir op");
    };

    ReadDirStream::new(path)
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

fn duplicate_fd(fd: RawFd) -> io::Result<OwnedFd> {
    let duplicated = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
    // SAFETY: F_DUPFD_CLOEXEC returned a fresh descriptor and ownership is
    // transferred to this wrapper exactly once.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
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
        // Full st_mode (type + permission bits), matching std and the Linux
        // backend rather than masking to permission bits only.
        mode: metadata.mode(),
        len: metadata.len(),
        platform: Default::default(),
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
        mode: u32::from(stat.st_mode),
        len: stat.st_size as u64,
        platform: Default::default(),
    }
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn blocking_read_owns_fd_across_drop_and_reuse() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let dir = std::env::current_dir()
            .expect("repository directory")
            .join("target")
            .join("macos-fd-reuse-tests");
        std::fs::create_dir_all(&dir).expect("create fixture directory");
        let original_path = dir.join(format!("original-{}-{unique}", std::process::id()));
        let replacement_path = dir.join(format!("replacement-{}-{unique}", std::process::id()));
        std::fs::write(&original_path, b"original").expect("write original fixture");
        std::fs::write(&replacement_path, b"replacement").expect("write replacement fixture");

        let original = std::fs::File::open(&original_path).expect("open original");
        let replacement = std::fs::File::open(&replacement_path).expect("open replacement");
        let original_fd = original.as_raw_fd();
        let job = read_job(original_fd, Some(0), 8).expect("prepare blocking read");
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = Arc::clone(&gate);
        let worker = std::thread::spawn(move || {
            let (released, changed) = &*worker_gate;
            let mut released = released.lock().expect("gate poisoned");
            while !*released {
                released = changed.wait(released).expect("gate poisoned");
            }
            job()
        });

        drop(original);
        let reused = unsafe { libc::dup2(replacement.as_raw_fd(), original_fd) };
        assert_eq!(
            reused,
            original_fd,
            "dup2 failed to reuse descriptor: {}",
            io::Error::last_os_error()
        );
        // SAFETY: dup2 created a fresh descriptor at original_fd and this
        // wrapper takes ownership of it exactly once.
        let reused = unsafe { OwnedFd::from_raw_fd(reused) };
        let (released, changed) = &*gate;
        *released.lock().expect("gate poisoned") = true;
        changed.notify_one();

        let bytes = worker
            .join()
            .expect("blocking worker should not panic")
            .expect("blocking read");
        assert_eq!(bytes, b"original");

        drop(reused);
        drop(replacement);
        std::fs::remove_file(original_path).expect("remove original fixture");
        std::fs::remove_file(replacement_path).expect("remove replacement fixture");
    }
}

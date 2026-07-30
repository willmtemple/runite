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

use std::io;
use std::mem::ManuallyDrop;
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

use windows_sys::Win32::Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_BEGIN,
    FILE_CURRENT, FILE_END, FILE_FLAG_OVERLAPPED, SetFilePointerEx,
};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

pub(crate) use crate::fs::ReadDirStream;
use crate::op::completion::completion_for_current_thread;
use crate::op::fs::{FileType, FsOp, MetadataTarget, RawMetadata};
use crate::sys::blocking::spawn_blocking;
use crate::sys::handle::{OwnedFile, PlatformMetadata, RawFile};
use crate::sys::windows::overlapped;

/// Closes an owned descriptor.
///
/// Windows has no asynchronous close, so this releases the handle and reports
/// success. `CloseHandle` fails only for an invalid or close-protected handle,
/// both of which are programmer error rather than I/O error, so there is
/// nothing meaningful to surface.
pub(crate) async fn close(fd: crate::sys::handle::OwnedFile) -> io::Result<()> {
    drop(fd);
    Ok(())
}

pub async fn open(op: FsOp) -> io::Result<OwnedFile> {
    let FsOp::Open { path, options } = op else {
        unreachable!("open backend called with non-open op");
    };

    let handle = offload(move || {
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
    adopt_handle(handle)
}

pub async fn read(op: FsOp) -> io::Result<Vec<u8>> {
    let FsOp::Read { fd, offset, len } = op else {
        unreachable!("read backend called with non-read op");
    };

    match offset {
        Some(offset) => overlapped::read_at(fd, len, offset).await,
        None => {
            fd.ensure_current()?;
            let position = seek(fd.clone(), std::io::SeekFrom::Current(0))?;
            let data = overlapped::read_at(fd.clone(), len, position).await?;
            advance_cursor(fd, position, data.len() as u64)?;
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
            fd.ensure_current()?;
            let position = seek(fd.clone(), std::io::SeekFrom::Current(0))?;
            let written = overlapped::write_at(fd.clone(), data, position).await?;
            advance_cursor(fd, position, written as u64)?;
            Ok(written)
        }
    }
}

/// Moves the shared file cursor past a completed cursor-based operation.
///
fn advance_cursor(fd: RawFile, position: u64, transferred: u64) -> io::Result<()> {
    seek(
        fd,
        std::io::SeekFrom::Start(position.saturating_add(transferred)),
    )
    .map(|_| ())
}

pub async fn metadata(op: FsOp) -> io::Result<RawMetadata> {
    let FsOp::Metadata {
        target,
        follow_symlinks,
    } = op
    else {
        unreachable!("metadata backend called with non-metadata op");
    };

    if let MetadataTarget::File(fd) = &target {
        fd.ensure_current()?;
    }

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
                let file = borrow_file(&fd);
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

    fd.ensure_current()?;
    offload(move || borrow_file(&fd).sync_all()).await
}

pub async fn sync_data(op: FsOp) -> io::Result<()> {
    let FsOp::SyncData { fd } = op else {
        unreachable!("sync_data backend called with non-sync_data op");
    };

    fd.ensure_current()?;
    offload(move || borrow_file(&fd).sync_data()).await
}

pub async fn set_len(op: FsOp) -> io::Result<()> {
    let FsOp::SetLen { fd, len } = op else {
        unreachable!("set_len backend called with non-set_len op");
    };

    fd.ensure_current()?;
    offload(move || borrow_file(&fd).set_len(len)).await
}

/// Repositions the file's kernel cursor. `SetFilePointerEx` is a fast
/// metadata operation that does not block, so it runs inline on the event
/// loop.
pub fn seek(fd: RawFile, pos: std::io::SeekFrom) -> io::Result<u64> {
    fd.ensure_current()?;
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

    let affinity = fd.affinity()?;

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
    let handle = unsafe { OwnedHandle::from_raw_handle(duplicated) };

    // A duplicate shares the already-associated file object. Propagate the
    // proven affinity instead of treating every ERROR_INVALID_PARAMETER as a
    // successful association (which would also accept a foreign IOCP).
    Ok(OwnedFile::bound(handle, affinity))
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

/// Borrows a raw handle as a [`std::fs::File`] without taking ownership, so
/// std's handle-based metadata/flush/truncate wrappers can be reused. The
/// `ManuallyDrop` prevents the borrowed handle from being closed.
fn borrow_file(fd: &RawFile) -> ManuallyDrop<std::fs::File> {
    // SAFETY: `fd` names a handle the caller keeps open for the duration of
    // the blocking operation; `ManuallyDrop` ensures ownership never
    // transfers.
    ManuallyDrop::new(unsafe { std::fs::File::from_raw_handle(fd.as_handle()) })
}

pub(crate) fn adopt_handle(handle: OwnedHandle) -> io::Result<OwnedFile> {
    let affinity = overlapped::associate_raw_handle(handle.as_raw_handle())?;
    Ok(OwnedFile::bound(handle, affinity))
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

#[cfg(test)]
mod tests {
    use std::os::windows::io::OwnedHandle;
    use std::sync::Arc;
    use std::time::Duration;

    use super::borrow_file;
    use crate::platform::runtime_shared::test_support::ExecutionGate;
    use crate::sys::blocking::{install_task_hook, spawn_blocking};
    use crate::sys::handle::{OwnedFile, raw_file};

    #[test]
    fn accepted_blocking_job_owns_its_handle() {
        let path = std::env::current_dir()
            .expect("current directory")
            .join("target")
            .join(format!("windows-owned-blocking-{}", std::process::id()));
        std::fs::write(&path, b"owned").expect("write fixture");

        let file = std::fs::File::open(&path).expect("open fixture");
        let owner = OwnedFile::unbound(OwnedHandle::from(file));
        let operation = raw_file(&owner);
        let gate = Arc::new(ExecutionGate::default());
        let release = gate.release_on_drop();
        let hook = install_task_hook(Arc::clone(&gate) as Arc<_>);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);

        spawn_blocking(move || {
            let result = borrow_file(&operation)
                .metadata()
                .map(|metadata| metadata.len());
            sender.send(result).expect("send blocking result");
        })
        .expect("submit blocking job");
        assert!(
            gate.wait_until_arrived(Duration::from_secs(5)),
            "blocking job did not reach execution gate"
        );

        drop(owner);
        drop(hook);
        release.release();

        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("receive blocking result")
                .expect("metadata through retained handle"),
            5
        );
        assert!(
            gate.wait_until_completed(Duration::from_secs(5)),
            "blocking job did not complete"
        );
        std::fs::remove_file(path).expect("remove fixture");
    }
}

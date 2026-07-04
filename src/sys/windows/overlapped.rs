//! Overlapped-operation submission machinery.
//!
//! Every asynchronous Windows operation follows the same protocol (see
//! `docs/WINDOWS.md`):
//!
//! 1. Heap-allocate one packet context: `OVERLAPPED` header + owned buffers +
//!    the [`CompletionHandle`]. The allocation is leaked into the kernel at
//!    submit time.
//! 2. Call the Win32 submission function. Synchronous *failure* means no
//!    completion packet will arrive — reclaim the box and surface the error
//!    inline. Synchronous *success* still posts a packet (skip-on-success is
//!    not enabled), so it is treated exactly like a pending submission.
//! 3. The driver dequeues the packet, reads the `NTSTATUS` from
//!    `OVERLAPPED.Internal`, and runs the completion thunk, which reconstructs
//!    the box, maps the raw result, and resolves the completion. The buffers
//!    die with the box — after the packet, never before, which is what makes
//!    the runtime-owned staging buffer model sound.
//! 4. Dropping the future runs a cancel callback that issues
//!    `CancelIoEx(handle, overlapped)`; the operation then completes with
//!    `ERROR_OPERATION_ABORTED` and its packet reclaims the context as usual.

use std::io;

use windows_sys::Win32::Foundation::{ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF, HANDLE};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::{CancelIoEx, OVERLAPPED};

use crate::op::completion::{CompletionHandle, completion_for_current_thread};
use crate::platform::current::runtime::with_current_driver;
use crate::platform::windows::driver::{OverlappedHeader, OverlappedResult};
use crate::sys::handle::RawFile;

/// One in-flight overlapped operation: the driver-visible header followed by
/// the operation's owned state and result mapper.
#[repr(C)]
struct OverlappedOp<D, M, T> {
    header: OverlappedHeader,
    data: D,
    map: Option<M>,
    handle: CompletionHandle<io::Result<T>>,
}

/// Monomorphized completion thunk stored in the packet header.
///
/// # Safety
///
/// `ptr` must be the `Box::into_raw` pointer of an `OverlappedOp<D, M, T>`,
/// and this must be its only invocation.
unsafe fn complete_thunk<D, M, T>(ptr: *mut OverlappedHeader, result: OverlappedResult)
where
    M: FnOnce(D, OverlappedResult) -> io::Result<T>,
    T: Send + 'static,
{
    // SAFETY: forwarded contract — `ptr` is the unique leaked pointer to a
    // live `OverlappedOp<D, M, T>` whose header sits at offset 0.
    let mut op = unsafe { Box::from_raw(ptr.cast::<OverlappedOp<D, M, T>>()) };
    let map = op
        .map
        .take()
        .expect("overlapped completion must run exactly once");
    let value = map(op.data, result);
    op.handle.complete(value);
}

/// Submits one overlapped operation and awaits its completion packet.
///
/// * `cancel_handle` — the OS handle the operation runs on, used only for
///   `CancelIoEx` when the future is dropped. The caller must guarantee the
///   handle outlives the returned future (the public wrappers do: their
///   pending-operation futures never outlive the owning handle object).
/// * `data` — operation-owned state (staging buffers, address storage). It is
///   moved into the packet context, so pointers into it that are handed to the
///   kernel stay stable for the life of the operation.
/// * `start` — invokes the Win32 submission call. `Ok(())` means a completion
///   packet *will* arrive (immediately-successful calls still post one);
///   `Err` means submission failed synchronously and no packet will arrive.
/// * `map` — translates the raw completion into the operation's result, with
///   access to the owned state.
pub(crate) async fn submit<D, S, M, T>(
    cancel_handle: RawFile,
    data: D,
    start: S,
    map: M,
) -> io::Result<T>
where
    D: 'static,
    S: FnOnce(&mut D, *mut OVERLAPPED) -> io::Result<()>,
    M: FnOnce(D, OverlappedResult) -> io::Result<T> + 'static,
    T: Send + 'static,
{
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();

    let op = Box::new(OverlappedOp {
        header: OverlappedHeader::new(complete_thunk::<D, M, T>),
        data,
        map: Some(map),
        handle: handle.clone(),
    });
    let ptr = Box::into_raw(op);
    let overlapped = ptr.cast::<OVERLAPPED>();

    // SAFETY: `ptr` was just leaked and is not aliased; the kernel only takes
    // ownership of it once `start` succeeds.
    let started = start(unsafe { &mut (*ptr).data }, overlapped);

    match started {
        Ok(()) => {
            // The packet context now belongs to the kernel until the packet is
            // dispatched. Wire up drop-cancellation; the aborted operation's
            // packet still arrives and reclaims the context.
            let cancel_target = cancel_handle;
            let cancel_overlapped = overlapped as usize;
            handle.set_cancel(move || {
                // SAFETY: dispatch, drop, and cancel all run on the owning
                // runtime thread, so the packet cannot have been freed here:
                // if it had been dispatched, `finished` would be set and this
                // callback would not run. A completed-but-undequeued operation
                // makes this a no-op (`ERROR_NOT_FOUND`).
                unsafe {
                    CancelIoEx(
                        cancel_target.as_handle() as HANDLE,
                        cancel_overlapped as *const OVERLAPPED,
                    );
                }
            });
            future.await
        }
        Err(error) => {
            // No packet will arrive: reclaim the context and fail inline.
            // SAFETY: submission failed, so the kernel never took the pointer;
            // this is its unique reclamation.
            drop(unsafe { Box::from_raw(ptr) });
            handle.complete(Err(error));
            future.await
        }
    }
}

/// Associates a file/pipe handle with the current thread's completion port.
pub(crate) fn associate_file(fd: RawFile) -> io::Result<()> {
    with_current_driver(|driver| driver.associate_handle(fd.as_handle()))
}

/// Associates a file/pipe handle, tolerating handles whose underlying file
/// object is already associated with a completion port.
///
/// Duplicated handles (`try_clone`, `WSADuplicateSocketW`) share the original
/// file object, and a file object can only ever be bound to one port — the
/// association call then fails with `ERROR_INVALID_PARAMETER`. Completions for
/// the duplicate are delivered to the original port, whose completion routing
/// wakes the operation's owner thread either way.
pub(crate) fn associate_file_reused(fd: RawFile) -> io::Result<()> {
    match associate_file(fd) {
        Err(error) if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER_CODE) => Ok(()),
        result => result,
    }
}

const ERROR_INVALID_PARAMETER_CODE: i32 =
    windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER as i32;

/// Overlapped `ReadFile` at an explicit offset (or offset 0 for pipes).
///
/// End-of-stream conditions surface as errors on Windows overlapped reads —
/// `ERROR_HANDLE_EOF` for files past the end, `ERROR_BROKEN_PIPE` for pipes
/// whose writer closed. Both map to the Unix "read returns 0" convention.
pub(crate) async fn read_at(fd: RawFile, len: usize, offset: u64) -> io::Result<Vec<u8>> {
    let buffer = vec![0u8; len.max(1)];
    let result = submit(
        fd,
        buffer,
        |buffer, overlapped| {
            // SAFETY: `overlapped` points at the packet header; the offset
            // union fields are ours to set before submission.
            unsafe {
                (*overlapped).Anonymous.Anonymous.Offset = offset as u32;
                (*overlapped).Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
            }
            // SAFETY: `buffer` lives in the packet context, so it stays valid
            // and unmoved until the completion packet reclaims it.
            let ok = unsafe {
                ReadFile(
                    fd.as_handle(),
                    buffer.as_mut_ptr(),
                    u32::try_from(len).unwrap_or(u32::MAX),
                    std::ptr::null_mut(),
                    overlapped,
                )
            };
            check_overlapped_submission(ok)
        },
        |mut buffer, result| match result.into_result() {
            Ok(read) => {
                buffer.truncate(read);
                Ok(buffer)
            }
            Err(error) => Err(error),
        },
    )
    .await;

    // End-of-stream can surface either from the completion packet or as a
    // synchronous submission failure (e.g. the pipe peer already closed when
    // `ReadFile` was called); both map to the Unix 0-byte convention.
    match result {
        Err(error) if is_end_of_stream(&error) => Ok(Vec::new()),
        other => other,
    }
}

/// Overlapped `WriteFile` at an explicit offset (or offset 0 for pipes).
pub(crate) async fn write_at(fd: RawFile, data: Vec<u8>, offset: u64) -> io::Result<usize> {
    submit(
        fd,
        data,
        |data, overlapped| {
            // SAFETY: as in `read_at`.
            unsafe {
                (*overlapped).Anonymous.Anonymous.Offset = offset as u32;
                (*overlapped).Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
            }
            // SAFETY: `data` lives in the packet context until the packet
            // reclaims it, so the kernel-visible pointer stays valid.
            let ok = unsafe {
                WriteFile(
                    fd.as_handle(),
                    data.as_ptr(),
                    u32::try_from(data.len()).unwrap_or(u32::MAX),
                    std::ptr::null_mut(),
                    overlapped,
                )
            };
            check_overlapped_submission(ok)
        },
        |_data, result| result.into_result(),
    )
    .await
}

/// Maps a `ReadFile`/`WriteFile` return into the submission protocol:
/// `Ok(())` when a completion packet will arrive, `Err` otherwise.
pub(crate) fn check_overlapped_submission(ok: i32) -> io::Result<()> {
    if ok != 0 {
        // Synchronous success still posts a packet on an associated handle.
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_IO_PENDING as i32) {
        Ok(())
    } else {
        Err(error)
    }
}

pub(crate) fn is_end_of_stream(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == ERROR_HANDLE_EOF as i32 || code == ERROR_BROKEN_PIPE as i32
    )
}

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

use std::future::{Future, poll_fn};
use std::io;
use std::task::Poll;
use std::time::Duration;

use windows_sys::Win32::Foundation::{
    ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF, ERROR_OPERATION_ABORTED, HANDLE,
};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::{CancelIoEx, OVERLAPPED};

use crate::op::completion::{CompletionHandle, completion_for_current_thread};
use crate::platform::current::runtime::with_current_driver;
use crate::platform::windows::driver::{OverlappedHeader, OverlappedResult};
use crate::sys::handle::{OverlappedOwner, RawFile};

/// One in-flight overlapped operation: the driver-visible header followed by
/// the operation's owned state and result mapper.
#[repr(C)]
struct OverlappedOp<D, M, T> {
    header: OverlappedHeader,
    _owner: OverlappedOwner,
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
/// * `owner` — an affinity-checked reference to the OS object. The packet
///   retains it through terminal completion and uses the same handle for
///   `CancelIoEx` if the future is dropped.
/// * `data` — operation-owned state (staging buffers, address storage). It is
///   moved into the packet context, so pointers into it that are handed to the
///   kernel stay stable for the life of the operation.
/// * `start` — invokes the Win32 submission call. `Ok(())` means a completion
///   packet *will* arrive (immediately-successful calls still post one);
///   `Err` means submission failed synchronously and no packet will arrive.
/// * `map` — translates the raw completion into the operation's result, with
///   access to the owned state.
pub(crate) async fn submit<D, S, M, T>(
    owner: OverlappedOwner,
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
    submit_inner(owner, data, start, map, None).await
}

pub(crate) async fn submit_with_timeout<D, S, M, T>(
    owner: OverlappedOwner,
    data: D,
    start: S,
    map: M,
    timeout: Duration,
) -> io::Result<T>
where
    D: 'static,
    S: FnOnce(&mut D, *mut OVERLAPPED) -> io::Result<()>,
    M: FnOnce(D, OverlappedResult) -> io::Result<T> + 'static,
    T: Send + 'static,
{
    submit_inner(owner, data, start, map, Some(timeout)).await
}

async fn submit_inner<D, S, M, T>(
    owner: OverlappedOwner,
    data: D,
    start: S,
    map: M,
    timeout: Option<Duration>,
) -> io::Result<T>
where
    D: 'static,
    S: FnOnce(&mut D, *mut OVERLAPPED) -> io::Result<()>,
    M: FnOnce(D, OverlappedResult) -> io::Result<T> + 'static,
    T: Send + 'static,
{
    owner.ensure_current()?;
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();

    let op = Box::new(OverlappedOp {
        header: OverlappedHeader::new(complete_thunk::<D, M, T>),
        _owner: owner.clone(),
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
            let cancel_target = owner;
            let cancel_overlapped = overlapped as usize;
            let cancel_for_drop = cancel_target.clone();
            handle.set_cancel(move || {
                // SAFETY: dispatch, drop, and cancel all run on the owning
                // runtime thread, so the packet cannot have been freed here:
                // if it had been dispatched, `finished` would be set and this
                // callback would not run. A completed-but-undequeued operation
                // makes this a no-op (`ERROR_NOT_FOUND`).
                cancel_operation(&cancel_for_drop, cancel_overlapped as *const OVERLAPPED);
            });
            await_terminal(future, timeout, move || {
                cancel_operation(&cancel_target, cancel_overlapped as *const OVERLAPPED);
            })
            .await
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

async fn await_terminal<T>(
    future: impl Future<Output = io::Result<T>>,
    timeout: Option<Duration>,
    cancel: impl FnOnce(),
) -> io::Result<T> {
    let Some(timeout) = timeout else {
        return future.await;
    };
    await_terminal_after(future, crate::time::sleep(timeout), cancel).await
}

async fn await_terminal_after<T>(
    future: impl Future<Output = io::Result<T>>,
    deadline: impl Future<Output = ()>,
    cancel: impl FnOnce(),
) -> io::Result<T> {
    let mut future = std::pin::pin!(future);
    let mut deadline = std::pin::pin!(deadline);
    let mut cancel = Some(cancel);
    let mut expired = false;

    poll_fn(|cx| {
        // Completion wins when both the IOCP packet and deadline are visible
        // in the same turn.
        if let Poll::Ready(result) = future.as_mut().poll(cx) {
            return Poll::Ready(match result {
                Err(error)
                    if expired && error.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32) =>
                {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "operation timed out",
                    ))
                }
                result => result,
            });
        }

        if !expired && deadline.as_mut().poll(cx).is_ready() {
            expired = true;
            cancel
                .take()
                .expect("deadline cancellation runs at most once")();
        }

        Poll::Pending
    })
    .await
}

fn cancel_operation(owner: &OverlappedOwner, overlapped: *const OVERLAPPED) {
    // SAFETY: `owner` keeps the underlying kernel object alive, and
    // `overlapped` remains allocated until the terminal completion packet.
    unsafe {
        CancelIoEx(owner.as_handle() as HANDLE, overlapped);
    }
}

pub(crate) fn associate_raw_handle(
    handle: std::os::windows::io::RawHandle,
) -> io::Result<crate::platform::windows::driver::DriverId> {
    with_current_driver(|driver| driver.associate_handle(handle))
}

/// Overlapped `ReadFile` at an explicit offset (or offset 0 for pipes).
///
/// End-of-stream conditions surface as errors on Windows overlapped reads —
/// `ERROR_HANDLE_EOF` for files past the end, `ERROR_BROKEN_PIPE` for pipes
/// whose writer closed. Both map to the Unix "read returns 0" convention.
pub(crate) async fn read_at(fd: RawFile, len: usize, offset: u64) -> io::Result<Vec<u8>> {
    let buffer = vec![0u8; len.max(1)];
    let result = submit(
        fd.clone().into(),
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
        fd.clone().into(),
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::{Future, ready};
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};

    use super::{ERROR_OPERATION_ABORTED, await_terminal_after};

    struct Gate<T> {
        state: Rc<RefCell<GateState<T>>>,
    }

    struct GateState<T> {
        value: Option<T>,
        waker: Option<Waker>,
    }

    impl<T> Gate<T> {
        fn new() -> (Self, Rc<RefCell<GateState<T>>>) {
            let state = Rc::new(RefCell::new(GateState {
                value: None,
                waker: None,
            }));
            (
                Self {
                    state: Rc::clone(&state),
                },
                state,
            )
        }
    }

    impl<T> Future for Gate<T> {
        type Output = T;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let mut state = self.state.borrow_mut();
            match state.value.take() {
                Some(value) => Poll::Ready(value),
                None => {
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }

    fn complete<T>(state: &Rc<RefCell<GateState<T>>>, value: T) {
        let waker = {
            let mut state = state.borrow_mut();
            state.value = Some(value);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    #[test]
    fn completion_wins_when_deadline_is_also_ready() {
        let cancelled = Rc::new(std::cell::Cell::new(false));
        let mark_cancelled = Rc::clone(&cancelled);
        let result = crate::block_on(await_terminal_after(
            ready(Ok::<_, std::io::Error>(17)),
            ready(()),
            move || mark_cancelled.set(true),
        ));

        assert_eq!(result.unwrap(), 17);
        assert!(!cancelled.get());
    }

    #[test]
    fn deadline_waits_for_aborted_terminal_result() {
        let (operation, state) = Gate::new();
        let result = crate::block_on(await_terminal_after(operation, ready(()), move || {
            complete(
                &state,
                Err::<usize, _>(std::io::Error::from_raw_os_error(
                    ERROR_OPERATION_ABORTED as i32,
                )),
            );
        }));

        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn successful_completion_after_cancel_request_is_returned() {
        let (operation, state) = Gate::new();
        let result = crate::block_on(await_terminal_after(operation, ready(()), move || {
            complete(&state, Ok::<_, std::io::Error>(23))
        }));

        assert_eq!(result.unwrap(), 23);
    }
}

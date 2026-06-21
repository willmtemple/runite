//! Async standard-input helpers.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use std::sync::{Arc, Mutex};

use crate::io::AsyncRead;
use crate::op::completion::completion_for_current_thread;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use crate::platform::linux_x86_64::runtime::with_current_driver;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use crate::platform::linux_x86_64::uring::{IORING_OP_READ, IoUringCqe, IoUringSqe};
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use std::cell::Cell;

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
thread_local! {
    static STDIN_URING_SUPPORTED: Cell<Option<bool>> = const { Cell::new(None) };
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const FILE_CURSOR: u64 = u64::MAX;
const READ_CHUNK_BYTES: usize = 1024;

type PendingStdinRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;

/// Async line-oriented stdin reader.
///
/// The reader is `io_uring`-first. When the active stdin fd rejects `IORING_OP_READ`, the module
/// falls back to a helper-thread blocking read on the same duplicated fd.
pub struct Stdin {
    fd: OwnedFd,
    buffer: Vec<u8>,
    pending_read: Option<PendingStdinRead>,
}

/// Opens an async stdin reader.
pub fn stdin() -> io::Result<Stdin> {
    let raw = cvt(unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_DUPFD_CLOEXEC, 0) })?;
    Ok(Stdin {
        fd: unsafe { OwnedFd::from_raw_fd(raw) },
        buffer: Vec::new(),
        pending_read: None,
    })
}

impl Stdin {
    /// Reads a single UTF-8 line, including the trailing newline when present.
    ///
    /// Returns `Ok(None)` on EOF.
    pub async fn read_line(&mut self) -> io::Result<Option<String>> {
        loop {
            if let Some(index) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let line = self.buffer.drain(..=index).collect::<Vec<_>>();
                return decode_line(line).map(Some);
            }

            let mut chunk = vec![0; READ_CHUNK_BYTES];
            let read = self.read(&mut chunk).await?;
            if read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                let line = std::mem::take(&mut self.buffer);
                return decode_line(line).map(Some);
            }

            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    /// Reads bytes from standard input into `buf`.
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let fd = self.fd.as_raw_fd();
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            let support = STDIN_URING_SUPPORTED.with(Cell::get);
            if support != Some(false) {
                match submit_uring_read(fd, buf.len()).await {
                    Ok(bytes) => {
                        STDIN_URING_SUPPORTED.with(|state| state.set(Some(true)));
                        let read = bytes.len();
                        buf[..read].copy_from_slice(&bytes);
                        return Ok(read);
                    }
                    Err(error) if should_fallback_to_offload(&error) => {
                        STDIN_URING_SUPPORTED.with(|state| state.set(Some(false)));
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        let len = buf.len();
        offload(move || {
            let mut buffer = vec![0; len];
            let read = blocking_read(fd, &mut buffer)?;
            buffer.truncate(read);
            Ok(buffer)
        })
        .await
        .map(|bytes| {
            let read = bytes.len();
            buf[..read].copy_from_slice(&bytes);
            read
        })
    }
}

impl AsyncRead for Stdin {
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
            this.pending_read = Some(stdin_read_future(this.fd.as_raw_fd(), buf.len()));
        }

        match this
            .pending_read
            .as_mut()
            .expect("pending stdin read must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                this.pending_read = None;
                let bytes = result?;
                let read = bytes.len();
                if read > buf.len() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "read completed with more bytes than destination buffer can hold",
                    )));
                }
                buf[..read].copy_from_slice(&bytes);
                Poll::Ready(Ok(read))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn stdin_read_future(fd: RawFd, len: usize) -> PendingStdinRead {
    Box::pin(async move {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            let support = STDIN_URING_SUPPORTED.with(Cell::get);
            if support != Some(false) {
                match submit_uring_read(fd, len).await {
                    Ok(bytes) => {
                        STDIN_URING_SUPPORTED.with(|state| state.set(Some(true)));
                        return Ok(bytes);
                    }
                    Err(error) if should_fallback_to_offload(&error) => {
                        STDIN_URING_SUPPORTED.with(|state| state.set(Some(false)));
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        offload(move || {
            let mut buffer = vec![0; len];
            let read = blocking_read(fd, &mut buffer)?;
            buffer.truncate(read);
            Ok(buffer)
        })
        .await
    })
}

async fn offload<T: Send + 'static>(
    task: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let handle_for_task = handle.clone();
    if let Err(error) =
        crate::sys::blocking::spawn_blocking(move || handle_for_task.complete(task()))
    {
        handle.complete(Err(error));
    }
    future.await
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
async fn submit_uring_read(fd: RawFd, len: usize) -> io::Result<Vec<u8>> {
    let buffer = Arc::new(Mutex::new(vec![0; len].into_boxed_slice()));
    let ptr = buffer.lock().unwrap().as_mut_ptr();
    let capacity = len;
    submit_uring_guarded(
        move |sqe| {
            sqe.opcode = IORING_OP_READ;
            sqe.fd = fd;
            sqe.addr = ptr as u64;
            sqe.len = capacity as u32;
            sqe.off = FILE_CURSOR;
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

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
async fn submit_uring_guarded<T: Send + 'static, M>(
    fill: impl FnOnce(&mut IoUringSqe),
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

fn blocking_read(fd: RawFd, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        let read =
            unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), buffer.len()) };
        if read >= 0 {
            return Ok(read as usize);
        }

        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(error);
    }
}

fn decode_line(bytes: Vec<u8>) -> io::Result<String> {
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn cqe_to_result(cqe: IoUringCqe) -> io::Result<i32> {
    if cqe.res < 0 {
        Err(io::Error::from_raw_os_error(-cqe.res))
    } else {
        Ok(cqe.res)
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn should_fallback_to_offload(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
    )
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

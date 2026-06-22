//! Async standard stream helpers.
//!
//! [`Stdin`] reads from fd 0, [`Stdout`] writes to fd 1, and [`Stderr`] writes
//! to fd 2 through the runtime's platform I/O backend. The handles duplicate the
//! process stdio descriptors, so dropping them does not close the process-wide
//! standard streams.
//!
//! # Terminal UIs
//!
//! Terminal applications can use [`stdin`] for async reads from the TTY,
//! [`stdout`] for rendering, and [`crate::signal::unix::SignalKind::WindowChange`]
//! for resize notifications. `Stdin` implements [`AsyncRead`] directly and can
//! be read with byte-sized buffers, which lets a raw-mode TTY feed key bytes to
//! a parser as they arrive.
//!
//! `runite` intentionally does not provide a TUI framework, termios/raw-mode
//! management, or escape-sequence parsing. Applications should enable raw mode
//! themselves, or use a terminal crate such as `crossterm`, then compose that
//! with these async fd handles. The runtime is well-suited as the async I/O
//! substrate under terminal UIs just as it is under graphical applications.
//!
//! # Examples
//!
//! ```no_run
//! use runite::io::AsyncWriteExt;
//!
//! runite::queue_future(async {
//!     let mut out = runite::stdout().expect("stdout should open");
//!     out.write_all(b"hello from runite\n")
//!         .await
//!         .expect("stdout write should succeed");
//! });
//!
//! runite::run();
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use std::sync::{Arc, Mutex};

use crate::io::{AsyncRead, AsyncWrite};
use crate::op::completion::completion_for_current_thread;
use crate::op::fs::FsOp;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use crate::platform::linux_x86_64::runtime::with_current_driver;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use crate::platform::linux_x86_64::uring::{IORING_OP_READ, IoUringCqe, IoUringSqe};
use crate::sys::current::fs as sys_fs;
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
type PendingStandardWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;

/// Async line-oriented stdin reader.
///
/// The reader is `io_uring`-first. When the active stdin fd rejects `IORING_OP_READ`, the module
/// falls back to a helper-thread blocking read on the same duplicated fd.
///
/// Create one with [`stdin`].
pub struct Stdin {
    fd: OwnedFd,
    buffer: Vec<u8>,
    pending_read: Option<PendingStdinRead>,
}

/// Async writer for standard output.
///
/// Created by [`stdout`], this handle duplicates file descriptor 1 and
/// implements [`AsyncWrite`] for runtime-driven writes. Dropping it does not
/// close the process-wide stdout stream.
pub struct Stdout {
    writer: StandardWriter,
}

/// Async writer for standard error.
///
/// Created by [`stderr`], this handle duplicates file descriptor 2 and
/// implements [`AsyncWrite`] for runtime-driven writes. Dropping it does not
/// close the process-wide stderr stream.
pub struct Stderr {
    writer: StandardWriter,
}

struct StandardWriter {
    fd: OwnedFd,
    pending_write: Option<PendingStandardWrite>,
}

/// Opens an async stdin reader.
///
/// The returned [`Stdin`] owns a duplicate of the process stdin descriptor.
pub fn stdin() -> io::Result<Stdin> {
    Ok(Stdin {
        fd: duplicate_fd(libc::STDIN_FILENO)?,
        buffer: Vec::new(),
        pending_read: None,
    })
}

/// Opens an async stdout writer.
///
/// The returned [`Stdout`] owns a duplicate of the process stdout descriptor.
pub fn stdout() -> io::Result<Stdout> {
    Ok(Stdout {
        writer: StandardWriter::new(duplicate_fd(libc::STDOUT_FILENO)?),
    })
}

/// Opens an async stderr writer.
///
/// The returned [`Stderr`] owns a duplicate of the process stderr descriptor.
pub fn stderr() -> io::Result<Stderr> {
    Ok(Stderr {
        writer: StandardWriter::new(duplicate_fd(libc::STDERR_FILENO)?),
    })
}

impl Stdin {
    /// Reads a single UTF-8 line, including the trailing newline when present.
    ///
    /// Returns `Ok(None)` on EOF.
    ///
    /// Invalid UTF-8 is reported as [`io::ErrorKind::InvalidData`].
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
    ///
    /// Returns the number of bytes copied into `buf`, or `0` if `buf` is empty
    /// or stdin reaches EOF.
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

impl Stdout {
    /// Writes bytes to standard output.
    ///
    /// For extension methods such as `write_all` and `flush`, use the
    /// [`AsyncWriteExt`](crate::io::AsyncWriteExt) trait.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf).await
    }
}

impl Stderr {
    /// Writes bytes to standard error.
    ///
    /// For extension methods such as `write_all` and `flush`, use the
    /// [`AsyncWriteExt`](crate::io::AsyncWriteExt) trait.
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf).await
    }
}

impl StandardWriter {
    fn new(fd: OwnedFd) -> Self {
        Self {
            fd,
            pending_write: None,
        }
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        standard_write_future(self.fd.as_raw_fd(), buf.to_vec()).await
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if self.pending_write.is_none() {
            self.pending_write = Some(standard_write_future(self.fd.as_raw_fd(), buf.to_vec()));
        }

        match self
            .pending_write
            .as_mut()
            .expect("pending standard stream write must exist")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(result) => {
                self.pending_write = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
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

impl AsyncWrite for Stdout {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Stderr {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
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

fn standard_write_future(fd: RawFd, data: Vec<u8>) -> PendingStandardWrite {
    Box::pin(sys_fs::write(FsOp::Write {
        fd,
        offset: None,
        data,
    }))
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
        // SAFETY: `fd` is expected to remain open for the duration of the call,
        // and `buffer` points to `buffer.len()` bytes of writable memory owned
        // exclusively through `&mut [u8]`.
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

fn duplicate_fd(fd: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: `fd` is passed by value; on success `fcntl` returns a new
    // close-on-exec descriptor owned by the caller.
    let raw = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
    // SAFETY: `raw` was just returned by `F_DUPFD_CLOEXEC`, so it is a valid,
    // uniquely owned file descriptor to transfer into `OwnedFd`.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
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

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::sync::{Arc, Mutex};

    use crate::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn stdout_and_stderr_write_successfully() {
        let stdout_written = Arc::new(Mutex::new(None::<usize>));
        let stderr_written = Arc::new(Mutex::new(None::<usize>));

        {
            let stdout_written = Arc::clone(&stdout_written);
            let stderr_written = Arc::clone(&stderr_written);
            crate::queue_future(async move {
                let mut out = stdout().expect("stdout should open");
                let mut err = stderr().expect("stderr should open");

                let out_bytes = out
                    .write(b"runite stdout async write test\n")
                    .await
                    .expect("stdout write should succeed");
                let err_bytes = err
                    .write(b"runite stderr async write test\n")
                    .await
                    .expect("stderr write should succeed");

                *stdout_written.lock().expect("stdout mutex poisoned") = Some(out_bytes);
                *stderr_written.lock().expect("stderr mutex poisoned") = Some(err_bytes);
            });
        }

        crate::run();

        assert_eq!(
            *stdout_written.lock().expect("stdout mutex poisoned"),
            Some(b"runite stdout async write test\n".len())
        );
        assert_eq!(
            *stderr_written.lock().expect("stderr mutex poisoned"),
            Some(b"runite stderr async write test\n".len())
        );
    }

    #[test]
    fn stdout_writes_to_tty_fd() {
        let (master, slave) = open_pty();
        let written = Arc::new(Mutex::new(None::<usize>));

        {
            let written = Arc::clone(&written);
            crate::queue_future(async move {
                let mut out = Stdout {
                    writer: StandardWriter::new(slave),
                };
                let bytes = out
                    .write_all(b"tty output\n")
                    .await
                    .map(|()| b"tty output\n".len())
                    .expect("tty stdout write should succeed");
                *written.lock().expect("written mutex poisoned") = Some(bytes);
            });
        }

        crate::run();

        assert_eq!(
            *written.lock().expect("written mutex poisoned"),
            Some(b"tty output\n".len())
        );

        let mut buffer = [0u8; 64];
        let read = blocking_read(master.as_raw_fd(), &mut buffer).expect("pty master should read");
        assert_eq!(&buffer[..read], b"tty output\r\n");
    }

    #[test]
    fn stdin_reads_single_byte_from_tty_fd() {
        let (master, slave) = open_pty();
        write_fd(master.as_raw_fd(), b"x\n").expect("pty master should write input");

        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));
        {
            let observed = Arc::clone(&observed);
            crate::queue_future(async move {
                let mut input = Stdin {
                    fd: slave,
                    buffer: Vec::new(),
                    pending_read: None,
                };
                let mut byte = [0u8; 1];
                let read = input
                    .read(&mut byte)
                    .await
                    .expect("single-byte tty stdin read should succeed");
                *observed.lock().expect("observed mutex poisoned") = Some(byte[..read].to_vec());
            });
        }

        crate::run();

        assert_eq!(
            observed.lock().expect("observed mutex poisoned").as_deref(),
            Some(b"x".as_slice())
        );
    }

    fn open_pty() -> (OwnedFd, OwnedFd) {
        let mut master = -1;
        let mut slave = -1;
        // SAFETY: `master` and `slave` are valid out-pointers. Null optional
        // pointers request the default name, termios, and window size.
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        assert_eq!(rc, 0, "openpty should succeed");
        // SAFETY: `openpty` initialized `master` with an owned file descriptor.
        let master = unsafe { OwnedFd::from_raw_fd(master) };
        // SAFETY: `openpty` initialized `slave` with an owned file descriptor.
        let slave = unsafe { OwnedFd::from_raw_fd(slave) };
        (master, slave)
    }

    fn write_fd(fd: RawFd, data: &[u8]) -> io::Result<usize> {
        loop {
            // SAFETY: `fd` is open for the duration of this test helper, and
            // `data` points to `data.len()` initialized bytes.
            let written =
                unsafe { libc::write(fd, data.as_ptr().cast::<libc::c_void>(), data.len()) };
            if written >= 0 {
                return Ok(written as usize);
            }

            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
    }
}

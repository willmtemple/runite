//! Async standard stream helpers.
//!
//! This module opens runtime-aware handles for process standard input, output,
//! and error. [`Stdin`] reads from standard input, while [`Stdout`] and
//! [`Stderr`] write through the runtime's platform I/O backend. Each handle
//! duplicates the process stdio descriptor, so dropping it does not close the
//! process-wide standard stream.
//!
//! The handles are thread-affine like other runite I/O objects: create and poll
//! them on the runtime thread that owns them. Tasks do not migrate between
//! threads.
//!
//! `Stdout` and `Stderr` perform write-through async writes via the active
//! backend: Linux uses `io_uring`, while macOS aarch64 and Windows offload
//! blocking writes to the blocking pool. runite does not add userspace buffering
//! for these writers. Their `poll_flush` and `poll_close` methods are no-ops;
//! `flush()` only observes that previously awaited writes have completed and
//! does not call libc `fflush`, a terminal flush, or `fsync`.
//!
//! `Stdin` uses the platform backend for reads. macOS and Windows always offload
//! a blocking read (Windows console handles do not support overlapped I/O);
//! Linux first tries `io_uring` and falls back to the blocking pool when the
//! descriptor does not support runtime-native reads.
//!
//! # Terminal UIs
//!
//! Terminal applications can use [`stdin`] for async reads from the TTY,
//! [`stdout`] for rendering, and (on Unix)
//! `runite::signal::unix::SignalKind::WindowChange` for resize notifications.
//! `Stdin` implements [`AsyncRead`] directly and can be read with byte-sized
//! buffers, which lets a raw-mode TTY feed key bytes to a parser as they arrive.
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
//! runite::spawn(async {
//!     let mut out = runite::stdout().expect("stdout should open");
//!     out.write_all(b"hello from runite\n")
//!         .await
//!         .expect("stdout write should succeed");
//! });
//!
//! runite::run();
//! ```
//!
//! Reading from standard input follows the same event-loop pattern:
//!
//! ```no_run
//! use runite::io::AsyncReadExt;
//!
//! runite::spawn(async {
//!     let mut input = runite::stdin().expect("stdin should open");
//!     let mut byte = [0; 1];
//!     let read = input
//!         .read(&mut byte)
//!         .await
//!         .expect("stdin read should succeed");
//!     if read != 0 {
//!         eprintln!("first byte: {}", byte[0]);
//!     }
//! });
//!
//! runite::run();
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use crate::io::{AsyncRead, AsyncWrite};
use crate::op::completion::completion_for_current_thread;
use crate::sys::handle::OwnedFile;

const READ_CHUNK_BYTES: usize = 1024;

type PendingStdinRead = Pin<Box<dyn Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PendingStandardWrite = Pin<Box<dyn Future<Output = io::Result<usize>> + 'static>>;

/// Async reader for standard input.
///
/// `Stdin` owns a duplicate of the process standard input descriptor and
/// implements [`AsyncRead`] for byte-oriented reads. It also provides
/// [`read_line`](Self::read_line) for simple line-oriented input. On Linux, the
/// reader tries `io_uring` first and falls back to a helper-thread blocking read
/// if the active descriptor does not support runtime-native reads. On macOS and
/// Windows, reads are always offloaded to the blocking pool.
///
/// `read_line` keeps an internal buffer and may read beyond the returned line;
/// bytes after the newline are saved for the next call.
///
/// Create one with [`stdin`].
pub struct Stdin {
    fd: OwnedFile,
    buffer: Vec<u8>,
    pending_read: Option<PendingStdinRead>,
    /// Bytes a completed read produced that overflowed a smaller caller buffer;
    /// served before any new read so no bytes are lost. See
    /// [`ReadOverflow`](crate::io::ReadOverflow).
    read_overflow: Option<Box<crate::io::ReadOverflow>>,
}

/// Async writer for standard output.
///
/// Created by [`stdout`], this handle duplicates the process stdout descriptor
/// and implements [`AsyncWrite`] for runtime-driven write-through writes. A
/// single write may complete after writing fewer bytes than requested; use
/// [`AsyncWriteExt::write_all`](crate::io::AsyncWriteExt::write_all) when the
/// whole buffer must be written. `poll_flush` and `poll_close` are no-ops, and
/// `flush()` does not call libc `fflush` or `fsync`.
///
/// Dropping it does not close the process-wide stdout stream.
pub struct Stdout {
    writer: StandardWriter,
}

/// Async writer for standard error.
///
/// Created by [`stderr`], this handle duplicates the process stderr descriptor
/// and implements [`AsyncWrite`] for runtime-driven write-through writes. A
/// single write may complete after writing fewer bytes than requested; use
/// [`AsyncWriteExt::write_all`](crate::io::AsyncWriteExt::write_all) when the
/// whole buffer must be written. `poll_flush` and `poll_close` are no-ops, and
/// `flush()` does not call libc `fflush` or `fsync`.
///
/// Dropping it does not close the process-wide stderr stream.
pub struct Stderr {
    writer: StandardWriter,
}

struct StandardWriter {
    fd: OwnedFile,
    pending_write: Option<PendingStandardWrite>,
}

/// Opens an async stdin reader.
///
/// The returned [`Stdin`] owns a duplicate of the process stdin descriptor.
///
/// # Examples
///
/// ```no_run
/// use runite::io::AsyncReadExt;
///
/// runite::spawn(async {
///     let mut input = runite::stdin().expect("stdin should open");
///     let mut buffer = [0; 8];
///     let _read = input.read(&mut buffer).await.expect("stdin should read");
/// });
///
/// runite::run();
/// ```
pub fn stdin() -> io::Result<Stdin> {
    Ok(Stdin {
        fd: imp::duplicate_stdin()?,
        buffer: Vec::new(),
        pending_read: None,
        read_overflow: None,
    })
}

/// Opens an async stdout writer.
///
/// The returned [`Stdout`] owns a duplicate of the process stdout descriptor.
///
/// # Examples
///
/// ```no_run
/// use runite::io::AsyncWriteExt;
///
/// runite::spawn(async {
///     let mut out = runite::stdout().expect("stdout should open");
///     out.write_all(b"rendered frame\n")
///         .await
///         .expect("stdout should write");
/// });
///
/// runite::run();
/// ```
pub fn stdout() -> io::Result<Stdout> {
    Ok(Stdout {
        writer: StandardWriter::new(imp::duplicate_stdout()?),
    })
}

/// Opens an async stderr writer.
///
/// The returned [`Stderr`] owns a duplicate of the process stderr descriptor.
///
/// # Examples
///
/// ```no_run
/// use runite::io::AsyncWriteExt;
///
/// runite::spawn(async {
///     let mut err = runite::stderr().expect("stderr should open");
///     err.write_all(b"diagnostic\n")
///         .await
///         .expect("stderr should write");
/// });
///
/// runite::run();
/// ```
pub fn stderr() -> io::Result<Stderr> {
    Ok(Stderr {
        writer: StandardWriter::new(imp::duplicate_stderr()?),
    })
}

impl Stdin {
    /// Reads a single UTF-8 line, including the trailing newline when present.
    ///
    /// Returns `Ok(None)` on EOF.
    ///
    /// Invalid UTF-8 is reported as [`io::ErrorKind::InvalidData`].
    ///
    /// This method reads chunks into an internal buffer, so it may read beyond
    /// the line it returns. Buffered bytes are preserved for subsequent calls.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut input = runite::stdin().expect("stdin should open");
    ///     if let Some(line) = input.read_line().await.expect("stdin should read") {
    ///         eprintln!("line length: {}", line.len());
    ///     }
    /// });
    ///
    /// runite::run();
    /// ```
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
    ///
    /// For extension methods such as `read_exact` and `read_to_end`, use the
    /// [`AsyncReadExt`](crate::io::AsyncReadExt) trait.
    ///
    /// # Cancel safety
    ///
    /// On the Linux `io_uring` path this method is cancel-safe: a read that
    /// completes after its future is dropped is stashed on the handle and served
    /// by the next read, so no bytes are lost.
    ///
    /// The blocking-offload fallback (macOS, Windows, and Linux kernels without
    /// `io_uring` stdin read support) is **not** cancel-safe. A blocking
    /// read cannot be interrupted, so if the returned future is dropped
    /// while a read is in progress, the byte(s) that read consumes are lost
    /// (they will not be returned to a later read), and the pool worker running
    /// it stays blocked until input arrives. Avoid dropping a stdin read future
    /// (e.g. in a `select!`) on those platforms. A dedicated buffered stdin
    /// reader that closes this gap is planned post-0.1.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut input = runite::stdin().expect("stdin should open");
    ///     let mut byte = [0; 1];
    ///     let _read = input.read(&mut byte).await.expect("stdin should read");
    /// });
    ///
    /// runite::run();
    /// ```
    pub async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Delegate to the AsyncRead path so the in-flight read is stashed on the
        // handle. On the Linux io_uring path this makes the read cancel-safe (a
        // completed-but-unclaimed read is served next via the overflow buffer);
        // the blocking-offload fallback still cannot cancel an in-progress
        // blocking read (a cancel-safe buffered stdin reader is a roadmap item).
        core::future::poll_fn(|cx| Pin::new(&mut *self).poll_read(cx, buf)).await
    }
}

impl Stdout {
    /// Writes bytes to standard output.
    ///
    /// The write is sent through the platform backend immediately and may write
    /// fewer bytes than `buf.len()`. Use
    /// [`write_all`](crate::io::AsyncWriteExt::write_all) to retry until the
    /// full buffer is written. `flush()` is a no-op for durability and libc
    /// buffering; it does not call `fflush` or `fsync`.
    ///
    /// For extension methods such as `write_all` and `flush`, use the
    /// [`AsyncWriteExt`](crate::io::AsyncWriteExt) trait.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut out = runite::stdout().expect("stdout should open");
    ///     let bytes = out.write(b"partial frame\n").await.expect("stdout should write");
    ///     assert!(bytes > 0);
    /// });
    ///
    /// runite::run();
    /// ```
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf).await
    }
}

impl Stderr {
    /// Writes bytes to standard error.
    ///
    /// The write is sent through the platform backend immediately and may write
    /// fewer bytes than `buf.len()`. Use
    /// [`write_all`](crate::io::AsyncWriteExt::write_all) to retry until the
    /// full buffer is written. `flush()` is a no-op for durability and libc
    /// buffering; it does not call `fflush` or `fsync`.
    ///
    /// For extension methods such as `write_all` and `flush`, use the
    /// [`AsyncWriteExt`](crate::io::AsyncWriteExt) trait.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut err = runite::stderr().expect("stderr should open");
    ///     let bytes = err.write(b"warning\n").await.expect("stderr should write");
    ///     assert!(bytes > 0);
    /// });
    ///
    /// runite::run();
    /// ```
    pub async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf).await
    }
}

impl StandardWriter {
    fn new(fd: OwnedFile) -> Self {
        Self {
            fd,
            pending_write: None,
        }
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        imp::standard_write_future(crate::sys::handle::raw_file(&self.fd), buf.to_vec()).await
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if self.pending_write.is_none() {
            self.pending_write = Some(imp::standard_write_future(
                crate::sys::handle::raw_file(&self.fd),
                buf.to_vec(),
            ));
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

        // Serve any surplus from a previous read before submitting a new one.
        if let Some(overflow) = this.read_overflow.as_mut() {
            let n = overflow.drain_into(buf);
            if overflow.is_drained() {
                this.read_overflow = None;
            }
            return Poll::Ready(Ok(n));
        }

        if this.pending_read.is_none() {
            this.pending_read = Some(imp::stdin_read_future(
                crate::sys::handle::raw_file(&this.fd),
                buf.len(),
            ));
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
                let n = bytes.len().min(buf.len());
                buf[..n].copy_from_slice(&bytes[..n]);
                // Retain any bytes that did not fit rather than discarding them.
                if bytes.len() > n {
                    this.read_overflow = Some(Box::new(crate::io::ReadOverflow::new(&bytes[n..])));
                }
                Poll::Ready(Ok(n))
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

fn decode_line(bytes: Vec<u8>) -> io::Result<String> {
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Platform backend for standard-stream handles: descriptor duplication, the
/// stdin read future, and the write-through future. The public wrappers above
/// are platform-neutral; only this module knows how the bytes actually move.
#[cfg(unix)]
mod imp {
    use std::io;
    use std::os::fd::{FromRawFd, RawFd};

    use super::{PendingStandardWrite, PendingStdinRead, offload};
    use crate::op::fs::FsOp;
    use crate::sys::current::fs as sys_fs;
    use crate::sys::handle::{OwnedFile, RawFile};

    #[cfg(target_os = "linux")]
    use crate::op::completion::completion_for_current_thread;
    #[cfg(target_os = "linux")]
    use crate::platform::linux::runtime::with_current_driver;
    #[cfg(target_os = "linux")]
    use crate::platform::linux::uring::{IORING_OP_READ, IoUringCqe, IoUringSqe};
    #[cfg(target_os = "linux")]
    use std::cell::Cell;
    #[cfg(target_os = "linux")]
    use std::sync::{Arc, Mutex};

    #[cfg(target_os = "linux")]
    thread_local! {
        static STDIN_URING_SUPPORTED: Cell<Option<bool>> = const { Cell::new(None) };
    }

    #[cfg(target_os = "linux")]
    const FILE_CURSOR: u64 = u64::MAX;

    pub(super) fn duplicate_stdin() -> io::Result<OwnedFile> {
        duplicate_fd(libc::STDIN_FILENO)
    }

    pub(super) fn duplicate_stdout() -> io::Result<OwnedFile> {
        duplicate_fd(libc::STDOUT_FILENO)
    }

    pub(super) fn duplicate_stderr() -> io::Result<OwnedFile> {
        duplicate_fd(libc::STDERR_FILENO)
    }

    pub(super) fn stdin_read_future(fd: RawFile, len: usize) -> PendingStdinRead {
        Box::pin(async move {
            #[cfg(target_os = "linux")]
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

    pub(super) fn standard_write_future(fd: RawFile, data: Vec<u8>) -> PendingStandardWrite {
        Box::pin(sys_fs::write(FsOp::Write {
            fd,
            offset: None,
            data,
        }))
    }

    #[cfg(target_os = "linux")]
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

    #[cfg(target_os = "linux")]
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
            let _ = with_current_driver(|driver| {
                driver.cancel_operation_with_guard(token, Some(guard))
            });
        });

        future.await
    }

    pub(super) fn blocking_read(fd: RawFd, buffer: &mut [u8]) -> io::Result<usize> {
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

    pub(super) fn duplicate_fd(fd: RawFd) -> io::Result<OwnedFile> {
        // SAFETY: `fd` is passed by value; on success `fcntl` returns a new
        // close-on-exec descriptor owned by the caller.
        let raw = cvt(unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) })?;
        // SAFETY: `raw` was just returned by `F_DUPFD_CLOEXEC`, so it is a valid,
        // uniquely owned file descriptor to transfer into `OwnedFd`.
        Ok(unsafe { OwnedFile::from_raw_fd(raw) })
    }

    #[cfg(target_os = "linux")]
    fn cqe_to_result(cqe: IoUringCqe) -> io::Result<i32> {
        if cqe.res < 0 {
            Err(io::Error::from_raw_os_error(-cqe.res))
        } else {
            Ok(cqe.res)
        }
    }

    #[cfg(target_os = "linux")]
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
}

#[cfg(windows)]
mod imp {
    use std::io;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};

    use windows_sys::Win32::Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_BROKEN_PIPE, GetLastError, HANDLE,
        INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
    use windows_sys::Win32::System::Console::{
        GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    use super::{PendingStandardWrite, PendingStdinRead, offload};
    use crate::sys::handle::{OwnedFile, RawFile};

    pub(super) fn duplicate_stdin() -> io::Result<OwnedFile> {
        duplicate_std_handle(STD_INPUT_HANDLE)
    }

    pub(super) fn duplicate_stdout() -> io::Result<OwnedFile> {
        duplicate_std_handle(STD_OUTPUT_HANDLE)
    }

    pub(super) fn duplicate_stderr() -> io::Result<OwnedFile> {
        duplicate_std_handle(STD_ERROR_HANDLE)
    }

    /// Console handles do not support overlapped I/O and cannot be associated
    /// with a completion port, so stdin reads always run on the blocking pool.
    pub(super) fn stdin_read_future(fd: RawFile, len: usize) -> PendingStdinRead {
        Box::pin(async move {
            offload(move || {
                let mut buffer = vec![0; len];
                let read = blocking_read(fd, &mut buffer)?;
                buffer.truncate(read);
                Ok(buffer)
            })
            .await
        })
    }

    pub(super) fn standard_write_future(fd: RawFile, data: Vec<u8>) -> PendingStandardWrite {
        Box::pin(async move { offload(move || blocking_write(fd, &data)).await })
    }

    fn duplicate_std_handle(which: u32) -> io::Result<OwnedFile> {
        // SAFETY: `GetStdHandle` takes no pointers; failure is reported through
        // the return value checked below.
        let source = unsafe { GetStdHandle(which) };
        if source == INVALID_HANDLE_VALUE || source.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "process standard stream is not available",
            ));
        }

        let mut duplicated: HANDLE = std::ptr::null_mut();
        // SAFETY: `source` was just returned by `GetStdHandle`, both process
        // handles are the current-process pseudo handle, and `duplicated` is a
        // valid out-pointer.
        let ok = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                source,
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
        // SAFETY: on success `duplicated` is a fresh handle owned exclusively by
        // this call.
        Ok(unsafe { OwnedHandle::from_raw_handle(duplicated) })
    }

    pub(super) fn blocking_read(fd: RawFile, buffer: &mut [u8]) -> io::Result<usize> {
        let mut read = 0u32;
        // SAFETY: `fd` names a handle that remains open for the duration of the
        // call, and `buffer` points to `buffer.len()` writable bytes owned
        // exclusively through `&mut [u8]`.
        let ok = unsafe {
            ReadFile(
                fd.as_handle(),
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).unwrap_or(u32::MAX),
                &mut read,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            // A closed pipe peer reports `ERROR_BROKEN_PIPE`; map it to the
            // Unix "read returns 0 at EOF" convention.
            // SAFETY: no intervening API call has replaced the thread error.
            let error = unsafe { GetLastError() };
            if error == ERROR_BROKEN_PIPE {
                return Ok(0);
            }
            return Err(io::Error::from_raw_os_error(error as i32));
        }
        Ok(read as usize)
    }

    fn blocking_write(fd: RawFile, data: &[u8]) -> io::Result<usize> {
        let mut written = 0u32;
        // SAFETY: `fd` names a handle that remains open for the duration of the
        // call, and `data` points to `data.len()` initialized bytes.
        let ok = unsafe {
            WriteFile(
                fd.as_handle(),
                data.as_ptr(),
                u32::try_from(data.len()).unwrap_or(u32::MAX),
                &mut written,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(written as usize)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    #[cfg(unix)]
    use crate::io::AsyncWriteExt;

    use super::*;

    #[test]
    fn stdout_and_stderr_write_successfully() {
        let stdout_written = Arc::new(Mutex::new(None::<usize>));
        let stderr_written = Arc::new(Mutex::new(None::<usize>));

        {
            let stdout_written = Arc::clone(&stdout_written);
            let stderr_written = Arc::clone(&stderr_written);
            crate::spawn(async move {
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

    #[cfg(unix)]
    #[test]
    fn stdout_writes_to_tty_fd() {
        let (master, slave) = open_pty();
        // macOS (BSD) ptys discard the slave's pending output queue when the
        // last slave descriptor closes. `Stdout` owns `slave` and drops it when
        // the task finishes, so hold an extra slave-side descriptor open until
        // after the master is drained — mirroring real usage where the stdout
        // descriptor outlives any individual write.
        let slave_keepalive =
            imp::duplicate_fd(std::os::fd::AsRawFd::as_raw_fd(&slave)).expect("dup slave fd");
        let written = Arc::new(Mutex::new(None::<usize>));

        {
            let written = Arc::clone(&written);
            crate::spawn(async move {
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
        let read = imp::blocking_read(std::os::fd::AsRawFd::as_raw_fd(&master), &mut buffer)
            .expect("pty master should read");
        assert_eq!(&buffer[..read], b"tty output\r\n");
        drop(slave_keepalive);
    }

    #[cfg(unix)]
    #[test]
    fn stdin_reads_single_byte_from_tty_fd() {
        let (master, slave) = open_pty();
        write_fd(std::os::fd::AsRawFd::as_raw_fd(&master), b"x\n")
            .expect("pty master should write input");

        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));
        {
            let observed = Arc::clone(&observed);
            crate::spawn(async move {
                let mut input = Stdin {
                    fd: slave,
                    buffer: Vec::new(),
                    pending_read: None,
                    read_overflow: None,
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

    #[test]
    fn stdin_read_line_drains_buffered_read_ahead_before_reading_fd() {
        let fd = imp::duplicate_stdin().expect("dup stdin fd");
        let observed = Arc::new(Mutex::new(None::<Vec<String>>));

        {
            let observed = Arc::clone(&observed);
            crate::spawn(async move {
                let mut input = Stdin {
                    fd,
                    buffer: b"first\nsecond\n".to_vec(),
                    pending_read: None,
                    read_overflow: None,
                };
                let first = input
                    .read_line()
                    .await
                    .expect("first buffered line")
                    .expect("first line should exist");
                let second = input
                    .read_line()
                    .await
                    .expect("second buffered line")
                    .expect("second line should exist");
                *observed.lock().expect("observed mutex poisoned") = Some(vec![first, second]);
            });
        }

        crate::run();

        assert_eq!(
            *observed.lock().expect("observed mutex poisoned"),
            Some(vec!["first\n".to_string(), "second\n".to_string()])
        );
    }

    #[test]
    fn stdin_read_line_reports_invalid_buffered_utf8() {
        let fd = imp::duplicate_stdin().expect("dup stdin fd");
        let observed = Arc::new(Mutex::new(None::<io::ErrorKind>));

        {
            let observed = Arc::clone(&observed);
            crate::spawn(async move {
                let mut input = Stdin {
                    fd,
                    buffer: b"bad \xff\n".to_vec(),
                    pending_read: None,
                    read_overflow: None,
                };
                let error = input
                    .read_line()
                    .await
                    .expect_err("invalid buffered UTF-8 should fail");
                *observed.lock().expect("observed mutex poisoned") = Some(error.kind());
            });
        }

        crate::run();

        assert_eq!(
            *observed.lock().expect("observed mutex poisoned"),
            Some(io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn stdout_and_stderr_flush_and_close_are_noops() {
        use crate::io::AsyncWrite;

        let mut cx = Context::from_waker(std::task::Waker::noop());
        let stdout_fd = imp::duplicate_stdout().expect("dup stdout fd");
        let stderr_fd = imp::duplicate_stderr().expect("dup stderr fd");
        let mut out = Stdout {
            writer: StandardWriter::new(stdout_fd),
        };
        let mut err = Stderr {
            writer: StandardWriter::new(stderr_fd),
        };

        assert!(Pin::new(&mut out).poll_flush(&mut cx).is_ready());
        assert!(Pin::new(&mut out).poll_close(&mut cx).is_ready());
        assert!(Pin::new(&mut err).poll_flush(&mut cx).is_ready());
        assert!(Pin::new(&mut err).poll_close(&mut cx).is_ready());
    }

    #[cfg(unix)]
    fn open_pty() -> (crate::sys::handle::OwnedFile, crate::sys::handle::OwnedFile) {
        use std::os::fd::FromRawFd;

        let mut master = -1;
        let mut slave = -1;
        // SAFETY: `master` and `slave` are valid out-pointers. Null optional
        // pointers request the default name, termios, and window size.
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, 0, "openpty should succeed");
        // SAFETY: `openpty` initialized `master` with an owned file descriptor.
        let master = unsafe { crate::sys::handle::OwnedFile::from_raw_fd(master) };
        // SAFETY: `openpty` initialized `slave` with an owned file descriptor.
        let slave = unsafe { crate::sys::handle::OwnedFile::from_raw_fd(slave) };
        (master, slave)
    }

    #[cfg(unix)]
    fn write_fd(fd: std::os::fd::RawFd, data: &[u8]) -> io::Result<usize> {
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

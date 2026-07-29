//! Async standard stream helpers.
//!
//! This module opens runtime-aware handles for process standard input, output,
//! and error. [`Stdin`] reads from standard input, while [`Stdout`] and
//! [`Stderr`] write through the runtime's platform I/O backend. Output handles
//! duplicate their process descriptors, while all [`Stdin`] handles share one
//! process-wide reader and its bounded buffer. Dropping a handle does not close
//! a process standard stream.
//!
//! The handles are thread-affine like other runite I/O objects: create and poll
//! them on the runtime thread that owns them. Tasks do not migrate between
//! threads.
//!
//! `Stdout` and `Stderr` perform write-through async writes via the active
//! backend: Linux uses `io_uring`, while macOS aarch64 and Windows offload
//! blocking writes to the blocking pool. runite does not add userspace buffering
//! for these writers. `poll_flush` and `poll_close` wait for a write the handle
//! still owns and report its failure, but never call libc `fflush`, a terminal
//! flush, or `fsync`, and never close the process stream.
//!
//! `Stdin` uses one dedicated blocking reader thread on every platform. That
//! thread owns a duplicate of the process input handle and, only while a read
//! is pending, reads ahead into a bounded 64 KiB process-wide buffer. Runtime
//! tasks only wait for buffered availability, so cancelling a read never loses
//! bytes or strands a shared blocking-pool worker. Spawning a runite child with
//! inherited stdin pauses the reader until that child's exit is observed or
//! its handle is dropped. Windows rejects an inherited-console spawn while a
//! parent console read is active because console-host reads cannot always be
//! cancelled strongly enough to guarantee a lossless handoff.
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

use core::pin::Pin;
use core::sync::atomic::{AtomicUsize, Ordering};
use core::task::{Context, Poll};
use std::io::{self, IoSlice};
use std::sync::{Arc, Mutex, OnceLock};

use crate::io::{AsyncRead, AsyncWrite, IoFuture, ReadState, WriteState};
#[cfg(any(test, windows, target_os = "macos"))]
use crate::op::completion::completion_for_current_thread;
use crate::sys::handle::OwnedFile;

mod stdin_reader;

const READ_CHUNK_BYTES: usize = 1024;

type PendingStandardWrite = IoFuture<usize>;

static PROCESS_STDIN_READER: OnceLock<Arc<stdin_reader::StdinReader>> = OnceLock::new();
static PROCESS_STDIN_INIT: Mutex<()> = Mutex::new(());
static PROCESS_STDIN_HANDOFFS: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct InheritedStdinHandoff {
    active: bool,
}

pub(crate) fn handoff_stdin_to_child() -> io::Result<InheritedStdinHandoff> {
    let _guard = PROCESS_STDIN_INIT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    PROCESS_STDIN_HANDOFFS.fetch_add(1, Ordering::Relaxed);
    if let Some(reader) = PROCESS_STDIN_READER.get()
        && let Err(error) = reader.pause_for_handoff()
    {
        PROCESS_STDIN_HANDOFFS.fetch_sub(1, Ordering::Relaxed);
        return Err(error);
    }
    Ok(InheritedStdinHandoff { active: true })
}

impl Drop for InheritedStdinHandoff {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let _guard = PROCESS_STDIN_INIT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = PROCESS_STDIN_HANDOFFS.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0);
        if let Some(reader) = PROCESS_STDIN_READER.get() {
            reader.resume_after_handoff();
        }
        self.active = false;
    }
}

/// Async reader for standard input.
///
/// Every handle shares the process-wide bounded stdin reader and implements
/// [`AsyncRead`] for byte-oriented reads. It also provides
/// [`next_line`](Self::next_line) for simple line-oriented input. The dedicated
/// reader thread owns the duplicated operating-system handle; `Stdin` itself
/// contains no raw handle.
///
/// Multiple handles compete for the same byte stream. A completed read removes
/// bytes exactly once; cancelling a pending read removes only that handle's
/// waiter.
///
/// `next_line` keeps partial lines on the handle but leaves bytes after a
/// newline in the shared process buffer.
///
/// Create one with [`stdin`].
pub struct Stdin {
    // Pending reads must be dropped before the shared reader reference.
    read_state: ReadState,
    buffer: Vec<u8>,
    reader: Arc<stdin_reader::StdinReader>,
    waiter_id: u64,
}

/// Async writer for standard output.
///
/// Created by [`stdout`], this handle duplicates the process stdout descriptor
/// and implements [`AsyncWrite`] for runtime-driven write-through writes. A
/// single write may complete after writing fewer bytes than requested; use
/// [`AsyncWriteExt::write_all`](crate::io::AsyncWriteExt::write_all) when the
/// whole buffer must be written. `poll_flush` and `poll_close` wait for a write
/// this handle still owns and surface its failure; neither calls libc `fflush`
/// or `fsync`, and neither closes the process stream.
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
/// whole buffer must be written. `poll_flush` and `poll_close` wait for a write
/// this handle still owns and surface its failure; neither calls libc `fflush`
/// or `fsync`, and neither closes the process stream.
///
/// Dropping it does not close the process-wide stderr stream.
pub struct Stderr {
    writer: StandardWriter,
}

struct StandardWriter {
    // Pending writes must be dropped before the descriptor owner.
    write_state: WriteState,
    fd: Arc<OwnedFile>,
}

/// Opens an async stdin reader.
///
/// All returned handles consume from one process-wide, bounded reader. The
/// dedicated reader thread is started lazily by the first successful call and
/// does not consume process input until a read is pending. A transient setup
/// failure is returned to that caller without preventing a later retry.
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
    let reader = get_or_try_init(&PROCESS_STDIN_READER, &PROCESS_STDIN_INIT, || {
        let reader = stdin_reader::StdinReader::spawn(imp::duplicate_stdin()?)?;
        for _ in 0..PROCESS_STDIN_HANDOFFS.load(Ordering::Relaxed) {
            reader.pause_for_handoff()?;
        }
        Ok(reader)
    })?;
    Ok(Stdin::from_reader(reader))
}

fn get_or_try_init<T>(
    cell: &OnceLock<Arc<T>>,
    init_lock: &Mutex<()>,
    init: impl FnOnce() -> io::Result<Arc<T>>,
) -> io::Result<Arc<T>> {
    if let Some(value) = cell.get() {
        return Ok(Arc::clone(value));
    }

    let _guard = init_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(value) = cell.get() {
        return Ok(Arc::clone(value));
    }

    let value = init()?;
    match cell.set(Arc::clone(&value)) {
        Ok(()) => Ok(value),
        Err(_) => Ok(Arc::clone(
            cell.get()
                .expect("stdin reader must be initialized while holding its lock"),
        )),
    }
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
    fn from_reader(reader: Arc<stdin_reader::StdinReader>) -> Self {
        let waiter_id = reader.new_waiter_id();
        Self {
            read_state: ReadState::default(),
            buffer: Vec::new(),
            reader,
            waiter_id,
        }
    }

    /// Reads a single UTF-8 line, including the trailing newline when present.
    ///
    /// Returns `Ok(None)` on EOF.
    ///
    /// Invalid UTF-8 is reported as [`io::ErrorKind::InvalidData`].
    ///
    /// Partial lines are retained across waits. Bytes following a newline stay
    /// in the process-wide buffer for another call or another handle.
    ///
    /// Named `next_line` rather than `read_line` because it is not the `std`
    /// shape: it allocates and returns the line, and reports end of input as
    /// `None`. [`BufReader::read_line`](crate::io::BufReader::read_line)
    /// follows `std` — it appends to a caller-supplied `String` and reports end
    /// of input as `Ok(0)`. Two methods with one name and incompatible end-of-
    /// input conventions in the same crate was a trap worth removing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut input = runite::stdin().expect("stdin should open");
    ///     if let Some(line) = input.next_line().await.expect("stdin should read") {
    ///         eprintln!("line length: {}", line.len());
    ///     }
    /// });
    ///
    /// runite::run();
    /// ```
    pub async fn next_line(&mut self) -> io::Result<Option<String>> {
        loop {
            if let Some(index) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let line = self.buffer.drain(..=index).collect::<Vec<_>>();
                return decode_line(line).map(Some);
            }

            let mut chunk = vec![0; READ_CHUNK_BYTES];
            let read = self.read_line_chunk(&mut chunk).await?;
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

    async fn read_line_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_buffered(buf, true).await
    }

    async fn read_buffered(&mut self, buf: &mut [u8], stop_at_newline: bool) -> io::Result<usize> {
        core::future::poll_fn(|cx| self.poll_buffered(cx, buf, stop_at_newline)).await
    }

    /// The single read path, shared by `next_line`, the inherent `read`, and
    /// the `AsyncRead` impl, so all three agree on cancellation.
    fn poll_buffered(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
        stop_at_newline: bool,
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if !stop_at_newline && let Some(read) = self.drain_line_buffer(buf) {
            return Poll::Ready(Ok(read));
        }

        let reader = Arc::clone(&self.reader);
        let waiter_id = self.waiter_id;
        self.read_state.poll_slice(cx, buf, move |len| {
            reader.read_future(waiter_id, len, stop_at_newline)
        })
    }

    fn drain_line_buffer(&mut self, buf: &mut [u8]) -> Option<usize> {
        if self.buffer.is_empty() {
            return None;
        }
        let read = buf.len().min(self.buffer.len());
        buf[..read].copy_from_slice(&self.buffer[..read]);
        self.buffer.drain(..read);
        Some(read)
    }
}

impl Drop for Stdin {
    fn drop(&mut self) {
        self.reader.abandon(self.waiter_id);
    }
}

impl Stdout {}

impl Stderr {}

impl StandardWriter {
    fn new(fd: OwnedFile) -> Self {
        Self {
            write_state: WriteState::default(),
            fd: Arc::new(fd),
        }
    }

    /// Test-only convenience. The public writers reach this through
    /// `AsyncWrite::poll_write_operation`; nothing in the crate awaits a
    /// `StandardWriter` directly outside tests.
    #[cfg(test)]
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let generation = crate::io::next_operation_id();
        core::future::poll_fn(|cx| self.poll_write(cx, buf, generation)).await
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let fd = Arc::clone(&self.fd);
        self.write_state
            .poll_write(cx, generation, buf, move |data| {
                imp::standard_write_future(fd, data)
            })
    }
}

impl AsyncRead for Stdin {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_buffered(cx, buf, false)
    }
}

impl AsyncWrite for Stdout {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_operation(cx, buf, 0)
    }

    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.get_mut().writer.poll_write(cx, buf, generation)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_operation(cx, bufs, 0)
    }

    fn poll_write_vectored_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        match bufs.iter().find(|buf| !buf.is_empty()) {
            Some(buf) => self.poll_write_operation(cx, buf, generation),
            None => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // An abandoned write stays owned by the stream, so returning `Ok`
        // unconditionally would report bytes as visible while they are still in
        // flight and would swallow that operation's error.
        self.get_mut().writer.write_state.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The process stream itself is never closed; closing still has to wait
        // for a write this handle owns.
        self.get_mut().writer.write_state.poll_flush(cx)
    }
}

impl AsyncWrite for Stderr {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_operation(cx, buf, 0)
    }

    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        self.get_mut().writer.poll_write(cx, buf, generation)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored_operation(cx, bufs, 0)
    }

    fn poll_write_vectored_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        match bufs.iter().find(|buf| !buf.is_empty()) {
            Some(buf) => self.poll_write_operation(cx, buf, generation),
            None => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // An abandoned write stays owned by the stream, so returning `Ok`
        // unconditionally would report bytes as visible while they are still in
        // flight and would swallow that operation's error.
        self.get_mut().writer.write_state.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The process stream itself is never closed; closing still has to wait
        // for a write this handle owns.
        self.get_mut().writer.write_state.poll_flush(cx)
    }
}

#[cfg(any(test, windows, target_os = "macos"))]
async fn offload<T: Send + 'static>(
    task: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (future, handle) = completion_for_current_thread::<io::Result<T>>();
    let handle_for_task = handle.clone();
    if let Err(error) = crate::sys::blocking::spawn_blocking_owned(task, move |outcome| {
        handle_for_task.complete(
            outcome
                .unwrap_or_else(|_| Err(io::Error::other("blocking standard I/O task panicked"))),
        );
    }) {
        handle.complete(Err(error));
    }
    future.await
}

fn decode_line(bytes: Vec<u8>) -> io::Result<String> {
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Platform backend for standard-stream handle duplication and blocking system
/// calls. The dedicated stdin thread is platform-neutral and owns the handle it
/// passes to `blocking_stdin_read`.
#[cfg(unix)]
mod imp {
    use std::io;
    use std::os::fd::{FromRawFd, RawFd};
    use std::sync::Arc;

    use super::PendingStandardWrite;
    #[cfg(not(target_os = "linux"))]
    use super::offload;
    #[cfg(target_os = "linux")]
    use crate::op::fs::FsOp;
    #[cfg(target_os = "linux")]
    use crate::sys::current::fs as sys_fs;
    use crate::sys::handle::{OwnedFile, raw_file};

    pub(super) fn duplicate_stdin() -> io::Result<OwnedFile> {
        duplicate_fd(libc::STDIN_FILENO)
    }

    pub(super) fn duplicate_stdout() -> io::Result<OwnedFile> {
        duplicate_fd(libc::STDOUT_FILENO)
    }

    pub(super) fn duplicate_stderr() -> io::Result<OwnedFile> {
        duplicate_fd(libc::STDERR_FILENO)
    }

    pub(super) fn blocking_stdin_read(source: &OwnedFile, buffer: &mut [u8]) -> io::Result<usize> {
        blocking_read(raw_file(source), buffer)
    }

    pub(super) fn should_retry_stdin_read(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::Interrupted
    }

    pub(super) fn stdin_handoff_requires_idle(_source: &OwnedFile) -> bool {
        false
    }

    pub(super) fn standard_write_future(fd: Arc<OwnedFile>, data: Vec<u8>) -> PendingStandardWrite {
        #[cfg(target_os = "linux")]
        {
            Box::pin(async move {
                sys_fs::write(FsOp::Write {
                    fd: raw_file(&fd),
                    offset: None,
                    data,
                })
                .await
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            Box::pin(async move { offload(move || blocking_write(raw_file(&fd), &data)).await })
        }
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

    #[cfg(not(target_os = "linux"))]
    fn blocking_write(fd: RawFd, data: &[u8]) -> io::Result<usize> {
        loop {
            // SAFETY: the accepted blocking job owns the descriptor through an
            // `Arc<OwnedFile>` for this call, and `data` is initialized.
            let written =
                unsafe { libc::write(fd, data.as_ptr().cast::<libc::c_void>(), data.len()) };
            if written >= 0 {
                return Ok(written as usize);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
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
    use core::ffi::c_void;
    use std::io;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use std::sync::Arc;

    use windows_sys::Win32::Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_BROKEN_PIPE, ERROR_OPERATION_ABORTED,
        GetLastError, HANDLE, INVALID_HANDLE_VALUE, SetLastError,
    };
    use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
    use windows_sys::Win32::System::Console::{
        GetConsoleMode, GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    use super::{PendingStandardWrite, offload};
    use crate::sys::handle::{OwnedFile, RawFile, raw_file};

    #[repr(C)]
    struct IoStatusBlock {
        status: isize,
        information: usize,
    }

    #[repr(C)]
    struct FileModeInformation {
        mode: u32,
    }

    #[link(name = "ntdll")]
    unsafe extern "system" {
        fn RtlNtStatusToDosError(status: i32) -> u32;
        fn NtQueryInformationFile(
            file_handle: HANDLE,
            io_status_block: *mut IoStatusBlock,
            file_information: *mut c_void,
            length: u32,
            file_information_class: i32,
        ) -> i32;
    }

    pub(super) fn duplicate_stdin() -> io::Result<OwnedFile> {
        let source = duplicate_std_handle(STD_INPUT_HANDLE)?;
        validate_stdin_handle(&source)?;
        Ok(source)
    }

    pub(super) fn duplicate_stdout() -> io::Result<OwnedFile> {
        duplicate_std_handle(STD_OUTPUT_HANDLE)
    }

    pub(super) fn duplicate_stderr() -> io::Result<OwnedFile> {
        duplicate_std_handle(STD_ERROR_HANDLE)
    }

    pub(super) fn blocking_stdin_read(source: &OwnedFile, buffer: &mut [u8]) -> io::Result<usize> {
        blocking_read(raw_file(source), buffer)
    }

    pub(super) fn should_retry_stdin_read(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::Interrupted
            || error.raw_os_error() == Some(ERROR_OPERATION_ABORTED as i32)
    }

    pub(super) fn stdin_handoff_requires_idle(source: &OwnedFile) -> bool {
        let mut console_mode = 0u32;
        // SAFETY: `source` owns the queried handle and `console_mode` is a
        // valid out-pointer.
        (unsafe { GetConsoleMode(raw_file(source).as_handle(), &mut console_mode) }) != 0
    }

    pub(super) fn validate_stdin_handle(source: &OwnedFile) -> io::Result<()> {
        const FILE_MODE_INFORMATION_CLASS: i32 = 16;
        const FILE_SYNCHRONOUS_IO_ALERT: u32 = 0x10;
        const FILE_SYNCHRONOUS_IO_NONALERT: u32 = 0x20;

        let handle = raw_file(source).as_handle();
        if stdin_handoff_requires_idle(source) {
            return Ok(());
        }

        let mut status = IoStatusBlock {
            status: 0,
            information: 0,
        };
        let mut mode = FileModeInformation { mode: 0 };
        // SAFETY: `mode` matches FileModeInformation, and `source` keeps
        // `handle` live for the duration of the query.
        let result = unsafe {
            NtQueryInformationFile(
                handle,
                &mut status,
                (&raw mut mode).cast(),
                std::mem::size_of::<FileModeInformation>() as u32,
                FILE_MODE_INFORMATION_CLASS,
            )
        };
        if result != 0 {
            // SAFETY: `result` came directly from an NT API.
            let error = unsafe { RtlNtStatusToDosError(result) };
            return Err(io::Error::from_raw_os_error(error as i32));
        }
        if mode.mode & (FILE_SYNCHRONOUS_IO_ALERT | FILE_SYNCHRONOUS_IO_NONALERT) == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overlapped Windows stdin handles are not supported by the dedicated reader",
            ));
        }
        Ok(())
    }

    pub(super) fn standard_write_future(fd: Arc<OwnedFile>, data: Vec<u8>) -> PendingStandardWrite {
        Box::pin(async move { offload(move || blocking_write(raw_file(&fd), &data)).await })
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
        Ok(OwnedFile::unbound(unsafe {
            OwnedHandle::from_raw_handle(duplicated)
        }))
    }

    pub(super) fn blocking_read(fd: RawFile, buffer: &mut [u8]) -> io::Result<usize> {
        let mut read = 0u32;
        // SAFETY: clearing the current thread's last-error slot lets a
        // successful zero-byte console read report a fresh abort status.
        unsafe { SetLastError(0) };
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
        // SAFETY: no Win32 call intervened after ReadFile.
        let error = unsafe { GetLastError() };
        finish_blocking_read(ok, read, error)
    }

    pub(super) fn finish_blocking_read(ok: i32, read: u32, error: u32) -> io::Result<usize> {
        if ok != 0 {
            if read == 0 && error == ERROR_OPERATION_ABORTED {
                return Err(io::Error::from_raw_os_error(error as i32));
            }
            return Ok(read as usize);
        }
        if error == ERROR_BROKEN_PIPE {
            return Ok(0);
        }
        Err(io::Error::from_raw_os_error(error as i32))
    }

    pub(super) fn blocking_write(fd: RawFile, data: &[u8]) -> io::Result<usize> {
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
    use core::future::Future;
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::future::poll_fn;
    use std::io;
    use std::sync::{Arc, Mutex};

    use crate::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    struct PendingOnce<T> {
        pending: bool,
        result: Option<io::Result<T>>,
    }

    impl<T> PendingOnce<T> {
        fn new(result: io::Result<T>) -> Self {
            Self {
                pending: true,
                result: Some(result),
            }
        }
    }

    impl<T: Unpin> Future for PendingOnce<T> {
        type Output = io::Result<T>;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.pending {
                self.pending = false;
                Poll::Pending
            } else {
                Poll::Ready(self.result.take().expect("polled after completion"))
            }
        }
    }

    #[test]
    fn blocking_offload_panic_terminalizes_its_completion() {
        let observed = Arc::new(Mutex::new(None::<io::ErrorKind>));
        {
            let observed = Arc::clone(&observed);
            crate::spawn(async move {
                let error = offload(|| -> io::Result<()> {
                    panic!("blocking standard I/O test panic");
                })
                .await
                .expect_err("panic must become a terminal I/O error");
                *observed.lock().unwrap() = Some(error.kind());
            });
        }

        crate::run();
        assert_eq!(*observed.lock().unwrap(), Some(io::ErrorKind::Other));
    }

    #[test]
    fn failed_stdin_initialization_can_be_retried_and_success_is_cached() {
        let cell = OnceLock::new();
        let init_lock = Mutex::new(());

        let error = get_or_try_init(&cell, &init_lock, || {
            Err::<Arc<usize>, _>(io::Error::new(
                io::ErrorKind::WouldBlock,
                "transient setup failure",
            ))
        })
        .expect_err("the injected first initialization must fail");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(cell.get().is_none());

        let initialized =
            get_or_try_init(&cell, &init_lock, || Ok(Arc::new(42))).expect("retry succeeds");
        let cached = get_or_try_init(&cell, &init_lock, || {
            panic!("a successful initialization must be cached")
        })
        .expect("cached value");
        assert!(Arc::ptr_eq(&initialized, &cached));
    }

    #[test]
    fn cancelled_stdin_read_preserves_later_input_and_retains_its_operation() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        let mut abandoned = [0u8; 8];

        crate::block_on(async {
            let mut pending = Box::pin(input.read(&mut abandoned));
            poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(pending);
            // The operation is retained rather than abandoned, which is what
            // makes the cancelled read cancel-safe: the next read claims it.
            assert_eq!(reader.waiter_count(), 1);

            write_test_pipe(&mut writer, b"kept").expect("write after cancellation");
            let mut observed = [0u8; 4];
            assert_eq!(
                input.read(&mut observed).await.expect("replacement read"),
                4
            );
            assert_eq!(&observed, b"kept");
        });

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    /// A read cancelled in one mode leaves an operation the *other* mode can
    /// claim, and claiming it makes progress rather than waiting.
    ///
    /// `ReadState` resumes a pending operation without consulting the new
    /// caller's mode, because the mode is bound when the operation starts. That
    /// is sound here only because `stop_at_newline` bounds how much the reader
    /// takes rather than making it wait for a newline — so a byte read that
    /// inherits a line-mode operation gets a possibly-shorter read, which its
    /// contract already allows, and never a stall.
    #[test]
    fn a_byte_read_can_claim_a_cancelled_line_read_s_operation() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);

        // Park a line read with nothing to read, so a line-mode operation is
        // pending and no bytes are buffered on the handle.
        let mut pending = Box::pin(input.next_line());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        drop(pending);
        assert_eq!(
            reader.waiter_count(),
            1,
            "the cancelled line read should leave its operation claimable"
        );

        // Bytes with no newline: a reader that waited for one would never
        // return.
        write_test_pipe(&mut writer, b"abc").expect("write unterminated bytes");

        let mut observed = [0u8; 8];
        let read = crate::block_on(async {
            crate::time::timeout(std::time::Duration::from_secs(5), input.read(&mut observed))
                .await
                .expect("claiming the operation must make progress, not stall")
        })
        .expect("byte read should succeed");
        assert_eq!(&observed[..read], b"abc");

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn stdin_reader_does_not_consume_without_demand() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        assert!(reader.wait_for_idle(std::time::Duration::from_secs(5)));
        write_test_pipe(&mut writer, b"held").expect("write idle stdin bytes");
        assert_eq!(reader.buffered_len(), 0);

        let mut observed = [0u8; 4];
        assert_eq!(
            crate::block_on(input.read(&mut observed)).expect("demanded read"),
            4
        );
        assert_eq!(&observed, b"held");

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn inherited_stdin_handoff_pauses_and_resumes_pending_reader() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        let mut byte = [0u8; 1];
        let mut pending = Box::pin(input.read(&mut byte));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        assert!(reader.wait_for_active(std::time::Duration::from_secs(5)));

        reader
            .pause_for_handoff()
            .expect("interruptible reader should pause");
        write_test_pipe(&mut writer, b"x").expect("write while handed off");
        assert_eq!(reader.buffered_len(), 0);
        reader.resume_after_handoff();

        assert!(reader.wait_for_buffered(1, std::time::Duration::from_secs(5)));
        assert!(matches!(pending.as_mut().poll(&mut cx), Poll::Ready(Ok(1))));
        drop(pending);
        assert_eq!(&byte, b"x");

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn cancelled_stdin_next_line_retains_its_partial_prefix() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        write_test_pipe(&mut writer, b"par").expect("write partial line");
        let mut pending = Box::pin(input.next_line());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        assert!(reader.wait_for_buffered(3, std::time::Duration::from_secs(5)));
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        drop(pending);
        // Retained, not abandoned: the next line read resumes the same
        // line-mode operation.
        assert_eq!(reader.waiter_count(), 1);

        write_test_pipe(&mut writer, b"tial\n").expect("finish partial line");
        assert_eq!(
            crate::block_on(input.next_line())
                .expect("replacement line read")
                .as_deref(),
            Some("partial\n")
        );

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn cancelled_next_line_prefix_precedes_inherent_and_trait_reads() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        write_test_pipe(&mut writer, b"prefix").expect("write partial line");
        let mut pending = Box::pin(input.next_line());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        assert!(reader.wait_for_buffered(6, std::time::Duration::from_secs(5)));
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        drop(pending);

        write_test_pipe(&mut writer, b"-tail").expect("write bytes after prefix");
        let mut inherent = [0u8; 3];
        assert_eq!(
            crate::block_on(input.read(&mut inherent)).expect("inherent prefix read"),
            3
        );
        assert_eq!(&inherent, b"pre");

        let mut trait_read = [0u8; 8];
        assert!(matches!(
            Pin::new(&mut input).poll_read(&mut cx, &mut trait_read),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(&trait_read[..3], b"fix");

        let mut tail = [0u8; 5];
        assert_eq!(
            crate::block_on(input.read(&mut tail)).expect("shared tail read"),
            5
        );
        assert_eq!(&tail, b"-tail");

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn standard_writer_does_not_reuse_an_abandoned_write_count() {
        let path = std::env::current_dir()
            .expect("current directory")
            .join("target")
            .join(format!("stdio-pending-write-{}", std::process::id()));
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            let path = path.clone();
            crate::spawn(async move {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("open fixture");
                #[cfg(unix)]
                let fd: OwnedFile = file.into();
                #[cfg(windows)]
                let fd = OwnedFile::unbound(std::os::windows::io::OwnedHandle::from(file));
                let mut writer = StandardWriter::new(fd);
                let old = b"old".to_vec();
                let old_generation = crate::io::next_operation_id();

                poll_fn(|cx| {
                    assert!(
                        writer
                            .write_state
                            .poll_write(cx, old_generation, &old, |_| {
                                Box::pin(PendingOnce::new(Ok(old.len())))
                            })
                            .is_pending()
                    );
                    Poll::Ready(())
                })
                .await;

                assert_eq!(writer.write(b"new bytes").await.expect("new write"), 9);
                drop(writer);
                *observed.lock().unwrap() = Some(std::fs::read(&path).expect("read fixture"));
                std::fs::remove_file(&path).expect("remove fixture");
            });
        }

        crate::run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"new bytes".as_slice())
        );
    }

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

        // The line discipline may deliver the translated output (`\n` → `\r\n`)
        // to the master in more than one chunk; read until the full line is in.
        let expected = b"tty output\r\n";
        let mut buffer = [0u8; 64];
        let mut filled = 0;
        while filled < expected.len() {
            let read = imp::blocking_read(
                std::os::fd::AsRawFd::as_raw_fd(&master),
                &mut buffer[filled..],
            )
            .expect("pty master should read");
            assert_ne!(read, 0, "pty master hit EOF before the full line arrived");
            filled += read;
        }
        assert_eq!(&buffer[..filled], expected);
        drop(slave_keepalive);
    }

    #[cfg(unix)]
    #[test]
    fn stdin_reads_single_byte_from_tty_fd() {
        let (master, slave) = open_pty();
        let reader =
            stdin_reader::StdinReader::spawn_for_test(slave, stdin_reader::BUFFER_CAPACITY)
                .expect("spawn pty stdin reader");
        write_fd(std::os::fd::AsRawFd::as_raw_fd(&master), b"x\n")
            .expect("pty master should write input");

        let mut input = Stdin::from_reader(Arc::clone(&reader));
        let mut byte = [0u8; 1];
        let read = crate::block_on(input.read(&mut byte))
            .expect("single-byte tty stdin read should succeed");
        assert_eq!(&byte[..read], b"x");
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn stdin_read_ahead_survives_sequential_handles() {
        let (mut first, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        let mut second = Stdin::from_reader(Arc::clone(&reader));
        write_test_pipe(&mut writer, b"abcdef").expect("write read-ahead bytes");

        let mut prefix = [0u8; 2];
        assert_eq!(
            crate::block_on(first.read(&mut prefix)).expect("first handle read"),
            2
        );
        assert_eq!(&prefix, b"ab");
        assert_eq!(reader.buffered_len(), 4);
        drop(first);

        let mut suffix = [0u8; 4];
        assert_eq!(
            crate::block_on(second.read(&mut suffix)).expect("second handle read"),
            4
        );
        assert_eq!(&suffix, b"cdef");

        drop(writer);
        let mut eof = [0u8; 1];
        assert_eq!(
            crate::block_on(second.read(&mut eof)).expect("stdin EOF"),
            0
        );
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn stdin_next_line_preserves_read_ahead_and_reports_invalid_utf8() {
        let (mut first, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        let mut second = Stdin::from_reader(Arc::clone(&reader));
        write_test_pipe(&mut writer, b"first\nsecond\nbad \xff\n").expect("write line input");
        drop(writer);

        crate::block_on(async {
            assert_eq!(
                first.next_line().await.expect("first line").as_deref(),
                Some("first\n")
            );
            assert_eq!(
                second.next_line().await.expect("second line").as_deref(),
                Some("second\n")
            );
            assert_eq!(
                second
                    .next_line()
                    .await
                    .expect_err("invalid UTF-8 should fail")
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert!(
                second
                    .next_line()
                    .await
                    .expect("EOF after bad line")
                    .is_none()
            );
        });

        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn multiple_pending_stdin_handles_consume_each_byte_once() {
        let (mut first, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        let mut second = Stdin::from_reader(Arc::clone(&reader));
        let mut first_byte = [0u8; 1];
        let mut second_byte = [0u8; 1];
        let mut first_read = Box::pin(first.read(&mut first_byte));
        let mut second_read = Box::pin(second.read(&mut second_byte));
        let mut cx = Context::from_waker(std::task::Waker::noop());

        assert!(first_read.as_mut().poll(&mut cx).is_pending());
        assert!(second_read.as_mut().poll(&mut cx).is_pending());
        assert_eq!(reader.waiter_count(), 2);

        write_test_pipe(&mut writer, b"xy").expect("write waiter bytes");
        assert!(reader.wait_for_buffered(2, std::time::Duration::from_secs(5)));
        assert!(matches!(
            first_read.as_mut().poll(&mut cx),
            Poll::Ready(Ok(1))
        ));
        assert!(matches!(
            second_read.as_mut().poll(&mut cx),
            Poll::Ready(Ok(1))
        ));
        drop(first_read);
        drop(second_read);

        let mut observed = [first_byte[0], second_byte[0]];
        observed.sort_unstable();
        assert_eq!(&observed, b"xy");
        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn stdin_drains_buffer_before_reporting_eof() {
        let (mut input, reader, mut writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        write_test_pipe(&mut writer, b"tail").expect("write EOF fixture");
        drop(writer);

        crate::block_on(async {
            let mut bytes = [0u8; 8];
            assert_eq!(input.read(&mut bytes).await.expect("buffered tail"), 4);
            assert_eq!(&bytes[..4], b"tail");
            assert_eq!(input.read(&mut bytes).await.expect("EOF"), 0);
            assert_eq!(input.read(&mut bytes).await.expect("stable EOF"), 0);
        });

        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
        assert!(reader.interrupt_released());
    }

    #[test]
    fn stdin_reader_errors_are_terminal_and_repeatable() {
        fn fail_read(_source: &OwnedFile, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected stdin failure",
            ))
        }

        let (source, mut writer) = test_pipe();
        write_test_pipe(&mut writer, b"x").expect("make test source readable");
        let reader = stdin_reader::StdinReader::spawn_with_reader_for_test(
            source,
            stdin_reader::BUFFER_CAPACITY,
            fail_read,
        )
        .expect("spawn failing stdin reader");
        let mut input = Stdin::from_reader(Arc::clone(&reader));

        crate::block_on(async {
            let mut byte = [0u8; 1];
            for _ in 0..2 {
                let error = input
                    .read(&mut byte)
                    .await
                    .expect_err("terminal reader error");
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert_eq!(error.to_string(), "injected stdin failure");
            }
        });

        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
        assert!(reader.interrupt_released());
    }

    #[test]
    fn transient_stdin_read_errors_are_retried() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CALLS: AtomicUsize = AtomicUsize::new(0);

        fn interrupt_once(source: &OwnedFile, buffer: &mut [u8]) -> io::Result<usize> {
            if CALLS.fetch_add(1, Ordering::AcqRel) == 0 {
                Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "injected transient interruption",
                ))
            } else {
                imp::blocking_stdin_read(source, buffer)
            }
        }

        CALLS.store(0, Ordering::Release);
        let (source, mut writer) = test_pipe();
        let reader = stdin_reader::StdinReader::spawn_with_reader_for_test(
            source,
            stdin_reader::BUFFER_CAPACITY,
            interrupt_once,
        )
        .expect("spawn retrying stdin reader");
        let mut input = Stdin::from_reader(Arc::clone(&reader));
        write_test_pipe(&mut writer, b"x").expect("write retry fixture");

        let mut byte = [0u8; 1];
        assert_eq!(
            crate::block_on(input.read(&mut byte)).expect("read after interruption"),
            1
        );
        assert_eq!(&byte, b"x");
        assert!(CALLS.load(Ordering::Acquire) >= 2);

        drop(writer);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[test]
    fn dedicated_stdin_buffer_applies_bounded_backpressure() {
        const CAPACITY: usize = 8;
        let data = *b"0123456789abcdef";
        let (mut input, reader, mut writer) = test_stdin(CAPACITY);
        let mut first_byte = [0u8; 1];
        let mut pending = Box::pin(input.read(&mut first_byte));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        write_test_pipe(&mut writer, &data).expect("write bounded-buffer fixture");
        assert!(reader.wait_for_buffered(CAPACITY, std::time::Duration::from_secs(5)));
        assert_eq!(reader.buffered_len(), CAPACITY);
        assert_eq!(reader.max_buffered_len(), CAPACITY);
        drop(pending);
        drop(writer);

        let observed = crate::block_on(async {
            let mut observed = Vec::new();
            let mut chunk = [0u8; 3];
            loop {
                let read = input.read(&mut chunk).await.expect("bounded stdin read");
                if read == 0 {
                    break;
                }
                observed.extend_from_slice(&chunk[..read]);
            }
            observed
        });

        assert_eq!(observed, data);
        assert!(reader.max_buffered_len() <= CAPACITY);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    /// Regression: `shutdown_and_wait` returns the instant `thread_exited` is
    /// published, so everything that flag promises must already be done. The
    /// interrupt used to be released *after* publication, letting a caller
    /// observe an exited reader whose interrupt was still installed — a window
    /// narrow enough to surface as roughly 1 failure in 300 on Windows.
    ///
    /// Asserting the post-condition after `shutdown_and_wait` only samples the
    /// race; this checks the ordering itself, recorded while both locks are
    /// held.
    #[test]
    fn interrupt_is_released_before_thread_exit_is_published() {
        let (_input, reader, _writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);

        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
        assert!(
            reader.interrupt_released_at_exit_publish(),
            "the interrupt must already be released when thread exit is published"
        );
        assert!(reader.interrupt_released());
    }

    #[test]
    fn stdin_shutdown_wakes_pending_reads_and_stops_its_thread() {
        let (mut input, reader, _writer) = test_stdin(stdin_reader::BUFFER_CAPACITY);
        let mut byte = [0u8; 1];
        let mut pending = Box::pin(input.read(&mut byte));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());

        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
        let Poll::Ready(Err(error)) = pending.as_mut().poll(&mut cx) else {
            panic!("shutdown must terminalize the pending read");
        };
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(reader.interrupt_released());
    }

    #[test]
    fn stdin_source_drops_only_after_reader_thread_exits() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let (source, _writer) = test_pipe();
        let source_dropped = Arc::new(AtomicBool::new(false));
        let source_dropped_by_reader = Arc::clone(&source_dropped);
        let reader = stdin_reader::StdinReader::spawn_with_drop_hook_for_test(
            source,
            stdin_reader::BUFFER_CAPACITY,
            move || source_dropped_by_reader.store(true, Ordering::Release),
        )
        .expect("spawn drop-observed reader");
        let mut input = Stdin::from_reader(Arc::clone(&reader));
        let mut byte = [0u8; 1];
        let mut pending = Box::pin(input.read(&mut byte));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        drop(pending);
        drop(input);
        assert!(!source_dropped.load(Ordering::Acquire));

        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
        assert!(reader.interrupt_released());
        assert!(source_dropped.load(Ordering::Acquire));
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
        let empty = [IoSlice::new(&[]), IoSlice::new(&[])];

        assert!(Pin::new(&mut out).poll_flush(&mut cx).is_ready());
        assert!(Pin::new(&mut out).poll_close(&mut cx).is_ready());
        assert!(matches!(
            Pin::new(&mut out).poll_write_vectored_operation(&mut cx, &empty, 1),
            Poll::Ready(Ok(0))
        ));
        assert!(Pin::new(&mut err).poll_flush(&mut cx).is_ready());
        assert!(Pin::new(&mut err).poll_close(&mut cx).is_ready());
        assert!(matches!(
            Pin::new(&mut err).poll_write_vectored_operation(&mut cx, &empty, 2),
            Poll::Ready(Ok(0))
        ));
    }

    #[cfg(windows)]
    #[test]
    fn windows_stdin_operation_abort_is_retryable() {
        use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

        let error = imp::finish_blocking_read(1, 0, ERROR_OPERATION_ABORTED)
            .expect_err("successful zero-byte aborted console read must not become EOF");
        assert!(imp::should_retry_stdin_read(&error));
        assert_eq!(
            imp::finish_blocking_read(1, 0, 0).expect("clean zero-byte read is EOF"),
            0
        );
    }

    #[test]
    fn active_console_policy_rejects_inherited_handoff() {
        let (source, _writer) = test_pipe();
        let reader = stdin_reader::StdinReader::spawn_with_idle_handoff_for_test(
            source,
            stdin_reader::BUFFER_CAPACITY,
        )
        .expect("spawn console-policy reader");
        assert!(reader.wait_for_idle(std::time::Duration::from_secs(5)));
        reader
            .pause_for_handoff()
            .expect("idle console reader can be handed off");
        reader.resume_after_handoff();

        let mut input = Stdin::from_reader(Arc::clone(&reader));
        let mut byte = [0u8; 1];
        let mut pending = Box::pin(input.read(&mut byte));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        assert!(reader.wait_for_active(std::time::Duration::from_secs(5)));
        let error = reader
            .pause_for_handoff()
            .expect_err("active console read cannot be handed off safely");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

        drop(pending);
        drop(input);
        assert!(reader.shutdown_and_wait(std::time::Duration::from_secs(5)));
    }

    #[cfg(windows)]
    #[test]
    fn windows_overlapped_named_pipe_stdin_is_rejected() {
        use std::os::windows::io::{FromRawHandle, OwnedHandle};
        use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;
        use windows_sys::Win32::System::Pipes::{
            CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_TYPE_BYTE, PIPE_WAIT,
        };

        let (synchronous, _writer) = test_pipe();
        imp::validate_stdin_handle(&synchronous)
            .expect("synchronous anonymous-pipe stdin should be supported");

        const PIPE_ACCESS_INBOUND: u32 = 0x0000_0001;
        let name = format!(
            r"\\.\pipe\runite-stdin-mode-{}",
            crate::io::next_operation_id()
        )
        .encode_utf16()
        .chain(Some(0))
        .collect::<Vec<_>>();
        // SAFETY: `name` is NUL-terminated, all sizes are finite, and null
        // security attributes request the process defaults.
        let handle = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                1,
                0,
                4096,
                0,
                std::ptr::null_mut(),
            )
        };
        assert_ne!(
            handle,
            INVALID_HANDLE_VALUE,
            "create overlapped named pipe: {}",
            io::Error::last_os_error()
        );
        // SAFETY: successful CreateNamedPipeW returned a fresh owned handle.
        let source = OwnedFile::unbound(unsafe { OwnedHandle::from_raw_handle(handle) });
        let error = imp::validate_stdin_handle(&source)
            .expect_err("overlapped stdin must not use synchronous ReadFile");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    fn test_stdin(capacity: usize) -> (Stdin, Arc<stdin_reader::StdinReader>, TestPipeWriter) {
        let (source, writer) = test_pipe();
        let reader = stdin_reader::StdinReader::spawn_for_test(source, capacity)
            .expect("spawn test stdin reader");
        (Stdin::from_reader(Arc::clone(&reader)), reader, writer)
    }

    #[cfg(unix)]
    type TestPipeWriter = std::os::unix::net::UnixStream;

    #[cfg(unix)]
    fn test_pipe() -> (OwnedFile, TestPipeWriter) {
        use std::os::fd::{FromRawFd, IntoRawFd};

        let (source, writer) =
            std::os::unix::net::UnixStream::pair().expect("create stdin test socket pair");
        let raw = source.into_raw_fd();
        // SAFETY: `into_raw_fd` transferred sole ownership of `raw`.
        let source = unsafe { OwnedFile::from_raw_fd(raw) };
        (source, writer)
    }

    #[cfg(unix)]
    fn write_test_pipe(writer: &mut TestPipeWriter, data: &[u8]) -> io::Result<()> {
        std::io::Write::write_all(writer, data)
    }

    #[cfg(windows)]
    type TestPipeWriter = OwnedFile;

    #[cfg(windows)]
    fn test_pipe() -> (OwnedFile, TestPipeWriter) {
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::System::Pipes::CreatePipe;

        let mut source: HANDLE = std::ptr::null_mut();
        let mut writer: HANDLE = std::ptr::null_mut();
        // SAFETY: both handles are valid out-pointers; null security attributes
        // request non-inheritable handles with the default buffer size.
        let ok = unsafe { CreatePipe(&mut source, &mut writer, std::ptr::null_mut(), 0) };
        assert_ne!(
            ok,
            0,
            "create stdin test pipe: {}",
            io::Error::last_os_error()
        );
        // SAFETY: successful `CreatePipe` returned two fresh owned handles.
        let source = OwnedFile::unbound(unsafe {
            std::os::windows::io::OwnedHandle::from_raw_handle(source)
        });
        // SAFETY: successful `CreatePipe` returned two fresh owned handles.
        let writer = OwnedFile::unbound(unsafe {
            std::os::windows::io::OwnedHandle::from_raw_handle(writer)
        });
        (source, writer)
    }

    #[cfg(windows)]
    fn write_test_pipe(writer: &mut TestPipeWriter, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let written = imp::blocking_write(crate::sys::handle::raw_file(writer), data)?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "stdin test pipe write made no progress",
                ));
            }
            data = &data[written..];
        }
        Ok(())
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

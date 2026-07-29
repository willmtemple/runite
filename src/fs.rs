//! Portable async filesystem primitives.
//!
//! This module provides file, directory, and metadata operations that integrate
//! with runite's event-loop-per-thread runtime. The public surface intentionally
//! mirrors [`std::fs`] where that shape makes sense, while using async methods
//! for operations that may block the caller.
//!
//! runite futures and handles are thread-affine: create and poll filesystem
//! futures on the runtime thread that owns them. Tasks do not migrate between
//! runtime threads, and there is no work-stealing scheduler. Same-thread wakeups
//! resume as microtasks, while backend completions and blocking-pool callbacks
//! re-enter the runtime as macrotasks.
//!
//! # Backend model
//!
//! On Linux, regular filesystem operations use the runtime's `io_uring`
//! completion backend where an opcode exists. On macOS aarch64, filesystem work
//! is offloaded to runite's blocking thread pool so slow disk or metadata calls
//! do not block the event-loop thread. On Windows, file reads and writes run as
//! overlapped operations on the thread's I/O completion port, while open,
//! metadata, directory, flush, and truncation work is offloaded to the blocking
//! pool. Directory iteration is also blocking-pool-backed on Linux because
//! `std::fs::read_dir`/directory scanning can block and is not modeled as an
//! `io_uring` operation here.
//!
//! This differs from Tokio's default multi-threaded scheduler and async-std:
//! runite keeps tasks on a JavaScript-style, thread-local event loop and mixes
//! completion-based filesystem I/O with blocking-pool offload only where the
//! platform requires it.
//!
//! Cancellation semantics:
//! - Dropping an I/O future cancels interest in the result.
//! - The runtime issues best-effort kernel cancellation where supported.
//! - The underlying OS operation may still complete after the future is dropped.
//! - Dropping a [`ReadDir`] cancels its blocking-pool producer, discards queued
//!   entries, and drops any stored directory iterator. A bounded batch already
//!   running may still need to finish its current filesystem call first.
//!
//! # Examples
//!
//! File examples perform real filesystem I/O, so this example is compile-tested
//! but not run by doctests:
//!
//! ```no_run
//! runite::spawn(async {
//!     runite::fs::write("runite-example.txt", b"hello").await.unwrap();
//!     let contents = runite::fs::read_to_string("runite-example.txt").await.unwrap();
//!     assert_eq!(contents, "hello");
//!     runite::fs::remove_file("runite-example.txt").await.unwrap();
//! });
//! runite::run();
//! ```

use alloc::rc::Rc;
use alloc::sync::Arc;

use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::io::{self, IoSlice};
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};

use crate::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite, AsyncWriteExt, CursorState, Stream,
    WriteOperation,
};

#[cfg(target_os = "linux")]
pub mod watch;

use crate::op::fs::{
    FileType as RawFileType, FsOp, MetadataTarget, OpenOptions as OpOpenOptions,
    RawDirEntry as OpDirEntry, RawMetadata,
};
use crate::platform::current::runtime::{ThreadHandle, current_thread_handle};
use crate::sys::blocking::spawn_blocking;
use crate::sys::current::fs as sys_fs;
use crate::sys::handle::{OwnedFile, RawFile, raw_file};

struct FileInner {
    fd: OwnedFile,
}

/// Async file handle.
///
/// `File` supports both cursor-based sequential I/O and offset-based positioned
/// I/O. It is not [`Clone`]; use [`try_clone`](Self::try_clone) to duplicate the
/// underlying file descriptor asynchronously. As with [`std::fs::File::try_clone`],
/// duplicated handles share the kernel-managed file cursor.
/// Sequential reads, writes, and seeks are also available through
/// [`AsyncRead`], [`AsyncWrite`], and [`AsyncSeek`].
///
/// Use [`File::open`] and [`File::create`] for common cases or [`OpenOptions`]
/// for detailed access-mode control.
pub struct File {
    // Pending operations must be dropped before the descriptor owner.
    state: Rc<RefCell<CursorState>>,
    direct_write: Option<WriteOperation>,
    inner: Arc<FileInner>,
}

impl std::fmt::Debug for File {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("File").finish_non_exhaustive()
    }
}

/// Builder used to configure how a [`File`] is opened.
///
/// Options mirror [`std::fs::OpenOptions`]: callers opt in to read, write,
/// append, truncation, and creation behavior before calling
/// [`open`](Self::open).
pub struct OpenOptions {
    inner: OpOpenOptions,
}

impl std::fmt::Debug for OpenOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenOptions")
            .finish_non_exhaustive()
    }
}

/// File metadata returned by [`metadata`] or [`File::metadata`].
///
/// Metadata exposes the file type, byte length, and platform mode bits reported
/// by the active backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Metadata {
    inner: RawMetadata,
}

/// Async directory-entry stream returned by [`read_dir`].
///
/// Call [`next_entry`](Self::next_entry) to pull entries one at a time, or use
/// the [`Stream`] implementation with the runtime's stream extension traits.
///
/// The stream starts an eager blocking-pool batch when it is created. Entries
/// cross a fixed-size bounded queue. Each blocking-pool job advances the
/// directory iterator only until the queue is full or one bounded batch is
/// complete, then returns its worker to the shared pool. Draining the queue
/// requests another batch.
///
/// Dropping the stream marks the scan cancelled, discards buffered entries,
/// drops the stored iterator, and releases runtime liveness. Cancellation is
/// cooperative around filesystem calls: an iterator call already executing in
/// the OS may finish before the blocking job observes the drop.
pub struct ReadDir {
    inner: sys_fs::ReadDirStream,
}

impl std::fmt::Debug for ReadDir {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ReadDir").finish_non_exhaustive()
    }
}

/// Directory entry yielded by [`ReadDir::next_entry`].
///
/// Each entry carries its path and file name and can resolve fresh metadata on
/// demand with [`metadata`](Self::metadata).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirEntry {
    inner: OpDirEntry,
}

const READ_DIR_BUFFER_CAPACITY: usize = 32;

pub(crate) struct ReadDirStream {
    consumer: ReadDirConsumer<OpDirEntry>,
}

impl ReadDirStream {
    pub(crate) fn new(path: PathBuf) -> io::Result<Self> {
        let consumer = read_dir_channel(
            current_thread_handle(),
            READ_DIR_BUFFER_CAPACITY,
            move || {
                std::fs::read_dir(path).map(|entries| {
                    entries.map(|entry| {
                        entry.map(|entry| {
                            let file_name = entry.file_name();
                            OpDirEntry {
                                path: entry.path(),
                                file_name,
                            }
                        })
                    })
                })
            },
        )?;

        Ok(Self { consumer })
    }

    pub(crate) async fn next_entry(&mut self) -> io::Result<Option<OpDirEntry>> {
        core::future::poll_fn(|cx| self.consumer.poll_next(cx)).await
    }
}

struct ReadDirConsumer<T> {
    shared: Arc<ReadDirShared<T>>,
}

type ReadDirIterator<T> = Box<dyn Iterator<Item = io::Result<T>> + Send + 'static>;
type ReadDirOpen<T> = Box<dyn FnOnce() -> io::Result<ReadDirIterator<T>> + Send + 'static>;
type ReadDirJob = Box<dyn FnOnce() + Send + 'static>;
type ReadDirScheduler = Arc<dyn Fn(ReadDirJob) -> io::Result<()> + Send + Sync + 'static>;

struct ReadDirShared<T> {
    state: Mutex<ReadDirQueue<T>>,
    capacity: usize,
    schedule: ReadDirScheduler,
    owner: ThreadHandle,
    pending: AtomicBool,
}

struct ReadDirBatchGuard<T> {
    shared: Arc<ReadDirShared<T>>,
    armed: bool,
}

struct ReadDirQueue<T> {
    source: Option<ReadDirSource<T>>,
    entries: VecDeque<io::Result<T>>,
    terminal_error: Option<io::Error>,
    waker: Option<Waker>,
    batch_active: bool,
    refill_requested: bool,
    done: bool,
    cancelled: bool,
    #[cfg(test)]
    peak_buffered: usize,
}

enum ReadDirSource<T> {
    Open(ReadDirOpen<T>),
    Entries(ReadDirIterator<T>),
}

fn read_dir_channel<T, I>(
    owner: ThreadHandle,
    capacity: usize,
    open: impl FnOnce() -> io::Result<I> + Send + 'static,
) -> io::Result<ReadDirConsumer<T>>
where
    T: Send + 'static,
    I: Iterator<Item = io::Result<T>> + Send + 'static,
{
    read_dir_channel_with_scheduler(owner, capacity, open, spawn_blocking)
}

fn read_dir_channel_with_scheduler<T, I>(
    owner: ThreadHandle,
    capacity: usize,
    open: impl FnOnce() -> io::Result<I> + Send + 'static,
    schedule: impl Fn(ReadDirJob) -> io::Result<()> + Send + Sync + 'static,
) -> io::Result<ReadDirConsumer<T>>
where
    T: Send + 'static,
    I: Iterator<Item = io::Result<T>> + Send + 'static,
{
    assert!(capacity > 0, "read_dir buffer capacity must be non-zero");
    owner.begin_async_operation();
    let shared = Arc::new(ReadDirShared {
        state: Mutex::new(ReadDirQueue {
            source: Some(ReadDirSource::Open(Box::new(move || {
                open().map(|entries| Box::new(entries) as ReadDirIterator<T>)
            }))),
            entries: VecDeque::with_capacity(capacity),
            terminal_error: None,
            waker: None,
            batch_active: false,
            refill_requested: false,
            done: false,
            cancelled: false,
            #[cfg(test)]
            peak_buffered: 0,
        }),
        capacity,
        schedule: Arc::new(schedule),
        owner,
        pending: AtomicBool::new(true),
    });

    let consumer = ReadDirConsumer {
        shared: Arc::clone(&shared),
    };
    if let Err(error) = shared.request_batch() {
        drop(consumer);
        return Err(error);
    }
    Ok(consumer)
}

impl<T: Send + 'static> ReadDirConsumer<T> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<T>>> {
        loop {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(entry) = state.entries.pop_front() {
                let request_refill = !state.done && state.entries.len() <= self.shared.capacity / 2;
                drop(state);
                if request_refill {
                    let _ = self.shared.request_batch();
                }
                return Poll::Ready(entry.map(Some));
            }
            if let Some(error) = state.terminal_error.take() {
                return Poll::Ready(Err(error));
            }
            if state.done {
                return Poll::Ready(Ok(None));
            }

            let old_waker = state.waker.replace(cx.waker().clone());
            drop(state);
            drop(old_waker);
            if self.shared.request_batch().is_err() {
                continue;
            }
            return Poll::Pending;
        }
    }

    #[cfg(test)]
    fn observer(&self) -> ReadDirObserver<T> {
        ReadDirObserver {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for ReadDirConsumer<T> {
    fn drop(&mut self) {
        self.shared.cancel();
    }
}

impl<T> ReadDirShared<T> {
    fn complete(&self, error: Option<io::Error>) {
        let (source, waker) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.done {
                (None, None)
            } else {
                state.done = true;
                state.batch_active = false;
                state.refill_requested = false;
                let source = state.source.take();
                if !state.cancelled {
                    state.terminal_error = error;
                    (source, state.waker.take())
                } else {
                    (source, None)
                }
            }
        };

        drop(source);
        if let Some(waker) = waker {
            waker.wake();
        }
        self.release_pending();
    }

    fn cancel(&self) {
        let (source, entries, terminal_error, waker) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.cancelled = true;
            state.done = true;
            state.batch_active = false;
            state.refill_requested = false;
            (
                state.source.take(),
                core::mem::take(&mut state.entries),
                state.terminal_error.take(),
                state.waker.take(),
            )
        };

        drop(source);
        drop(entries);
        drop(terminal_error);
        drop(waker);
        self.release_pending();
    }

    fn release_pending(&self) {
        if self.pending.swap(false, Ordering::AcqRel) {
            self.owner.finish_async_operation();
        }
    }
}

impl<T: Send + 'static> ReadDirShared<T> {
    fn request_batch(self: &Arc<Self>) -> io::Result<()> {
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.done || state.cancelled {
                return Ok(());
            }
            if state.batch_active {
                state.refill_requested = true;
                return Ok(());
            }
            if state.source.is_none() {
                return Ok(());
            }
            state.batch_active = true;
            state.refill_requested = false;
        }

        self.submit_batch()
    }

    fn submit_batch(self: &Arc<Self>) -> io::Result<()> {
        let shared = Arc::clone(self);
        match (self.schedule)(Box::new(move || shared.run_batch())) {
            Ok(()) => Ok(()),
            Err(error) => {
                let returned = io::Error::new(error.kind(), error.to_string());
                if !self.defer_failed_refill(&error) {
                    self.complete(Some(error));
                }
                Err(returned)
            }
        }
    }

    /// Whether a failed submission can be retried instead of ending the scan.
    ///
    /// A full blocking-pool queue is transient. If the consumer still has
    /// buffered entries it can make progress and drive another refill from a
    /// later poll, so a half-read directory need not fail. With nothing
    /// buffered there is no subsequent poll to retry from -- the consumer would
    /// spin or park forever -- so that case stays terminal.
    fn defer_failed_refill(&self, error: &io::Error) -> bool {
        if error.kind() != io::ErrorKind::WouldBlock {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.done || state.cancelled || state.entries.is_empty() {
            return false;
        }
        state.batch_active = false;
        state.refill_requested = true;
        true
    }

    fn run_batch(self: Arc<Self>) {
        let mut guard = ReadDirBatchGuard {
            shared: Arc::clone(&self),
            armed: true,
        };
        self.run_batch_inner();
        guard.armed = false;
    }

    fn run_batch_inner(self: &Arc<Self>) {
        let source = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.done || state.cancelled {
                state.batch_active = false;
                return;
            }
            state.source.take()
        };
        let Some(source) = source else {
            self.complete(Some(io::Error::other(
                "read_dir producer lost its iterator state",
            )));
            return;
        };

        let mut entries = match source {
            ReadDirSource::Open(open) => match open() {
                Ok(entries) => entries,
                Err(error) => {
                    self.complete(Some(error));
                    return;
                }
            },
            ReadDirSource::Entries(entries) => entries,
        };

        for _ in 0..self.capacity {
            {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if state.done || state.cancelled {
                    state.batch_active = false;
                    return;
                }
                if state.entries.len() >= self.capacity {
                    drop(state);
                    self.finish_batch(entries);
                    return;
                }
            }

            let Some(entry) = entries.next() else {
                self.complete(None);
                return;
            };

            let waker = {
                let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if state.done || state.cancelled {
                    state.batch_active = false;
                    return;
                }
                debug_assert!(state.entries.len() < self.capacity);
                state.entries.push_back(entry);
                #[cfg(test)]
                {
                    state.peak_buffered = state.peak_buffered.max(state.entries.len());
                }
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }

        self.finish_batch(entries);
    }

    fn finish_batch(self: &Arc<Self>, entries: ReadDirIterator<T>) {
        let schedule_next = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.done || state.cancelled {
                state.batch_active = false;
                false
            } else {
                debug_assert!(state.source.is_none());
                state.source = Some(ReadDirSource::Entries(entries));
                state.batch_active = false;
                if state.refill_requested && state.entries.len() < self.capacity {
                    state.batch_active = true;
                    state.refill_requested = false;
                    true
                } else {
                    false
                }
            }
        };

        if schedule_next {
            let _ = self.submit_batch();
        }
    }
}

impl<T> Drop for ReadDirBatchGuard<T> {
    fn drop(&mut self) {
        if self.armed {
            self.shared
                .complete(Some(io::Error::other("read_dir producer batch panicked")));
        }
    }
}

#[cfg(test)]
struct ReadDirObserver<T> {
    shared: Arc<ReadDirShared<T>>,
}

#[cfg(test)]
impl<T> ReadDirObserver<T> {
    fn buffered(&self) -> usize {
        self.shared.state.lock().unwrap().entries.len()
    }

    fn peak_buffered(&self) -> usize {
        self.shared.state.lock().unwrap().peak_buffered
    }

    fn is_cancelled(&self) -> bool {
        self.shared.state.lock().unwrap().cancelled
    }
}

impl File {
    /// Closes the descriptor, ordering the close behind operations already
    /// submitted against it.
    ///
    /// Dropping a handle closes its descriptor too, and for most code that is
    /// the right thing. This exists for the case dropping cannot serve: on
    /// Linux the close goes through the ring, so it is sequenced behind
    /// in-flight operations on the same descriptor. A plain `close(2)` from
    /// `Drop` is not — the kernel keeps the underlying file alive until those
    /// operations finish, but frees the descriptor *number* immediately, so a
    /// racing `open` elsewhere can be handed it while this handle's operations
    /// still name it. macOS and Windows have no asynchronous close and gain
    /// only the outcome reporting.
    ///
    /// Named `close_descriptor` rather than `close` because
    /// [`crate::io::AsyncWriteExt::close`] already exists
    /// and means something else — it flushes and closes the *writer*, leaving
    /// the descriptor alive. An inherent `close` would shadow it, so the two
    /// would look identical at the call site and do different things.
    ///
    /// Do not reach for this to catch close errors. Rust's libs team declined
    /// to add `File::close` to the standard library on the grounds that
    /// `close(2)` error reporting is too unreliable to build portable APIs on,
    /// and that reasoning applies here: use `sync_all` if durability matters.
    ///
    /// # Errors
    ///
    /// Returns an error only if the close itself was submitted and failed.
    /// [`CloseOutcome::StillShared`](crate::io::CloseOutcome::StillShared) is
    /// **not** an error — see that type.
    pub async fn close_descriptor(self) -> io::Result<crate::io::CloseOutcome> {
        let Self {
            state,
            direct_write,
            inner,
        } = self;
        drop(state);
        drop(direct_write);
        match std::sync::Arc::try_unwrap(inner) {
            Err(_) => Ok(crate::io::CloseOutcome::StillShared),
            Ok(inner) => {
                sys_fs::close(inner.fd).await?;
                Ok(crate::io::CloseOutcome::Closed)
            }
        }
    }

    /// Opens an existing file for reading.
    ///
    /// This is a convenience wrapper for `OpenOptions::new().read(true)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use runite::io::AsyncReadExt;
    ///
    /// runite::spawn(async {
    ///     let mut file = runite::fs::File::open("input.txt").await.unwrap();
    ///     let mut contents = String::new();
    ///     file.read_to_string(&mut contents).await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path).await
    }

    /// Opens a file for writing, creating or truncating it first.
    ///
    /// This is a convenience wrapper for write-only create-and-truncate access.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
    }

    /// Synchronizes file contents and metadata to stable storage.
    pub async fn sync_all(&self) -> io::Result<()> {
        sys_fs::sync_all(FsOp::SyncAll { fd: self.raw_fd() }).await
    }

    /// Synchronizes file contents to stable storage.
    ///
    /// Metadata that is not needed to retrieve the file contents may be omitted,
    /// matching the behavior of [`std::fs::File::sync_data`].
    pub async fn sync_data(&self) -> io::Result<()> {
        sys_fs::sync_data(FsOp::SyncData { fd: self.raw_fd() }).await
    }

    /// Reads bytes starting at `offset` without using the shared file cursor.
    ///
    /// Positioned reads are useful when multiple tasks share a cloned handle and
    /// must avoid racing on the kernel-managed cursor.
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.read_impl(Some(offset), buf).await
    }

    /// Reads exactly `buf.len()` bytes starting at `offset`.
    pub async fn read_exact_at(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let read = self.read_at(offset, buf).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            offset = offset.saturating_add(read as u64);
            buf = &mut buf[read..];
        }
        Ok(())
    }

    /// Writes bytes starting at `offset` without using the shared file cursor.
    ///
    /// Positioned writes are useful when multiple tasks share a cloned handle and
    /// must avoid racing on the kernel-managed cursor.
    pub async fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.write_impl(Some(offset), buf).await
    }

    /// Writes the entire buffer starting at `offset`.
    pub async fn write_all_at(&self, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let written = self.write_at(offset, buf).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            offset = offset.saturating_add(written as u64);
            buf = &buf[written..];
        }
        Ok(())
    }

    /// Returns metadata for this file handle.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        sys_fs::metadata(FsOp::Metadata {
            target: MetadataTarget::File(self.raw_fd()),
            follow_symlinks: true,
        })
        .await
        .map(Metadata::from_raw)
    }

    /// Truncates or extends the underlying file to `len` bytes.
    pub async fn set_len(&self, len: u64) -> io::Result<()> {
        sys_fs::set_len(FsOp::SetLen {
            fd: self.raw_fd(),
            len,
        })
        .await
    }

    /// Duplicates the underlying file description.
    ///
    /// As with [`std::fs::File::try_clone`], the cloned handle shares
    /// kernel-managed cursor state with this handle. Positioned I/O methods such
    /// as [`read_at`](Self::read_at) avoid that shared cursor. Sequential writes
    /// are queued in first-poll order, and a live write's completion remains
    /// associated with the future that submitted it even when another clone
    /// drives the shared cursor.
    pub async fn try_clone(&self) -> io::Result<Self> {
        let fd = sys_fs::try_clone(FsOp::Duplicate { fd: self.raw_fd() }).await?;
        Ok(File::from_owned_file_with_state(fd, Rc::clone(&self.state)))
    }

    fn from_owned_file(fd: OwnedFile) -> Self {
        Self::from_owned_file_with_state(fd, Rc::new(RefCell::new(CursorState::default())))
    }

    fn from_owned_file_with_state(fd: OwnedFile, state: Rc<RefCell<CursorState>>) -> Self {
        Self {
            state,
            direct_write: None,
            inner: Arc::new(FileInner { fd }),
        }
    }

    fn raw_fd(&self) -> RawFile {
        raw_file(&self.inner.fd)
    }

    async fn read_impl(&self, offset: Option<u64>, buf: &mut [u8]) -> io::Result<usize> {
        let data = sys_fs::read(FsOp::Read {
            fd: self.raw_fd(),
            offset,
            len: buf.len(),
        })
        .await?;

        let read = data.len();
        buf[..read].copy_from_slice(&data);
        Ok(read)
    }

    async fn write_impl(&self, offset: Option<u64>, buf: &[u8]) -> io::Result<usize> {
        sys_fs::write(FsOp::Write {
            fd: self.raw_fd(),
            offset,
            data: buf.to_vec(),
        })
        .await
    }
}

fn rewind_file_cursor(fd: RawFile, bytes: usize) -> io::Result<()> {
    if bytes == 0 {
        return Ok(());
    }
    let bytes = i64::try_from(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "retained read overflow exceeds seek range",
        )
    })?;
    sys_fs::seek(fd, std::io::SeekFrom::Current(-bytes)).map(|_| ())
}

impl AsyncRead for File {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();

        let fd = this.raw_fd();
        this.state
            .borrow_mut()
            .poll_read_slice(cx, buf, move |len| {
                Box::pin(sys_fs::read(FsOp::Read {
                    fd,
                    offset: None,
                    len,
                }))
            })
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let generation = self
            .as_mut()
            .get_mut()
            .direct_write
            .get_or_insert_with(WriteOperation::new)
            .generation();
        let result = self.as_mut().poll_write_operation(cx, buf, generation);
        if result.is_ready() {
            self.get_mut().direct_write = None;
        }
        result
    }

    fn poll_write_operation(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let this = self.get_mut();
        let rewind_fd = this.raw_fd();
        let fd = this.raw_fd();
        this.state.borrow_mut().poll_write(
            cx,
            generation,
            buf,
            |bytes| rewind_file_cursor(rewind_fd, bytes),
            move |data| {
                Box::pin(sys_fs::write(FsOp::Write {
                    fd,
                    offset: None,
                    data,
                }))
            },
        )
    }

    fn poll_write_vectored_operation(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        generation: u64,
    ) -> Poll<io::Result<usize>> {
        match bufs.iter().find(|buf| !buf.is_empty()) {
            Some(buf) => self.as_mut().poll_write_operation(cx, buf, generation),
            None => Poll::Ready(Ok(0)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The write state is shared across clones of this handle, so an
        // abandoned write on any clone is still owned here. Returning `Ok`
        // without draining would report bytes as visible while they are in
        // flight and would discard that operation's error.
        self.get_mut().state.borrow_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The descriptor closes with the handle; closing still has to flush.
        self.get_mut().state.borrow_mut().poll_flush(cx)
    }
}

impl AsyncSeek for File {
    fn poll_seek(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        position: std::io::SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        let rewind_fd = this.raw_fd();
        let fd = this.raw_fd();
        match this
            .state
            .borrow_mut()
            .poll_reconcile(cx, |bytes| rewind_file_cursor(rewind_fd, bytes))
        {
            Poll::Ready(Ok(())) => Poll::Ready(sys_fs::seek(fd, position)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl OpenOptions {
    /// Creates a blank set of open options.
    ///
    /// No access mode is enabled by default; call methods such as
    /// [`read`](Self::read) or [`write`](Self::write) before opening.
    pub fn new() -> Self {
        Self {
            inner: OpOpenOptions::default(),
        }
    }

    /// Controls read access.
    pub fn read(&mut self, value: bool) -> &mut Self {
        self.inner.read = value;
        self
    }

    /// Controls write access.
    pub fn write(&mut self, value: bool) -> &mut Self {
        self.inner.write = value;
        self
    }

    /// Controls append mode.
    ///
    /// When append is enabled, writes are placed at the end of the file by the
    /// operating system.
    pub fn append(&mut self, value: bool) -> &mut Self {
        self.inner.append = value;
        self
    }

    /// Controls whether the file is truncated after opening.
    pub fn truncate(&mut self, value: bool) -> &mut Self {
        self.inner.truncate = value;
        self
    }

    /// Controls whether the file is created if it does not already exist.
    pub fn create(&mut self, value: bool) -> &mut Self {
        self.inner.create = value;
        self
    }

    /// Controls whether opening must create a brand-new file.
    ///
    /// When enabled, opening fails if the path already exists.
    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.inner.create_new = value;
        self
    }

    /// Mutable access to the platform open-options payload, for the
    /// OS-specific extension traits in [`crate::os`].
    #[cfg(windows)]
    pub(crate) fn platform_options_mut(&mut self) -> &mut crate::sys::handle::PlatformOpenOptions {
        &mut self.inner.platform
    }

    /// Opens a file with the configured options.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// runite::spawn(async {
    ///     let mut options = runite::fs::OpenOptions::new();
    ///     let file = options
    ///         .read(true)
    ///         .write(true)
    ///         .open("example.txt")
    ///         .await
    ///         .unwrap();
    ///     let _ = file.metadata().await.unwrap();
    /// });
    /// runite::run();
    /// ```
    pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        sys_fs::open(FsOp::Open {
            path: path.as_ref().to_path_buf(),
            options: self.inner.clone(),
        })
        .await
        .map(File::from_owned_file)
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl Metadata {
    fn from_raw(inner: RawMetadata) -> Self {
        Self { inner }
    }

    /// Returns the file length in bytes.
    pub fn len(&self) -> u64 {
        self.inner.len
    }

    /// Returns `true` if the file length is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if this metadata describes a regular file.
    pub fn is_file(&self) -> bool {
        self.inner.file_type == RawFileType::File
    }

    /// Returns `true` if this metadata describes a directory.
    pub fn is_dir(&self) -> bool {
        self.inner.file_type == RawFileType::Directory
    }

    /// Returns `true` if this metadata describes a symbolic link.
    pub fn is_symlink(&self) -> bool {
        self.inner.file_type == RawFileType::Symlink
    }

    /// Returns the full POSIX `st_mode` — file-type bits *and* permission bits —
    /// matching `std::os::unix::fs::MetadataExt::mode`.
    ///
    /// This is consistent across the Linux and macOS backends. On Windows the
    /// value is synthesized: the file-type bits are the `S_IFMT` equivalents
    /// and the write permission bits reflect `FILE_ATTRIBUTE_READONLY`. Use
    /// `runite::os::windows::fs::MetadataExt::file_attributes` for the native
    /// attribute bits. To extract just the permission bits, mask with
    /// `0o7777`.
    pub fn mode(&self) -> u32 {
        self.inner.mode
    }

    /// The platform metadata payload, for the OS-specific extension traits in
    /// [`crate::os`].
    #[cfg(windows)]
    pub(crate) fn platform_metadata(&self) -> &crate::sys::handle::PlatformMetadata {
        &self.inner.platform
    }
}

impl ReadDir {
    /// Returns the next directory entry, or `None` once the stream is exhausted.
    pub async fn next_entry(&mut self) -> io::Result<Option<DirEntry>> {
        self.inner
            .next_entry()
            .await
            .map(|entry| entry.map(|inner| DirEntry { inner }))
    }
}

impl Stream for ReadDir {
    type Item = io::Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut next = core::pin::pin!(this.inner.next_entry());
        match next.as_mut().poll(cx) {
            Poll::Ready(Ok(Some(entry))) => Poll::Ready(Some(Ok(DirEntry { inner: entry }))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl DirEntry {
    /// Returns the full path to this directory entry.
    ///
    /// The path is the directory path passed to [`read_dir`] joined with this
    /// entry's file name.
    pub fn path(&self) -> PathBuf {
        self.inner.path.clone()
    }

    /// Returns the file name portion of this directory entry.
    pub fn file_name(&self) -> &OsStr {
        self.inner.file_name.as_os_str()
    }

    /// Resolves metadata for this entry.
    ///
    /// Metadata is fetched when this method is called, not cached when the entry
    /// is yielded.
    pub async fn metadata(&self) -> io::Result<Metadata> {
        metadata(self.path()).await
    }
}

/// Reads the entire contents of a file into memory.
///
/// # Examples
///
/// ```no_run
/// runite::spawn(async {
///     let bytes = runite::fs::read("input.bin").await.unwrap();
///     assert!(!bytes.is_empty());
/// });
/// runite::run();
/// ```
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let mut file = File::open(path.as_ref()).await?;
    let mut output = Vec::new();
    file.read_to_end(&mut output).await?;
    Ok(output)
}

/// Reads the entire contents of a UTF-8 file into a [`String`].
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    let bytes = read(path).await?;
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Replaces the contents of a file with `data`, creating it if needed.
///
/// Existing contents are truncated before the new bytes are written.
pub async fn write(path: impl AsRef<Path>, data: impl AsRef<[u8]>) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(data.as_ref()).await
}

/// Returns metadata for a filesystem path.
///
/// Symbolic links are followed.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sys_fs::metadata(FsOp::Metadata {
        target: MetadataTarget::Path(path.as_ref().to_path_buf()),
        follow_symlinks: true,
    })
    .await
    .map(Metadata::from_raw)
}

/// Returns metadata for a filesystem path **without** following symbolic links.
///
/// Unlike [`metadata`], if `path` is a symlink this reports the link itself, so
/// [`Metadata::is_symlink`] can be `true`. Mirrors [`std::fs::symlink_metadata`].
pub async fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    sys_fs::metadata(FsOp::Metadata {
        target: MetadataTarget::Path(path.as_ref().to_path_buf()),
        follow_symlinks: false,
    })
    .await
    .map(Metadata::from_raw)
}

/// Creates a single directory.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::create_dir(FsOp::CreateDir {
        path: path.as_ref().to_path_buf(),
        mode: 0o777,
    })
    .await
}

/// Creates a directory and any missing parent directories.
///
/// Existing **directories** along the path are accepted and treated as success.
/// If any component already exists as a non-directory (for example, the final
/// component is a regular file), this returns an [`io::ErrorKind::AlreadyExists`]
/// error, matching [`std::fs::create_dir_all`].
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    let path = path.as_ref();
    let mut current = PathBuf::new();

    for component in path.components() {
        current.push(component.as_os_str());
        if current.as_os_str().is_empty() {
            continue;
        }
        // Drive prefixes (`C:`) and the root directory always exist and are
        // not creatable; attempting them reports access errors on Windows
        // rather than `AlreadyExists`.
        if matches!(
            component,
            std::path::Component::Prefix(_) | std::path::Component::RootDir
        ) {
            continue;
        }

        match create_dir(&current).await {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // An existing component is only acceptable if it is itself a
                // directory. Matching `std::fs::create_dir_all`, a path whose
                // final component is an existing file (or other non-directory)
                // must surface an error rather than silently succeed.
                match metadata(&current).await {
                    Ok(existing) if existing.is_dir() => {}
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("{} exists and is not a directory", current.display()),
                        ));
                    }
                    Err(metadata_error) => return Err(metadata_error),
                }
            }
            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// Removes a file.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::remove_file(FsOp::RemoveFile {
        path: path.as_ref().to_path_buf(),
    })
    .await
}

/// Removes an empty directory.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::remove_dir(FsOp::RemoveDir {
        path: path.as_ref().to_path_buf(),
    })
    .await
}

/// Renames or moves a filesystem entry.
///
/// Replacement behavior is platform-specific and matches the underlying
/// operating system operation.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    sys_fs::rename(FsOp::Rename {
        from: from.as_ref().to_path_buf(),
        to: to.as_ref().to_path_buf(),
    })
    .await
}

/// Opens an async directory-entry stream.
///
/// The returned stream applies bounded backpressure to its blocking directory
/// scan. Dropping it cancels further iteration and wakes a scan waiting for
/// buffer space.
///
/// # Examples
///
/// ```
/// runite::spawn(async {
///     let mut entries = runite::fs::read_dir(".").await.unwrap();
///     let _ = entries.next_entry().await.unwrap();
/// });
/// runite::run();
/// ```
pub async fn read_dir(path: impl AsRef<Path>) -> io::Result<ReadDir> {
    sys_fs::read_dir(FsOp::ReadDir {
        path: path.as_ref().to_path_buf(),
    })
    .map(|inner| ReadDir { inner })
}

// -- File-descriptor interop (Unix only) -------------------------------------
//
// `#[cfg(unix)]` because these expose raw/owned file descriptors, which the
// Windows backend would replace with `AsHandle`/`AsRawHandle`.

#[cfg(unix)]
impl AsFd for File {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }
}

#[cfg(unix)]
impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.fd.as_raw_fd()
    }
}

#[cfg(unix)]
impl File {
    /// Adopts an open owned file descriptor.
    ///
    /// # Errors
    ///
    /// On Unix this cannot fail today; the `io::Result` exists so one
    /// signature works on every platform. On Windows adoption validates
    /// completion-port affinity and can fail, and **the handle is closed
    /// on failure** rather than returned to the caller.
    pub fn from_owned(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self::from_owned_file(fd))
    }

    /// Adopts a [`std::fs::File`], returning an async [`File`] that shares the
    /// same open file description.
    ///
    /// Files do not need non-blocking mode (the driver handles them via
    /// `io_uring` on Linux and the blocking pool on macOS), so this simply
    /// transfers ownership of the descriptor.
    pub fn from_std(file: std::fs::File) -> io::Result<Self> {
        Self::from_owned(OwnedFd::from(file))
    }
}

#[cfg(unix)]
impl TryFrom<OwnedFd> for File {
    type Error = io::Error;

    fn try_from(fd: OwnedFd) -> io::Result<Self> {
        Self::from_owned(fd)
    }
}

// -- Handle interop (Windows only) --------------------------------------------
//
// The Windows analogs of the Unix fd-interop impls above: files are exposed
// through `AsHandle`/`AsRawHandle`, and adoption binds the handle to the
// current runtime thread's I/O completion port so overlapped reads and writes
// can complete. Synchronous, packet-suppressing, and foreign-IOCP handles are
// rejected.

#[cfg(windows)]
mod windows_interop {
    use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, OwnedHandle, RawHandle};

    use super::File;

    impl AsHandle for File {
        fn as_handle(&self) -> BorrowedHandle<'_> {
            self.inner.fd.as_handle()
        }
    }

    impl AsRawHandle for File {
        fn as_raw_handle(&self) -> RawHandle {
            self.inner.fd.as_raw_handle()
        }
    }

    impl File {
        /// Strictly adopts an overlapped owned handle into the current IOCP.
        ///
        /// # Errors
        ///
        /// Fails for a synchronous handle, one that suppresses completion
        /// packets on synchronous success, or one already associated with a
        /// different completion port. **The handle is closed on failure** — it
        /// is consumed either way and is not handed back to the caller.
        pub fn from_owned(handle: OwnedHandle) -> std::io::Result<Self> {
            crate::sys::windows::fs::adopt_handle(handle).map(Self::from_owned_file)
        }

        /// Adopts a [`std::fs::File`], returning an async [`File`] that shares
        /// the same open file object.
        ///
        /// The handle is associated with the current runtime thread's I/O
        /// completion port. For fully asynchronous reads and writes, open the
        /// file with the `FILE_FLAG_OVERLAPPED` custom flag (e.g. via
        /// [`OpenOptionsExt::custom_flags`](std::os::windows::fs::OpenOptionsExt::custom_flags)
        /// or runite's own [`OpenOptions`](super::OpenOptions), which sets it
        /// automatically). Synchronous handles, handles configured to suppress
        /// successful completion packets, and handles already associated with
        /// another IOCP are rejected.
        pub fn from_std(file: std::fs::File) -> std::io::Result<Self> {
            Self::from_owned(OwnedHandle::from(file))
        }
    }

    impl TryFrom<OwnedHandle> for File {
        type Error = std::io::Error;

        fn try_from(handle: OwnedHandle) -> std::io::Result<Self> {
            Self::from_owned(handle)
        }
    }
}

#[cfg(test)]
mod read_dir_tests;

#[cfg(test)]
mod tests {
    use super::{
        OpenOptions, create_dir_all, metadata, read, read_dir, read_to_string, remove_dir,
        remove_file, rename, write,
    };
    use crate::io::{AsyncReadExt as _, StreamExt};
    use crate::spawn;
    use crate::{queue_macrotask, run};
    use std::collections::BTreeSet;
    use std::ffi::OsString;
    use std::future::{Future, poll_fn};
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::process;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::task::Poll;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn unique_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("runite-{label}-{}-{nanos}", process::id()))
    }

    #[test]
    fn async_fs_round_trip() {
        let _guard = test_lock().lock().unwrap();
        let root = unique_path("fs-round-trip");
        let nested = root.join("nested");
        let file_path = nested.join("hello.txt");
        let renamed_path = nested.join("renamed.txt");
        let output = Arc::new(Mutex::new(None::<String>));

        {
            let output = Arc::clone(&output);
            queue_macrotask(move || {
                spawn(async move {
                    create_dir_all(&nested)
                        .await
                        .expect("dir creation should succeed");
                    write(&file_path, b"hello world")
                        .await
                        .expect("initial write should succeed");

                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&file_path)
                        .await
                        .expect("open should succeed");
                    file.write_at(6, b"runtime")
                        .await
                        .expect("positioned write should succeed");
                    file.sync_all().await.expect("sync should succeed");

                    let mut prefix = [0u8; 5];
                    file.read_exact_at(0, &mut prefix)
                        .await
                        .expect("positioned read should succeed");
                    assert_eq!(&prefix, b"hello");

                    let meta = file.metadata().await.expect("metadata should succeed");
                    assert!(meta.is_file());
                    assert!(meta.len() >= 13);

                    let cloned = file.try_clone().await.expect("clone should succeed");
                    cloned.set_len(13).await.expect("truncate should succeed");

                    rename(&file_path, &renamed_path)
                        .await
                        .expect("rename should succeed");
                    let text = read_to_string(&renamed_path)
                        .await
                        .expect("read_to_string should succeed");
                    assert_eq!(text, "hello runtime");

                    let bytes = read(&renamed_path).await.expect("read should succeed");
                    assert_eq!(bytes, b"hello runtime");

                    let path_meta = metadata(&renamed_path)
                        .await
                        .expect("path metadata should work");
                    assert!(path_meta.is_file());

                    *output.lock().unwrap() = Some(text);

                    remove_file(&renamed_path)
                        .await
                        .expect("remove_file should succeed");
                    remove_dir(&nested)
                        .await
                        .expect("remove nested dir should succeed");
                    remove_dir(&root)
                        .await
                        .expect("remove root dir should succeed");
                });
            });
        }

        run();

        assert_eq!(output.lock().unwrap().as_deref(), Some("hello runtime"));
    }

    #[test]
    fn async_read_dir_streams_entries() {
        let _guard = test_lock().lock().unwrap();
        let root = unique_path("fs-read-dir");
        let one = root.join("one.txt");
        let two = root.join("two.txt");
        let seen: Arc<Mutex<BTreeSet<OsString>>> = Arc::new(Mutex::new(BTreeSet::new()));

        {
            let seen = Arc::clone(&seen);
            queue_macrotask(move || {
                spawn(async move {
                    create_dir_all(&root)
                        .await
                        .expect("dir creation should succeed");
                    write(&one, b"1").await.expect("write one should succeed");
                    write(&two, b"2").await.expect("write two should succeed");

                    let mut dir = read_dir(&root).await.expect("read_dir should succeed");
                    while let Some(entry) = dir.next_entry().await.expect("stream should succeed") {
                        seen.lock()
                            .unwrap()
                            .insert(entry.file_name().to_os_string());
                    }

                    remove_file(&one).await.expect("remove one should succeed");
                    remove_file(&two).await.expect("remove two should succeed");
                    remove_dir(&root).await.expect("remove root should succeed");
                });
            });
        }

        run();

        let seen = seen.lock().unwrap();
        assert!(seen.contains(&OsString::from("one.txt")));
        assert!(seen.contains(&OsString::from("two.txt")));
    }

    #[test]
    fn read_dir_stream_yields_entries() {
        let _guard = test_lock().lock().unwrap();
        let root = unique_path("fs-read-dir-stream");
        let files = ["alpha.txt", "beta.txt", "gamma.txt"];
        let seen: Arc<Mutex<Option<BTreeSet<OsString>>>> = Arc::new(Mutex::new(None));

        {
            let seen = Arc::clone(&seen);
            queue_macrotask(move || {
                spawn(async move {
                    create_dir_all(&root)
                        .await
                        .expect("dir creation should succeed");
                    for file in files {
                        write(root.join(file), file.as_bytes())
                            .await
                            .expect("file write should succeed");
                    }

                    let dir = read_dir(&root).await.expect("read_dir should succeed");
                    let entries = dir
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .expect("stream should succeed");
                    let names = entries
                        .into_iter()
                        .map(|entry| entry.file_name().to_os_string())
                        .collect::<BTreeSet<_>>();
                    *seen.lock().unwrap() = Some(names);

                    for file in files {
                        remove_file(root.join(file))
                            .await
                            .expect("remove file should succeed");
                    }
                    remove_dir(&root).await.expect("remove root should succeed");
                });
            });
        }

        run();

        let seen = seen.lock().unwrap();
        let seen = seen.as_ref().expect("task should record entries");
        for file in files {
            assert!(seen.contains(&OsString::from(file)));
        }
    }

    #[test]
    fn read_borrows_user_buffer() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("borrowed-read");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                spawn(async move {
                    write(&path, b"borrowed buffer")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("open should succeed");
                    let mut buf = [0u8; 8];
                    let read = file.read(&mut buf).await.expect("read should succeed");
                    assert_eq!(read, 8);
                    *observed.lock().unwrap() = Some(buf.to_vec());
                    remove_file(&path).await.expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"borrowed".as_slice())
        );
    }

    #[test]
    fn read_to_end_collects_full_file() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("read-to-end");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                spawn(async move {
                    write(&path, b"full file contents")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("open should succeed");
                    let mut out = b"prefix:".to_vec();
                    let read = file
                        .read_to_end(&mut out)
                        .await
                        .expect("read_to_end should succeed");
                    assert_eq!(read, b"full file contents".len());
                    *observed.lock().unwrap() = Some(out);
                    remove_file(&path).await.expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"prefix:full file contents".as_slice())
        );
    }

    #[test]
    fn read_drop_during_inflight_does_not_uaf() {
        let _guard = test_lock().lock().unwrap();
        let path = unique_path("drop-inflight-read");
        let observed = Arc::new(Mutex::new(None::<Vec<u8>>));

        {
            let observed = Arc::clone(&observed);
            queue_macrotask(move || {
                spawn(async move {
                    write(&path, b"cancel smoke test")
                        .await
                        .expect("fixture write should succeed");
                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("open should succeed");

                    let mut dropped_buf = [0xAAu8; 64];
                    {
                        let mut read = Box::pin(file.read(&mut dropped_buf));
                        let _ = poll_fn(|cx| Poll::Ready(Pin::as_mut(&mut read).poll(cx))).await;
                    }

                    for _ in 0..32 {
                        crate::yield_now().await;
                    }

                    let mut file = OpenOptions::new()
                        .read(true)
                        .open(&path)
                        .await
                        .expect("reopen should succeed");
                    let mut buf = [0u8; 17];
                    let read = file
                        .read(&mut buf)
                        .await
                        .expect("second read should succeed");
                    *observed.lock().unwrap() = Some(buf[..read].to_vec());
                    remove_file(&path).await.expect("cleanup should succeed");
                });
            });
        }

        run();
        assert_eq!(
            observed.lock().unwrap().as_deref(),
            Some(b"cancel smoke test".as_slice())
        );
    }
}

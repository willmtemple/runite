//! File-descriptor readiness helpers backed by the runtime driver.
//!
//! These helpers are useful when integrating custom descriptor types with
//! `runite` without writing a full async wrapper. They borrow a descriptor
//! (anything implementing [`AsFd`]) rather than taking
//! ownership, and [`wait_readable`]/[`wait_writable`] are one-shot readiness
//! waits rather than persistent registrations.
//!
//! Readiness means "try the real I/O operation again." A readable notification
//! can race with other consumers or report an error/hangup condition, so callers
//! should keep their descriptor nonblocking and perform the actual `read` in a
//! `WouldBlock` retry loop.
//!
//! runite is event-loop-per-thread. Wait futures should be polled on the runtime
//! thread that created them; tasks and descriptor registrations do not migrate to
//! another worker.
//!
//! # Platform behavior
//!
//! On Linux, readiness uses one-shot `io_uring` poll operations with
//! best-effort kernel cancellation when the future is dropped. On macOS aarch64,
//! readiness is registered with kqueue; cancellation is queued back to the owner
//! thread with [`crate::ThreadHandle::queue_macrotask`]. If that queue is full or
//! closed, cancellation completion is best-effort and driver cleanup may be left
//! to runtime shutdown.
//!
//! This module is only available on Unix targets: readiness waits are inherently
//! file-descriptor based and have no equivalent on the completion-based Windows
//! backend.
//!
//! # Examples
//!
//! ```no_run
//! use std::io::{Read, Write};
//! use std::os::fd::AsFd;
//! use std::os::unix::net::UnixStream;
//!
//! let (mut reader, mut writer) = UnixStream::pair()?;
//!
//! runite::spawn(async move {
//!     runite::fd::wait_readable(reader.as_fd())
//!         .await
//!         .expect("reader should become readable");
//!     let mut bytes = [0; 5];
//!     reader.read_exact(&mut bytes).expect("read should succeed");
//!     assert_eq!(&bytes, b"ready");
//! });
//!
//! std::thread::spawn(move || {
//!     writer.write_all(b"ready").expect("write should succeed");
//! });
//!
//! runite::run();
//! # std::io::Result::Ok(())
//! ```

use std::io;
use std::os::fd::{AsFd, AsRawFd};

/// Waits until the given descriptor becomes readable or reports an error/hangup
/// condition.
///
/// Accepts anything that borrows a file descriptor ([`AsFd`]) —
/// for example `&std::net::TcpStream`, a [`BorrowedFd`](std::os::fd::BorrowedFd),
/// or one of runite's own I/O types. The descriptor is kept borrowed for the
/// lifetime of the returned future, so it cannot be closed out from under the
/// wait.
///
/// Dropping the future requests cancellation, but cancellation is best-effort:
/// on macOS it is queued back to the owner thread and may be dropped if that
/// queue is full, with cleanup left to runtime shutdown.
///
/// On readiness, callers must perform their own read and handle nonblocking
/// errors according to the descriptor's mode.
///
/// # Examples
///
/// ```no_run
/// use std::io::{Read, Write};
/// use std::os::fd::AsFd;
/// use std::os::unix::net::UnixStream;
///
/// let (mut reader, mut writer) = UnixStream::pair()?;
///
/// runite::spawn(async move {
///     runite::fd::wait_readable(reader.as_fd())
///         .await
///         .expect("reader should become readable");
///     let mut bytes = [0; 5];
///     reader.read_exact(&mut bytes).expect("read should succeed");
///     assert_eq!(&bytes, b"ready");
/// });
///
/// std::thread::spawn(move || {
///     writer.write_all(b"ready").expect("write should succeed");
/// });
///
/// runite::run();
/// # std::io::Result::Ok(())
/// ```
pub async fn wait_readable<Fd: AsFd>(fd: Fd) -> io::Result<()> {
    let raw = fd.as_fd().as_raw_fd();
    // `fd` is held across the await, keeping the descriptor borrowed (and, for an
    // owned handle, open) for the whole wait.
    crate::sys::current::fd::wait_readable(raw).await
}

/// Waits until the given descriptor becomes writable or reports an error/hangup
/// condition.
///
/// The write-readiness counterpart of [`wait_readable`]; see it for the
/// borrowing, cancellation, and readiness-retry semantics.
pub async fn wait_writable<Fd: AsFd>(fd: Fd) -> io::Result<()> {
    let raw = fd.as_fd().as_raw_fd();
    crate::sys::current::fd::wait_writable(raw).await
}

/// Drains a nonblocking descriptor, delivering each chunk as it arrives.
///
/// This is the readiness loop that [`wait_readable`] otherwise asks every
/// caller to write, and it exists because that loop carries three pieces of
/// load-bearing knowledge the raw API does not express — each of which is a
/// bug when missed rather than an inefficiency.
///
/// **Chunks are delivered before the loop parks.** `on_chunk` runs for every
/// read as it completes, so whatever the caller does with the bytes has
/// already happened by the time this waits for more. Written by hand, the
/// natural shape is to accumulate and flush after the loop, and then a burst
/// that ends mid-frame stays invisible until the *next* write arrives — for a
/// terminal, a shell prompt that appears seconds late or not at all.
///
/// **`Interrupted` retries rather than parking.** A signal arriving mid-read
/// is not a reason to wait for readiness that has already been reported.
///
/// **The caller sets its own budget.** Returning
/// [`ControlFlow::Break`](core::ops::ControlFlow::Break) stops
/// the drain and returns `Ok(())`, so a consumer sharing its thread with a
/// frame clock can bound how much it processes at once. Without that, a
/// descriptor producing faster than the caller consumes — `cat` of a large
/// file into a terminal — starves everything else on the loop for as long as
/// it takes.
///
/// Returns when the descriptor reports end of input, when `on_chunk` breaks,
/// or on error. The descriptor must already be nonblocking; a blocking one
/// will stall the event loop inside `read`.
///
/// # Examples
///
/// ```no_run
/// # async fn example(fd: std::os::fd::OwnedFd) -> std::io::Result<()> {
/// use std::ops::ControlFlow;
///
/// let mut buffer = vec![0; 64 * 1024];
/// let mut budget: usize = 1024 * 1024;
///
/// runite::fd::read_chunks(&fd, &mut buffer, |chunk| {
///     // Consume immediately: this runs before the loop waits for more.
///     budget = budget.saturating_sub(chunk.len());
///     if budget == 0 {
///         ControlFlow::Break(())
///     } else {
///         ControlFlow::Continue(())
///     }
/// })
/// .await?;
/// # Ok(())
/// # }
/// ```
pub async fn read_chunks<Fd: AsFd>(
    fd: &Fd,
    buffer: &mut [u8],
    mut on_chunk: impl FnMut(&[u8]) -> core::ops::ControlFlow<()>,
) -> io::Result<()> {
    let raw = fd.as_fd().as_raw_fd();
    if buffer.is_empty() {
        return Ok(());
    }
    loop {
        // SAFETY: `raw` is borrowed from `fd` for the whole call, and `buffer`
        // points to `buffer.len()` writable bytes.
        let read = unsafe {
            libc::read(
                raw,
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
            )
        };
        if read > 0 {
            let read = read as usize;
            if on_chunk(&buffer[..read]).is_break() {
                return Ok(());
            }
            continue;
        }
        if read == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => wait_readable(fd.as_fd()).await?,
            _ => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::wait_readable;
    use crate::{queue_macrotask, run, spawn};
    use std::os::fd::BorrowedFd;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn wait_readable_resolves_for_pipe() {
        let mut fds = [0; 2];
        // SAFETY: `fds.as_mut_ptr()` points to two writable `c_int` slots that
        // `pipe` initializes on success.
        let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(result, 0, "pipe should succeed");
        let read_fd = fds[0];
        let write_fd = fds[1];

        let observed = Arc::new(AtomicBool::new(false));
        queue_macrotask({
            let observed = Arc::clone(&observed);
            move || {
                spawn(async move {
                    // SAFETY: `read_fd` is the open read end and stays open until
                    // this task closes it below, outliving the borrow.
                    let borrowed = unsafe { BorrowedFd::borrow_raw(read_fd) };
                    wait_readable(borrowed)
                        .await
                        .expect("pipe read end should become readable");
                    observed.store(true, Ordering::SeqCst);

                    let mut byte = 0u8;
                    // SAFETY: `read_fd` is the open read end of the pipe, and
                    // `byte` is valid writable storage for one byte.
                    let read = unsafe {
                        libc::read(
                            read_fd,
                            &mut byte as *mut u8 as *mut libc::c_void,
                            std::mem::size_of::<u8>(),
                        )
                    };
                    assert_eq!(read, 1);
                    // SAFETY: `read_fd` is owned by this test path and is
                    // closed exactly once after the pending read completes.
                    unsafe {
                        libc::close(read_fd);
                    }
                });

                std::thread::spawn(move || {
                    let byte = 1u8;
                    // SAFETY: `write_fd` is the open write end of the pipe, and
                    // `byte` is initialized storage for the one byte written.
                    let written = unsafe {
                        libc::write(
                            write_fd,
                            &byte as *const u8 as *const libc::c_void,
                            std::mem::size_of::<u8>(),
                        )
                    };
                    assert_eq!(written, 1);
                    // SAFETY: `write_fd` is owned by this spawned writer and is
                    // closed exactly once after the byte is written.
                    unsafe {
                        libc::close(write_fd);
                    }
                });
            }
        });

        run();
        assert!(observed.load(Ordering::SeqCst));
    }

    /// The three properties `read_chunks` exists to encode: chunks arrive
    /// before the loop parks, a break stops the drain, and end of input ends
    /// it.
    #[test]
    fn read_chunks_delivers_before_parking_and_honours_a_break() {
        use std::cell::RefCell;
        use std::ops::ControlFlow;
        use std::rc::Rc;

        let mut fds = [0; 2];
        // SAFETY: two writable `c_int` slots that `pipe` initializes.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read_fd, write_fd) = (fds[0], fds[1]);
        // SAFETY: the read end is open; O_NONBLOCK is required by the contract.
        unsafe { libc::fcntl(read_fd, libc::F_SETFL, libc::O_NONBLOCK) };

        // SAFETY: `write_fd` is the open write end.
        let write = |bytes: &[u8]| unsafe {
            libc::write(write_fd, bytes.as_ptr().cast::<libc::c_void>(), bytes.len())
        };
        assert!(write(b"first") > 0);

        let seen: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let collected = Rc::clone(&seen);

        queue_macrotask(move || {
            spawn(async move {
                // SAFETY: `read_fd` stays open until this task closes it.
                let borrowed = unsafe { BorrowedFd::borrow_raw(read_fd) };
                let mut buffer = [0u8; 64];
                let mut chunks = 0;
                super::read_chunks(&borrowed, &mut buffer, |chunk| {
                    collected
                        .borrow_mut()
                        .push(String::from_utf8_lossy(chunk).into_owned());
                    chunks += 1;
                    // Stop after the first chunk: the drain must honour this
                    // rather than continuing to end of input.
                    if chunks == 1 {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .await
                .expect("read_chunks should not error");
                // SAFETY: this task owns the descriptor's lifetime here.
                unsafe { libc::close(read_fd) };
            });
        });
        run();
        // SAFETY: the write end is still open and owned by this test.
        unsafe { libc::close(write_fd) };

        let seen = seen.borrow();
        assert_eq!(
            seen.as_slice(),
            ["first"],
            "the chunk should arrive, and the break should stop the drain"
        );
    }

    /// End of input ends the drain without an error, which is what a consumer
    /// keys on to notice its peer is gone.
    #[test]
    fn read_chunks_returns_at_end_of_input() {
        use std::ops::ControlFlow;

        let mut fds = [0; 2];
        // SAFETY: two writable `c_int` slots that `pipe` initializes.
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read_fd, write_fd) = (fds[0], fds[1]);
        // SAFETY: the read end is open.
        unsafe { libc::fcntl(read_fd, libc::F_SETFL, libc::O_NONBLOCK) };
        // Close the write end immediately: the reader sees end of input.
        // SAFETY: the write end is open and unused.
        unsafe { libc::close(write_fd) };

        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        queue_macrotask(move || {
            spawn(async move {
                // SAFETY: `read_fd` stays open until this task closes it.
                let borrowed = unsafe { BorrowedFd::borrow_raw(read_fd) };
                let mut buffer = [0u8; 16];
                super::read_chunks(&borrowed, &mut buffer, |_| ControlFlow::Continue(()))
                    .await
                    .expect("end of input is not an error");
                flag.store(true, Ordering::Release);
                // SAFETY: this task owns the descriptor's lifetime here.
                unsafe { libc::close(read_fd) };
            });
        });
        run();
        assert!(finished.load(Ordering::Acquire));
    }
}

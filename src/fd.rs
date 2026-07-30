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

use std::io::{self, IsTerminal};
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

/// Why a [`read_chunks`] drain ended.
///
/// The distinction is load-bearing. A consumer draining a pseudoterminal keys
/// its teardown on end of input — that is how it learns the child exited —
/// while a self-imposed stop means the descriptor is still live and should be
/// read again. One success value for both would make those indistinguishable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Drain {
    /// The peer is gone; further reads will not produce data.
    ///
    /// Usually this is `read` returning zero. A terminal is the exception
    /// worth naming: Linux hangs a pseudoterminal controller up with `EIO`
    /// rather than end of file once the last user-side descriptor closes.
    /// [`read_chunks`] reports `EIO` from a terminal here anyway, so a
    /// consumer keys its teardown on one outcome however the platform spells
    /// the hangup. `EIO` from anything else is still an error.
    EndOfInput,
    /// `on_chunk` returned [`ControlFlow::Break`](core::ops::ControlFlow::Break),
    /// or the supplied buffer was empty. The descriptor is still live.
    Stopped,
}

impl Drain {
    /// Whether the drain ended because the descriptor reached end of input.
    pub fn is_end_of_input(self) -> bool {
        matches!(self, Self::EndOfInput)
    }
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
/// the drain and returns [`Drain::Stopped`], so a consumer sharing its thread
/// with a frame clock can bound how much it processes at once. Without that, a
/// descriptor producing faster than the caller consumes — `cat` of a large
/// file into a terminal — starves everything else on the loop for as long as
/// it takes.
///
/// The returned [`Drain`] says *which* of those ended the loop, which callers
/// need: end of input usually means the peer is gone and the consumer should
/// tear down, while a self-imposed stop means come back for more. Collapsing
/// them into one success value would make a terminal unable to tell "the shell
/// exited" from "I hit my byte budget".
///
/// The descriptor must already be nonblocking; a blocking one will stall the
/// event loop inside `read`.
///
/// # Platform behavior
///
/// A pseudoterminal controller whose last user-side descriptor has closed
/// reports `EIO` on Linux rather than end of file. `EIO` from a terminal is
/// therefore reported as [`Drain::EndOfInput`], so a consumer keying teardown
/// on its child exiting does not have to know which platform's pty it is
/// holding. `EIO` from a descriptor that is not a terminal — a genuine failure
/// on a file or a socket — is still returned as an error.
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
/// let outcome = runite::fd::read_chunks(&fd, &mut buffer, |chunk| {
///     // Consume immediately: this runs before the loop waits for more.
///     budget = budget.saturating_sub(chunk.len());
///     if budget == 0 {
///         ControlFlow::Break(())
///     } else {
///         ControlFlow::Continue(())
///     }
/// })
/// .await?;
///
/// if outcome.is_end_of_input() {
///     // The peer is gone; tear down rather than waiting for more.
/// }
/// # Ok(())
/// # }
/// ```
pub async fn read_chunks<Fd: AsFd>(
    fd: &Fd,
    buffer: &mut [u8],
    mut on_chunk: impl FnMut(&[u8]) -> core::ops::ControlFlow<()>,
) -> io::Result<Drain> {
    let raw = fd.as_fd().as_raw_fd();
    if buffer.is_empty() {
        return Ok(Drain::Stopped);
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
                return Ok(Drain::Stopped);
            }
            continue;
        }
        if read == 0 {
            return Ok(Drain::EndOfInput);
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => wait_readable(fd.as_fd()).await?,
            // Linux hangs a pty controller up with `EIO` where an ordinary
            // descriptor reports end of input. Passing that through hands the
            // one consumer `Drain` was designed for an I/O failure for the
            // very event it keys teardown on, and makes the answer depend on
            // whose pty it is. Narrowed to terminals so a real `EIO` on a file
            // or a socket still surfaces as the failure it is. The `isatty`
            // behind `is_terminal` overwrites `errno`, so it runs only after
            // the error has been captured.
            _ if error.raw_os_error() == Some(libc::EIO) && fd.as_fd().is_terminal() => {
                return Ok(Drain::EndOfInput);
            }
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
                let outcome = super::read_chunks(&borrowed, &mut buffer, |chunk| {
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
                assert_eq!(
                    outcome,
                    super::Drain::Stopped,
                    "a caller break must be distinguishable from end of input"
                );
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
                let outcome =
                    super::read_chunks(&borrowed, &mut buffer, |_| ControlFlow::Continue(()))
                        .await
                        .expect("end of input is not an error");
                assert_eq!(
                    outcome,
                    super::Drain::EndOfInput,
                    "a consumer keys its teardown on this"
                );
                flag.store(true, Ordering::Release);
                // SAFETY: this task owns the descriptor's lifetime here.
                unsafe { libc::close(read_fd) };
            });
        });
        run();
        assert!(finished.load(Ordering::Acquire));
    }

    /// A pseudoterminal controller reaches end of input when its last
    /// user-side descriptor closes, even though Linux spells that `EIO`.
    ///
    /// This is the case the [`super::Drain`] type was added for: it is how a
    /// terminal consumer learns its child exited.
    #[test]
    fn read_chunks_treats_a_hung_up_pty_as_end_of_input() {
        use std::ops::ControlFlow;
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        let mut controller = -1;
        let mut user = -1;
        // SAFETY: both out-pointers are valid; null optional pointers request
        // the default name, termios, and window size.
        let rc = unsafe {
            libc::openpty(
                &mut controller,
                &mut user,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, 0, "openpty should succeed");
        // SAFETY: `openpty` initialized both with owned, open descriptors.
        let (controller, user) =
            unsafe { (OwnedFd::from_raw_fd(controller), OwnedFd::from_raw_fd(user)) };
        // SAFETY: the controller is open; O_NONBLOCK is required by the contract.
        unsafe { libc::fcntl(controller.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) };

        let line = b"child output\n";
        // SAFETY: the user side is open, and `line` is initialized storage.
        let written = unsafe {
            libc::write(
                user.as_raw_fd(),
                line.as_ptr().cast::<libc::c_void>(),
                line.len(),
            )
        };
        assert_eq!(written as usize, line.len());
        // The child exiting: its last descriptor on the user side goes away.
        drop(user);

        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        queue_macrotask(move || {
            spawn(async move {
                let mut buffer = [0u8; 256];
                let outcome =
                    super::read_chunks(&controller, &mut buffer, |_| ControlFlow::Continue(()))
                        .await
                        .expect("a hung-up pty is a finished drain, not a failure");
                // How much of the pending output survives the hangup differs
                // between Linux and the BSDs, so only the outcome is asserted.
                assert_eq!(
                    outcome,
                    super::Drain::EndOfInput,
                    "a terminal consumer keys its teardown on this"
                );
                flag.store(true, Ordering::Release);
            });
        });
        run();
        assert!(finished.load(Ordering::Acquire));
    }

    /// The pty rule is narrowed to terminals, so `EIO` from anything else is
    /// still the failure it is rather than a silent, empty drain.
    ///
    /// `/proc/self/mem` is the cheapest reliable `EIO` source: reading it at
    /// offset zero addresses the unmapped first page. That makes the test
    /// Linux-only, which is also the only platform where a hangup can be
    /// confused with a failure.
    #[cfg(target_os = "linux")]
    #[test]
    fn read_chunks_still_fails_on_a_non_terminal_eio() {
        use std::ops::ControlFlow;
        use std::os::fd::AsRawFd;

        let mem = std::fs::File::open("/proc/self/mem").expect("/proc/self/mem should open");
        // SAFETY: the file is open for the whole call; the contract wants the
        // descriptor nonblocking.
        unsafe { libc::fcntl(mem.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) };

        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        queue_macrotask(move || {
            spawn(async move {
                let mut buffer = [0u8; 64];
                let error = super::read_chunks(&mem, &mut buffer, |_| ControlFlow::Continue(()))
                    .await
                    .expect_err("EIO on a file is not end of input");
                assert_eq!(error.raw_os_error(), Some(libc::EIO));
                flag.store(true, Ordering::Release);
            });
        });
        run();
        assert!(finished.load(Ordering::Acquire));
    }
}

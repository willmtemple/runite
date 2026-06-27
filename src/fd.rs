//! File-descriptor readiness helpers backed by the runtime driver.
//!
//! These helpers are useful when integrating custom descriptor types with
//! `runite` without writing a full async wrapper. They operate on a borrowed raw
//! file descriptor: runite does not take ownership of the descriptor, and
//! `wait_readable` is a one-shot readiness wait rather than a persistent
//! registration. The public API currently exposes readable readiness; writable
//! readiness is used internally by runtime adapters such as child pipes.
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
//! On Linux x86_64, readiness uses one-shot `io_uring` poll operations with
//! best-effort kernel cancellation when the future is dropped. On macOS aarch64,
//! readiness is registered with kqueue; cancellation is queued back to the owner
//! thread with [`crate::ThreadHandle::queue_macrotask`]. If that queue is full or
//! closed, cancellation completion is best-effort and driver cleanup may be left
//! to runtime shutdown.
//!
//! # Examples
//!
//! ```no_run
//! use std::io::{Read, Write};
//! use std::os::fd::AsRawFd;
//! use std::os::unix::net::UnixStream;
//!
//! let (mut reader, mut writer) = UnixStream::pair()?;
//! let read_fd = reader.as_raw_fd();
//!
//! runite::spawn(async move {
//!     runite::fd::wait_readable(read_fd)
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
use std::os::fd::RawFd;

/// Waits until `fd` becomes readable or reports an error/hangup condition.
///
/// The descriptor should remain open and should not be reused for a different
/// resource while the future is pending. Dropping the future requests
/// cancellation, but cancellation is best-effort: on macOS it is queued back to
/// the owner thread and may be dropped if that queue is full, with cleanup left
/// to runtime shutdown. Avoid assumptions that an immediately reused raw fd is
/// unrelated to an in-flight or just-cancelled wait.
///
/// On readiness, callers must perform their own read and handle nonblocking
/// errors according to the descriptor's mode.
///
/// # Examples
///
/// ```no_run
/// use std::io::{Read, Write};
/// use std::os::fd::AsRawFd;
/// use std::os::unix::net::UnixStream;
///
/// let (mut reader, mut writer) = UnixStream::pair()?;
/// let read_fd = reader.as_raw_fd();
///
/// runite::spawn(async move {
///     runite::fd::wait_readable(read_fd)
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
pub async fn wait_readable(fd: RawFd) -> io::Result<()> {
    crate::sys::current::fd::wait_readable(fd).await
}

#[cfg(test)]
mod tests {
    use super::wait_readable;
    use crate::{queue_macrotask, run, spawn};
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
                    wait_readable(read_fd)
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
}

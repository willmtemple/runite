//! File-descriptor readiness helpers backed by the runtime driver.
//!
//! These helpers are useful when integrating custom descriptor types with
//! `runite` without writing a full async wrapper.
//!
//! # Examples
//!
//! ```no_run
//! use std::io::Write;
//! use std::os::fd::AsRawFd;
//! use std::os::unix::net::UnixStream;
//!
//! let (reader, mut writer) = UnixStream::pair()?;
//! let read_fd = reader.as_raw_fd();
//!
//! runite::spawn(async move {
//!     runite::fd::wait_readable(read_fd)
//!         .await
//!         .expect("reader should become readable");
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
/// The descriptor must remain open until the returned future completes or is
/// dropped. On readiness, callers should perform their own read and handle
/// nonblocking errors according to the descriptor's mode.
///
/// # Examples
///
/// ```no_run
/// use std::io::Write;
/// use std::os::fd::AsRawFd;
/// use std::os::unix::net::UnixStream;
///
/// let (reader, mut writer) = UnixStream::pair()?;
/// let read_fd = reader.as_raw_fd();
///
/// runite::spawn(async move {
///     runite::fd::wait_readable(read_fd)
///         .await
///         .expect("reader should become readable");
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

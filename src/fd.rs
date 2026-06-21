//! File-descriptor readiness helpers backed by the runtime driver.

use std::io;
use std::os::fd::RawFd;

/// Waits until `fd` becomes readable or reports an error/hangup condition.
pub async fn wait_readable(fd: RawFd) -> io::Result<()> {
    crate::sys::current::fd::wait_readable(fd).await
}

#[cfg(test)]
mod tests {
    use super::wait_readable;
    use crate::{queue_future, queue_task, run};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn wait_readable_resolves_for_pipe() {
        let mut fds = [0; 2];
        let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(result, 0, "pipe should succeed");
        let read_fd = fds[0];
        let write_fd = fds[1];

        let observed = Arc::new(AtomicBool::new(false));
        queue_task({
            let observed = Arc::clone(&observed);
            move || {
                queue_future(async move {
                    wait_readable(read_fd)
                        .await
                        .expect("pipe read end should become readable");
                    observed.store(true, Ordering::SeqCst);

                    let mut byte = 0u8;
                    let read = unsafe {
                        libc::read(
                            read_fd,
                            &mut byte as *mut u8 as *mut libc::c_void,
                            std::mem::size_of::<u8>(),
                        )
                    };
                    assert_eq!(read, 1);
                    unsafe {
                        libc::close(read_fd);
                    }
                });

                std::thread::spawn(move || {
                    let byte = 1u8;
                    let written = unsafe {
                        libc::write(
                            write_fd,
                            &byte as *const u8 as *const libc::c_void,
                            std::mem::size_of::<u8>(),
                        )
                    };
                    assert_eq!(written, 1);
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

//! macOS fd readiness backend.

use std::io;
use std::os::fd::RawFd;

use crate::op::completion::completion_for_current_thread;
use crate::platform::current::driver::FdInterest;
use crate::platform::current::runtime::{
    QueueError, cancel_fd_readiness, current_thread_handle, with_current_driver,
};

/// Waits until `fd` becomes readable or reports an error/hangup condition.
pub async fn wait_readable(fd: RawFd) -> io::Result<()> {
    wait_fd_readiness(fd, FdInterest::Readable).await
}

/// Waits until `fd` becomes writable or reports an error/hangup condition.
pub async fn wait_writable(fd: RawFd) -> io::Result<()> {
    wait_fd_readiness(fd, FdInterest::Writable).await
}

async fn wait_fd_readiness(fd: RawFd, interest: FdInterest) -> io::Result<()> {
    let (future, handle) = completion_for_current_thread::<io::Result<()>>();
    let owner = current_thread_handle();
    let token =
        with_current_driver(|driver| driver.register_fd_readiness(fd, interest, handle.clone()));
    match token {
        Ok(token) => {
            handle.set_cancel({
                let handle = handle.clone();
                move || {
                    let queued_handle = handle.clone();
                    let queued = owner.queue_macrotask(move || {
                        cancel_fd_readiness(token);
                        queued_handle.finish(None);
                    });
                    match queued {
                        Ok(()) => {}
                        Err(QueueError::Closed) => handle.finish(None),
                        Err(QueueError::Full) => {
                            // Cancellation must not block waiting for remote
                            // capacity; complete locally and leave driver
                            // cleanup to runtime shutdown.
                            tracing::error!(
                                target: crate::trace_targets::SCHEDULER,
                                event = "fd_cancel_dropped",
                                "dropping fd-readiness cancellation because the remote queue is full"
                            );
                            handle.finish(None);
                        }
                    }
                }
            });
        }
        Err(error) => {
            handle.complete(Err(error));
        }
    }

    future.await
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::time::Duration;

    use super::*;

    fn pipe_pair() -> (OwnedFd, OwnedFd) {
        let mut fds = [0; 2];
        let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(result, 0, "pipe failed: {}", io::Error::last_os_error());
        // SAFETY: pipe returned two fresh descriptors and ownership transfers
        // to these wrappers exactly once.
        unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
    }

    #[test]
    fn cancelled_waiter_cannot_delete_registration_for_reused_fd() {
        crate::block_on(async {
            let (old_read, old_write) = pipe_pair();
            let (replacement_read, replacement_write) = pipe_pair();
            let old_fd = old_read.as_raw_fd();

            let old_wait = crate::spawn(wait_readable(old_fd));
            crate::time::sleep(Duration::from_millis(10)).await;
            assert!(!old_wait.is_finished());

            old_wait.abort();
            drop(old_read);
            drop(old_write);

            let reused = unsafe { libc::dup2(replacement_read.as_raw_fd(), old_fd) };
            assert_eq!(
                reused,
                old_fd,
                "dup2 failed to reuse descriptor: {}",
                io::Error::last_os_error()
            );
            // SAFETY: dup2 created a new descriptor at old_fd and this wrapper
            // takes ownership of that descriptor exactly once.
            let reused_read = unsafe { OwnedFd::from_raw_fd(reused) };

            let replacement_wait = crate::spawn(wait_readable(reused_read.as_raw_fd()));
            let byte = 1u8;
            let written = unsafe {
                libc::write(
                    replacement_write.as_raw_fd(),
                    (&byte as *const u8).cast::<libc::c_void>(),
                    1,
                )
            };
            assert_eq!(written, 1, "write failed: {}", io::Error::last_os_error());

            let abort = replacement_wait.abort_handle();
            match crate::time::timeout(Duration::from_secs(2), replacement_wait).await {
                Ok(result) => result
                    .expect("replacement waiter should not be aborted")
                    .expect("replacement fd should become readable"),
                Err(_) => {
                    abort.abort();
                    panic!("replacement fd waiter was stranded");
                }
            }

            let error = old_wait.await.expect_err("old waiter should be aborted");
            assert!(error.is_aborted());
        });
    }
}

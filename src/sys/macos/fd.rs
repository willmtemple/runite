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

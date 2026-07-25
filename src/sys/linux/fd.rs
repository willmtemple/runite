//! Linux fd readiness backend.

use std::io;
use std::os::fd::RawFd;

use crate::op::completion::local_completion_for_current_thread;
use crate::platform::current::runtime::{
    cancel_operation_on_owner, current_thread_handle, with_current_driver,
};
use crate::platform::linux::uring::{IORING_OP_POLL_ADD, IoUringCqe};

/// Waits until `fd` becomes readable or reports an error/hangup condition.
pub async fn wait_readable(fd: RawFd) -> io::Result<()> {
    submit_poll(fd, libc::POLLIN | libc::POLLERR | libc::POLLHUP).await
}

/// Waits until `fd` becomes writable or reports an error/hangup condition.
pub async fn wait_writable(fd: RawFd) -> io::Result<()> {
    submit_poll(fd, libc::POLLOUT | libc::POLLERR | libc::POLLHUP).await
}

async fn submit_poll(fd: RawFd, mask: i16) -> io::Result<()> {
    let owner = current_thread_handle();
    let (future, handle) = local_completion_for_current_thread::<io::Result<()>>();
    let callback_handle = handle.clone();
    let token = with_current_driver(|driver| {
        driver.submit_operation(
            move |sqe| {
                sqe.opcode = IORING_OP_POLL_ADD;
                sqe.fd = fd;
                sqe.len = 0;
                sqe.op_flags = mask as u32;
            },
            move |cqe| {
                callback_handle.complete(cqe_to_result(cqe));
            },
        )
    })?;

    handle.set_cancel(move || {
        cancel_operation_on_owner(owner, token, None);
    });

    future.await
}

fn cqe_to_result(cqe: IoUringCqe) -> io::Result<()> {
    if cqe.res < 0 {
        return Err(io::Error::from_raw_os_error(-cqe.res));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::submit_poll;
    use std::future::{Future, poll_fn};
    use std::sync::mpsc;
    use std::task::Poll;
    use std::time::Duration;

    struct ForceSend<T>(T);
    // SAFETY: this wrapper is used only to exercise the defensive owner-ring
    // cancellation path. The wrapped future is never polled after migration;
    // it is dropped immediately on the foreign runtime thread.
    unsafe impl<T> Send for ForceSend<T> {}

    #[test]
    fn migrated_readiness_drop_cancels_on_owner_ring() {
        let mut fds = [0; 2];
        // SAFETY: pipe2 initializes both array elements on success.
        assert_eq!(
            unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) },
            0
        );
        let read_fd = fds[0];
        let write_fd = fds[1];
        let (future_tx, future_rx) = mpsc::sync_channel(1);
        let (done_tx, done_rx) = mpsc::sync_channel(1);

        let owner = std::thread::spawn(move || {
            crate::spawn(async move {
                let mut future = Box::pin(submit_poll(
                    read_fd,
                    libc::POLLIN | libc::POLLERR | libc::POLLHUP,
                ));
                poll_fn(|cx| match future.as_mut().poll(cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(result) => {
                        panic!("readiness unexpectedly completed before migration: {result:?}")
                    }
                })
                .await;
                future_tx
                    .send(ForceSend(future))
                    .expect("foreign runtime should receive the future");
            });
            crate::run();
            done_tx.send(()).expect("test should still be listening");
        });

        let foreign = std::thread::spawn(move || {
            let _ = crate::platform::linux::runtime::current_thread_handle();
            let future = future_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("owner should publish the pending future");
            drop(future);
        });

        let completed = done_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        if !completed {
            let byte = 1u8;
            // SAFETY: `write_fd` is the live pipe write end and `byte` is one
            // initialized byte. This unblocks cleanup before the assertion.
            let _ = unsafe {
                libc::write(
                    write_fd,
                    (&byte as *const u8).cast::<libc::c_void>(),
                    std::mem::size_of::<u8>(),
                )
            };
            let _ = done_rx.recv_timeout(Duration::from_secs(2));
        }

        foreign.join().expect("foreign runtime should exit");
        owner.join().expect("owner runtime should exit");
        // SAFETY: the test owns both pipe descriptors and closes each once.
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        assert!(
            completed,
            "dropping on a foreign runtime must cancel the owner ring's poll"
        );
    }
}

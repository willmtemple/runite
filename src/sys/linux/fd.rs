//! Linux fd readiness backend.

use std::io;
use std::os::fd::RawFd;

use crate::op::completion::completion_for_current_thread;
use crate::platform::current::runtime::with_current_driver;
use crate::platform::linux_x86_64::uring::{IORING_OP_POLL_ADD, IoUringCqe};

/// Waits until `fd` becomes readable or reports an error/hangup condition.
pub async fn wait_readable(fd: RawFd) -> io::Result<()> {
    submit_poll(fd, libc::POLLIN | libc::POLLERR | libc::POLLHUP).await
}

/// Waits until `fd` becomes writable or reports an error/hangup condition.
pub async fn wait_writable(fd: RawFd) -> io::Result<()> {
    submit_poll(fd, libc::POLLOUT | libc::POLLERR | libc::POLLHUP).await
}

async fn submit_poll(fd: RawFd, mask: i16) -> io::Result<()> {
    let (future, handle) = completion_for_current_thread::<io::Result<()>>();
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
        let _ = with_current_driver(|driver| driver.cancel_operation(token));
    });

    future.await
}

fn cqe_to_result(cqe: IoUringCqe) -> io::Result<()> {
    if cqe.res < 0 {
        return Err(io::Error::from_raw_os_error(-cqe.res));
    }
    Ok(())
}

//! Linux runtime shim.
//!
//! All scheduler, timer-heap, future-task, and worker bookkeeping lives in
//! [`crate::platform::runtime_shared`]. This file just:
//!
//!   * implements [`Runtime`](crate::platform::runtime_shared::Runtime) for a
//!     marker type so the
//!     shared code can mint fresh driver/notifier pairs and read the
//!     monotonic clock, and
//!   * re-exports the generic public scheduler entry points with the marker
//!     fixed, so callers continue to write `runite::queue_macrotask(..)`
//!     without turbofish.

use std::future::Future;
use std::io;
use std::time::Duration;

use super::driver::{self, Driver};
use crate::platform::runtime_shared as shared;

pub use shared::{
    AbortHandle, CancelOnDrop, IntervalHandle, JoinHandle, QueueError, RuntimeId, ThreadHandle,
    TimeoutHandle, TimerCancel, TurnId, WorkerHandle, YieldNow, current_runtime_id, current_turn,
    on_shutdown, shutdown, yield_now,
};

/// Marker type used to monomorphize the shared scheduler for this platform.
pub(crate) struct LinuxRuntime;

impl shared::Runtime for LinuxRuntime {
    fn create_driver_pair()
    -> io::Result<(Box<dyn shared::DriverBackend>, Box<dyn shared::Notifier>)> {
        let (driver, notifier) = driver::create_driver()?;
        Ok((Box::new(driver), Box::new(notifier)))
    }

    fn monotonic_now() -> io::Result<Duration> {
        driver::monotonic_now()
    }
}

pub fn current_thread_handle() -> ThreadHandle {
    shared::current_thread_handle::<LinuxRuntime>()
}

pub(crate) fn try_current_thread_handle() -> Option<ThreadHandle> {
    shared::try_current_thread_handle()
}

pub(crate) fn with_current_driver<T>(f: impl FnOnce(&Driver) -> T) -> T {
    shared::with_current_driver_any::<LinuxRuntime, Driver, T>(f)
}

pub(crate) fn cancel_operation_on_owner(owner: ThreadHandle, token: u64) {
    let cancel = move || {
        let _ = with_current_driver(|driver| driver.cancel_operation(token));
    };

    if owner.is_current() {
        cancel();
    } else {
        let _ = owner.queue_internal_wake(cancel);
    }
}

pub fn queue_task<F>(task: F)
where
    F: FnOnce() + 'static,
{
    shared::queue_task::<LinuxRuntime, F>(task)
}

pub fn queue_microtask<F>(task: F)
where
    F: FnOnce() + 'static,
{
    shared::queue_microtask::<LinuxRuntime, F>(task)
}

pub fn queue_future<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    shared::queue_future::<LinuxRuntime, F>(future)
}

pub fn timeout<F>(delay: Duration, callback: F) -> TimeoutHandle
where
    F: FnOnce() + 'static,
{
    shared::timeout::<LinuxRuntime, F>(delay, callback)
}

pub fn interval<F>(delay: Duration, callback: F) -> IntervalHandle
where
    F: FnMut() + 'static,
{
    shared::interval::<LinuxRuntime, F>(delay, callback)
}

pub fn spawn_worker<Init, Exit>(initial_task: Init, on_exit: Exit) -> WorkerHandle
where
    Init: FnOnce() + Send + 'static,
    Exit: FnOnce() + 'static,
{
    shared::spawn_worker::<LinuxRuntime, Init, Exit>(initial_task, on_exit)
}

pub fn run() {
    shared::run::<LinuxRuntime>()
}

pub fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    shared::block_on::<LinuxRuntime, F>(future)
}

pub fn try_block_on<F>(future: F) -> io::Result<F::Output>
where
    F: Future,
{
    shared::try_block_on::<LinuxRuntime, F>(future)
}

pub fn run_until_stalled() {
    shared::run_until_stalled::<LinuxRuntime>()
}

pub fn run_ready_tasks() {
    shared::run_ready_tasks::<LinuxRuntime>()
}

pub fn monotonic_now() -> Duration {
    shared::monotonic_now::<LinuxRuntime>()
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::{LinuxRuntime, current_thread_handle, run_until_stalled};
    use crate::op::fs::FsOp;
    use crate::platform::runtime_shared::test_support;
    use crate::{QueueError, spawn};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    #[test]
    fn runtime_executes_local_and_remote_work() {
        test_support::runtime_executes_local_and_remote_work::<LinuxRuntime>();
    }

    #[test]
    fn runtime_waits_for_cross_thread_operation_completion() {
        test_support::runtime_waits_for_cross_thread_operation_completion::<LinuxRuntime>();
    }

    #[test]
    fn zero_interval_fires_once_per_turn_without_spinning() {
        test_support::zero_interval_fires_once_per_turn_without_spinning::<LinuxRuntime>();
    }

    #[test]
    fn dormant_turn_records_cost_nothing() {
        test_support::dormant_turn_records_cost_nothing::<LinuxRuntime>();
    }

    #[test]
    fn pending_read_teardown_quiesces_before_retained_handle_drops() {
        let mut fds = [0; 2];
        // SAFETY: pipe2 initializes both descriptor slots on success.
        assert_eq!(
            unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) },
            0
        );
        // SAFETY: pipe2 returned fresh descriptors owned by this test.
        let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        let (handle_tx, handle_rx) = std::sync::mpsc::sync_channel(1);

        std::thread::spawn(move || {
            handle_tx
                .send(current_thread_handle())
                .expect("test should retain the thread handle");
            spawn(async move {
                let _ = crate::sys::linux::fs::read(FsOp::Read {
                    fd: reader.as_raw_fd(),
                    offset: None,
                    len: 64,
                })
                .await;
                drop(reader);
            });
            run_until_stalled();
        })
        .join()
        .expect("runtime thread should tear down cleanly");

        let handle = handle_rx.recv().expect("runtime should publish its handle");
        assert!(handle.is_closed());
        assert!(matches!(
            handle.queue_macrotask(|| {}),
            Err(QueueError::Closed)
        ));
        drop(writer);
        drop(handle);
    }
}

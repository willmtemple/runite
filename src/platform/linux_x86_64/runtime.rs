//! Linux x86_64 runtime shim.
//!
//! All scheduler, timer-heap, future-task, and worker bookkeeping lives in
//! [`crate::platform::runtime_shared`]. This file just:
//!
//!   * implements [`Runtime`](crate::platform::runtime_shared::Runtime) for a
//!     marker type so the
//!     shared code can mint fresh driver/notifier pairs and read the
//!     monotonic clock, and
//!   * re-exports the generic public scheduler entry points with the marker
//!     fixed, so callers continue to write `runite::queue_task(..)`
//!     without turbofish.

use std::future::Future;
use std::io;
use std::time::Duration;

use super::driver::{self, Driver};
use crate::platform::runtime_shared as shared;

pub use shared::{
    AbortHandle, IntervalHandle, JoinHandle, QueueError, ThreadHandle, TimeoutHandle, WorkerHandle,
    YieldNow, clear_interval, clear_timeout, yield_now,
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

pub fn set_timeout<F>(delay: Duration, callback: F) -> TimeoutHandle
where
    F: FnOnce() + 'static,
{
    shared::set_timeout::<LinuxRuntime, F>(delay, callback)
}

pub fn set_interval<F>(delay: Duration, callback: F) -> IntervalHandle
where
    F: FnMut() + 'static,
{
    shared::set_interval::<LinuxRuntime, F>(delay, callback)
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

pub fn run_until_stalled() {
    shared::run_until_stalled::<LinuxRuntime>()
}

pub fn run_ready_tasks() {
    shared::run_ready_tasks::<LinuxRuntime>()
}

#[cfg(test)]
mod tests {
    use super::LinuxRuntime;
    use crate::platform::runtime_shared::test_support;

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
}

//! Backend-independent runtime scheduler.
//!
//! Both `platform/linux_x86_64/runtime.rs` and `platform/macos_aarch64/runtime.rs`
//! used to carry near-identical copies of the scheduler, timer heap, future
//! task plumbing, join handles, and worker-thread bookkeeping. All of that
//! shared code now lives here; each platform `runtime.rs` is a thin shim
//! that:
//!
//!   * implements the [`Runtime`] trait via a marker type so the shared code
//!     knows how to access the per-thread `ThreadState` and how to create new
//!     driver/notifier pairs, and
//!   * re-exports the generic public functions with concrete type parameters,
//!     so users continue to write `runite::queue_task(..)` without
//!     turbofish.
//!
//! ## Type erasure
//!
//! The scheduler holds the driver as `Box<dyn DriverBackend>` and the
//! cross-thread notifier as `Box<dyn Notifier>`. Driver-specific entry points
//! (`Driver::cancel_operation` on Linux, `Driver::cancel_fd_readiness` on
//! macOS) live in the per-platform `runtime.rs` and downcast through
//! [`DriverBackend::as_any`].

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

pub(crate) mod driver_backend;
pub(crate) mod future_task;
pub(crate) mod handles;
pub(crate) mod scheduler;
pub(crate) mod state;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod timer;

pub(crate) type LocalTask = Box<dyn FnOnce() + 'static>;
pub(crate) type SendTask = Box<dyn FnOnce() + Send + 'static>;
pub(crate) type LocalBoxFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;
pub(crate) type IntervalCallback = Rc<RefCell<Box<dyn FnMut()>>>;

/// If the microtask queue runs more than this many tasks in a single turn
/// without yielding to the macrotask queue, a warning is emitted.
pub(crate) const MICROTASK_STARVATION_THRESHOLD: u64 = 1000;

pub(crate) type LocalTaskQueue = VecDeque<LocalTask>;
pub(crate) type MacroTaskQueue<T> = VecDeque<T>;

pub use driver_backend::{DriverBackend, Notifier, ReadyEvents};
#[allow(unused_imports)]
pub(crate) use future_task::{FutureTask, JoinState};
pub use handles::{
    IntervalHandle, JoinHandle, QueueError, ThreadHandle, TimeoutHandle, WorkerHandle, YieldNow,
};
pub use scheduler::{
    Runtime, clear_interval, clear_timeout, current_thread_handle, queue_future, queue_microtask,
    queue_task, run, run_ready_tasks, run_until_stalled, set_interval, set_timeout, spawn_worker,
    yield_now,
};
pub(crate) use scheduler::{try_current_thread_handle, with_current_driver_any};
#[allow(unused_imports)]
pub(crate) use state::{ChildWorker, MacroTask, ThreadShared, ThreadState, WorkerCompletion};

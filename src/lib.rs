//! Runtime, driver, async I/O, and channel primitives for runite.
//!
//! `runite` is a platform runtime substrate built around a single-threaded event loop with
//! explicit worker threads, JavaScript-style microtask/macrotask scheduling, and
//! platform-specific async I/O backends (io_uring on Linux, kqueue on macOS).
//!
//! Most users will start with:
//!
//! - [`main`] or [`async_main`] for executable entry points
//! - [`run`], [`queue_task`], [`queue_microtask`], and [`queue_future`] for event-loop work
//! - [`fs`], [`net`], [`time`], and [`channel`] for async runtime services
//!
//! # Platform support
//!
//! `runite` currently targets:
//! - Linux `x86_64`
//! - macOS `aarch64`

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
)))]
compile_error!("runite currently supports Linux x86_64 and macOS aarch64.");

extern crate alloc;

pub(crate) mod trace_targets {
    pub const DRIVER: &str = "runite::driver";
    pub const RUNTIME: &str = "runite::runtime";
    pub const SCHEDULER: &str = "runite::scheduler";

    #[cfg(debug_assertions)]
    pub const TIMER: &str = "runite::timer";
    #[cfg(debug_assertions)]
    pub const ASYNC: &str = "runite::async";
}

pub mod channel;
pub mod fd;
pub mod fs;
pub mod io;
pub mod net;
#[doc(hidden)]
pub mod op;
#[doc(hidden)]
pub mod platform;
pub mod signal;
pub mod stdio;
pub mod sync;
#[doc(hidden)]
pub mod sys;
pub mod task;
pub mod time;

#[doc(hidden)]
pub mod macros;

/// Marks a synchronous `fn main()` as the runtime entry point.
///
/// The macro generates a real Rust `main` that queues the function body onto the main runtime
/// thread before calling [`run`].
pub use runite_proc_macros::main;

/// Marks an `async fn main()` as the runtime entry point.
///
/// The macro generates a real Rust `main` that queues the returned future onto the main runtime
/// thread before calling [`run`].
pub use runite_proc_macros::async_main;

/// Driver primitives re-exported from the active backend.
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
pub use platform::current::driver::{
    Driver, ReadyEvents, ThreadNotifier, create_driver, monotonic_now,
};
/// Runtime/event-loop primitives re-exported from the active backend.
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
pub use platform::current::runtime::{
    IntervalHandle, JoinHandle, QueueError, ThreadHandle, TimeoutHandle, WorkerHandle,
    clear_interval, clear_timeout, current_thread_handle, queue_future, queue_microtask,
    queue_task, run, run_ready_tasks, run_until_stalled, set_interval, set_timeout, spawn_worker,
    yield_now,
};

/// Spawns blocking work on the shared OS-thread pool and returns a future that
/// resolves to the closure's result. See [`task::spawn_blocking`].
pub use task::{BlockingJoinHandle, JoinError, spawn_blocking};

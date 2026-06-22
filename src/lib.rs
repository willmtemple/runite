//! Runtime, driver, async I/O, and channel primitives for runite.
//!
//! `runite` is a platform runtime substrate built around a single-threaded event loop with
//! explicit worker threads, JavaScript-style microtask/macrotask scheduling, and
//! platform-specific async I/O backends (io_uring on Linux, kqueue on macOS).
//!
//! Most users will start with:
//!
//! - [`main`] for executable entry points (sync or `async fn main`)
//! - [`run`], [`queue_task`], [`queue_microtask`], and [`queue_future`] for event-loop work
//! - [`fs`], [`net`], [`process`], [`time`], and [`channel`] for async runtime services
//! - [`channel::broadcast`] and [`channel::watch`] for fan-out and state-change channels
//! - [`io::BufReader`]/[`io::BufWriter`] for buffered I/O
//! - [`net::TcpStream::into_split`] and listener `incoming()` streams for connection tasks
//! - [`stdout`] and [`stderr`] for async standard output streams
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
pub mod process;
pub mod signal;
pub mod stdio;
pub mod sync;
#[doc(hidden)]
pub mod sys;
pub mod task;
pub mod time;

#[doc(hidden)]
pub mod macros;

/// Marks `fn main` as the runtime entry point.
///
/// Works for both a synchronous `fn main()` and an `async fn main()`: the macro
/// inspects the signature and dispatches accordingly. It generates a real Rust
/// `main` that queues the function body (or its returned future) onto the main
/// runtime thread before calling [`run`].
pub use runite_proc_macros::main;

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
    AbortHandle, IntervalHandle, JoinHandle, QueueError, ThreadHandle, TimeoutHandle, WorkerHandle,
    clear_interval, clear_timeout, current_thread_handle, queue_future, queue_microtask,
    queue_task, run, run_ready_tasks, run_until_stalled, set_interval, set_timeout, spawn_worker,
    yield_now,
};

/// Standard stream primitives.
pub use stdio::{Stderr, Stdin, Stdout, stderr, stdin, stdout};

/// Spawns blocking work on the shared OS-thread pool and returns a future that
/// resolves to the closure's result. See [`task::spawn_blocking`].
pub use task::{BlockingJoinHandle, JoinError, spawn_blocking};

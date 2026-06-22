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

#![deny(missing_docs)]

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
pub(crate) mod op;
pub(crate) mod platform;
pub mod process;
pub mod signal;
pub mod stdio;
pub mod sync;
pub(crate) mod sys;
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

#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
pub use runtime_api::*;

/// The crate's core event-loop API.
///
/// Defined in one place so each item carries its own documentation (rather than
/// inheriting a single blanket summary from a grouped re-export) and so the
/// per-platform `runtime.rs` shims stay free of duplicated doc comments. The
/// items are glob-re-exported at the crate root, which is their public path.
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod runtime_api {
    use core::future::Future;
    use core::time::Duration;

    use crate::platform::current::runtime as imp;

    // Handle and marker types; their documentation lives at the definition site
    // and is inlined here through these plain (undocumented) re-exports.
    pub use crate::platform::current::runtime::{
        AbortHandle, IntervalHandle, JoinHandle, QueueError, ThreadHandle, TimeoutHandle,
        WorkerHandle, YieldNow, yield_now,
    };

    /// Queues a one-shot closure to run as a macrotask on the current runtime thread.
    ///
    /// Macrotasks run after the microtask queue has been fully drained, in FIFO
    /// order with respect to other macrotasks (timers, I/O completions, and other
    /// queued tasks). To run async work instead, use [`queue_future`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let ran = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&ran);
    /// runite::queue_task(move || flag.set(true));
    /// runite::run();
    /// assert!(ran.get());
    /// ```
    pub fn queue_task<F>(task: F)
    where
        F: FnOnce() + 'static,
    {
        imp::queue_task(task)
    }

    /// Queues a one-shot closure to run as a microtask on the current runtime thread.
    ///
    /// Microtasks run ahead of macrotasks: the runtime fully drains the microtask
    /// queue before servicing the next macrotask or polling the I/O driver. Use
    /// this for work that must complete before the loop yields to I/O again.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let order = Rc::new(Cell::new(String::new()));
    /// let a = Rc::clone(&order);
    /// let b = Rc::clone(&order);
    /// runite::queue_task(move || a.set(a.take() + "task;"));
    /// runite::queue_microtask(move || b.set(b.take() + "micro;"));
    /// runite::run();
    /// // The microtask drains before the queued macrotask runs.
    /// assert_eq!(order.take(), "micro;task;");
    /// ```
    pub fn queue_microtask<F>(task: F)
    where
        F: FnOnce() + 'static,
    {
        imp::queue_microtask(task)
    }

    /// Spawns `future` onto the current runtime thread and returns a [`JoinHandle`].
    ///
    /// The future runs concurrently with other tasks on this thread. Awaiting the
    /// returned handle yields `Result<T, JoinError>`: `Ok` with the output, or
    /// [`Err(JoinError::Aborted)`](crate::task::JoinError) if the task was aborted.
    /// Dropping the handle detaches the task; it keeps running to completion.
    ///
    /// The future is `!Send` and never migrates off this thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let out = Rc::new(Cell::new(0u32));
    /// let sink = Rc::clone(&out);
    /// runite::queue_future(async move {
    ///     let handle = runite::queue_future(async { 21u32 });
    ///     let value = handle.await.expect("task should not be aborted");
    ///     sink.set(value * 2);
    /// });
    /// runite::run();
    /// assert_eq!(out.get(), 42);
    /// ```
    pub fn queue_future<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        imp::queue_future(future)
    }

    /// Schedules `callback` to run once after at least `delay` has elapsed.
    ///
    /// Returns a [`TimeoutHandle`]; call [`TimeoutHandle::cancel`] before it
    /// fires to cancel it. For async code, prefer
    /// [`time::sleep`](crate::time::sleep).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    /// use std::time::Duration;
    ///
    /// let fired = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&fired);
    /// runite::timeout(Duration::from_millis(1), move || flag.set(true));
    /// runite::run();
    /// assert!(fired.get());
    /// ```
    pub fn timeout<F>(delay: Duration, callback: F) -> TimeoutHandle
    where
        F: FnOnce() + 'static,
    {
        imp::timeout(delay, callback)
    }

    /// Schedules `callback` to run repeatedly, once per `delay` interval.
    ///
    /// Returns an [`IntervalHandle`]; call [`IntervalHandle::cancel`] to stop
    /// it. The runtime will not exit while an interval is active, so callers
    /// must cancel it to allow [`run`] to return.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    /// use std::time::Duration;
    ///
    /// let ticks = Rc::new(Cell::new(0u32));
    /// let counter = Rc::clone(&ticks);
    /// let slot: Rc<std::cell::RefCell<Option<runite::IntervalHandle>>> =
    ///     Rc::new(std::cell::RefCell::new(None));
    /// let slot_in_cb = Rc::clone(&slot);
    /// let handle = runite::interval(Duration::from_millis(1), move || {
    ///     let n = counter.get() + 1;
    ///     counter.set(n);
    ///     if n == 3 {
    ///         slot_in_cb.borrow().as_ref().unwrap().cancel();
    ///     }
    /// });
    /// *slot.borrow_mut() = Some(handle);
    /// runite::run();
    /// assert_eq!(ticks.get(), 3);
    /// ```
    pub fn interval<F>(delay: Duration, callback: F) -> IntervalHandle
    where
        F: FnMut() + 'static,
    {
        imp::interval(delay, callback)
    }

    /// Spawns a new OS thread running its own independent runtime event loop.
    ///
    /// `initial_task` (which must be `Send`, since it crosses to the new thread)
    /// runs first on the worker; `on_exit` runs on the worker as it shuts down.
    /// Returns a [`WorkerHandle`] for joining or queueing further work via
    /// [`ThreadHandle::queue_task`]. This is the building block for scaling across
    /// cores: start one worker per core. See the crate's architecture guide.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::mpsc;
    ///
    /// let (tx, rx) = mpsc::channel();
    /// let _worker = runite::spawn_worker(
    ///     move || {
    ///         runite::queue_future(async move {
    ///             tx.send(7u32).unwrap();
    ///         });
    ///     },
    ///     || {},
    /// );
    /// assert_eq!(rx.recv().unwrap(), 7);
    /// ```
    pub fn spawn_worker<Init, Exit>(initial_task: Init, on_exit: Exit) -> WorkerHandle
    where
        Init: FnOnce() + Send + 'static,
        Exit: FnOnce() + 'static,
    {
        imp::spawn_worker(initial_task, on_exit)
    }

    /// Returns a [`ThreadHandle`] to the current runtime thread.
    ///
    /// The handle is `Send` and can be moved to other threads to queue work back
    /// onto this one with [`ThreadHandle::queue_task`].
    ///
    /// # Panics
    ///
    /// Panics if the current thread cannot initialize its runtime driver.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::queue_future(async {
    ///     let _handle = runite::current_thread_handle();
    /// });
    /// runite::run();
    /// ```
    pub fn current_thread_handle() -> ThreadHandle {
        imp::current_thread_handle()
    }

    /// Runs the current thread's event loop until all work is complete.
    ///
    /// Drives queued tasks, microtasks, timers, and I/O completions until the
    /// runtime is idle (no pending tasks, futures, timers, or active intervals),
    /// then returns. This is what [`main`](crate::main) calls after queueing the
    /// entry point.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let done = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&done);
    /// runite::queue_future(async move { flag.set(true); });
    /// runite::run();
    /// assert!(done.get());
    /// ```
    pub fn run() {
        imp::run()
    }

    /// Drives the event loop until it would next block waiting on the I/O driver.
    ///
    /// Runs all currently ready tasks, microtasks, and expired timers, then
    /// returns without sleeping for I/O — useful for embedding the runtime inside
    /// another event loop. Unlike [`run`], it does not wait for pending I/O.
    pub fn run_until_stalled() {
        imp::run_until_stalled()
    }

    /// Runs only the tasks and microtasks that are ready right now, then returns.
    ///
    /// Does not arm timers or poll the I/O driver. Intended for fine-grained
    /// manual driving of the loop when embedding the runtime.
    pub fn run_ready_tasks() {
        imp::run_ready_tasks()
    }
}

// Standard-stream handles and constructors; documentation is inlined from the
// `stdio` module's definitions.
pub use stdio::{Stderr, Stdin, Stdout, stderr, stdin, stdout};

// Blocking-offload API; documentation is inlined from the `task` module's
// definitions.
pub use task::{BlockingJoinHandle, JoinError, spawn_blocking};

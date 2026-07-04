//! Async runtime, I/O, and concurrency primitives for `runite`.
//!
//! `runite` is an event-loop-per-thread async runtime. Each runtime thread owns
//! a single-threaded event loop with JavaScript-style microtask/macrotask
//! scheduling, backed by a platform-specific async I/O backend (io_uring on
//! Linux, kqueue on macOS). Tasks on a thread are `!Send` and never migrate, so
//! most runtime state needs no locking; explicit [worker threads](spawn_worker)
//! provide parallelism and communicate through [channels](channel) and
//! [`ThreadHandle`]s.
//!
//! Unlike Tokio's default runtime or async-std, there is no work-stealing
//! multithreaded scheduler. Continuations and wakeups are queued as microtasks
//! on the same runtime thread, while timers, I/O callbacks, cross-thread wakes,
//! and [`queue_macrotask`] work run as macrotasks after the microtask queue has
//! drained.
//!
//! # Getting started
//!
//! The usual entry point is the [`#[runite::main]`](macro@main) attribute, which
//! drives the event loop to completion around your `main`:
//!
//! ```no_run
//! #[runite::main]
//! async fn main() {
//!     let contents = runite::fs::read_to_string("Cargo.toml").await.unwrap();
//!     println!("{} bytes", contents.len());
//! }
//! ```
//!
//! You can also drive the loop yourself. [`spawn`] schedules async work
//! and [`run`] runs the current thread until everything queued is complete —
//! handy for embedding the runtime or writing tests:
//!
//! ```
//! use std::rc::Rc;
//! use std::cell::Cell;
//! use std::time::Duration;
//!
//! let total = Rc::new(Cell::new(0u32));
//! let result = Rc::clone(&total);
//!
//! runite::spawn(async move {
//!     let (tx, mut rx) = runite::channel::mpsc::channel(8);
//!     runite::spawn(async move {
//!         for value in 1..=3 {
//!             runite::time::sleep(Duration::from_millis(1)).await;
//!             tx.send(value).await.unwrap();
//!         }
//!     });
//!     let mut sum = 0;
//!     while let Some(value) = rx.recv().await {
//!         sum += value;
//!     }
//!     result.set(sum);
//! });
//!
//! runite::run();
//! assert_eq!(total.get(), 6);
//! ```
//!
//! # Where to look next
//!
//! - [`main`](macro@main) for executable entry points (sync or `async fn main`)
//! - [`run`], [`queue_macrotask`], [`queue_microtask`], and [`spawn`] for
//!   driving and feeding the event loop
//! - [`spawn_worker`] and [`ThreadHandle`] for multi-threaded work
//! - [`fs`], [`net`], [`process`], [`time`], [`signal`], and [`stdio`] for async
//!   runtime services
//! - [`channel`] for `mpsc`/`oneshot`/`broadcast`/`watch` channels
//! - [`sync`] for [`Mutex`](sync::Mutex), [`Semaphore`](sync::Semaphore),
//!   [`RwLock`](sync::RwLock), [`Notify`](sync::Notify), and
//!   [`OnceCell`](sync::OnceCell)
//! - [`io`] for the crate's `AsyncRead`/`AsyncWrite`/`Stream` traits and
//!   [`BufReader`](io::BufReader)/[`BufWriter`](io::BufWriter)
//! - [`task::JoinSet`] for structured ownership of local child tasks
//! - [`task::spawn_blocking`] for offloading blocking work to a thread pool
//!
//! # Cargo features
//!
//! - `hyper` — integrate `runite` sockets with the [`hyper`] HTTP library.
//! - `futures-compat` — adapters between `runite`'s I/O traits and the
//!   `futures-io` ecosystem (see the `io::compat` module, enabled by this
//!   feature).
//!
//! [`hyper`]: https://docs.rs/hyper
//!
//! # Platform support
//!
//! `runite` currently targets:
//! - Linux (io_uring) on `x86_64` and `aarch64`
//! - macOS `aarch64` (kqueue)
//!
//! A Windows port (IOCP) is in progress. Building for any other target raises a
//! compile error.
//!
//! ## Minimum Linux kernel
//!
//! The io_uring backend targets **Linux 6.1 or newer** (the current LTS line),
//! which is what CI and the maintainers test against. It may run on older
//! kernels subject to the feature notes below, but that is not tested.
//!
//! Hard requirements (no fallback — the runtime will not function without them):
//! - **5.6** — the base ring: `openat`/`read`/`write`/`fsync`/`statx`/`close`
//!   and friends, which every file and socket operation builds on.
//! - **5.18** — `IORING_OP_MSG_RING`, used to wake one runtime thread from
//!   another. A single-threaded runtime can run without it, but
//!   [`spawn_worker`]-based multithreading (and any cross-thread
//!   [`ThreadHandle`] wake) requires 5.18+.
//!
//! Soft requirements (a synchronous syscall fallback runs transparently on
//! older kernels, so only native-io_uring performance is affected):
//! - File truncation ([`OpenOptions::truncate`](fs::OpenOptions::truncate),
//!   [`File::set_len`](fs::File::set_len)) uses `IORING_OP_FTRUNCATE` (6.9) and
//!   falls back to `ftruncate(2)`.
//! - The socket lifecycle operations — `socket` (5.19), `bind`/`listen` (6.11),
//!   and `connect`/`accept`/`shutdown`/`send`/`recv` — fall back to their
//!   blocking equivalents when the kernel lacks the opcode.
//!
//! So the recommended 6.1 LTS floor exercises every feature; the only hard
//! lower bounds are 5.6 (single-threaded) and 5.18 (multithreaded).

#![deny(missing_docs)]

#[cfg(not(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64"))))]
compile_error!("runite currently supports Linux (x86_64, aarch64) and macOS aarch64.");

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

#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
pub use runtime_api::*;

/// The crate's core event-loop API.
///
/// Defined in one place so each item carries its own documentation (rather than
/// inheriting a single blanket summary from a grouped re-export) and so the
/// per-platform `runtime.rs` shims stay free of duplicated doc comments. The
/// items are glob-re-exported at the crate root, which is their public path.
#[cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]
mod runtime_api {
    use core::future::Future;

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
    /// queued tasks). To run async work instead, use [`spawn`].
    ///
    /// # Panics
    ///
    /// Panics if the current thread's runtime state or driver cannot be
    /// initialized.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let ran = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&ran);
    /// runite::queue_macrotask(move || flag.set(true));
    /// runite::run();
    /// assert!(ran.get());
    /// ```
    pub fn queue_macrotask<F>(task: F)
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
    /// # Panics
    ///
    /// Panics if the current thread's runtime state or driver cannot be
    /// initialized.
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
    /// runite::queue_macrotask(move || a.set(a.take() + "task;"));
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
    /// The future is `!Send` and never migrates off this thread. It is first
    /// scheduled as a microtask; its first poll happens when the runtime drains
    /// the microtask queue. Subsequent wakeups are also scheduled as microtasks.
    ///
    /// # Panics
    ///
    /// Panics if the current thread's runtime state or driver cannot be
    /// initialized.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let out = Rc::new(Cell::new(0u32));
    /// let sink = Rc::clone(&out);
    /// runite::spawn(async move {
    ///     let handle = runite::spawn(async { 21u32 });
    ///     let value = handle.await.expect("task should not be aborted");
    ///     sink.set(value * 2);
    /// });
    /// runite::run();
    /// assert_eq!(out.get(), 42);
    /// ```
    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        imp::queue_future(future)
    }

    /// Spawns a new OS thread running its own independent runtime event loop.
    ///
    /// `initial_task` (which must be `Send`, since it crosses to the new thread)
    /// runs first on the worker. After the worker completes, `on_exit` is queued
    /// as a macrotask on the parent runtime thread, so its captured state does
    /// not need to be `Send`.
    /// Returns a [`WorkerHandle`] for joining or queueing further work via
    /// [`ThreadHandle::queue_macrotask`]. This is the building block for scaling across
    /// cores: start one worker per core. See the crate's architecture guide.
    ///
    /// # Panics
    ///
    /// Panics if the parent runtime state cannot be initialized, the worker
    /// runtime driver cannot be created, or the OS thread cannot be spawned.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::mpsc;
    ///
    /// let (tx, rx) = mpsc::channel();
    /// let _worker = runite::spawn_worker(
    ///     move || {
    ///         runite::spawn(async move {
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
    /// onto this one with [`ThreadHandle::queue_macrotask`].
    ///
    /// # Panics
    ///
    /// Panics if the current thread cannot initialize its runtime driver.
    ///
    /// # Examples
    ///
    /// ```
    /// runite::spawn(async {
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
    /// # Panics
    ///
    /// Panics if runtime or driver initialization fails, or if the platform
    /// driver returns an unexpected error while polling or waiting.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let done = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&done);
    /// runite::spawn(async move { flag.set(true); });
    /// runite::run();
    /// assert!(done.get());
    /// ```
    pub fn run() {
        imp::run()
    }

    /// Drives the current thread's event loop until `future` completes, then
    /// returns its output.
    ///
    /// This is the value-returning entry point: where [`run`] drives the loop to
    /// quiescence and returns `()`, `block_on` returns as soon as the given
    /// future resolves, leaving any other tasks you have spawned queued for a
    /// later `run`/`block_on`. The future is driven in place rather than spawned,
    /// so — unlike [`spawn`] — it may borrow local state and need not be `Send`
    /// or `'static`.
    ///
    /// # Panics
    ///
    /// Panics if runtime or driver initialization fails, if the driver returns
    /// an unexpected error, or if called from within a task already running on
    /// this thread (the event loop cannot be re-entered).
    ///
    /// # Examples
    ///
    /// ```
    /// let sum = runite::block_on(async {
    ///     let mut total = 0;
    ///     for value in 1..=4 {
    ///         total += value;
    ///         runite::yield_now().await;
    ///     }
    ///     total
    /// });
    /// assert_eq!(sum, 10);
    /// ```
    pub fn block_on<F>(future: F) -> F::Output
    where
        F: core::future::Future,
    {
        imp::block_on(future)
    }

    /// Drives the event loop until it would next block waiting on the I/O driver.
    ///
    /// Runs all currently ready tasks, microtasks, and expired timers, then
    /// returns without sleeping for I/O — useful for embedding the runtime inside
    /// another event loop. Unlike [`run`], it does not wait for pending I/O.
    ///
    /// # Panics
    ///
    /// Panics if runtime or driver initialization fails, or if the platform
    /// driver returns an unexpected error while polling ready events.
    pub fn run_until_stalled() {
        imp::run_until_stalled()
    }

    /// Runs only the tasks and microtasks that are ready right now, then returns.
    ///
    /// Does not arm timers or poll the I/O driver. Intended for fine-grained
    /// manual driving of the loop when embedding the runtime.
    ///
    /// # Panics
    ///
    /// Panics if the current thread's runtime state or driver cannot be
    /// initialized.
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

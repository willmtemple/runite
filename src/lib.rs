#![cfg_attr(windows, allow(clippy::arc_with_non_send_sync))]
// Windows socket owners are intentionally !Send because of IOCP affinity;
// the portable split-handle representation still uses Arc for local sharing.

//! An event-loop-per-thread async runtime with JavaScript-style scheduling,
//! built for interactive applications.
//!
//! Each `runite` runtime thread owns a single-threaded event loop with
//! JavaScript-style microtask/macrotask scheduling, backed by a
//! platform-specific async I/O backend (io_uring on Linux, kqueue on macOS,
//! IOCP on Windows). Tasks on a thread are `!Send` and never migrate, so
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
//! # The scheduling guarantee
//!
//! One property of that ordering is worth stating as a contract, because
//! layers built on this runtime batch on it:
//!
//! > **A microtask queued during a turn runs before the next macrotask.**
//!
//! A turn drains driver events, remote tasks and completed workers, then runs
//! every microtask to quiescence, then runs at most one macrotask. Because the
//! macrotask is the *last* phase, a microtask queued from inside one drains in
//! the following turn's checkpoint — a different turn, and still before that
//! turn's macrotask. The guarantee is about ordering, not about staying within
//! a single turn, which matters if you are also reading [`current_turn`].
//!
//! This is what makes coalescing possible. A reactive layer that schedules one
//! flush microtask when a value changes can rely on that flush happening before
//! anything else macro-scheduled observes the graph, so consecutive writes
//! collapse into one effect run and no one sees a half-propagated state.
//!
//! [`yield_now`] participates in the same rule: it is a microtask, so a task
//! that yields resumes before a pending macrotask rather than behind it. A loop
//! that processes a large input in chunks and yields between them therefore
//! gives the loop a turn without surrendering its place to unrelated work.
//!
//! The runtime does **not** impose a scheduling budget. Nothing preempts a
//! microtask, and nothing defers one past a macrotask to be fair. A microtask
//! chain that never yields will starve macrotasks by design — the same way
//! recursive `Promise.resolve().then` starves a browser — and a warning is
//! emitted when a checkpoint crosses a large number of microtasks while a
//! macrotask is waiting. That warning counts queue *length*, not time: a single
//! long-running microtask is invisible to it, and to everything else. Cooperative
//! yielding is the only mechanism.
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
//! Both of those start the thread's runtime as a side effect and panic if the
//! machine will not have one. [`Builder`] makes that step explicit and
//! recoverable, and is where a platform's tuning knobs are reached:
//!
//! ```no_run
//! # fn main() -> std::io::Result<()> {
//! let runtime = runite::Builder::new().build()?;
//! runtime.run();
//! # Ok(())
//! # }
//! ```
//!
//! # Where to look next
//!
//! - [`main`](macro@main) for executable entry points (sync or `async fn main`)
//! - [`Builder`] and [`Runtime`] for configured, fallible startup
//! - [`run`], [`queue_macrotask`], [`queue_microtask`], and [`spawn`] for
//!   driving and feeding the event loop
//! - [`spawn_worker`], [`WorkerHandle`], and [`ThreadHandle`] for multi-threaded work
//! - [`fs`], [`net`], [`process`], [`time`], [`signal`], and [`stdio`] for async
//!   runtime services
//! - [`channel`] for `mpsc`/`oneshot`/`broadcast`/`watch` channels
//! - [`sync`] for [`Mutex`](sync::Mutex), [`Semaphore`](sync::Semaphore),
//!   [`RwLock`](sync::RwLock), [`Notify`](sync::Notify), and
//!   [`OnceCell`](sync::OnceCell)
//! - [`io`] for the crate's `AsyncRead`/`AsyncBufRead`/`AsyncWrite`/`AsyncSeek`/`Stream` traits and
//!   [`BufReader`](io::BufReader)/[`BufWriter`](io::BufWriter)
//! - [`task::JoinSet`] for structured ownership of local child tasks
//! - [`task::spawn_blocking`] for offloading blocking work to a thread pool
//!
//! Upgrading from 0.1? Two changes are invisible to the compiler — [`run`] now
//! cancels tasks still pending at quiescence, and [`select!`](macro@select)
//! no longer polls arms in lexical order. See the 0.1 → 0.2 migration guide in
//! the repository for the full list.
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
//! - Windows (IOCP) on `x86_64`
//!
//! Building for any other target raises a compile error. On Windows, sockets,
//! files, and child-process pipes are driven by overlapped I/O through one
//! completion port per runtime thread; `runite::fd` and `runite::net::unix`
//! are Unix-only. See `docs/WINDOWS.md` in the repository for the backend
//! design.
//!
//! ## Minimum Linux kernel
//!
//! The io_uring backend recommends **Linux 6.1 or newer**. The hard floor is
//! 5.6; newer opcodes are selected opportunistically. CI runs on GitHub-hosted
//! Ubuntu runners (currently 6.8+) without pinning a kernel version, so the
//! fallback paths below are exercised by opcode-capability injection tests
//! rather than against an actual older kernel.
//!
//! Hard requirements (no fallback — the runtime will not function without them):
//! - **5.6** — the base ring: `openat`/`read`/`write`/`fsync`/`statx`/`close`
//!   and friends, which every file and socket operation builds on.
//!
//! Optional kernel acceleration:
//! - **5.18** — `IORING_OP_MSG_RING`, preferred for cross-thread runtime wakes.
//!   Older kernels transparently use a nonblocking `eventfd` watched by the
//!   target ring, including for blocking-pool completions and [`spawn_worker`].
//!
//! Other fallbacks affect native-io_uring coverage, not API availability:
//! - File truncation ([`OpenOptions::truncate`](fs::OpenOptions::truncate),
//!   [`File::set_len`](fs::File::set_len)) uses `IORING_OP_FTRUNCATE` (6.9) and
//!   falls back to `ftruncate(2)`.
//! - Directory operations ([`create_dir`](fs::create_dir),
//!   [`rename`](fs::rename), [`remove_file`](fs::remove_file),
//!   [`remove_dir`](fs::remove_dir)) use `IORING_OP_MKDIRAT` (5.15),
//!   `IORING_OP_RENAMEAT` (5.11), and `IORING_OP_UNLINKAT` (5.11), falling back
//!   to the corresponding `*at(2)` syscall on the blocking pool.
//! - The socket lifecycle operations — `socket` (5.19), `bind`/`listen` (6.11),
//!   and later `connect`/`accept`/`shutdown`/`send`/`recv` opcodes — fall back
//!   to nonblocking control calls or an io_uring readiness wait, never a
//!   blocking-pool data operation.
//!
//! Thus the recommended 6.1 baseline does not imply every newer native opcode.
//! The hard lower bound remains 5.6 for both single- and multithreaded runtimes.

#![deny(missing_docs)]
// docs.rs passes --cfg docsrs (see [package.metadata.docs.rs]); `doc_cfg`
// (which subsumed `doc_auto_cfg` in nightly 1.92) then annotates feature-gated
// items with their required feature in the rendered docs. Plain stable builds
// never see this attribute, so the crate stays stable-Rust clean.
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
)))]
compile_error!("runite currently supports Linux (x86_64, aarch64), macOS aarch64, and Windows.");

extern crate alloc;

pub(crate) mod trace_targets {
    pub const DRIVER: &str = "runite::driver";
    pub const RUNTIME: &str = "runite::runtime";
    pub const SCHEDULER: &str = "runite::scheduler";

    // Unconditional (not #[cfg(debug_assertions)]): always-on logging (e.g.
    // the task-panic report) uses these too, and cfg-gated constants create a
    // "compiles under dev, breaks under `cargo bench`/release" trap.
    pub const TIMER: &str = "runite::timer";
    pub const ASYNC: &str = "runite::async";

    /// Signal delivery. Emitted only on Windows today, where the console
    /// control handler runs on a thread the runtime does not own.
    #[cfg(windows)]
    pub const SIGNAL: &str = "runite::signal";
}

#[cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]
mod builder;
pub mod channel;
#[cfg(unix)]
pub mod fd;
pub mod fs;
#[cfg(feature = "hyper")]
pub mod hyper_rt;
pub mod io;
pub mod metrics;
pub mod net;
pub(crate) mod op;
pub mod os;
pub(crate) mod platform;
pub mod process;
pub mod signal;
pub mod stdio;
pub mod sync;
pub(crate) mod sys;
pub mod task;
pub mod time;

#[cfg(test)]
mod logic_safety_tests;

#[doc(hidden)]
pub mod macros;

pub use runite_proc_macros::{main, test};

// Explicit runtime construction; documentation lives at the definition site.
#[cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]
pub use builder::{Builder, Runtime};

#[cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]
pub use runtime_api::*;

/// The crate's core event-loop API.
///
/// Defined in one place so each item carries its own documentation (rather than
/// inheriting a single blanket summary from a grouped re-export) and so the
/// per-platform `runtime.rs` shims stay free of duplicated doc comments. The
/// items are glob-re-exported at the crate root, which is their public path.
#[cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_arch = "aarch64"),
    windows
))]
mod runtime_api {
    use core::future::Future;

    use crate::platform::current::runtime as imp;

    // Handle and marker types; their documentation lives at the definition site
    // and is inlined here through these plain (undocumented) re-exports.
    pub use crate::platform::current::runtime::{
        AbortHandle, CancelOnDrop, IntervalHandle, JoinHandle, QueueError, ThreadHandle,
        TimeoutHandle, TimerCancel, TurnId, WorkerHandle, YieldNow, yield_now,
    };
    pub use crate::platform::runtime_shared::handles::{WorkerJoin, WorkerJoinError};

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
    /// returned handle yields `Result<T, JoinError>`: `Ok` with the output,
    /// [`Err(JoinError::Aborted)`](crate::task::JoinError) after explicit
    /// abort, or [`Err(JoinError::Cancelled)`](crate::task::JoinError) if
    /// `run()` reaches quiescence with no scheduler-visible wake source.
    /// Dropping the handle detaches the task; it remains scheduled but may
    /// still be shutdown-cancelled at quiescence.
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
    /// Returns a [`WorkerHandle`] for queueing further work or awaiting full
    /// worker teardown with [`WorkerHandle::join`]. This is the building block
    /// for scaling across cores: start one worker per core. See the crate's
    /// architecture guide.
    ///
    /// The worker's runtime inherits the spawning thread's [`Builder`](crate::Builder)
    /// configuration, transitively, so a process that trimmed its I/O backend
    /// to fit a resource limit does not undo that with every worker it starts.
    ///
    /// # Panics
    ///
    /// Panics if the parent runtime state cannot be initialized, the worker
    /// runtime driver cannot be created, or the worker/reaper OS threads cannot
    /// be spawned.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::mpsc;
    ///
    /// let (tx, rx) = mpsc::channel();
    /// let worker = runite::spawn_worker(
    ///     move || {
    ///         runite::spawn(async move {
    ///             tx.send(7u32).unwrap();
    ///         });
    ///     },
    ///     || {},
    /// );
    /// runite::block_on(worker.join()).expect("worker should exit normally");
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
    /// Drives queued tasks, microtasks, timers, and I/O completions until no
    /// ready work, pending timers, live child workers, or in-flight async
    /// operations remain, then returns. On an ordinary thread, the driver
    /// remains installed for later `run`/`block_on` entries. This is what
    /// [`main`](crate::main) calls after queueing the entry point.
    ///
    /// A spawned task that is still pending at that point is resolved to
    /// [`JoinError::Cancelled`](crate::task::JoinError) and its future is
    /// dropped. Note that a task is only kept alive by a wake source the
    /// scheduler can see: runite channels, timers, I/O, `spawn_blocking`,
    /// signals, and [`WorkerHandle::join`] all register liveness, but a bare
    /// [`Waker`](std::task::Waker) clone handed to a foreign thread does not.
    ///
    /// **Changed in 0.2**: in 0.1 a pending task outlived `run()` and could be
    /// resumed by a later call. See the
    /// [migration guide](https://github.com/willmtemple/runite/blob/main/docs/MIGRATING-0.2.md).
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

    /// Drives `future` to completion, reporting startup failure instead of
    /// panicking on it.
    ///
    /// Identical to [`block_on`] once the runtime is running.
    /// The difference is only at the boundary: creating this thread's platform
    /// driver can fail, and `block_on` treats that as unrecoverable.
    ///
    /// Use this when an application needs to say something useful about not
    /// starting. Driver creation fails for reasons that are about the machine
    /// rather than the program, and none of them are the application's fault:
    ///
    /// - `io_uring` is disabled by a container or hardening policy, so there is
    ///   no I/O backend at all (`ErrorKind::Unsupported`).
    /// - The locked-memory budget is exhausted, commonly because a profiler in
    ///   the same process charges its sample buffers to it
    ///   (`ErrorKind::QuotaExceeded`).
    ///
    /// A panic in those situations gives the user a backtrace through the
    /// runtime and no way to act. An error lets the program explain itself, or
    /// fall back to a synchronous path, and exit with a status of its choosing.
    ///
    /// Only startup is fallible here. An error produced *by* the future is the
    /// future's own and is returned inside `Ok`.
    ///
    /// [`Builder`](crate::Builder) is the same recovery story with configuration
    /// attached, and separates starting the runtime from driving it.
    ///
    /// # Panics
    ///
    /// Panics if the driver returns an unexpected error while running, or if
    /// called from within a task already running on this thread (the event loop
    /// cannot be re-entered). Neither is a startup condition.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> std::process::ExitCode {
    /// use std::process::ExitCode;
    ///
    /// match runite::try_block_on(async { 6 * 7 }) {
    ///     Ok(answer) => {
    ///         assert_eq!(answer, 42);
    ///         ExitCode::SUCCESS
    ///     }
    ///     Err(error) => {
    ///         eprintln!("could not start the runtime: {error}");
    ///         ExitCode::FAILURE
    ///     }
    /// }
    /// # }
    /// ```
    pub fn try_block_on<F>(future: F) -> std::io::Result<F::Output>
    where
        F: core::future::Future,
    {
        imp::try_block_on(future)
    }

    /// Drives the event loop until it would next block waiting on the I/O driver.
    ///
    /// Runs all currently ready tasks, microtasks, and expired timers, then
    /// returns without sleeping for I/O — useful for embedding the runtime inside
    /// another event loop. Unlike [`run`], it does not wait for pending I/O.
    ///
    /// # Panics
    ///
    /// Panics if runtime or driver initialization fails, if the platform
    /// driver returns an unexpected error while polling ready events, or if
    /// called while this thread is already driving the runtime.
    pub fn run_until_stalled() {
        imp::run_until_stalled()
    }

    /// Registers a closure to run when this thread's runtime is torn down.
    ///
    /// Hooks run once, in registration order, at the start of teardown — while
    /// the runtime is still intact, before spawned tasks are cancelled and
    /// before the platform driver is destroyed. That ordering is the point: a
    /// hook that ran after cancellation would be handed a runtime that can no
    /// longer do anything.
    ///
    /// This is keyed to *teardown*, not to an entry point returning.
    /// [`run`], [`run_until_stalled`] and
    /// [`run_ready_tasks`] all return routinely — a host
    /// driving the loop with the last of those returns constantly and means
    /// nothing by it — so a hook defined as "runs when `run()` returns" would
    /// fire spuriously for such a host and never for the case this exists for.
    ///
    /// The intended use is releasing a resource the runtime cannot see: sending
    /// a final signal to a child process, flushing a log, telling a peer the
    /// process is going away. Without it, an application that must do such work
    /// on the way out has no place to put it, and typically resorts to
    /// [`std::process::exit`], which skips every destructor in the process.
    ///
    /// A hook that panics is reported and does not stop the remaining hooks or
    /// abort teardown: a half-torn-down runtime is worse than a reported panic.
    /// Hooks are `FnOnce` and `!Send`, and run on their own runtime thread.
    ///
    /// # Teardown has to happen for a hook to run
    ///
    /// On Unix, a thread that simply exits tears its runtime down through a TLS
    /// destructor, so hooks run without the application doing anything.
    ///
    /// **On Windows they do not.** TLS destructors there run while the loader
    /// lock is held, where running arbitrary user code or closing a completion
    /// port can deadlock process shutdown — so runite deliberately does not,
    /// and an ordinary thread that exits never runs its hooks. Runtime-owned
    /// workers are unaffected; they tear down explicitly before exiting.
    ///
    /// Call [`shutdown`] to tear the runtime down on the caller's own stack.
    /// It works identically everywhere, and on Windows it is the only way a
    /// hook on an application-owned thread will run at all. It is also worth
    /// preferring on Unix, where TLS destructor order is otherwise deciding
    /// when your hooks run relative to the rest of the thread's state.
    ///
    /// # Panics
    ///
    /// Panics if called from a thread with no runtime installed.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let released = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&released);
    ///
    /// runite::queue_macrotask(move || {
    ///     runite::on_shutdown(move || flag.set(true));
    /// });
    /// runite::run();
    /// // The hook runs at thread teardown, not when `run` returns.
    /// ```
    pub fn on_shutdown<F>(hook: F)
    where
        F: FnOnce() + 'static,
    {
        imp::on_shutdown(hook);
    }

    /// Tears this thread's runtime down now, running its [`on_shutdown`] hooks.
    ///
    /// Hooks run, spawned tasks are cancelled, and the platform driver is
    /// destroyed — the same teardown a thread performs on the way out, but on
    /// the caller's own stack and at a point the application chooses.
    ///
    /// Call it after [`run`] or [`block_on`] returns, when the thread is done
    /// with the runtime.
    ///
    /// # Why this exists rather than relying on the thread exiting
    ///
    /// On Windows, teardown at thread exit cannot run user code: TLS
    /// destructors hold the loader lock, and running arbitrary `Drop`
    /// implementations or closing a completion port there can deadlock process
    /// shutdown. So an application-owned thread that just exits never runs its
    /// shutdown hooks, and this is the only way to make them run.
    ///
    /// It is worth calling on Unix too, where hooks would otherwise run at a
    /// moment decided by TLS destructor order relative to everything else the
    /// thread owns.
    ///
    /// Calling it on a thread with no runtime installed does nothing, so it is
    /// safe in cleanup paths that cannot easily tell.
    ///
    /// The runtime can be used again afterwards: the next call that needs one
    /// initializes a fresh runtime for the thread, with no hooks registered.
    ///
    /// # Panics
    ///
    /// Panics if called from within a task or callback running on this
    /// runtime — the loop cannot tear itself down while it is being driven —
    /// or if the runtime is already tearing down.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::cell::Cell;
    ///
    /// let released = Rc::new(Cell::new(false));
    /// let flag = Rc::clone(&released);
    ///
    /// runite::queue_macrotask(move || {
    ///     runite::on_shutdown(move || flag.set(true));
    /// });
    /// runite::run();
    /// assert!(!released.get(), "the hook is keyed to teardown, not to `run`");
    ///
    /// runite::shutdown();
    /// assert!(released.get());
    /// ```
    pub fn shutdown() {
        imp::shutdown();
    }

    /// Returns the identifier of the event-loop turn currently being driven.
    ///
    /// A **turn** is one iteration of the loop: drain driver events, drain
    /// remote tasks, flush completed workers, run every microtask to
    /// quiescence, then run at most one macrotask. [`TurnId`] is a stable,
    /// process-wide, monotonically increasing key for that iteration. It is
    /// never reused.
    ///
    /// This exists so a consumer with its own diagnostics can join its records
    /// against the runtime's: stamp your record with the turn it happened in
    /// and match on equality afterwards. A reactive layer that flushes during
    /// the microtask checkpoint, for example, can tag that flush and know
    /// exactly which turn drove it — rather than guessing from wall-clock
    /// order across two separate captures.
    ///
    /// Returns `None` when the calling thread is not inside a turn: outside
    /// the loop entirely, or on a thread that is not a runtime thread.
    /// Every entry point that drives the loop produces turns —
    /// [`run`], [`block_on`], [`run_until_stalled`], and
    /// [`run_ready_tasks`] — so a host embedding the
    /// runtime sees them too.
    ///
    /// The value carries no information about what the turn did; it is only a
    /// key.
    ///
    /// # Stamp at the moment the work happens
    ///
    /// Call this from inside the callback that observes the work, not
    /// afterwards. A layer whose own diagnostics are delivered synchronously
    /// gets the right answer for free — a callback fired while a value is
    /// written stamps the writing turn, a callback fired when work is drained
    /// stamps the draining turn — and those are legitimately different turns.
    ///
    /// A microtask queued by a macrotask runs in the *next* turn, because the
    /// macrotask is the last phase of its own. The ordering guarantee is
    /// unaffected — the microtask still precedes the next macrotask — but the
    /// two carry different identifiers, so code that expects a piece of work
    /// and the work that scheduled it to share a turn will misread it.
    ///
    /// It follows that an **aggregate covering a span of work may cover more
    /// than one turn**, and attributing it to a single turn would be wrong in a
    /// way that looks right. Stamp individual events for attribution; report
    /// aggregates as volume, without a turn.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::cell::Cell;
    /// use std::rc::Rc;
    ///
    /// assert!(runite::current_turn().is_none(), "not in a turn yet");
    ///
    /// let seen = Rc::new(Cell::new(None));
    /// let recorder = Rc::clone(&seen);
    /// runite::queue_microtask(move || recorder.set(runite::current_turn()));
    /// runite::run();
    ///
    /// assert!(seen.get().is_some(), "a microtask runs inside a turn");
    /// assert!(runite::current_turn().is_none(), "and the turn ends with the loop");
    /// ```
    pub fn current_turn() -> Option<TurnId> {
        imp::current_turn()
    }

    /// Runs only the tasks and microtasks that are ready right now, then returns.
    ///
    /// Does not arm timers or poll the I/O driver. Intended for fine-grained
    /// manual driving of the loop when embedding the runtime.
    ///
    /// # Panics
    ///
    /// Panics if the current thread's runtime state or driver cannot be
    /// initialized, or if called while this thread is already driving the
    /// runtime.
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

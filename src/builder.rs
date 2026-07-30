//! Explicit, recoverable runtime construction.
//!
//! The entry points at the crate root start the calling thread's runtime as a
//! side effect of being called, with no way to influence how. This module adds
//! the other order: describe the runtime, start it, and get an error back if
//! the machine will not have it.

use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use std::io;
use std::rc::Rc;

use crate::platform::current::runtime as imp;
use crate::platform::runtime_shared::RuntimeConfig;

/// Configures and starts the calling thread's runtime.
///
/// Every other entry point — [`run`](crate::run), [`block_on`](crate::block_on),
/// [`spawn`](crate::spawn), a bare [`#[runite::main]`](macro@crate::main) —
/// starts a default runtime on first use and panics if it cannot. A `Builder`
/// separates those two things: [`build`](Self::build) is the one fallible step,
/// and it hands back a [`Runtime`] proving the step succeeded.
///
/// ```
/// let runtime = runite::Builder::new().build().expect("runtime should start");
/// assert_eq!(runtime.block_on(async { 6 * 7 }), 42);
/// ```
///
/// # Platform knobs
///
/// The portable builder has no tuning options, because the three backends have
/// almost no settings in common and one that is honoured on Linux but ignored
/// on macOS would be worse than absent. Backend-specific settings live on
/// extension traits.
#[cfg_attr(
    target_os = "linux",
    doc = "See [`os::linux::BuilderExt`](crate::os::linux::BuilderExt) for the \
           io_uring submission-queue size."
)]
#[cfg_attr(
    not(target_os = "linux"),
    doc = "On Linux, `runite::os::linux::BuilderExt` sets the io_uring \
           submission-queue size; this backend has no such knob."
)]
///
/// # Relationship to the entry-point attributes
///
/// A `Builder` inside a [`#[runite::main]`](macro@crate::main) or
/// [`#[runite::test]`](macro@crate::test) body is normally refused with
/// [`AlreadyExists`](io::ErrorKind::AlreadyExists): an `async` body is already
/// being driven by a runtime, and an attribute carrying settings built one
/// before the body ran. The one shape where the body precedes the runtime is a
/// *bare* attribute on a synchronous `fn` — that body runs before the
/// attribute's trailing [`run`](crate::run) — so a `build()` there succeeds and
/// the trailing `run` drives what it built. Nothing is gained by relying on
/// that: those attributes take the same settings themselves —
/// `#[runite::main(ring_entries = 32)]` — and a `Builder` is for a
/// hand-written `fn main` that wants to handle a startup failure rather than
/// panic on it.
///
/// # Relationship to `try_block_on`
///
/// [`try_block_on`](crate::try_block_on) is the same recovery story without the
/// configuration: it starts a default runtime and reports startup failure
/// through its own return value. Reach for a `Builder` when the runtime needs
/// settings, or when startup and the work it will drive belong in different
/// places in the program.
#[derive(Clone, Debug, Default)]
pub struct Builder {
    pub(crate) config: RuntimeConfig,
}

impl Builder {
    /// Creates a builder describing the default runtime.
    ///
    /// `Builder::new().build()` produces the same runtime the implicit entry
    /// points do; the difference is only that failure is returned rather than
    /// raised.
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts the runtime on the calling thread.
    ///
    /// On success the thread has a runtime, exactly as though an entry point
    /// had initialized one lazily: [`spawn`](crate::spawn),
    /// [`run`](crate::run), [`block_on`](crate::block_on) and the rest go on
    /// working, and they will not start a second one. The returned [`Runtime`]
    /// is a token for the one that now exists, not a separate instance.
    ///
    /// # Errors
    ///
    /// - [`AlreadyExists`](io::ErrorKind::AlreadyExists) if this thread already
    ///   has a runtime. A runtime is configured by the call that creates it and
    ///   nothing else; the driver a later configuration would describe has
    ///   already been built, so accepting one and doing nothing with it would
    ///   report success for a request that had no effect. This is why the call
    ///   that starts the runtime must be the *first* runtime call the thread
    ///   makes — `runite::spawn(..)` before `Builder::build()` loses.
    /// - [`InvalidInput`](io::ErrorKind::InvalidInput) if a setting is out of
    ///   range for this platform. Nothing is created in that case.
    /// - Whatever the platform driver reports, unchanged. On Linux the two that
    ///   matter are [`Unsupported`](io::ErrorKind::Unsupported) when io_uring is
    ///   disabled by a container or hardening policy and
    ///   [`QuotaExceeded`](io::ErrorKind::QuotaExceeded) when the locked-memory
    ///   budget is exhausted — commonly because a profiler in the same process
    ///   charges its sample buffers to it.
    ///
    /// [`shutdown`](crate::shutdown) removes the thread's runtime and with it
    /// the reason for `AlreadyExists`, so it is the way to re-configure a
    /// thread that has already started one.
    ///
    /// # Panics
    ///
    /// Panics if called from a destructor that runs after the thread's runtime
    /// TLS has already been released — a `thread_local!` initialized before
    /// runite's, so destroyed after it. The thread is on its way out and
    /// nothing can install a runtime on it. Every entry point shares this
    /// floor, including [`block_on`](crate::block_on) and
    /// [`try_block_on`](crate::try_block_on); it is not specific to `build`.
    ///
    /// # Examples
    ///
    /// Exit with a diagnostic rather than a backtrace when the machine will not
    /// run a runtime:
    ///
    /// ```no_run
    /// # fn main() -> std::process::ExitCode {
    /// use std::process::ExitCode;
    ///
    /// let runtime = match runite::Builder::new().build() {
    ///     Ok(runtime) => runtime,
    ///     Err(error) => {
    ///         eprintln!("could not start the runtime: {error}");
    ///         return ExitCode::FAILURE;
    ///     }
    /// };
    ///
    /// runtime.run();
    /// ExitCode::SUCCESS
    /// # }
    /// ```
    pub fn build(self) -> io::Result<Runtime> {
        imp::build_runtime(self.config)?;
        Ok(Runtime {
            _thread_bound: PhantomData,
        })
    }
}

/// A started runtime, bound to the thread that built it.
///
/// # This is a token, not an owner
///
/// runite is event-loop-per-thread: a runtime *is* the thread's state, held in
/// thread-local storage and released when the thread ends. There is no
/// free-floating runtime object to own, so `Runtime` owns nothing. It records
/// that [`Builder::build`] succeeded on this thread, which is what makes its
/// methods infallible where the free functions are not: they cannot fail at
/// startup, because startup already happened.
///
/// Three consequences follow, and none of them are hidden:
///
/// - **Dropping it shuts nothing down.** The thread keeps its runtime, and the
///   free functions keep working. Teardown happens when the thread exits, or
///   when a runtime-owned worker finishes.
/// - **It is `!Send` and `!Sync`.** The runtime it refers to is this thread's,
///   and tasks on it never migrate. A `Runtime` moved elsewhere would name a
///   loop the new thread cannot drive, so the type system refuses the move:
///
///   ```compile_fail
///   let runtime = runite::Builder::new().build().expect("runtime should start");
///   std::thread::spawn(move || runtime.run());
///   ```
/// - **There is at most one per thread.** A second [`Builder::build`] fails,
///   including after this value is dropped, because dropping did not remove the
///   runtime it names.
///
/// Every method here has a free-function equivalent at the crate root with the
/// same semantics; those panic on a startup failure this type has already ruled
/// out.
pub struct Runtime {
    // `Rc` is the conventional !Send + !Sync marker and costs nothing here.
    _thread_bound: PhantomData<Rc<()>>,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Written out rather than derived: the marker field carries no
        // information, and rendering it would print the `PhantomData` that
        // implements the thread affinity.
        f.debug_struct("Runtime").finish_non_exhaustive()
    }
}

impl Runtime {
    /// Runs the event loop until all work is complete. See [`run`](crate::run).
    ///
    /// # Panics
    ///
    /// Panics if the platform driver returns an unexpected error while polling
    /// or waiting, or if called from inside a task already running on this
    /// thread.
    pub fn run(&self) {
        imp::run()
    }

    /// Drives the loop until `future` completes, then returns its output. See
    /// [`block_on`](crate::block_on).
    ///
    /// # Panics
    ///
    /// Panics if the platform driver returns an unexpected error, or if called
    /// from within a task already running on this thread (the event loop cannot
    /// be re-entered).
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        imp::block_on(future)
    }

    /// Drives the loop until it would next block on I/O. See
    /// [`run_until_stalled`](crate::run_until_stalled).
    ///
    /// # Panics
    ///
    /// Panics if the platform driver returns an unexpected error while polling
    /// ready events, or if called while this thread is already driving the
    /// runtime.
    pub fn run_until_stalled(&self) {
        imp::run_until_stalled()
    }

    /// Runs only the work that is ready right now. See
    /// [`run_ready_tasks`](crate::run_ready_tasks).
    ///
    /// # Panics
    ///
    /// Panics if called while this thread is already driving the runtime.
    pub fn run_ready_tasks(&self) {
        imp::run_ready_tasks()
    }
}

//! Linux-specific extensions to runite types.
//!
//! Everything here concerns the io_uring backend and has no counterpart in the
//! kqueue or IOCP backends. Keeping it behind an extension trait is what stops
//! a portable program from compiling against a setting that two of the three
//! platforms would silently discard.

use crate::Builder;
use crate::platform::linux::uring::{MAX_RING_ENTRIES, MIN_RING_ENTRIES};

/// Linux-specific extensions to [`Builder`].
pub trait BuilderExt {
    /// Sets the number of submission-queue entries in this thread's io_uring.
    ///
    /// The default is 256, which is generous for an interactive workload and
    /// modest against a typical `RLIMIT_MEMLOCK`. Lower it when something else
    /// in the process is drawing on the same locked-memory budget, and raise it
    /// when a thread routinely has more operations in flight than the ring can
    /// hold in one `io_uring_enter`.
    ///
    /// The value is a *submission* capacity, not a limit on outstanding
    /// operations: the driver queues submissions that do not fit and publishes
    /// them as the ring drains. A small ring costs extra `io_uring_enter` calls
    /// under load, not correctness.
    ///
    /// # Why this is not on `Builder`
    ///
    /// kqueue and IOCP have nothing corresponding to a ring size. Putting this
    /// on the portable builder would mean a call that does something on one
    /// platform and nothing at all on another, which is a bug that only shows
    /// up as a memory-limit failure on the platform nobody tested.
    ///
    /// # Locked memory
    ///
    /// io_uring pins its rings against `RLIMIT_MEMLOCK`, and so do other things
    /// — `perf record` charges its sample buffers to the same budget, which is
    /// why profiling a runite program can fail at startup with
    /// [`QuotaExceeded`](std::io::ErrorKind::QuotaExceeded) on a machine with
    /// tens of gigabytes free. Shrinking the ring is the knob that makes the
    /// program fit next to the profiler; raising the limit or profiling with
    /// smaller buffers (`perf record -m 32`) are the alternatives.
    ///
    /// # Worker threads
    ///
    /// [`spawn_worker`](crate::spawn_worker) creates a runtime on a new thread,
    /// and that runtime inherits this setting from the thread that spawned it,
    /// transitively. The budget this knob exists to fit inside is per *process*,
    /// so a main thread trimmed to 32 entries with workers silently taking 256
    /// each would defeat the point on exactly the machines where it matters.
    ///
    /// # Accepted values
    ///
    /// A power of two from 2 to 32768 inclusive. Anything else makes
    /// [`build`](Builder::build) fail with
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput) rather than being
    /// adjusted: `io_uring_setup(2)` rounds up to a power of two and clamps to
    /// `IORING_MAX_ENTRIES`, both without saying so, and a caller sizing a ring
    /// against a memory budget that quietly received a larger one has been told
    /// the opposite of what it needs to know. The lower bound is the smallest
    /// ring that can hold the runtime's largest atomic submission, an operation
    /// published together with its linked timeout.
    ///
    /// The setting is validated by `build`, not here, so it stays chainable.
    ///
    /// # From an entry-point attribute
    ///
    /// [`#[runite::main]`](macro@crate::main) and
    /// [`#[runite::test]`](macro@crate::test) start the thread's runtime before
    /// the body runs, so a `Builder` inside one arrives too late and reports
    /// [`AlreadyExists`](std::io::ErrorKind::AlreadyExists). Give the setting
    /// to the attribute instead — it builds the runtime it names:
    ///
    /// ```
    /// #[runite::main(ring_entries = 32)]
    /// async fn main() {
    ///     let contents = runite::fs::read_to_string("Cargo.toml").await.unwrap();
    ///     assert!(contents.contains("runite"));
    /// }
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::os::linux::BuilderExt;
    ///
    /// let runtime = runite::Builder::new()
    ///     .ring_entries(32)
    ///     .build()
    ///     .expect("a 32-entry ring should be available");
    /// assert_eq!(runtime.block_on(async { 6 * 7 }), 42);
    /// ```
    ///
    /// ```
    /// use runite::os::linux::BuilderExt;
    ///
    /// let error = runite::Builder::new()
    ///     .ring_entries(100)
    ///     .build()
    ///     .expect_err("100 is not a power of two");
    /// assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    /// ```
    fn ring_entries(self, entries: u32) -> Self;
}

impl BuilderExt for Builder {
    fn ring_entries(mut self, entries: u32) -> Self {
        self.config.ring_entries = Some(entries);
        self
    }
}

// Referenced by the accepted-values documentation above; the assertion keeps
// that prose from drifting away from the constants the check actually uses.
const _: () = assert!(MIN_RING_ENTRIES == 2 && MAX_RING_ENTRIES == 32_768);

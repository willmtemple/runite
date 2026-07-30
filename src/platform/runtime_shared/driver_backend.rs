//! Driver backend trait: the minimal per-platform surface the shared
//! scheduler depends on.

use std::any::Any;
use std::io;
use std::time::Duration;

/// Bits of readiness reported by [`DriverBackend::poll`].
///
/// Every field is a best-effort hint — the driver is allowed to wake up
/// spuriously, in which case the scheduler simply finds nothing to do and
/// blocks again.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct ReadyEvents {
    /// One or more timer expirations are pending.
    pub timer: bool,
    /// One or more cross-thread wake notifications are pending.
    pub wake: bool,
    /// The driver dispatched at least one I/O completion.
    ///
    /// Reported by the driver rather than inferred from the runtime's
    /// `operations_completed` counter: that counter is bumped from the
    /// blocking pool too, so a delta over a turn's wall-clock window proves
    /// nothing about what *this* driver saw. Completions consumed inside
    /// [`DriverBackend::wait`] have no `ReadyEvents` of their own, so every
    /// backend holds the bit until the next `poll` reports it.
    pub io: bool,
}

/// The per-platform surface that the shared scheduler consumes.
///
/// All scheduling, timer management, and task plumbing in `runtime_shared`
/// operate through this trait. Platform-specific operations (e.g.
/// `cancel_operation`, `cancel_fd_readiness`) live on the concrete `Driver`
/// type and are reached by downcasting through [`Self::as_any`].
#[doc(hidden)]
pub trait DriverBackend: Send + 'static {
    /// Polls the driver without blocking.
    fn poll(&self) -> io::Result<Option<ReadyEvents>>;

    /// Blocks the current thread until at least one event is available or
    /// the currently armed timer expires.
    fn wait(&self) -> io::Result<()>;

    /// Updates (or clears) the currently armed runtime timer.
    fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()>;

    /// Drains any pending wake-notification count. Returns `Some(n)` if at
    /// least one wake was pending, otherwise `None`.
    fn drain_wake(&self) -> Option<u64>;

    /// Drains any pending timer-expiration count. Returns `Some(n)` if at
    /// least one timer fired, otherwise `None`.
    fn drain_timer(&self) -> Option<u64>;

    /// Installs any thread-local state the driver needs. Called once after
    /// the driver is moved into a runtime thread. The shared runtime owner is
    /// installed afterwards so its TLS destructor runs first at thread exit.
    fn bind_current_thread(&self);

    /// Tears down any thread-local state installed by
    /// [`Self::bind_current_thread`]. Runtime-owned workers call this from an
    /// explicit scope guard before their thread function returns. On Unix,
    /// arbitrary runtime threads also use it from final TLS teardown. Windows
    /// cannot safely run arbitrary cleanup from a TLS destructor under the
    /// loader lock, so an unscoped user thread's last-resort destructor only
    /// publishes closure and deliberately leaks the remaining driver state.
    fn unbind_current_thread(&self);

    /// Downcast hook used by per-platform `runtime.rs` shims to reach
    /// driver-specific entry points (e.g. `cancel_operation`).
    fn as_any(&self) -> &dyn Any;
}

/// Cross-thread wake-up trait.
///
/// One instance of a notifier is stored inside `ThreadShared`, which is
/// itself wrapped in `Arc` and shared across thread boundaries. Notifiers
/// do not need to be cloneable — they only need to be `Send + Sync` so
/// other threads can call [`Self::notify`].
#[doc(hidden)]
pub trait Notifier: Send + Sync + 'static {
    /// Wakes the runtime thread the notifier targets.
    fn notify(&self) -> io::Result<()>;
}

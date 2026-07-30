//! Per-thread runtime configuration.
//!
//! A [`RuntimeConfig`] is fixed when a thread's runtime state is installed and
//! is never mutated afterwards: the driver it describes has already been
//! created by then, so a later change could not take effect. It is stored on
//! [`ThreadState`](super::state::ThreadState) rather than in its own
//! thread-local so that [`spawn_worker`](super::scheduler::spawn_worker) can
//! read the spawning thread's configuration through the same accessor it
//! already uses for the parent handle.
//!
//! Every field is platform-conditional. A knob that one backend honours and
//! another silently ignores is the trap the public API is shaped to avoid, and
//! keeping the internal struct honest about which targets have the field means
//! a platform shim cannot accidentally read one that means nothing to it.

use std::io;

/// Configuration applied when a runtime thread's driver is created.
///
/// `Default` is what every entry point other than
/// [`Builder`](crate::Builder) installs, so the defaults here are the
/// documented behaviour of `run`, `block_on`, and friends.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeConfig {
    /// Submission-queue entries for this thread's `io_uring`. `None` selects
    /// [`DEFAULT_RING_ENTRIES`](crate::platform::linux::uring::DEFAULT_RING_ENTRIES).
    #[cfg(target_os = "linux")]
    pub(crate) ring_entries: Option<u32>,
}

impl RuntimeConfig {
    /// Rejects a configuration the platform cannot honour, before any driver
    /// is created.
    ///
    /// Validation is deferred to this point rather than performed in the
    /// setters so that the builder can stay chainable and report every failure
    /// through one `io::Result`.
    pub(crate) fn validate(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(entries) = self.ring_entries {
            crate::platform::linux::uring::check_ring_entries(entries)?;
        }
        Ok(())
    }
}

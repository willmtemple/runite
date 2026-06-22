//! Single-threaded async synchronization primitives.
//!
//! The types in this module coordinate futures that stay on one runite runtime
//! thread. They are intentionally `!Send`/`!Sync` and use thread-local wakeups
//! rather than cross-thread atomics.
//!
//! Use [`Mutex`] for exclusive access to shared task-local state, [`Semaphore`]
//! for limiting concurrent access, [`Notify`] for one-shot task wakeups, and
//! [`OnceCell`] for asynchronous initialize-once values.

mod mutex;
mod notify;
mod once_cell;
mod semaphore;

pub use mutex::{Mutex, MutexGuard};
pub use notify::Notify;
pub use once_cell::OnceCell;
pub use semaphore::{Permit, Semaphore};

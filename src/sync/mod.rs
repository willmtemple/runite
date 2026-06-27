//! Single-threaded async synchronization primitives.
//!
//! The types in this module coordinate futures that stay on one runite runtime
//! thread. They are intentionally `!Send`/`!Sync` and use thread-local wakeups
//! rather than cross-thread atomics.
//!
//! Use [`Mutex`] for exclusive access to shared task-local state, [`RwLock`] for
//! shared-or-exclusive access, [`Semaphore`] for limiting concurrent access,
//! [`Notify`] for one-shot task wakeups, and [`OnceCell`] for asynchronous
//! initialize-once values.
//!
//! # Examples
//!
//! ```
//! use std::cell::Cell;
//! use std::rc::Rc;
//!
//! let mutex = Rc::new(runite::sync::Mutex::new(0));
//! let observed = Rc::new(Cell::new(0));
//!
//! runite::queue_future({
//!     let mutex = Rc::clone(&mutex);
//!     let observed = Rc::clone(&observed);
//!     async move {
//!         let mut value = mutex.lock().await;
//!         *value = 42;
//!         observed.set(*value);
//!     }
//! });
//!
//! runite::run();
//!
//! assert_eq!(observed.get(), 42);
//! ```

mod mutex;
mod notify;
mod once_cell;
mod rw_lock;
mod semaphore;

pub use mutex::{Mutex, MutexGuard};
pub use notify::Notify;
pub use once_cell::OnceCell;
pub use rw_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{Permit, Semaphore};

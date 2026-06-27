//! Single-threaded async synchronization primitives.
//!
//! The types in this module coordinate futures that stay on one runite runtime
//! thread. They are intentionally `!Send`/`!Sync` and use thread-local wakeups
//! rather than cross-thread atomics.
//! They do not provide cross-thread fairness or synchronization: use channels,
//! [`crate::ThreadHandle`], or [`crate::WorkerHandle`] to coordinate between
//! runtime threads.
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
//! runite::spawn({
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
//!
//! `Notify` is useful for local one-shot wakeups:
//!
//! ```
//! use std::cell::Cell;
//! use std::rc::Rc;
//!
//! let notify = Rc::new(runite::sync::Notify::new());
//! let woke = Rc::new(Cell::new(false));
//!
//! runite::spawn({
//!     let notify = Rc::clone(&notify);
//!     let woke = Rc::clone(&woke);
//!     async move {
//!         notify.notified().await;
//!         woke.set(true);
//!     }
//! });
//!
//! runite::queue_macrotask({
//!     let notify = Rc::clone(&notify);
//!     move || notify.notify_one()
//! });
//!
//! runite::run();
//! assert!(woke.get());
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

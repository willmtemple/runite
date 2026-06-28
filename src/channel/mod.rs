//! Async channels for task and thread communication.
//!
//! The channel types in this module are userspace synchronization primitives.
//! They do not carry messages through the kernel; channel state lives in shared
//! Rust data structures, and kernel/runtime integration is only used to wake the
//! task that is waiting for readiness. If a sender completes a waiter on the
//! waiter's owning runtime thread, runite schedules a microtask. If completion
//! comes from another thread, runite queues a macrotask onto the owner thread
//! using the platform-specific remote wake path: `io_uring` `MSG_RING` on Linux,
//! and a pipe/eventfd-equivalent wakeup with `kqueue` on macOS aarch64.
//!
//! Message values are `T: Send` so producers can be used across threads, but
//! async waits are registered with a specific runtime thread and must be polled
//! from a runite event loop. Channels therefore provide communication between
//! tasks and threads without becoming a work-stealing scheduler.
//!
//! Choose a channel by the delivery pattern you need:
//!
//! - [`mpsc`] for work queues where many producers feed one consumer.
//! - [`oneshot`] for completing one request with one value.
//! - [`broadcast`] for fan-out where each receiver sees each message sent after it subscribes.
//! - [`watch`] for publishing the latest state, where receivers only need the newest value.
//!
//! # Examples
//!
//! ```
//! runite::spawn(async {
//!     let (tx, mut rx) = runite::channel::mpsc::channel(4);
//!     tx.send("queued work").await.unwrap();
//!     assert_eq!(rx.recv().await, Some("queued work"));
//! });
//!
//! runite::run();
//! ```

pub mod broadcast;
pub mod mpsc;
pub mod oneshot;
pub mod watch;

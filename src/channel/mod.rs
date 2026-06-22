//! Async channels for task and thread communication.
//!
//! The channel types in this module are userspace synchronization primitives. They do not carry
//! messages through the kernel; instead, channel state lives in shared Rust data structures and
//! the runtime uses `io_uring` `MSG_RING` notifications only to wake the owning runtime thread
//! when an async waiter becomes ready.
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
//! runite::queue_future(async {
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

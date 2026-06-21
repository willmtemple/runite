//! Async channels for inter-thread communication.
//!
//! The channel types in this module are userspace synchronization primitives. They do not carry
//! messages through the kernel; instead, channel state lives in shared Rust data structures and
//! the runtime uses `io_uring` `MSG_RING` notifications only to wake the owning runtime thread
//! when an async waiter becomes ready.
//!
//! The initial surface includes:
//!
//! - [`oneshot`] for single-value handoff
//! - [`mpsc`] for bounded and unbounded multi-producer/single-consumer queues

pub mod mpsc;
pub mod oneshot;

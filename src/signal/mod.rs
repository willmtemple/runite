//! Async signal handling.
//!
//! This module lets runtime tasks wait for process signals without blocking the
//! thread-local event loop. [`ctrl_c`] is the portable entry point for shutdown
//! handling, while [`unix`] exposes streams for specific Unix signal kinds such
//! as terminal resize notifications.
//!
//! POSIX signals are process-global, while this runtime is thread-local and
//! supports `!Send` futures. The Unix backend therefore uses one process-wide
//! async-signal-safe handler plus a dedicated blocking-pool reader task. The
//! handler records a pending bit and wakes a self-pipe/eventfd; the reader task
//! drains that fd and forwards signal notifications to every registered runtime
//! thread with [`crate::ThreadHandle::queue_macrotask`].
//!
//! # Examples
//!
//! ```no_run
//! use std::cell::Cell;
//! use std::rc::Rc;
//!
//! let shutting_down = Rc::new(Cell::new(false));
//!
//! runite::spawn({
//!     let shutting_down = Rc::clone(&shutting_down);
//!     async move {
//!         runite::signal::ctrl_c()
//!             .await
//!             .expect("Ctrl-C handler should install");
//!         shutting_down.set(true);
//!     }
//! });
//!
//! runite::run();
//! ```

pub mod unix;

/// Awaits a single Ctrl-C interrupt request.
///
/// This is a convenience wrapper around
/// [`unix::signal(unix::SignalKind::Interrupt)`](unix::signal). It completes
/// once and then drops its signal stream.
///
/// # Examples
///
/// ```no_run
/// runite::spawn(async {
///     runite::signal::ctrl_c()
///         .await
///         .expect("Ctrl-C handler should install");
///     eprintln!("received shutdown request");
/// });
///
/// runite::run();
/// ```
pub async fn ctrl_c() -> std::io::Result<()> {
    let mut signal = unix::signal(unix::SignalKind::Interrupt)?;
    signal.recv().await;
    Ok(())
}

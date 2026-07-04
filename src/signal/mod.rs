//! Async signal handling.
//!
//! This module lets runtime tasks wait for process signals without blocking the
//! thread-local event loop. [`ctrl_c`] is the portable entry point for shutdown
//! handling; the platform submodule (`unix` or `windows`) exposes streams for
//! specific event kinds, such as terminal resize notifications on Unix or
//! console close events on Windows.
//!
//! POSIX signals are process-global, while this runtime is thread-local and
//! supports `!Send` futures. The Unix backend therefore uses one process-wide
//! async-signal-safe handler plus a dedicated blocking-pool reader task. The
//! handler records a pending bit and wakes a self-pipe/eventfd; the reader task
//! drains that fd and forwards signal notifications as per-thread macrotasks
//! with [`crate::ThreadHandle::queue_macrotask`].
//!
//! This is different from Tokio's default multi-threaded scheduler and
//! async-std: runite cannot freely move `!Send` signal streams between worker
//! threads, so delivery fans out to the runtime threads that registered local
//! streams. Delivery is best-effort. Closed runtime threads are skipped, and a
//! wake for a live thread can be dropped if its macrotask queue is full.
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

#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

/// Awaits a single Ctrl-C interrupt request.
///
/// This is the portable shutdown-signal entry point: on Unix it wraps
/// `unix::signal(unix::SignalKind::Interrupt)`, and on Windows it wraps
/// `windows::ctrl_c`. It completes once and then drops its signal stream.
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
    #[cfg(unix)]
    {
        let mut signal = unix::signal(unix::SignalKind::Interrupt)?;
        signal.recv().await;
    }
    #[cfg(windows)]
    {
        let mut interrupts = windows::ctrl_c()?;
        interrupts.recv().await;
    }
    Ok(())
}

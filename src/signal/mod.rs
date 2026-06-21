//! Async signal handling.
//!
//! POSIX signals are process-global, while this runtime is thread-local and
//! supports `!Send` futures. The Unix backend therefore uses one process-wide
//! async-signal-safe handler plus a dedicated blocking-pool reader task. The
//! handler records a pending bit and wakes a self-pipe/eventfd; the reader task
//! drains that fd and forwards signal notifications to every registered runtime
//! thread with [`crate::ThreadHandle::queue_task`].

pub mod unix;

/// Awaits a single Ctrl-C (`SIGINT`).
pub async fn ctrl_c() -> std::io::Result<()> {
    let mut signal = unix::signal(unix::SignalKind::Interrupt)?;
    signal.recv().await;
    Ok(())
}

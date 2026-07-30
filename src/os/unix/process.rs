//! Unix-specific extensions to [`runite::process`](crate::process).
//!
//! Mirrors the parts of [`std::os::unix::process`] that runite's
//! [`crate::process::Command`] can honour.

use std::io;
use std::sync::Arc;

use crate::process::Command;
use crate::process::command::PreExec;

/// Unix-specific extensions to [`Command`].
///
/// Mirrors [`std::os::unix::process::CommandExt`], which is where the shape of
/// [`pre_exec`](Self::pre_exec) comes from.
pub trait CommandExt {
    /// Runs `hook` in the child between `fork` and `exec`.
    ///
    /// This is the only place an application can touch the child while it is
    /// still the child but not yet the new program — the window in which a
    /// process acquires a controlling terminal, changes session or process
    /// group, drops privileges, or adjusts resource limits.
    ///
    /// The hook may run more than once, because a [`Command`] may be spawned
    /// more than once. It takes `Fn` rather than `FnMut` for that reason;
    /// [`std::os::unix::process::CommandExt::pre_exec`] takes `FnMut` because a
    /// `std` `Command` owns its hook outright.
    ///
    /// Registering a second hook adds to the first rather than replacing it, as
    /// in `std`: every hook runs, in registration order. A builder that layers
    /// two concerns onto one [`Command`] therefore gets both, instead of
    /// silently losing whichever was registered first.
    ///
    /// Returning `Err` from a hook aborts the spawn, and [`Command::spawn`]
    /// returns that error. Hooks registered after it do not run.
    ///
    /// # Safety
    ///
    /// The hook runs in a forked child, in which only async-signal-safe
    /// operations are sound. In a multithreaded parent only this thread
    /// survives into the child, so any lock another thread held at `fork` is
    /// still held and can never be released — allocating, or anything that
    /// might allocate, can therefore deadlock. Confine the hook to raw
    /// syscalls, and do not allocate, take locks, or call into code that might.
    ///
    /// # Examples
    ///
    /// Give a child a controlling terminal — a new session, then
    /// `TIOCSCTTY` on the descriptor that will become its standard input.
    /// Without this the parent never sees end of file when the child exits.
    ///
    /// ```no_run
    /// # fn example(user: std::os::fd::OwnedFd) -> std::io::Result<()> {
    /// use std::os::fd::AsRawFd;
    /// use runite::os::unix::process::CommandExt;
    /// use runite::process::{Command, Stdio};
    ///
    /// let raw = user.as_raw_fd();
    /// let mut command = Command::new("sh");
    /// command.stdin(Stdio::from(user));
    ///
    /// // SAFETY: `setsid` and `ioctl` are async-signal-safe, and neither
    /// // allocates nor takes a lock.
    /// unsafe {
    ///     command.pre_exec(move || {
    ///         if libc::setsid() == -1 {
    ///             return Err(std::io::Error::last_os_error());
    ///         }
    ///         if libc::ioctl(raw, libc::TIOCSCTTY.into(), 0) == -1 {
    ///             return Err(std::io::Error::last_os_error());
    ///         }
    ///         Ok(())
    ///     });
    /// }
    ///
    /// let child = command.spawn()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Once the child owns the terminal this way, **read the controller
    /// concurrently with the child's exit, not after it.** A session leader's
    /// exit runs the terminal's teardown, which drains the pending output queue
    /// before tearing the line down; on BSD-derived kernels (including macOS)
    /// that drain blocks until something reads the controller. A parent that
    /// [`waits`](crate::process::Child::wait) for the child before reading the
    /// controller therefore deadlocks — the child cannot finish exiting until
    /// its output is read, and the read never begins until the child exits.
    /// Drive the controller read and the wait together, or drain the controller
    /// on another thread. Linux tolerates reading afterward; the concurrent read
    /// is correct everywhere.
    unsafe fn pre_exec(
        &mut self,
        hook: impl Fn() -> io::Result<()> + Send + Sync + 'static,
    ) -> &mut Self;
}

impl CommandExt for Command {
    unsafe fn pre_exec(
        &mut self,
        hook: impl Fn() -> io::Result<()> + Send + Sync + 'static,
    ) -> &mut Self {
        self.push_pre_exec(PreExec(Arc::new(hook)))
    }
}

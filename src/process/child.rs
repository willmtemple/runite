//! Handles for spawned subprocesses.
//!
//! A [`Child`] represents a running operating-system process plus any async
//! pipes requested at spawn time. Use it to inspect the process id, poll for
//! completion, wait asynchronously, or request termination.
//!
//! # Examples
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use runite::process::Command;
//!
//! let mut child = Command::new("true").spawn()?;
//! assert!(child.id().is_some());
//! assert!(child.wait().await?.success());
//! # Ok(())
//! # }
//! ```
//!
use std::io;

use super::{ChildStderr, ChildStdin, ChildStdout, ExitStatus};

/// A spawned child process.
///
/// A `Child` owns the operating-system process handle and any async standard
/// stream pipes requested through [`Command`](super::Command). Drop closes the
/// Rust-side handle but does not wait for the process or terminate it. The OS
/// child may keep running, and on Unix a completed child may remain unreaped
/// until some handle waits for it. Call [`wait`](Self::wait) to reap it.
pub struct Child {
    inner: crate::sys::current::process::Child,
    stdin_handoff: Option<crate::stdio::InheritedStdinHandoff>,
    /// Handle to child stdin when configured with [`super::Stdio::piped`].
    pub stdin: Option<ChildStdin>,
    /// Handle to child stdout when configured with [`super::Stdio::piped`].
    pub stdout: Option<ChildStdout>,
    /// Handle to child stderr when configured with [`super::Stdio::piped`].
    pub stderr: Option<ChildStderr>,
}

/// Reports the process identifier and which standard streams are piped.
///
/// Written by hand rather than derived: the platform child and the pipe types
/// have nothing useful to print, and a derive would force `Debug` onto every
/// backend internal to say so.
impl std::fmt::Debug for Child {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Child")
            .field("id", &self.id())
            .field("stdin", &self.stdin.as_ref().map(|_| "piped"))
            .field("stdout", &self.stdout.as_ref().map(|_| "piped"))
            .field("stderr", &self.stderr.as_ref().map(|_| "piped"))
            .finish()
    }
}

impl Child {
    pub(crate) fn from_inner(
        mut inner: crate::sys::current::process::Child,
        stdin_handoff: Option<crate::stdio::InheritedStdinHandoff>,
    ) -> Self {
        let stdin = inner.stdin.take().map(ChildStdin::from_pipe);
        let stdout = inner.stdout.take().map(ChildStdout::from_pipe);
        let stderr = inner.stderr.take().map(ChildStderr::from_pipe);
        Self {
            inner,
            stdin_handoff,
            stdin,
            stdout,
            stderr,
        }
    }

    /// Adopts an already-running process so its exit can be awaited.
    ///
    /// Use this when something other than [`Command`](super::Command) started
    /// the process — a `std::process::Command` spawned for an API runite does
    /// not have, or a helper that returns a pid. Within the supported case
    /// below, the returned `Child` is not a second-class handle: it simply has
    /// no standard-stream pipes, and [`wait`](Self::wait),
    /// [`try_wait`](Self::try_wait), [`id`](Self::id), and [`kill`](Self::kill)
    /// behave as they do for a spawned child.
    ///
    /// Exit notification is event-driven on every platform: a pidfd on Linux,
    /// a `kqueue` process filter on macOS, and a registered wait on the process
    /// handle on Windows. No thread is parked for the process's lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error if no process with this identifier exists. A process
    /// that has already exited **and been reaped** no longer exists, so adopting
    /// it fails rather than returning a `Child` whose `wait` never completes.
    ///
    /// Adoption is not an access check, and how much of one it happens to
    /// perform differs by platform: Linux opens a pidfd, which requires no
    /// rights over the target and so adopts any live pid, including pid 1;
    /// macOS probes with signal 0 and can report `EPERM`; Windows opens the
    /// process for synchronize and query-limited-information and reports
    /// `ERROR_ACCESS_DENIED` without them. Success says the pid was live, not
    /// that anything else will be permitted.
    ///
    /// On Unix, `pid` is validated before any syscall: zero, or a value beyond
    /// the platform's `pid_t`, is [`io::ErrorKind::InvalidInput`] rather than a
    /// lookup failure. Windows passes the value straight to the OS.
    ///
    /// # Caveats
    ///
    /// - **On Unix the process must be a direct child of this one.** Reading an
    ///   exit status requires being the parent, and nothing at adoption time can
    ///   tell whether the target is a child, so adoption succeeds for any live
    ///   pid and the failure surfaces later: for a non-child,
    ///   [`wait`](Self::wait), [`try_wait`](Self::try_wait) and
    ///   [`kill`](Self::kill) all fail *immediately* with `ECHILD` — `wait` does
    ///   not wait for the exit, and `kill` does not send the signal. Windows has
    ///   no such restriction.
    /// - **On Windows adoption may outrun what the caller is allowed to do.**
    ///   Terminate rights are asked for but not required, so a process the
    ///   caller may wait on but not terminate still adopts and
    ///   [`kill`](Self::kill) is the call that reports `ERROR_ACCESS_DENIED`.
    /// - **Nothing else may reap the process.** If another `Child`, a
    ///   `std::process::Child`, or a `SIGCHLD` handler calls `waitpid` for the
    ///   same pid, whichever gets there first takes the status and the other
    ///   sees an error.
    /// - **A pid is not a stable identity.** It can be reused once the process
    ///   is reaped, so a pid obtained long ago may name a different process by
    ///   the time it is adopted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Child;
    ///
    /// // Started elsewhere, for an API runite does not cover.
    /// let started = std::process::Command::new("sleep").arg("1").spawn()?;
    ///
    /// let mut child = Child::from_pid(started.id())?;
    /// let status = child.wait().await?;
    /// assert!(status.success());
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_pid(pid: u32) -> io::Result<Self> {
        Ok(Self::from_inner(
            crate::sys::current::process::from_pid(pid)?,
            None,
        ))
    }

    /// Returns the OS process identifier, if the child has not been reaped.
    ///
    /// The exact identifier is platform-specific and should be treated as an
    /// opaque process id.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let child = Command::new("sleep").arg("1").spawn()?;
    /// assert!(child.id().is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn id(&self) -> Option<u32> {
        self.inner.id()
    }

    /// Attempts to collect the exit status without blocking.
    ///
    /// Returns `Ok(None)` while the child is still running.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let mut child = Command::new("true").spawn()?;
    /// let _maybe_status = child.try_wait()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let status = self
            .inner
            .try_wait()
            .map(|status| status.map(ExitStatus::from_std))?;
        if status.is_some() {
            self.stdin_handoff = None;
        }
        Ok(status)
    }

    /// Waits asynchronously for the child to exit.
    ///
    /// Dropping the wait future cancels only that wait operation. It does not
    /// kill the child and does not reap an already-exited child; call `wait`
    /// again or use [`try_wait`](Self::try_wait) to collect the status.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let mut child = Command::new("true").spawn()?;
    /// let status = child.wait().await?;
    /// assert!(status.success());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.inner.wait().await.map(ExitStatus::from_std)?;
        self.stdin_handoff = None;
        Ok(status)
    }

    /// Sends a forceful termination request to the child.
    ///
    /// On Unix this sends `SIGKILL`. A successful return means the signal was
    /// accepted or the process was already gone; it does not mean the final exit
    /// status has been collected. Call [`wait`](Self::wait) afterward to observe
    /// and reap the final status.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let mut child = Command::new("sleep").arg("60").spawn()?;
    /// child.kill()?;
    /// let _status = child.wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn kill(&mut self) -> io::Result<()> {
        self.inner.kill()
    }
}

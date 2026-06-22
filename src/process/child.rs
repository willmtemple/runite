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
//! let mut child = Command::new("/bin/true").spawn()?;
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
/// Rust-side handle but does not wait for the process; call [`wait`](Self::wait)
/// to reap it.
pub struct Child {
    inner: crate::sys::current::process::Child,
    /// Handle to child stdin when configured with [`super::Stdio::piped`].
    pub stdin: Option<ChildStdin>,
    /// Handle to child stdout when configured with [`super::Stdio::piped`].
    pub stdout: Option<ChildStdout>,
    /// Handle to child stderr when configured with [`super::Stdio::piped`].
    pub stderr: Option<ChildStderr>,
}

impl Child {
    pub(crate) fn from_inner(mut inner: crate::sys::current::process::Child) -> Self {
        let stdin = inner.stdin.take().map(ChildStdin::from_pipe);
        let stdout = inner.stdout.take().map(ChildStdout::from_pipe);
        let stderr = inner.stderr.take().map(ChildStderr::from_pipe);
        Self {
            inner,
            stdin,
            stdout,
            stderr,
        }
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
    /// let child = Command::new("/bin/sleep").arg("1").spawn()?;
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
    /// let mut child = Command::new("/bin/true").spawn()?;
    /// let _maybe_status = child.try_wait()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.inner
            .try_wait()
            .map(|status| status.map(ExitStatus::from_std))
    }

    /// Waits asynchronously for the child to exit.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let mut child = Command::new("/bin/true").spawn()?;
    /// let status = child.wait().await?;
    /// assert!(status.success());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.inner.wait().await.map(ExitStatus::from_std)
    }

    /// Sends a forceful termination request to the child.
    ///
    /// This only asks the operating system to terminate the process; call
    /// [`wait`](Self::wait) afterward to observe the final status.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let mut child = Command::new("/bin/sleep").arg("60").spawn()?;
    /// child.kill()?;
    /// let _status = child.wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn kill(&mut self) -> io::Result<()> {
        self.inner.kill()
    }
}

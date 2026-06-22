//! Subprocess exit status values.
//!
//! This module wraps the platform exit status returned by a completed child
//! process and exposes portable success and exit-code queries, plus Unix signal
//! termination details on Unix platforms.
//!
//! # Examples
//!
//! ```no_run
//! use runite::process::Command;
//!
//! runite::queue_future(async {
//!     let status = Command::new("/bin/true")
//!         .status()
//!         .await
//!         .expect("process should run");
//!     assert!(status.success());
//! });
//!
//! runite::run();
//! ```
//!
use std::process::ExitStatus as StdExitStatus;

/// Exit status returned by a completed subprocess.
///
/// `ExitStatus` is returned by [`Child::wait`](super::Child::wait) and
/// [`Command::status`](super::Command::status). It keeps the platform-specific
/// representation private while exposing the common queries most callers need.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExitStatus(pub(crate) StdExitStatus);

impl ExitStatus {
    pub(crate) fn from_std(status: StdExitStatus) -> Self {
        Self(status)
    }

    /// Returns `true` if the process exited successfully.
    ///
    /// On most platforms this means the process returned exit code `0`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let status = Command::new("/bin/true").status().await?;
    /// assert!(status.success());
    /// # Ok(())
    /// # }
    /// ```
    pub fn success(&self) -> bool {
        self.0.success()
    }

    /// Returns the process exit code, if it exited normally.
    ///
    /// Returns [`None`] when the operating system reports that the process ended
    /// for another reason, such as a Unix signal.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let status = Command::new("/bin/true").status().await?;
    /// assert_eq!(status.code(), Some(0));
    /// # Ok(())
    /// # }
    /// ```
    pub fn code(&self) -> Option<i32> {
        self.0.code()
    }

    /// Returns the signal that terminated the process, if any.
    ///
    /// This method is available only on Unix platforms.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let mut child = Command::new("/bin/sleep").arg("60").spawn()?;
    /// child.kill()?;
    /// let status = child.wait().await?;
    /// #[cfg(unix)]
    /// assert!(status.signal().is_some());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(unix)]
    pub fn signal(&self) -> Option<i32> {
        use std::os::unix::process::ExitStatusExt;

        self.0.signal()
    }
}

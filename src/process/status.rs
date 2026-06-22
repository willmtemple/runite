use std::process::ExitStatus as StdExitStatus;

/// Exit status returned by a completed subprocess.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExitStatus(pub(crate) StdExitStatus);

impl ExitStatus {
    pub(crate) fn from_std(status: StdExitStatus) -> Self {
        Self(status)
    }

    /// Returns `true` if the process exited successfully.
    pub fn success(&self) -> bool {
        self.0.success()
    }

    /// Returns the process exit code, if it exited normally.
    pub fn code(&self) -> Option<i32> {
        self.0.code()
    }

    /// Returns the signal that terminated the process, if any.
    #[cfg(unix)]
    pub fn signal(&self) -> Option<i32> {
        use std::os::unix::process::ExitStatusExt;

        self.0.signal()
    }
}

use std::io;

use super::{ChildStderr, ChildStdin, ChildStdout, ExitStatus};

/// A spawned child process.
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
    pub fn id(&self) -> Option<u32> {
        self.inner.id()
    }

    /// Attempts to collect the exit status without blocking.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.inner
            .try_wait()
            .map(|status| status.map(ExitStatus::from_std))
    }

    /// Waits asynchronously for the child to exit.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.inner.wait().await.map(ExitStatus::from_std)
    }

    /// Sends a forceful termination request to the child.
    pub fn kill(&mut self) -> io::Result<()> {
        self.inner.kill()
    }
}

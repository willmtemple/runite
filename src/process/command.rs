//! Builders and standard-stream configuration for subprocesses.
//!
//! [`Command`] accumulates a program, arguments, environment changes, working
//! directory, and standard-stream choices before spawning a [`Child`](super::Child).
//! [`Stdio`] describes how each child stream is connected.
//!
//! Spawning delegates to [`std::process::Command`] and is synchronous on the
//! calling runtime thread. The returned [`Child`](super::Child), if any, becomes
//! async when waiting for process exit or driving piped stdio handles through
//! runite's fd-readiness backend.
//!
//! # Examples
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use runite::process::{Command, Stdio};
//!
//! let output = Command::new("echo")
//!     .arg("hello")
//!     .stdout(Stdio::piped())
//!     .output()
//!     .await?;
//! assert_eq!(output, b"hello\n");
//! # Ok(())
//! # }
//! ```
//!
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};

use super::{Child, ExitStatus};
use crate::io::AsyncReadExt;

/// Subprocess standard I/O configuration.
///
/// Use this with [`Command::stdin`], [`Command::stdout`], and
/// [`Command::stderr`] to decide whether a child inherits a standard stream,
/// connects it to the null device, or exposes it as an async pipe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stdio(pub(crate) StdioKind);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StdioKind {
    Inherit,
    Null,
    Piped,
}

impl Stdio {
    /// Inherits the parent process handle for this standard stream.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Stdio;
    ///
    /// let inherited = Stdio::inherit();
    /// ```
    pub fn inherit() -> Self {
        Self(StdioKind::Inherit)
    }

    /// Connects this standard stream to the platform null device.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Stdio;
    ///
    /// let discarded = Stdio::null();
    /// ```
    pub fn null() -> Self {
        Self(StdioKind::Null)
    }

    /// Creates an async pipe connected to the child handle.
    ///
    /// Use this when the parent task needs to asynchronously write child stdin
    /// or read child stdout/stderr.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Stdio;
    ///
    /// let piped = Stdio::piped();
    /// ```
    pub fn piped() -> Self {
        Self(StdioKind::Piped)
    }
}

#[derive(Clone, Debug)]
pub(crate) enum EnvChange {
    Set(OsString, OsString),
    Remove(OsString),
    Clear,
}

#[derive(Clone, Debug)]
pub(crate) struct CommandSpec {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub env: Vec<EnvChange>,
    pub current_dir: Option<PathBuf>,
    pub stdin: StdioKind,
    pub stdout: StdioKind,
    pub stderr: StdioKind,
}

/// Builder for spawning an async subprocess.
///
/// `Command` mirrors the shape of [`std::process::Command`] while returning
/// runtime-aware child handles and async pipes. Configuration methods mutate the
/// builder and return `&mut Self` so they can be chained before [`spawn`](Self::spawn),
/// [`status`](Self::status), or [`output`](Self::output).
///
/// Calling [`spawn`](Self::spawn) itself is synchronous and delegates to
/// [`std::process::Command::spawn`]. Async runtime integration begins with
/// [`Child::wait`](super::Child::wait) and with piped standard streams.
#[derive(Clone, Debug)]
pub struct Command {
    spec: CommandSpec,
}

impl Command {
    /// Creates a command that runs `program`.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let command = Command::new("echo");
    /// ```
    pub fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            spec: CommandSpec {
                program: program.as_ref().to_os_string(),
                args: Vec::new(),
                env: Vec::new(),
                current_dir: None,
                stdin: StdioKind::Inherit,
                stdout: StdioKind::Inherit,
                stderr: StdioKind::Inherit,
            },
        }
    }

    /// Adds one argument to the command line.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("echo");
    /// command.arg("hello");
    /// ```
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.spec.args.push(arg.as_ref().to_os_string());
        self
    }

    /// Adds multiple arguments to the command line.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("echo");
    /// command.args(["hello", "world"]);
    /// ```
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.spec
            .args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_os_string()));
        self
    }

    /// Sets or overrides an environment variable for the child.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("env");
    /// command.env("APP_MODE", "test");
    /// ```
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.spec.env.push(EnvChange::Set(
            key.as_ref().to_os_string(),
            value.as_ref().to_os_string(),
        ));
        self
    }

    /// Sets or overrides multiple environment variables for the child.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("env");
    /// command.envs([("APP_MODE", "test"), ("APP_COLOR", "never")]);
    /// ```
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (key, value) in vars {
            self.env(key, value);
        }
        self
    }

    /// Removes an environment variable from the child environment.
    ///
    /// The removal is applied after inherited environment handling and before
    /// the child starts.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("env");
    /// command.env_remove("APP_MODE");
    /// ```
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.spec
            .env
            .push(EnvChange::Remove(key.as_ref().to_os_string()));
        self
    }

    /// Clears the child environment.
    ///
    /// Variables added later with [`env`](Self::env) or [`envs`](Self::envs)
    /// are still included.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("env");
    /// command.env_clear().env("PATH", "/usr/bin");
    /// ```
    pub fn env_clear(&mut self) -> &mut Self {
        self.spec.env.push(EnvChange::Clear);
        self
    }

    /// Sets the child working directory.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::Command;
    ///
    /// let mut command = Command::new("pwd");
    /// command.current_dir(".");
    /// ```
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.spec.current_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Configures the child's standard input stream.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::{Command, Stdio};
    ///
    /// let mut command = Command::new("cat");
    /// command.stdin(Stdio::piped());
    /// ```
    pub fn stdin(&mut self, stdio: Stdio) -> &mut Self {
        self.spec.stdin = stdio.0;
        self
    }

    /// Configures the child's standard output stream.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::{Command, Stdio};
    ///
    /// let mut command = Command::new("echo");
    /// command.stdout(Stdio::piped());
    /// ```
    pub fn stdout(&mut self, stdio: Stdio) -> &mut Self {
        self.spec.stdout = stdio.0;
        self
    }

    /// Configures the child's standard error stream.
    ///
    /// # Examples
    ///
    /// ```
    /// use runite::process::{Command, Stdio};
    ///
    /// let mut command = Command::new("echo");
    /// command.stderr(Stdio::null());
    /// ```
    pub fn stderr(&mut self, stdio: Stdio) -> &mut Self {
        self.spec.stderr = stdio.0;
        self
    }

    /// Spawns the command and returns a handle to the running child.
    ///
    /// If any standard stream was configured with [`Stdio::piped`], the
    /// corresponding field on the returned [`Child`] contains an async pipe.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn example() -> std::io::Result<()> {
    /// use runite::process::{Command, Stdio};
    ///
    /// let mut child = Command::new("echo")
    ///     .arg("hello")
    ///     .stdout(Stdio::piped())
    ///     .spawn()?;
    /// assert!(child.stdout.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn(&mut self) -> io::Result<Child> {
        crate::sys::current::process::spawn(&self.spec).map(Child::from_inner)
    }

    /// Spawns the command and waits asynchronously for it to exit.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let status = Command::new("true").status().await?;
    /// assert!(status.success());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        self.spawn()?.wait().await
    }

    /// Spawns the command, captures stdout, and waits for it to exit.
    ///
    /// The command's standard output is forced to [`Stdio::piped`]. The returned
    /// bytes contain stdout only; a non-success exit status is reported as an
    /// [`io::Error`].
    ///
    /// If you configure [`stderr`](Self::stderr) as [`Stdio::piped`] before
    /// calling this, its output is drained and discarded concurrently so the
    /// child cannot deadlock by filling the stderr pipe buffer. To capture
    /// stderr yourself, spawn the child with [`spawn`](Self::spawn) and read the
    /// streams directly instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let output = Command::new("echo").arg("hello").output().await?;
    /// assert_eq!(output, b"hello\n");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn output(&mut self) -> io::Result<Vec<u8>> {
        self.stdout(Stdio::piped());
        let mut child = self.spawn()?;

        // If the caller pre-configured a piped stderr, drain it concurrently.
        // A child that writes enough to fill the stderr pipe buffer would
        // otherwise block on `write`, while we block reading stdout, deadlocking
        // the runtime thread. Reading both streams concurrently avoids this.
        let stderr_drain = child.stderr.take().map(|mut stderr| {
            crate::spawn(async move {
                let mut sink = Vec::new();
                let _ = stderr.read_to_end(&mut sink).await;
            })
        });

        let mut output = Vec::new();
        if let Some(stdout) = child.stdout.as_mut() {
            stdout.read_to_end(&mut output).await?;
        }
        if let Some(drain) = stderr_drain {
            let _ = drain.await;
        }
        let status = child.wait().await?;
        if status.success() {
            Ok(output)
        } else {
            Err(io::Error::other("process exited unsuccessfully"))
        }
    }
}

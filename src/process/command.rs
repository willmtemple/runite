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
//! use runite::process::Command;
//!
//! let output = Command::new("echo")
//!     .arg("hello")
//!     .output()
//!     .await?;
//! assert_eq!(output.stdout, b"hello\n");
//! # Ok(())
//! # }
//! ```
//!
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};

use super::{Child, ExitStatus};
use crate::io::AsyncReadExt;

/// The captured result of a process run by [`Command::output`].
///
/// Mirrors [`std::process::Output`]: the exit status plus the fully-buffered
/// standard output and standard error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Output {
    /// The status (exit code) the process terminated with.
    pub status: ExitStatus,
    /// The bytes the process wrote to standard output.
    pub stdout: Vec<u8>,
    /// The bytes the process wrote to standard error.
    pub stderr: Vec<u8>,
}

/// The owned OS object a [`Stdio`] can be built from.
///
/// A file descriptor on Unix, a handle on Windows.
#[cfg(unix)]
pub(crate) type OwnedStdio = std::os::fd::OwnedFd;
/// The owned OS object a [`Stdio`] can be built from.
///
/// A file descriptor on Unix, a handle on Windows.
#[cfg(windows)]
pub(crate) type OwnedStdio = std::os::windows::io::OwnedHandle;

/// Subprocess standard I/O configuration.
///
/// Use this with [`Command::stdin`], [`Command::stdout`], and
/// [`Command::stderr`] to decide whether a child inherits a standard stream,
/// connects it to the null device, exposes it as an async pipe, or is wired to
/// a descriptor the caller already owns.
///
/// Like [`std::process::Stdio`], this is neither `Clone` nor `Copy`: a variant
/// built from an owned descriptor owns that descriptor.
#[derive(Debug)]
pub struct Stdio(pub(crate) StdioKind);

#[derive(Debug)]
pub(crate) enum StdioKind {
    Inherit,
    Null,
    Piped,
    Raw(OwnedStdio),
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

/// Wires a child standard stream to a descriptor the caller already owns.
///
/// The descriptor is duplicated at each [`Command::spawn`], so the `Stdio`
/// stays usable across repeated spawns and the caller's original is unaffected.
/// This is how a child is attached to something runite does not model — a
/// pseudoterminal, a socket accepted elsewhere, a preopened log file.
///
/// # Examples
///
/// ```no_run
/// # fn example() -> std::io::Result<()> {
/// use std::fs::File;
/// use std::os::fd::OwnedFd;
/// use runite::process::Stdio;
///
/// let log: OwnedFd = File::create("child.log")?.into();
/// let stdout = Stdio::from(log);
/// # Ok(())
/// # }
/// ```
#[cfg(unix)]
impl From<std::os::fd::OwnedFd> for Stdio {
    fn from(fd: std::os::fd::OwnedFd) -> Self {
        Self(StdioKind::Raw(fd))
    }
}

/// Wires a child standard stream to a handle the caller already owns.
///
/// The handle is duplicated at each [`Command::spawn`], so the `Stdio` stays
/// usable across repeated spawns and the caller's original is unaffected.
///
/// # Examples
///
/// ```no_run
/// # fn example() -> std::io::Result<()> {
/// use std::fs::File;
/// use std::os::windows::io::OwnedHandle;
/// use runite::process::Stdio;
///
/// let log: OwnedHandle = File::create("child.log")?.into();
/// let stdout = Stdio::from(log);
/// # Ok(())
/// # }
/// ```
#[cfg(windows)]
impl From<std::os::windows::io::OwnedHandle> for Stdio {
    fn from(handle: std::os::windows::io::OwnedHandle) -> Self {
        Self(StdioKind::Raw(handle))
    }
}

/// A hook to run in the child between `fork` and `exec`.
///
/// Stored behind an [`Arc`](std::sync::Arc) so a [`Command`] can be spawned
/// more than once. The `Debug` impl is opaque because a closure has nothing
/// useful to print.
#[cfg(unix)]
#[derive(Clone)]
pub(crate) struct PreExec(pub(crate) std::sync::Arc<dyn Fn() -> io::Result<()> + Send + Sync>);

#[cfg(unix)]
impl std::fmt::Debug for PreExec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PreExec(..)")
    }
}

#[derive(Clone, Debug)]
pub(crate) enum EnvChange {
    Set(OsString, OsString),
    Remove(OsString),
    Clear,
}

#[derive(Debug)]
pub(crate) struct CommandSpec {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub env: Vec<EnvChange>,
    pub current_dir: Option<PathBuf>,
    pub stdin: StdioKind,
    pub stdout: StdioKind,
    pub stderr: StdioKind,
    #[cfg(unix)]
    pub pre_exec: Vec<PreExec>,
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
///
/// Like [`std::process::Command`], this is not `Clone`: a standard stream can
/// own a descriptor (see [`Stdio::from`]), and duplicating one implicitly would
/// hide a `dup` behind a `clone`.
#[derive(Debug)]
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
                #[cfg(unix)]
                pre_exec: Vec::new(),
            },
        }
    }

    /// Registers a hook to run in the child between `fork` and `exec`.
    ///
    /// Used internally by
    /// [`os::unix::process::CommandExt::pre_exec`](crate::os::unix::process::CommandExt::pre_exec),
    /// which carries the safety contract. Hooks accumulate rather than replace,
    /// so a builder that layers two of them does not silently lose the first.
    #[cfg(unix)]
    pub(crate) fn push_pre_exec(&mut self, hook: PreExec) -> &mut Self {
        self.spec.pre_exec.push(hook);
        self
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
    /// When stdin is inherited, runite's process-wide stdin reader is paused
    /// until the child's exit is observed or the returned handle is dropped.
    ///
    /// # Blocking
    ///
    /// `spawn` is synchronous and runs on the calling runtime thread. With
    /// inherited stdin it waits for the process-wide reader to release the
    /// terminal, which normally takes microseconds — an interrupt releases a
    /// reader parked in `poll`. It cannot un-issue a `read(2)` the reader has
    /// already entered, though, and on an interactive terminal that read
    /// returns only when the user types something. The wait is therefore
    /// bounded: past that bound `spawn` reports
    /// [`io::ErrorKind::WouldBlock`] rather than stalling the event loop
    /// indefinitely, and the caller may retry.
    ///
    /// Windows reports the same error immediately rather than waiting, because
    /// a console read there cannot be interrupted at all.
    ///
    /// To keep the event loop free of this entirely, configure stdin with
    /// [`Stdio::null`] or [`Stdio::piped`], or spawn from
    /// [`spawn_blocking`](crate::spawn_blocking).
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
        let stdin_handoff = matches!(self.spec.stdin, StdioKind::Inherit)
            .then(crate::stdio::handoff_stdin_to_child)
            .transpose()?;
        let inner = crate::sys::current::process::spawn(&self.spec)?;
        Ok(Child::from_inner(inner, stdin_handoff))
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

    /// Spawns the command, captures its output, and waits for it to exit.
    ///
    /// Returns an [`Output`] with the exit status and the fully-buffered stdout
    /// and stderr. Like [`std::process::Command::output`], this forces stdout and
    /// stderr to [`Stdio::piped`] and redirects stdin to [`Stdio::null`] (so a
    /// child that reads stdin sees EOF immediately rather than blocking). A
    /// non-zero exit status is **not** an error — inspect
    /// [`output.status`](Output::status) yourself. stdout and stderr are read
    /// concurrently so a child cannot deadlock by filling one pipe's buffer.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::process::Command;
    ///
    /// let output = Command::new("echo").arg("hello").output().await?;
    /// assert!(output.status.success());
    /// assert_eq!(output.stdout, b"hello\n");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn output(&mut self) -> io::Result<Output> {
        self.stdin(Stdio::null());
        self.stdout(Stdio::piped());
        self.stderr(Stdio::piped());
        let mut child = self.spawn()?;

        // Drain stderr on a separate task while reading stdout here, so a child
        // that fills one pipe's buffer while we block on the other cannot
        // deadlock the runtime thread.
        let stderr_reader = child.stderr.take().map(|mut stderr| {
            crate::spawn(async move {
                let mut buf = Vec::new();
                stderr.read_to_end(&mut buf).await.map(|_| buf)
            })
        });

        let mut stdout = Vec::new();
        if let Some(out) = child.stdout.as_mut() {
            out.read_to_end(&mut stdout).await?;
        }

        let stderr = match stderr_reader {
            Some(handle) => handle
                .await
                .expect("stderr reader task should not be aborted")?,
            None => Vec::new(),
        };

        let status = child.wait().await?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}

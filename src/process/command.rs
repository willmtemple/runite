use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};

use super::{Child, ExitStatus};
use crate::io::AsyncReadExt;

/// Subprocess standard I/O configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stdio(pub(crate) StdioKind);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StdioKind {
    Inherit,
    Null,
    Piped,
}

impl Stdio {
    /// Inherit the parent process handle.
    pub fn inherit() -> Self {
        Self(StdioKind::Inherit)
    }

    /// Connect the handle to the platform null device.
    pub fn null() -> Self {
        Self(StdioKind::Null)
    }

    /// Create an async pipe connected to the child handle.
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
#[derive(Clone, Debug)]
pub struct Command {
    spec: CommandSpec,
}

impl Command {
    /// Creates a command that runs `program`.
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

    /// Adds one argument.
    pub fn arg(&mut self, arg: impl AsRef<OsStr>) -> &mut Self {
        self.spec.args.push(arg.as_ref().to_os_string());
        self
    }

    /// Adds multiple arguments.
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

    /// Sets an environment variable for the child.
    pub fn env(&mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> &mut Self {
        self.spec.env.push(EnvChange::Set(
            key.as_ref().to_os_string(),
            value.as_ref().to_os_string(),
        ));
        self
    }

    /// Sets multiple environment variables for the child.
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
    pub fn env_remove(&mut self, key: impl AsRef<OsStr>) -> &mut Self {
        self.spec
            .env
            .push(EnvChange::Remove(key.as_ref().to_os_string()));
        self
    }

    /// Clears the child environment.
    pub fn env_clear(&mut self) -> &mut Self {
        self.spec.env.push(EnvChange::Clear);
        self
    }

    /// Sets the child working directory.
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.spec.current_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Configures child stdin.
    pub fn stdin(&mut self, stdio: Stdio) -> &mut Self {
        self.spec.stdin = stdio.0;
        self
    }

    /// Configures child stdout.
    pub fn stdout(&mut self, stdio: Stdio) -> &mut Self {
        self.spec.stdout = stdio.0;
        self
    }

    /// Configures child stderr.
    pub fn stderr(&mut self, stdio: Stdio) -> &mut Self {
        self.spec.stderr = stdio.0;
        self
    }

    /// Spawns the command.
    pub fn spawn(&mut self) -> io::Result<Child> {
        crate::sys::current::process::spawn(&self.spec).map(Child::from_inner)
    }

    /// Spawns the command and waits for it to exit.
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        self.spawn()?.wait().await
    }

    /// Spawns the command, captures stdout, and waits for it to exit.
    pub async fn output(&mut self) -> io::Result<Vec<u8>> {
        self.stdout(Stdio::piped());
        let mut child = self.spawn()?;
        let mut output = Vec::new();
        if let Some(stdout) = child.stdout.as_mut() {
            stdout.read_to_end(&mut output).await?;
        }
        let status = child.wait().await?;
        if status.success() {
            Ok(output)
        } else {
            Err(io::Error::other("process exited unsuccessfully"))
        }
    }
}

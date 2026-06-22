//! Linux subprocess backend.

use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command as StdCommand, ExitStatus as StdExitStatus};

use crate::process::pipe::Pipe;
use crate::process::{CommandSpec, EnvChange, StdioKind};

/// Spawned child process state.
pub(crate) struct Child {
    pid: Option<libc::pid_t>,
    pidfd: OwnedFd,
    status: Option<StdExitStatus>,
    pub(crate) stdin: Option<Pipe>,
    pub(crate) stdout: Option<Pipe>,
    pub(crate) stderr: Option<Pipe>,
}

/// Spawns a child process from a platform-neutral command spec.
pub(crate) fn spawn(spec: &CommandSpec) -> io::Result<Child> {
    let mut command = StdCommand::new(&spec.program);
    command.args(&spec.args);
    for change in &spec.env {
        match change {
            EnvChange::Set(key, value) => {
                command.env(key, value);
            }
            EnvChange::Remove(key) => {
                command.env_remove(key);
            }
            EnvChange::Clear => {
                command.env_clear();
            }
        }
    }
    if let Some(dir) = &spec.current_dir {
        command.current_dir(dir);
    }
    command.stdin(stdio(spec.stdin));
    command.stdout(stdio(spec.stdout));
    command.stderr(stdio(spec.stderr));

    let mut child = command.spawn()?;
    let pid = child.id() as libc::pid_t;
    let pidfd = match pidfd_open(pid) {
        Ok(pidfd) => pidfd,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };

    let stdin = child
        .stdin
        .take()
        .map(|pipe| pipe_from_raw_fd(pipe.into_raw_fd()))
        .transpose()?;
    let stdout = child
        .stdout
        .take()
        .map(|pipe| pipe_from_raw_fd(pipe.into_raw_fd()))
        .transpose()?;
    let stderr = child
        .stderr
        .take()
        .map(|pipe| pipe_from_raw_fd(pipe.into_raw_fd()))
        .transpose()?;

    Ok(Child {
        pid: Some(pid),
        pidfd,
        status: None,
        stdin,
        stdout,
        stderr,
    })
}

impl Child {
    pub fn id(&self) -> Option<u32> {
        self.pid.and_then(|pid| u32::try_from(pid).ok())
    }

    pub fn try_wait(&mut self) -> io::Result<Option<StdExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        let Some(pid) = self.pid else {
            return Ok(self.status);
        };
        match waitpid(pid, libc::WNOHANG)? {
            Some(status) => {
                self.pid = None;
                self.status = Some(status);
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    pub async fn wait(&mut self) -> io::Result<StdExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let Some(pid) = self.pid else {
            return self.status.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "child status is unavailable")
            });
        };

        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            crate::sys::current::fd::wait_readable(self.pidfd.as_raw_fd()).await?;
            if let Some(status) = waitpid(pid, libc::WNOHANG)? {
                self.pid = None;
                self.status = Some(status);
                return Ok(status);
            }
        }
    }

    pub fn kill(&mut self) -> io::Result<()> {
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        let Some(pid) = self.pid else {
            return Ok(());
        };
        loop {
            // SAFETY: `pid` is the process id returned by `spawn`; `SIGKILL` has
            // no pointer arguments and does not alias Rust memory.
            let result = unsafe { libc::kill(pid, libc::SIGKILL) };
            if result == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.raw_os_error() == Some(libc::ESRCH) && self.try_wait()?.is_some() {
                return Ok(());
            }
            return Err(error);
        }
    }
}

pub(crate) fn read_pipe_future(fd: RawFd, len: usize) -> PinPipeRead {
    Box::pin(read_pipe(fd, len))
}

pub(crate) fn write_pipe_future(fd: RawFd, data: Vec<u8>) -> PinPipeWrite {
    Box::pin(write_pipe(fd, data))
}

type PinPipeRead =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PinPipeWrite =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + 'static>>;

async fn read_pipe(fd: RawFd, len: usize) -> io::Result<Vec<u8>> {
    loop {
        let mut data = vec![0u8; len];
        // SAFETY: `fd` is expected to be an open nonblocking pipe read end, and
        // `data` owns `len` writable bytes for the duration of this call.
        let read = unsafe { libc::read(fd, data.as_mut_ptr().cast::<libc::c_void>(), len) };
        if read >= 0 {
            data.truncate(read as usize);
            return Ok(data);
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            crate::sys::current::fd::wait_readable(fd).await?;
        } else if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

async fn write_pipe(fd: RawFd, data: Vec<u8>) -> io::Result<usize> {
    loop {
        // SAFETY: `fd` is expected to be an open nonblocking pipe write end, and
        // `data` remains alive and immutable for the duration of this call.
        let written = unsafe { libc::write(fd, data.as_ptr().cast::<libc::c_void>(), data.len()) };
        if written >= 0 {
            return Ok(written as usize);
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            crate::sys::current::fd::wait_writable(fd).await?;
        } else if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn stdio(kind: StdioKind) -> std::process::Stdio {
    match kind {
        StdioKind::Inherit => std::process::Stdio::inherit(),
        StdioKind::Null => std::process::Stdio::null(),
        StdioKind::Piped => std::process::Stdio::piped(),
    }
}

fn pipe_from_raw_fd(fd: RawFd) -> io::Result<Pipe> {
    set_nonblocking(fd)?;
    set_cloexec(fd)?;
    // SAFETY: `fd` was transferred from a std child pipe with `into_raw_fd`, so
    // it is valid and uniquely owned here.
    Ok(Pipe::new(unsafe { OwnedFd::from_raw_fd(fd) }))
}

fn pidfd_open(pid: libc::pid_t) -> io::Result<OwnedFd> {
    // SAFETY: `syscall` is invoked with the pid returned by spawn and flags=0;
    // it returns either a fresh fd or -1 with errno set.
    let fd = cvt_long(unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) })? as RawFd;
    set_cloexec(fd)?;
    // SAFETY: `fd` is a fresh descriptor returned by successful pidfd_open.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn waitpid(pid: libc::pid_t, options: libc::c_int) -> io::Result<Option<StdExitStatus>> {
    loop {
        let mut status = 0;
        // SAFETY: `status` points to writable storage and `pid` is a child pid
        // returned by spawn. `options` is either 0 or WNOHANG.
        let result = unsafe { libc::waitpid(pid, &mut status, options) };
        if result > 0 {
            return Ok(Some(StdExitStatus::from_raw(status)));
        }
        if result == 0 {
            return Ok(None);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is valid for the duration of the fcntl call.
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFD) })?;
    // SAFETY: `fd` is valid, and the flags are the current descriptor flags plus
    // FD_CLOEXEC.
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) })?;
    Ok(())
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is valid for the duration of the fcntl call.
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    // SAFETY: `fd` is valid, and the flags are the current status flags plus
    // O_NONBLOCK.
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
    Ok(())
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

fn cvt_long(value: libc::c_long) -> io::Result<libc::c_long> {
    if value == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

#[allow(dead_code)]
fn _assert_os_str(_: &OsStr) {}

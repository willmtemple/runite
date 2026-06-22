//! macOS subprocess backend.

use std::io;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command as StdCommand, ExitStatus as StdExitStatus};
use std::time::Duration;

use crate::process::pipe::Pipe;
use crate::process::{CommandSpec, EnvChange, StdioKind};

pub(crate) struct Child {
    pid: Option<libc::pid_t>,
    status: Option<StdExitStatus>,
    pub(crate) stdin: Option<Pipe>,
    pub(crate) stdout: Option<Pipe>,
    pub(crate) stderr: Option<Pipe>,
}

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
            wait_proc_exit_kqueue(pid).await?;
        }
    }

    pub fn kill(&mut self) -> io::Result<()> {
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        if let Some(pid) = self.pid {
            // SAFETY: `pid` is the process id returned by spawn; `SIGKILL` has
            // no pointer arguments and does not alias Rust memory.
            let result = unsafe { libc::kill(pid, libc::SIGKILL) };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

type PinPipeRead =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PinPipeWrite =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + 'static>>;

pub(crate) fn read_pipe_future(fd: RawFd, len: usize) -> PinPipeRead {
    Box::pin(read_pipe(fd, len))
}

pub(crate) fn write_pipe_future(fd: RawFd, data: Vec<u8>) -> PinPipeWrite {
    Box::pin(write_pipe(fd, data))
}

async fn read_pipe(fd: RawFd, len: usize) -> io::Result<Vec<u8>> {
    loop {
        let mut data = vec![0u8; len];
        // SAFETY: `fd` is an open nonblocking pipe read end, and `data` owns
        // `len` writable bytes for the duration of this call.
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
        // SAFETY: `fd` is an open nonblocking pipe write end, and `data` remains
        // alive and immutable for the duration of this call.
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

async fn wait_proc_exit_kqueue(pid: libc::pid_t) -> io::Result<()> {
    let kqueue = Kqueue::new()?;
    if let Err(error) = kqueue.register_proc(pid) {
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        return Err(error);
    }
    loop {
        crate::time::sleep(Duration::from_millis(1)).await;
        if kqueue.poll_proc_exit()? {
            return Ok(());
        }
    }
}

struct Kqueue(RawFd);

impl Kqueue {
    fn new() -> io::Result<Self> {
        // SAFETY: `kqueue` takes no arguments and returns a fresh descriptor or
        // -1 with errno set.
        let fd = cvt(unsafe { libc::kqueue() })?;
        Ok(Self(fd))
    }

    fn register_proc(&self, pid: libc::pid_t) -> io::Result<()> {
        let event = libc::kevent {
            ident: pid as usize,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: `self.0` is an open kqueue descriptor and `event` points to a
        // fully initialized one-element changelist.
        let result =
            unsafe { libc::kevent(self.0, &event, 1, std::ptr::null_mut(), 0, std::ptr::null()) };
        cvt(result).map(|_| ())
    }

    fn poll_proc_exit(&self) -> io::Result<bool> {
        // SAFETY: `kevent` is a plain C struct where all-zero is a valid output
        // buffer before the kernel fills it.
        let mut event = unsafe { std::mem::zeroed::<libc::kevent>() };
        let timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `self.0` is an open kqueue descriptor, `event` is a writable
        // one-element event buffer, and `timeout` is a valid zero timeout.
        let result = unsafe { libc::kevent(self.0, std::ptr::null(), 0, &mut event, 1, &timeout) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result > 0)
        }
    }
}

impl Drop for Kqueue {
    fn drop(&mut self) {
        // SAFETY: `self.0` is owned by this Kqueue and is closed exactly once.
        unsafe {
            libc::close(self.0);
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

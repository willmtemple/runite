//! Windows subprocess backend.
//!
//! Children are spawned through [`std::process::Command`]. The parent ends of
//! std's piped standard streams are named-pipe handles that std creates with
//! `FILE_FLAG_OVERLAPPED` (anonymous pipes cannot overlap, which is why std
//! uses named pipes internally); the backend adopts them, associates them with
//! the runtime thread's completion port, and drives them with overlapped
//! `ReadFile`/`WriteFile` like every other handle.
//!
//! Child exit is event-driven: `RegisterWaitForSingleObject` parks the process
//! handle on the OS wait-thread pool and completes a runtime completion when
//! the process object signals, so no runtime or blocking-pool thread is held
//! for the child's lifetime.

use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle, RawHandle};
use std::process::{Command as StdCommand, ExitStatus as StdExitStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Threading::{
    INFINITE, RegisterWaitForSingleObject, UnregisterWaitEx, WT_EXECUTEINWAITTHREAD,
    WT_EXECUTEONLYONCE,
};

use crate::op::completion::{CompletionHandle, completion_for_current_thread};
use crate::process::pipe::Pipe;
use crate::process::{CommandSpec, EnvChange, StdioKind};
use crate::sys::handle::RawFile;
use crate::sys::windows::overlapped;

/// The process object a [`Child`] waits on.
///
/// A child runite spawned is a [`std::process::Child`], which reaps itself.
/// A child adopted by pid is a bare process handle opened with `OpenProcess`,
/// for which the backend does the exit-code query itself.
enum Process {
    Spawned(std::process::Child),
    Adopted { pid: u32, handle: OwnedHandle },
}

impl Process {
    fn raw_handle(&self) -> RawHandle {
        match self {
            Self::Spawned(child) => child.as_raw_handle(),
            Self::Adopted { handle, .. } => handle.as_raw_handle(),
        }
    }

    fn id(&self) -> u32 {
        match self {
            Self::Spawned(child) => child.id(),
            Self::Adopted { pid, .. } => *pid,
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<StdExitStatus>> {
        match self {
            Self::Spawned(child) => child.try_wait(),
            Self::Adopted { handle, .. } => adopted_try_wait(handle),
        }
    }

    fn kill(&mut self) -> io::Result<()> {
        match self {
            Self::Spawned(child) => child.kill(),
            Self::Adopted { handle, .. } => {
                // SAFETY: `handle` is a live process handle; the exit code is a
                // plain value. Adoption only prefers `PROCESS_TERMINATE`, so
                // this is where a handle that lacks it reports
                // `ERROR_ACCESS_DENIED`.
                let ok = unsafe {
                    windows_sys::Win32::System::Threading::TerminateProcess(
                        handle.as_raw_handle() as HANDLE,
                        1,
                    )
                };
                if ok == 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Polls an adopted process handle without blocking.
///
/// `GetExitCodeProcess` reports `STILL_ACTIVE` for a running process, which is
/// indistinguishable from a process that genuinely exited with that value, so
/// the handle's signalled state decides and the exit code is only read once the
/// wait says the process object is signalled.
fn adopted_try_wait(handle: &OwnedHandle) -> io::Result<Option<StdExitStatus>> {
    use std::os::windows::process::ExitStatusExt;
    use windows_sys::Win32::Foundation::{WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{GetExitCodeProcess, WaitForSingleObject};

    // SAFETY: `handle` is a live process handle opened with `SYNCHRONIZE`.
    let wait = unsafe { WaitForSingleObject(handle.as_raw_handle() as HANDLE, 0) };
    if wait == WAIT_TIMEOUT {
        return Ok(None);
    }
    if wait == WAIT_FAILED {
        return Err(io::Error::last_os_error());
    }
    debug_assert_eq!(wait, WAIT_OBJECT_0);

    let mut code = 0u32;
    // SAFETY: `handle` is live and opened with
    // `PROCESS_QUERY_LIMITED_INFORMATION`; `code` is a valid out-pointer.
    let ok = unsafe { GetExitCodeProcess(handle.as_raw_handle() as HANDLE, &mut code) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(Some(StdExitStatus::from_raw(code)))
}

/// The rights adoption actually needs: wait on the process object, then read
/// its exit code once it signals.
///
/// `SYNCHRONIZE` is a standard access right applying to every waitable object,
/// but windows-sys declares it once, as a `FILE_ACCESS_RIGHTS` under
/// `Storage::FileSystem`. The value is the same for a process handle.
const OBSERVE_PROCESS: u32 = windows_sys::Win32::Storage::FileSystem::SYNCHRONIZE
    | windows_sys::Win32::System::Threading::PROCESS_QUERY_LIMITED_INFORMATION;

/// Picks the access mask adoption opens with, preferring one that can also
/// terminate.
///
/// `PROCESS_TERMINATE` is a write-class right and is refused by the mandatory
/// integrity policy where `PROCESS_QUERY_LIMITED_INFORMATION` and `SYNCHRONIZE`
/// are granted, so demanding it up front would refuse adoption of processes the
/// caller may perfectly well wait on. Ask for it, settle without it, and let
/// `kill` be the call that reports the missing right. Only `ERROR_ACCESS_DENIED`
/// is retried; anything else (a pid that is not there, say) is the answer.
///
/// Takes the opener rather than a pid so the fallback can be tested without a
/// process the test is forbidden to terminate — which pids qualify depends on
/// the machine and on whether the session is elevated.
fn adopt_with(open: impl Fn(u32) -> io::Result<RawHandle>) -> io::Result<OwnedHandle> {
    use windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED;
    use windows_sys::Win32::System::Threading::PROCESS_TERMINATE;

    let raw = match open(OBSERVE_PROCESS | PROCESS_TERMINATE) {
        Ok(raw) => raw,
        Err(error) if error.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) => {
            open(OBSERVE_PROCESS)?
        }
        Err(error) => return Err(error),
    };
    // SAFETY: the opener returns a fresh handle that nothing else owns.
    Ok(unsafe { OwnedHandle::from_raw_handle(raw) })
}

/// Adopts an already-running process for exit notification.
pub(crate) fn from_pid(pid: u32) -> io::Result<Child> {
    use windows_sys::Win32::System::Threading::OpenProcess;

    let handle = adopt_with(|access| {
        // SAFETY: `OpenProcess` takes only scalars and returns null on failure.
        let raw = unsafe { OpenProcess(access, 0, pid) };
        if raw.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(raw as RawHandle)
        }
    })?;
    Ok(Child {
        inner: Some(Process::Adopted { pid, handle }),
        status: None,
        stdin: None,
        stdout: None,
        stderr: None,
    })
}

pub(crate) struct Child {
    inner: Option<Process>,
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
    command.stdin(stdio(&spec.stdin)?);
    command.stdout(stdio(&spec.stdout)?);
    command.stderr(stdio(&spec.stderr)?);

    let mut child = command.spawn()?;
    let stdin = child.stdin.take().map(adopt_pipe).transpose()?;
    let stdout = child.stdout.take().map(adopt_pipe).transpose()?;
    let stderr = child.stderr.take().map(adopt_pipe).transpose()?;

    Ok(Child {
        inner: Some(Process::Spawned(child)),
        status: None,
        stdin,
        stdout,
        stderr,
    })
}

impl Child {
    pub fn id(&self) -> Option<u32> {
        if self.status.is_some() {
            return None;
        }
        self.inner.as_ref().map(Process::id)
    }

    pub fn try_wait(&mut self) -> io::Result<Option<StdExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(status));
        }
        let Some(child) = self.inner.as_mut() else {
            return Ok(self.status);
        };
        match child.try_wait()? {
            Some(status) => {
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
        if self.inner.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "child status is unavailable",
            ));
        }
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(status);
            }
            let process = self
                .inner
                .as_ref()
                .expect("child handle is present until reaped")
                .raw_handle();
            wait_process_exit(process).await?;
        }
    }

    pub fn kill(&mut self) -> io::Result<()> {
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        let Some(child) = self.inner.as_mut() else {
            return Ok(());
        };
        match child.kill() {
            Ok(()) => Ok(()),
            // `TerminateProcess` can fail if the child exited in the
            // meantime; treat an already-exited child as success, matching
            // the Unix backends' ESRCH tolerance.
            Err(error) => {
                if self.try_wait()?.is_some() {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }
}

type PinPipeRead =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Vec<u8>>> + 'static>>;
type PinPipeWrite =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + 'static>>;

pub(crate) fn read_pipe_future(fd: RawFile, len: usize) -> PinPipeRead {
    // Pipes ignore the overlapped offset; EOF (`ERROR_BROKEN_PIPE`) maps to a
    // 0-byte read inside `read_at`.
    Box::pin(overlapped::read_at(fd, len, 0))
}

pub(crate) fn write_pipe_future(fd: RawFile, data: Vec<u8>) -> PinPipeWrite {
    Box::pin(overlapped::write_at(fd, data, 0))
}

/// Adopts a std child pipe end: takes ownership of the (overlapped-capable)
/// handle and binds it to the current runtime thread's completion port.
fn adopt_pipe<H: IntoRawHandle>(pipe: H) -> io::Result<Pipe> {
    // SAFETY: `into_raw_handle` transfers ownership; the handle is adopted
    // exactly once.
    let handle = unsafe { OwnedHandle::from_raw_handle(pipe.into_raw_handle()) };
    Ok(Pipe::new(crate::sys::windows::fs::adopt_handle(handle)?))
}

/// State shared between one `wait` registration and its wait-thread callback.
struct WaitContext {
    handle: CompletionHandle<io::Result<()>>,
    fired: AtomicBool,
}

/// Runs on an OS wait-pool thread when the process handle signals.
///
/// # Safety
///
/// `context` is the `Arc::into_raw` pointer minted by [`wait_process_exit`];
/// the wait is `WT_EXECUTEONLYONCE`, so this consumes the callback's reference
/// exactly once.
unsafe extern "system" fn child_exit_callback(context: *mut c_void, _timed_out: bool) {
    // SAFETY: forwarded contract.
    let context = unsafe { Arc::from_raw(context.cast_const().cast::<WaitContext>()) };
    context.fired.store(true, Ordering::Release);
    context.handle.clone().complete(Ok(()));
}

/// Owns one registered wait. Dropping it (including when the enclosing future
/// is cancelled) unregisters *blockingly*, after which the callback has either
/// run to completion or never will — making it safe to reclaim the callback's
/// context reference when it never fired.
struct WaitRegistration {
    wait_object: HANDLE,
    context: Arc<WaitContext>,
}

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        // SAFETY: `wait_object` came from a successful registration;
        // `INVALID_HANDLE_VALUE` requests a blocking unregister that waits for
        // an in-flight callback (which only completes a completion handle) to
        // finish.
        unsafe { UnregisterWaitEx(self.wait_object, INVALID_HANDLE_VALUE) };
        if !self.context.fired.load(Ordering::Acquire) {
            // The callback never ran and never will: reclaim its reference.
            // SAFETY: the raw reference minted for the callback is consumed
            // exactly once — by the callback or here, never both.
            unsafe { drop(Arc::from_raw(Arc::as_ptr(&self.context))) };
        }
    }
}

/// Resolves when the process object signals (i.e. the child exits).
async fn wait_process_exit(process: RawHandle) -> io::Result<()> {
    let (future, handle) = completion_for_current_thread::<io::Result<()>>();
    let context = Arc::new(WaitContext {
        handle: handle.clone(),
        fired: AtomicBool::new(false),
    });
    let context_for_callback = Arc::into_raw(Arc::clone(&context));

    let mut wait_object: HANDLE = std::ptr::null_mut();
    // SAFETY: `process` is a live process handle owned by the caller for the
    // duration of the wait (the registration is dropped before `Child` frees
    // it); the callback and its context stay valid until consumed as
    // described above.
    let registered = unsafe {
        RegisterWaitForSingleObject(
            &mut wait_object,
            process as HANDLE,
            Some(child_exit_callback),
            context_for_callback as *const c_void,
            INFINITE,
            WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD,
        )
    };
    if registered == 0 {
        let error = io::Error::last_os_error();
        // SAFETY: the registration failed, so the callback reference is
        // reclaimed here, exactly once.
        unsafe { drop(Arc::from_raw(context_for_callback)) };
        handle.complete(Err(error));
        return future.await;
    }

    // Declared after `context`, so it drops first — while the context is
    // still alive — whether the future completes or is cancelled mid-await.
    let registration = WaitRegistration {
        wait_object,
        context: Arc::clone(&context),
    };

    let result = future.await;
    drop(registration);
    result
}

fn stdio(kind: &StdioKind) -> io::Result<std::process::Stdio> {
    Ok(match kind {
        StdioKind::Inherit => std::process::Stdio::inherit(),
        StdioKind::Null => std::process::Stdio::null(),
        StdioKind::Piped => std::process::Stdio::piped(),
        // Duplicated rather than consumed, so the same `Command` can be spawned
        // again and the caller's handle stays theirs.
        StdioKind::Raw(handle) => std::process::Stdio::from(handle.try_clone()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::{Cell, RefCell};

    use windows_sys::Win32::Foundation::{ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_TERMINATE};

    /// A process the caller may wait on but not terminate is still adoptable:
    /// the observation rights are what `wait` and `try_wait` need, and refusing
    /// the whole call over a right only `kill` uses would lose the exit.
    ///
    /// The refusal is staged rather than found on the machine, because which
    /// pids deny `PROCESS_TERMINATE` while granting
    /// `PROCESS_QUERY_LIMITED_INFORMATION` depends on the box and on whether the
    /// session is elevated.
    #[test]
    fn adoption_settles_for_observation_rights() {
        let asked = RefCell::new(Vec::new());
        let handle = adopt_with(|access| {
            asked.borrow_mut().push(access);
            if access & PROCESS_TERMINATE != 0 {
                return Err(io::Error::from_raw_os_error(ERROR_ACCESS_DENIED as i32));
            }
            // SAFETY: `OpenProcess` takes only scalars and returns null on
            // failure.
            let raw = unsafe { OpenProcess(access, 0, std::process::id()) };
            assert!(!raw.is_null(), "{}", io::Error::last_os_error());
            Ok(raw as RawHandle)
        })
        .expect("adoption should settle for observation rights");

        assert_eq!(
            asked.into_inner(),
            vec![OBSERVE_PROCESS | PROCESS_TERMINATE, OBSERVE_PROCESS]
        );
        assert!(
            adopted_try_wait(&handle)
                .expect("this process should be queryable")
                .is_none(),
            "the test process is still running"
        );
    }

    /// The retry is for the permission case only. A pid that is not there must
    /// fail at adoption with the kernel's own error, not be probed twice and
    /// reported as something else.
    #[test]
    fn adoption_does_not_retry_a_failure_that_is_not_a_permission() {
        let attempts = Cell::new(0);
        let error = adopt_with(|_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::from_raw_os_error(ERROR_INVALID_PARAMETER as i32))
        })
        .expect_err("a non-permission failure should propagate");

        assert_eq!(attempts.get(), 1);
        assert_eq!(
            error.raw_os_error(),
            Some(ERROR_INVALID_PARAMETER as i32),
            "the kernel's error should survive"
        );
    }
}

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
use crate::sys::handle::{RawFile, raw_file};
use crate::sys::windows::overlapped;

pub(crate) struct Child {
    inner: Option<std::process::Child>,
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
    let stdin = child.stdin.take().map(adopt_pipe).transpose()?;
    let stdout = child.stdout.take().map(adopt_pipe).transpose()?;
    let stderr = child.stderr.take().map(adopt_pipe).transpose()?;

    Ok(Child {
        inner: Some(child),
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
        self.inner.as_ref().map(std::process::Child::id)
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
                .as_raw_handle();
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
    overlapped::associate_file(raw_file(&handle))?;
    Ok(Pipe::new(handle))
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

fn stdio(kind: StdioKind) -> std::process::Stdio {
    match kind {
        StdioKind::Inherit => std::process::Stdio::inherit(),
        StdioKind::Null => std::process::Stdio::null(),
        StdioKind::Piped => std::process::Stdio::piped(),
    }
}

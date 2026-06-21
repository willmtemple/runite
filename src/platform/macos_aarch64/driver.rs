//! Public runtime driver primitives for macOS.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::op::completion::CompletionHandle;
use crate::platform::runtime_shared::{DriverBackend, Notifier};

pub use crate::platform::runtime_shared::ReadyEvents;

type FdCompletion = CompletionHandle<io::Result<()>>;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) struct FdReadinessToken(u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum FdInterest {
    Readable,
    Writable,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct FdKey {
    fd: RawFd,
    interest: FdInterest,
}

struct FdWaiter {
    token: FdReadinessToken,
    completion: FdCompletion,
}

#[derive(Clone)]
struct NotifierInner {
    write_fd: RawFd,
    closed: Arc<AtomicBool>,
}

impl NotifierInner {
    fn notify(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "target runtime driver is closed",
            ));
        }

        let byte = 1u8;
        let written = unsafe {
            libc::write(
                self.write_fd,
                &byte as *const u8 as *const libc::c_void,
                std::mem::size_of::<u8>(),
            )
        };
        if written < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }
            return Err(error);
        }

        Ok(())
    }
}

#[derive(Clone)]
/// Cross-thread notifier for a runtime thread's driver.
pub struct ThreadNotifier {
    inner: NotifierInner,
}

impl ThreadNotifier {
    /// Sends a wake notification to the target runtime thread.
    pub fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

impl Notifier for ThreadNotifier {
    fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

/// Low-level macOS runtime driver backed by `kqueue` and a wake pipe.
pub struct Driver {
    kqueue_fd: RawFd,
    wake_read_fd: RawFd,
    wake_write_fd: RawFd,
    closed: Arc<AtomicBool>,
    timer_deadline: Cell<Option<Duration>>,
    pending_wakes: Cell<u64>,
    pending_timers: Cell<u64>,
    next_fd_token: Cell<u64>,
    fd_waiters: RefCell<HashMap<FdKey, FdWaiter>>,
}

/// Creates a new driver and its paired [`ThreadNotifier`].
pub fn create_driver() -> io::Result<(Driver, ThreadNotifier)> {
    let kqueue_fd = cvt(unsafe { libc::kqueue() })?;

    let mut pipe_fds = [0; 2];
    cvt(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) })?;
    let wake_read_fd = pipe_fds[0];
    let wake_write_fd = pipe_fds[1];

    set_nonblocking(wake_read_fd)?;
    set_nonblocking(wake_write_fd)?;

    let event = libc::kevent {
        ident: wake_read_fd as usize,
        filter: libc::EVFILT_READ,
        flags: libc::EV_ADD | libc::EV_ENABLE,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    };

    let submitted = unsafe {
        libc::kevent(
            kqueue_fd,
            &event,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    };
    if submitted < 0 {
        let error = io::Error::last_os_error();
        unsafe {
            libc::close(wake_read_fd);
            libc::close(wake_write_fd);
            libc::close(kqueue_fd);
        }
        return Err(error);
    }

    let closed = Arc::new(AtomicBool::new(false));
    let driver = Driver {
        kqueue_fd,
        wake_read_fd,
        wake_write_fd,
        closed: Arc::clone(&closed),
        timer_deadline: Cell::new(None),
        pending_wakes: Cell::new(0),
        pending_timers: Cell::new(0),
        next_fd_token: Cell::new(1),
        fd_waiters: RefCell::new(HashMap::new()),
    };

    let notifier = ThreadNotifier {
        inner: NotifierInner {
            write_fd: wake_write_fd,
            closed,
        },
    };

    Ok((driver, notifier))
}

impl Driver {
    pub(crate) fn bind_current_thread(&self) {}

    pub(crate) fn unbind_current_thread(&self) {}

    /// Polls the driver without blocking.
    pub fn poll(&self) -> io::Result<Option<ReadyEvents>> {
        let mut pending = ReadyEvents::default();
        if self.pending_wakes.get() > 0 {
            pending.wake = true;
        }
        if self.pending_timers.get() > 0 {
            pending.timer = true;
        }
        if pending.wake || pending.timer {
            return Ok(Some(pending));
        }
        self.process(Some(Duration::ZERO))
    }

    /// Blocks until at least one event is available.
    pub fn wait(&self) -> io::Result<()> {
        let now = monotonic_now()?;
        let timeout = self
            .timer_deadline
            .get()
            .map(|deadline| deadline.saturating_sub(now));
        let _ = self.process(timeout)?;
        Ok(())
    }

    /// Updates the currently armed timer deadline.
    pub fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()> {
        self.timer_deadline.set(deadline);
        Ok(())
    }

    /// Drains the accumulated wake notification count.
    ///
    /// Returns `Some(count)` with the number of wake notifications collected
    /// since the previous call, or `None` if no wake events were pending.
    pub fn drain_wake(&self) -> Option<u64> {
        let wakes = self.pending_wakes.replace(0);
        if wakes == 0 { None } else { Some(wakes) }
    }

    /// Drains the accumulated timer-expiration count.
    ///
    /// Returns `Some(count)` with the number of timer expirations collected
    /// since the previous call, or `None` if no timer events were pending.
    pub fn drain_timer(&self) -> Option<u64> {
        let timers = self.pending_timers.replace(0);
        if timers == 0 { None } else { Some(timers) }
    }

    pub(crate) fn register_fd_readiness(
        &self,
        fd: RawFd,
        interest: FdInterest,
        completion: FdCompletion,
    ) -> io::Result<FdReadinessToken> {
        let key = FdKey { fd, interest };
        let removed_stale_waiter = {
            let mut waiters = self.fd_waiters.borrow_mut();
            match waiters.get(&key) {
                Some(waiter) if !waiter.completion.is_interested() => {
                    waiters.remove(&key);
                    true
                }
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "fd readiness already has a waiter for this interest",
                    ));
                }
                None => false,
            }
        };
        if removed_stale_waiter {
            let _ = self.update_fd_interest(key, libc::EV_DELETE);
        }

        let token = self.allocate_fd_token();
        self.update_fd_interest(key, libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT)?;
        self.fd_waiters
            .borrow_mut()
            .insert(key, FdWaiter { token, completion });
        Ok(token)
    }

    pub(crate) fn cancel_fd_readiness(&self, token: FdReadinessToken) {
        let mut empty_key = None;
        {
            let mut waiters = self.fd_waiters.borrow_mut();
            for (key, entry) in waiters.iter() {
                if entry.token == token {
                    empty_key = Some(*key);
                    break;
                }
            }

            if let Some(key) = empty_key {
                waiters.remove(&key);
            }
        }

        if let Some(key) = empty_key {
            let _ = self.update_fd_interest(key, libc::EV_DELETE);
        }
    }

    fn process(&self, timeout: Option<Duration>) -> io::Result<Option<ReadyEvents>> {
        let mut ready = ReadyEvents::default();

        let mut events = [unsafe { std::mem::zeroed::<libc::kevent>() }; 16];
        let timeout_spec = timeout_to_timespec(timeout);
        let timeout_ptr = timeout_spec
            .as_ref()
            .map_or(std::ptr::null(), |value| value as *const libc::timespec);

        let result = unsafe {
            libc::kevent(
                self.kqueue_fd,
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                events.len() as i32,
                timeout_ptr,
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }

        let mut saw_any = false;
        let count = result.max(0) as usize;
        if count > 0 {
            saw_any = true;
            for event in events.iter().take(count) {
                if event.ident as RawFd == self.wake_read_fd {
                    ready.wake = true;
                    let wakes = drain_wake_pipe(self.wake_read_fd)?;
                    self.pending_wakes
                        .set(self.pending_wakes.get().saturating_add(wakes));
                } else if let Some(interest) = interest_from_filter(event.filter) {
                    self.complete_fd_waiters(event.ident as RawFd, interest, event);
                }
            }
        }

        if let Some(deadline) = self.timer_deadline.get()
            && monotonic_now()? >= deadline
        {
            ready.timer = true;
            saw_any = true;
            self.timer_deadline.set(None);
            self.pending_timers
                .set(self.pending_timers.get().saturating_add(1));
        }

        if saw_any { Ok(Some(ready)) } else { Ok(None) }
    }

    fn allocate_fd_token(&self) -> FdReadinessToken {
        let token = self.next_fd_token.get();
        self.next_fd_token.set(
            token
                .checked_add(1)
                .expect("fd readiness token space exhausted"),
        );
        FdReadinessToken(token)
    }

    fn update_fd_interest(&self, key: FdKey, flags: u16) -> io::Result<()> {
        let event = libc::kevent {
            ident: key.fd as usize,
            filter: filter_for_interest(key.interest),
            flags,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let submitted = unsafe {
            libc::kevent(
                self.kqueue_fd,
                &event,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if submitted < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn complete_fd_waiters(&self, fd: RawFd, interest: FdInterest, event: &libc::kevent) {
        let key = FdKey { fd, interest };
        let waiter = self.fd_waiters.borrow_mut().remove(&key);
        let Some(waiter) = waiter else {
            return;
        };

        let result = fd_event_result(event, interest);
        waiter.completion.complete(result);
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        unsafe {
            libc::close(self.wake_read_fd);
            libc::close(self.wake_write_fd);
            libc::close(self.kqueue_fd);
        }
    }
}

impl DriverBackend for Driver {
    fn poll(&self) -> io::Result<Option<ReadyEvents>> {
        Driver::poll(self)
    }

    fn wait(&self) -> io::Result<()> {
        Driver::wait(self)
    }

    fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()> {
        Driver::rearm_timer(self, deadline)
    }

    fn drain_wake(&self) -> Option<u64> {
        Driver::drain_wake(self)
    }

    fn drain_timer(&self) -> Option<u64> {
        Driver::drain_timer(self)
    }

    fn bind_current_thread(&self) {
        Driver::bind_current_thread(self)
    }

    fn unbind_current_thread(&self) {
        Driver::unbind_current_thread(self)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Returns the current monotonic clock reading.
pub fn monotonic_now() -> io::Result<Duration> {
    let mut now = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, now.as_mut_ptr()) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    let now = unsafe { now.assume_init() };
    Ok(Duration::new(now.tv_sec as u64, now.tv_nsec as u32))
}

fn cvt(value: libc::c_int) -> io::Result<libc::c_int> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}

fn filter_for_interest(interest: FdInterest) -> i16 {
    match interest {
        FdInterest::Readable => libc::EVFILT_READ,
        FdInterest::Writable => libc::EVFILT_WRITE,
    }
}

fn interest_from_filter(filter: i16) -> Option<FdInterest> {
    if filter == libc::EVFILT_READ {
        Some(FdInterest::Readable)
    } else if filter == libc::EVFILT_WRITE {
        Some(FdInterest::Writable)
    } else {
        None
    }
}

fn fd_event_result(event: &libc::kevent, interest: FdInterest) -> io::Result<()> {
    if event.flags & libc::EV_ERROR != 0 && event.data != 0 {
        Err(io::Error::from_raw_os_error(event.data as i32))
    } else if event.flags & libc::EV_EOF != 0 && interest == FdInterest::Writable {
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "fd write side reached EOF",
        ))
    } else {
        Ok(())
    }
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = cvt(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    cvt(unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) })?;
    Ok(())
}

fn timeout_to_timespec(timeout: Option<Duration>) -> Option<libc::timespec> {
    timeout.map(|value| libc::timespec {
        tv_sec: value.as_secs() as libc::time_t,
        tv_nsec: value.subsec_nanos() as libc::c_long,
    })
}

fn drain_wake_pipe(fd: RawFd) -> io::Result<u64> {
    let mut wakes = 0u64;
    let mut buf = [0u8; 256];

    loop {
        let read = unsafe {
            libc::read(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len() as libc::size_t,
            )
        };

        if read > 0 {
            wakes = wakes.saturating_add(read as u64);
            continue;
        }

        if read == 0 {
            break;
        }

        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            break;
        }
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }

        return Err(error);
    }

    Ok(wakes.max(1))
}

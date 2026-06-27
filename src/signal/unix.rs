//! Unix signal streams.
//!
//! Use [`signal`] to create a [`Signal`] stream for a specific [`SignalKind`],
//! then await [`Signal::recv`] each time that kind is observed. This is useful
//! for Unix-specific runtime integration such as graceful shutdown hooks,
//! reconfiguration on `SIGHUP`, or terminal UI redraws after `SIGWINCH`.
//!
//! This implementation deliberately uses a dedicated blocking-pool reader task
//! instead of a per-runtime-thread microtask drain. Signals are process-global,
//! so one async-signal-safe handler writes to one process-wide wake fd, and the
//! reader forwards observed signal kinds on a best-effort basis to live runtime
//! threads that have constructed a [`Signal`]. Forwarding uses
//! [`crate::ThreadHandle::queue_macrotask`]; if a target thread is closed or its
//! queue is full, that wake is dropped. Repeated calls to [`signal`] share the
//! same process-wide signal handler and create independent per-thread stream
//! handles.
//!
//! Existing non-default, non-ignored process handlers are not overwritten:
//! [`signal`] returns an error instead.
//!
//! # Examples
//!
//! ```no_run
//! runite::spawn(async {
//!     let mut resize = runite::signal::unix::signal(
//!         runite::signal::unix::SignalKind::WindowChange,
//!     )
//!     .expect("SIGWINCH handler should install");
//!
//!     resize.recv().await;
//!     eprintln!("terminal window changed size");
//! });
//!
//! runite::run();
//! ```

use std::cell::RefCell;
use std::future::poll_fn;
use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Poll, Waker};

use crate::ThreadHandle;

const SIGNAL_COUNT: usize = 7;

static DISPATCH: OnceLock<io::Result<SignalDispatch>> = OnceLock::new();
static WAKE_FD: AtomicI32 = AtomicI32::new(-1);
static PENDING: [AtomicBool; SIGNAL_COUNT] = [const { AtomicBool::new(false) }; SIGNAL_COUNT];
static WAKE_GENERATION: [AtomicU64; SIGNAL_COUNT] = [const { AtomicU64::new(0) }; SIGNAL_COUNT];

thread_local! {
    static THREAD_REGISTRATION: RefCell<Weak<ThreadRegistration>> = const { RefCell::new(Weak::new()) };
}

/// POSIX signal kind supported by the runtime.
///
/// Each variant maps to one Unix signal number. Register a stream with
/// [`signal`] and await events with [`Signal::recv`].
///
/// These variants are Unix-specific; use [`crate::signal::ctrl_c`] when only a
/// cross-platform interrupt/shutdown hook is needed.
///
/// # Examples
///
/// ```
/// use runite::signal::unix::SignalKind;
///
/// let kind = SignalKind::WindowChange;
/// ```
#[derive(Clone, Copy, Debug)]
pub enum SignalKind {
    /// `SIGINT`, commonly sent by Ctrl-C.
    Interrupt,
    /// `SIGTERM`.
    Terminate,
    /// `SIGHUP`.
    Hangup,
    /// `SIGQUIT`.
    Quit,
    /// `SIGUSR1`.
    User1,
    /// `SIGUSR2`.
    User2,
    /// `SIGWINCH`, sent when a terminal window changes size.
    WindowChange,
}

/// Per-signal async stream of received signal events.
///
/// Each [`Signal`] is tied to the runtime thread on which it was created and is
/// intentionally `!Send`. Dropping it unregisters that stream and lets the
/// runtime exit if no other async operations are live. Dropping the last stream
/// does not restore the previous process-wide `sigaction`; the runite handler
/// remains installed for the process.
pub struct Signal {
    last_seen: u64,
    state: Arc<SignalState>,
    registration: Arc<ThreadRegistration>,
    _not_send: PhantomData<Rc<()>>,
}

/// Registers interest in `kind` on the current runtime thread.
///
/// Repeated calls for the same kind share the process-wide `sigaction`
/// registration and return independent stream handles.
///
/// Successful installation changes signal handling for the whole process and
/// all runtime threads. The previous default or ignored disposition is not
/// restored when a [`Signal`] is dropped.
///
/// Returns an error if another non-default, non-ignored handler is already
/// installed for the requested signal.
///
/// # Examples
///
/// ```no_run
/// runite::spawn(async {
///     let mut sigterm = runite::signal::unix::signal(
///         runite::signal::unix::SignalKind::Terminate,
///     )
///     .expect("SIGTERM handler should install");
///
///     sigterm.recv().await;
///     eprintln!("termination requested");
/// });
///
/// runite::run();
/// ```
pub fn signal(kind: SignalKind) -> io::Result<Signal> {
    let dispatch = dispatch()?;
    let index = kind.index();
    dispatch.install_signal(kind)?;

    let registration = thread_registration(dispatch);
    let state = Arc::new(SignalState::new());
    registration.add_signal(index, &state);
    registration.thread.begin_async_operation();

    Ok(Signal {
        last_seen: state.generation.load(Ordering::Acquire),
        state,
        registration,
        _not_send: PhantomData,
    })
}

impl Signal {
    /// Waits for the next signal event observed by this stream.
    ///
    /// Signals are coalesced by kind; if several identical signals arrive before
    /// the stream is polled again, one wake may represent multiple process
    /// signal deliveries.
    ///
    /// The current implementation returns `Some(())` when an event is observed
    /// and never produces `None`. The `Option` leaves room for a future closed
    /// stream state without changing the method signature.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> std::io::Result<()> {
    /// use runite::signal::unix::{signal, SignalKind};
    ///
    /// let mut resize = signal(SignalKind::WindowChange)?;
    /// resize.recv().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv(&mut self) -> Option<()> {
        poll_fn(|cx| {
            let current = self.state.generation.load(Ordering::Acquire);
            if current != self.last_seen {
                self.last_seen = current;
                return Poll::Ready(Some(()));
            }

            let mut waker = self
                .state
                .waker
                .lock()
                .expect("signal stream waker mutex poisoned");
            *waker = Some(cx.waker().clone());

            let current = self.state.generation.load(Ordering::Acquire);
            if current != self.last_seen {
                self.last_seen = current;
                *waker = None;
                Poll::Ready(Some(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl Drop for Signal {
    fn drop(&mut self) {
        self.registration.thread.finish_async_operation();
    }
}

impl SignalKind {
    fn index(self) -> usize {
        match self {
            Self::Interrupt => 0,
            Self::Terminate => 1,
            Self::Hangup => 2,
            Self::Quit => 3,
            Self::User1 => 4,
            Self::User2 => 5,
            Self::WindowChange => 6,
        }
    }

    fn signum(self) -> libc::c_int {
        match self {
            Self::Interrupt => libc::SIGINT,
            Self::Terminate => libc::SIGTERM,
            Self::Hangup => libc::SIGHUP,
            Self::Quit => libc::SIGQUIT,
            Self::User1 => libc::SIGUSR1,
            Self::User2 => libc::SIGUSR2,
            Self::WindowChange => libc::SIGWINCH,
        }
    }
}

struct SignalState {
    generation: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

impl SignalState {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            waker: Mutex::new(None),
        }
    }

    fn notify(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        if let Some(waker) = self
            .waker
            .lock()
            .expect("signal stream waker mutex poisoned")
            .take()
        {
            waker.wake();
        }
    }
}

struct SignalSlot {
    streams: Mutex<Vec<Weak<SignalState>>>,
}

impl SignalSlot {
    fn new() -> Self {
        Self {
            streams: Mutex::new(Vec::new()),
        }
    }
}

struct ThreadRegistration {
    thread: ThreadHandle,
    slots: [SignalSlot; SIGNAL_COUNT],
}

impl ThreadRegistration {
    fn new(thread: ThreadHandle) -> Self {
        Self {
            thread,
            slots: std::array::from_fn(|_| SignalSlot::new()),
        }
    }

    fn add_signal(&self, index: usize, state: &Arc<SignalState>) {
        self.slots[index]
            .streams
            .lock()
            .expect("signal registration mutex poisoned")
            .push(Arc::downgrade(state));
    }

    fn notify(&self, index: usize) {
        let mut streams = self.slots[index]
            .streams
            .lock()
            .expect("signal registration mutex poisoned");
        streams.retain(|stream| {
            if let Some(stream) = stream.upgrade() {
                stream.notify();
                true
            } else {
                false
            }
        });
    }
}

struct SignalDispatch {
    installed: [AtomicBool; SIGNAL_COUNT],
    install_lock: Mutex<()>,
    registrations: Mutex<Vec<Weak<ThreadRegistration>>>,
    _read_fd: RawFd,
}

impl SignalDispatch {
    fn new() -> io::Result<Self> {
        let read_fd = create_wake_fd()?;
        crate::sys::blocking::spawn_blocking(move || reader_loop(read_fd)).inspect_err(|_| {
            close_fd(read_fd);
        })?;

        Ok(Self {
            installed: [const { AtomicBool::new(false) }; SIGNAL_COUNT],
            install_lock: Mutex::new(()),
            registrations: Mutex::new(Vec::new()),
            _read_fd: read_fd,
        })
    }

    fn install_signal(&self, kind: SignalKind) -> io::Result<()> {
        let index = kind.index();
        if self.installed[index].load(Ordering::Acquire) {
            return Ok(());
        }

        let _guard = self
            .install_lock
            .lock()
            .expect("signal install mutex poisoned");
        if self.installed[index].load(Ordering::Acquire) {
            return Ok(());
        }

        install_sigaction(kind.signum())?;
        self.installed[index].store(true, Ordering::Release);
        Ok(())
    }

    fn register_thread(&self, registration: &Arc<ThreadRegistration>) {
        self.registrations
            .lock()
            .expect("signal registration mutex poisoned")
            .push(Arc::downgrade(registration));
    }

    fn broadcast(&self, index: usize) {
        let registrations = {
            let mut registrations = self
                .registrations
                .lock()
                .expect("signal registration mutex poisoned");
            registrations.retain(|registration| {
                registration
                    .upgrade()
                    .map(|registration| !registration.thread.is_closed())
                    .unwrap_or(false)
            });
            registrations
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };

        for registration in registrations {
            let queued_registration = Arc::clone(&registration);
            let queued = registration
                .thread
                .queue_macrotask(move || queued_registration.notify(index));
            if queued.is_err() {
                continue;
            }
        }
    }
}

fn dispatch() -> io::Result<&'static SignalDispatch> {
    match DISPATCH.get_or_init(SignalDispatch::new) {
        Ok(dispatch) => Ok(dispatch),
        Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
    }
}

fn thread_registration(dispatch: &'static SignalDispatch) -> Arc<ThreadRegistration> {
    THREAD_REGISTRATION.with(|slot| {
        if let Some(registration) = slot.borrow().upgrade() {
            return registration;
        }

        let registration = Arc::new(ThreadRegistration::new(crate::current_thread_handle()));
        dispatch.register_thread(&registration);
        *slot.borrow_mut() = Arc::downgrade(&registration);
        registration
    })
}

extern "C" fn handle_signal(signum: libc::c_int) {
    if let Some(index) = signal_index(signum) {
        PENDING[index].store(true, Ordering::Release);

        let fd = WAKE_FD.load(Ordering::Relaxed);
        if fd >= 0 {
            #[cfg(target_os = "linux")]
            // SAFETY: `fd` is the process-wide nonblocking eventfd installed in
            // `WAKE_FD`; `value` is initialized storage for the 8 bytes eventfd
            // requires. `write` is async-signal-safe and the result is ignored.
            unsafe {
                let value: u64 = 1;
                let _ = libc::write(
                    fd,
                    (&value as *const u64).cast::<libc::c_void>(),
                    std::mem::size_of::<u64>(),
                );
            }

            #[cfg(target_os = "macos")]
            // SAFETY: `fd` is the process-wide nonblocking pipe write end
            // installed in `WAKE_FD`; `byte` is initialized storage for the
            // single byte written. `write` is async-signal-safe.
            unsafe {
                let byte: u8 = 1;
                let _ = libc::write(fd, (&byte as *const u8).cast::<libc::c_void>(), 1);
            }
        }
    }
}

fn signal_index(signum: libc::c_int) -> Option<usize> {
    match signum {
        libc::SIGINT => Some(0),
        libc::SIGTERM => Some(1),
        libc::SIGHUP => Some(2),
        libc::SIGQUIT => Some(3),
        libc::SIGUSR1 => Some(4),
        libc::SIGUSR2 => Some(5),
        libc::SIGWINCH => Some(6),
        _ => None,
    }
}

fn install_sigaction(signum: libc::c_int) -> io::Result<()> {
    // SAFETY: `oldact` and `action` are valid `sigaction` objects for the
    // kernel/libc calls. `signum` comes from `SignalKind::signum`, and the
    // installed handler has the C ABI and does only async-signal-safe work.
    unsafe {
        let mut oldact = MaybeUninit::<libc::sigaction>::uninit();
        if libc::sigaction(signum, ptr::null(), oldact.as_mut_ptr()) == -1 {
            return Err(io::Error::last_os_error());
        }
        let oldact = oldact.assume_init();
        if oldact.sa_sigaction != libc::SIG_DFL && oldact.sa_sigaction != libc::SIG_IGN {
            return Err(io::Error::other("signal already has a handler"));
        }

        let mut action = MaybeUninit::<libc::sigaction>::zeroed().assume_init();
        action.sa_sigaction = handle_signal as *const () as usize;
        action.sa_flags = libc::SA_RESTART | libc::SA_NOCLDSTOP;
        if libc::sigemptyset(&mut action.sa_mask) == -1 {
            return Err(io::Error::last_os_error());
        }
        if libc::sigaction(signum, &action, ptr::null_mut()) == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn create_wake_fd() -> io::Result<RawFd> {
    // SAFETY: `eventfd` takes only by-value arguments and returns a new owned
    // descriptor on success.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    WAKE_FD.store(fd, Ordering::Release);
    Ok(fd)
}

#[cfg(target_os = "macos")]
fn create_wake_fd() -> io::Result<RawFd> {
    let mut fds = [-1; 2];
    // SAFETY: `fds.as_mut_ptr()` points to two writable `c_int` slots that
    // `pipe` initializes with owned descriptors on success.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }

    if let Err(error) = set_cloexec(fds[0])
        .and_then(|_| set_cloexec(fds[1]))
        .and_then(|_| set_nonblocking(fds[0]))
        .and_then(|_| set_nonblocking(fds[1]))
    {
        close_fd(fds[0]);
        close_fd(fds[1]);
        return Err(error);
    }

    WAKE_FD.store(fds[1], Ordering::Release);
    Ok(fds[0])
}

#[cfg(target_os = "macos")]
fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is an open descriptor owned by the wake pipe setup path;
    // `F_GETFD` reads descriptor flags and uses no pointer arguments.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` remains open and `flags | FD_CLOEXEC` is passed by value to
    // update its descriptor flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is an open descriptor owned by the wake pipe setup path;
    // `F_GETFL` reads status flags and uses no pointer arguments.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` remains open and `flags | O_NONBLOCK` is passed by value to
    // update its file status flags.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn close_fd(fd: RawFd) {
    if fd >= 0 {
        // SAFETY: callers pass descriptors owned by the signal wake setup path;
        // the `fd >= 0` guard filters out sentinel values before closing.
        unsafe {
            libc::close(fd);
        }
    }
}

fn reader_loop(fd: RawFd) {
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };

    loop {
        // SAFETY: `pollfd` points to one initialized `pollfd` entry that remains
        // valid for the duration of the blocking `poll` call.
        let ready = unsafe { libc::poll(&mut pollfd, 1, -1) };
        if ready == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return;
        }

        drain_wake_fd(fd);
    }
}

#[cfg(target_os = "linux")]
fn drain_wake_fd(fd: RawFd) {
    loop {
        let mut value = 0u64;
        // SAFETY: `fd` is the open eventfd read end, and `value` is valid
        // writable storage for the 8-byte eventfd counter read.
        let read = unsafe {
            libc::read(
                fd,
                (&mut value as *mut u64).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if read == std::mem::size_of::<u64>() as isize {
            dispatch_pending();
            continue;
        }
        if read == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
        }
        break;
    }
}

#[cfg(target_os = "macos")]
fn drain_wake_fd(fd: RawFd) {
    let mut buffer = [0u8; 128];
    loop {
        // SAFETY: `fd` is the open nonblocking pipe read end, and `buffer`
        // points to initialized writable storage for `buffer.len()` bytes.
        let read =
            unsafe { libc::read(fd, buffer.as_mut_ptr().cast::<libc::c_void>(), buffer.len()) };
        if read > 0 {
            dispatch_pending();
            continue;
        }
        if read == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
        }
        break;
    }
}

fn dispatch_pending() {
    let Some(Ok(dispatch)) = DISPATCH.get() else {
        return;
    };

    for index in 0..SIGNAL_COUNT {
        if PENDING[index].swap(false, Ordering::AcqRel) {
            WAKE_GENERATION[index].fetch_add(1, Ordering::AcqRel);
            dispatch.broadcast(index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctrl_c_constructs() {
        let future = crate::signal::ctrl_c();
        drop(future);
    }

    #[test]
    fn signal_constructs_for_each_kind() {
        for kind in [
            SignalKind::Interrupt,
            SignalKind::Terminate,
            SignalKind::Hangup,
            SignalKind::Quit,
            SignalKind::User1,
            SignalKind::User2,
            SignalKind::WindowChange,
        ] {
            let first = signal(kind).expect("signal stream should construct");
            let second = signal(kind).expect("repeat registration should share process handler");
            drop(second);
            drop(first);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore]
    fn signal_receives_sigusr1_linux() {
        use std::sync::{Arc, Mutex};

        let received = Arc::new(Mutex::new(false));
        let received_task = Arc::clone(&received);
        let mut sigusr1 = signal(SignalKind::User1).expect("SIGUSR1 stream should construct");

        crate::spawn(async move {
            sigusr1.recv().await;
            *received_task.lock().expect("received mutex poisoned") = true;
        });

        // SAFETY: `getpid` takes no arguments and returns this process id;
        // `SIGUSR1` is a valid signal number for `kill`.
        let rc = unsafe { libc::kill(libc::getpid(), libc::SIGUSR1) };
        assert_eq!(rc, 0, "kill(SIGUSR1) should succeed");

        crate::run();

        assert!(
            *received.lock().expect("received mutex poisoned"),
            "SIGUSR1 recv future should complete"
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn signal_receives_sigwinch() {
        use std::sync::{Arc, Mutex};

        let received = Arc::new(Mutex::new(false));
        let received_task = Arc::clone(&received);
        let mut sigwinch =
            signal(SignalKind::WindowChange).expect("SIGWINCH stream should construct");

        crate::spawn(async move {
            sigwinch.recv().await;
            *received_task.lock().expect("received mutex poisoned") = true;
        });

        // SAFETY: `getpid` takes no arguments and returns this process id;
        // `SIGWINCH` is a valid signal number for `kill`.
        let rc = unsafe { libc::kill(libc::getpid(), libc::SIGWINCH) };
        assert_eq!(rc, 0, "kill(SIGWINCH) should succeed");

        crate::run();

        assert!(
            *received.lock().expect("received mutex poisoned"),
            "SIGWINCH recv future should complete"
        );
    }
}

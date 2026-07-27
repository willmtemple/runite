use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicU64, Ordering};
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::io::IoFuture;
use crate::platform::current::runtime::{ThreadHandle, try_current_thread_handle};
use crate::sys::handle::OwnedFile;

pub(super) const BUFFER_CAPACITY: usize = 64 * 1024;
const READ_CHUNK_BYTES: usize = 8 * 1024;

static NEXT_WAITER_ID: AtomicU64 = AtomicU64::new(1);

type ReadFn = fn(&OwnedFile, &mut [u8]) -> io::Result<usize>;
type DropHook = Option<Box<dyn FnOnce() + Send + 'static>>;

#[derive(Clone)]
pub(super) struct ErrorSnapshot {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    message: String,
}

impl ErrorSnapshot {
    pub(super) fn new(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: error.to_string(),
        }
    }

    pub(super) fn to_error(&self) -> io::Error {
        self.raw_os_error.map_or_else(
            || io::Error::new(self.kind, self.message.clone()),
            io::Error::from_raw_os_error,
        )
    }
}

pub(super) struct StdinReader {
    shared: Arc<Shared>,
    reject_active_handoff: bool,
}

impl StdinReader {
    pub(super) fn spawn(source: OwnedFile) -> io::Result<Arc<Self>> {
        let reject_active_handoff = super::imp::stdin_handoff_requires_idle(&source);
        Self::spawn_with_capacity(source, BUFFER_CAPACITY, reject_active_handoff)
    }

    fn spawn_with_capacity(
        source: OwnedFile,
        capacity: usize,
        reject_active_handoff: bool,
    ) -> io::Result<Arc<Self>> {
        Self::spawn_with_reader(
            source,
            capacity,
            super::imp::blocking_stdin_read,
            None,
            reject_active_handoff,
        )
    }

    fn spawn_with_reader(
        source: OwnedFile,
        capacity: usize,
        read: ReadFn,
        source_dropped: DropHook,
        reject_active_handoff: bool,
    ) -> io::Result<Arc<Self>> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stdin buffer capacity must be nonzero",
            ));
        }

        let shared = Arc::new(Shared::new(capacity));
        let worker_shared = Arc::clone(&shared);
        let interrupt = platform::spawn(move |worker_interrupt| {
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                read_loop(&worker_shared, source, &worker_interrupt, read)
            }))
            .unwrap_or_else(|_| {
                ThreadOutcome::Error(ErrorSnapshot::new(io::Error::other(
                    "dedicated stdin reader panicked",
                )))
            });
            if let Some(source_dropped) = source_dropped {
                source_dropped();
            }
            worker_shared.finish(outcome);
        })?;
        shared.install_interrupt(interrupt);

        Ok(Arc::new(Self {
            shared,
            reject_active_handoff,
        }))
    }

    pub(super) fn new_waiter_id(&self) -> u64 {
        loop {
            let id = NEXT_WAITER_ID.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
        }
    }

    pub(super) fn read_future(
        &self,
        waiter_id: u64,
        capacity: usize,
        stop_at_newline: bool,
    ) -> IoFuture<Vec<u8>> {
        Box::pin(ReadFuture {
            shared: Arc::clone(&self.shared),
            waiter_id,
            capacity,
            stop_at_newline,
            done: false,
            _liveness: StdinReadLiveness::acquire(),
        })
    }

    pub(super) fn abandon(&self, waiter_id: u64) {
        self.shared.abandon(waiter_id);
    }

    pub(super) fn pause_for_handoff(&self) -> io::Result<()> {
        self.shared.begin_handoff(self.reject_active_handoff)
    }

    pub(super) fn resume_after_handoff(&self) {
        self.shared.end_handoff();
    }

    fn request_shutdown(&self) {
        self.shared.request_shutdown();
    }

    #[cfg(test)]
    pub(super) fn spawn_for_test(source: OwnedFile, capacity: usize) -> io::Result<Arc<Self>> {
        Self::spawn_with_capacity(source, capacity, false)
    }

    #[cfg(test)]
    pub(super) fn spawn_with_reader_for_test(
        source: OwnedFile,
        capacity: usize,
        read: ReadFn,
    ) -> io::Result<Arc<Self>> {
        Self::spawn_with_reader(source, capacity, read, None, false)
    }

    #[cfg(test)]
    pub(super) fn spawn_with_drop_hook_for_test(
        source: OwnedFile,
        capacity: usize,
        source_dropped: impl FnOnce() + Send + 'static,
    ) -> io::Result<Arc<Self>> {
        Self::spawn_with_reader(
            source,
            capacity,
            super::imp::blocking_stdin_read,
            Some(Box::new(source_dropped)),
            false,
        )
    }

    #[cfg(test)]
    pub(super) fn spawn_with_idle_handoff_for_test(
        source: OwnedFile,
        capacity: usize,
    ) -> io::Result<Arc<Self>> {
        Self::spawn_with_capacity(source, capacity, true)
    }

    #[cfg(test)]
    pub(super) fn buffered_len(&self) -> usize {
        self.shared.lock().buffer.len()
    }

    #[cfg(test)]
    pub(super) fn max_buffered_len(&self) -> usize {
        self.shared.lock().max_buffered
    }

    #[cfg(test)]
    pub(super) fn waiter_count(&self) -> usize {
        self.shared.lock().waiters.len()
    }

    #[cfg(test)]
    pub(super) fn wait_for_buffered(&self, minimum: usize, timeout: std::time::Duration) -> bool {
        self.shared
            .wait_until(timeout, |state| state.buffer.len() >= minimum)
    }

    #[cfg(test)]
    pub(super) fn wait_for_idle(&self, timeout: std::time::Duration) -> bool {
        self.shared
            .wait_until(timeout, |state| state.reader_waiting && !state.io_active)
    }

    #[cfg(test)]
    pub(super) fn wait_for_active(&self, timeout: std::time::Duration) -> bool {
        self.shared.wait_until(timeout, |state| state.io_active)
    }

    #[cfg(test)]
    pub(super) fn shutdown_and_wait(&self, timeout: std::time::Duration) -> bool {
        self.request_shutdown();
        self.shared.wait_until(timeout, |state| state.thread_exited)
    }

    #[cfg(test)]
    pub(super) fn interrupt_released_at_exit_publish(&self) -> bool {
        self.shared
            .interrupt_released_at_exit_publish
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn interrupt_released(&self) -> bool {
        self.shared
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }
}

impl Drop for StdinReader {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

struct Shared {
    state: Mutex<State>,
    space_available: Condvar,
    changed: Condvar,
    io_gate: Mutex<()>,
    interrupt: Mutex<Option<platform::Interrupt>>,
    /// Records, at the instant `thread_exited` is published, whether the
    /// interrupt slot had already been drained. `shutdown_and_wait` returns on
    /// that publication, so this is the ordering a caller depends on.
    #[cfg(test)]
    interrupt_released_at_exit_publish: std::sync::atomic::AtomicBool,
    capacity: usize,
}

impl Shared {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(State::default()),
            space_available: Condvar::new(),
            changed: Condvar::new(),
            io_gate: Mutex::new(()),
            interrupt: Mutex::new(None),
            #[cfg(test)]
            interrupt_released_at_exit_publish: std::sync::atomic::AtomicBool::new(false),
            capacity,
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_capacity(&self) -> Option<usize> {
        let mut state = self.lock();
        loop {
            if state.shutdown_requested || !state.terminal.is_running() {
                state.io_active = false;
                state.reader_waiting = false;
                self.changed.notify_all();
                return None;
            }
            if state.handoffs == 0 && state.read_requested && state.buffer.len() < self.capacity {
                state.read_requested = false;
                state.io_active = true;
                state.reader_waiting = false;
                self.changed.notify_all();
                return Some(READ_CHUNK_BYTES.min(self.capacity - state.buffer.len()));
            }

            state.io_active = false;
            state.reader_waiting = true;
            self.changed.notify_all();
            state = self
                .space_available
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn is_shutdown_requested(&self) -> bool {
        self.lock().shutdown_requested
    }

    fn read_is_still_needed(&self) -> bool {
        let state = self.lock();
        !state.shutdown_requested && state.handoffs == 0 && !state.waiters.is_empty()
    }

    fn append(&self, bytes: &[u8]) -> bool {
        let wakers = {
            let mut state = self.lock();
            state.io_active = false;
            if state.shutdown_requested || !state.terminal.is_running() {
                self.changed.notify_all();
                return false;
            }

            debug_assert!(state.buffer.len() + bytes.len() <= self.capacity);
            state.buffer.extend(bytes);
            state.max_buffered = state.max_buffered.max(state.buffer.len());
            state.take_waiter_wakers()
        };

        self.changed.notify_all();
        wake_all(wakers);
        true
    }

    fn abandon(&self, waiter_id: u64) {
        let (removed, interrupt) = {
            let mut state = self.lock();
            let removed = state
                .waiters
                .iter()
                .position(|waiter| waiter.id == waiter_id)
                .map(|index| state.waiters.swap_remove(index));
            if state.waiters.is_empty() {
                state.read_requested = false;
            }
            (removed, state.waiters.is_empty() && state.io_active)
        };
        drop(removed);
        if interrupt {
            self.signal_interrupt();
        }
    }

    fn request_shutdown(&self) {
        let wakers = {
            let mut state = self.lock();
            state.shutdown_requested = true;
            state.read_requested = false;
            if state.terminal.is_running() {
                state.terminal = Terminal::Shutdown;
            }
            state.take_waiter_wakers()
        };

        self.space_available.notify_all();
        self.changed.notify_all();
        wake_all(wakers);
        self.signal_interrupt();
    }

    fn finish(&self, outcome: ThreadOutcome) {
        // `shutdown_and_wait` returns the moment `thread_exited` is published,
        // so anything that flag promises has to be done first. Releasing the
        // interrupt afterwards let a caller observe an exited thread whose
        // interrupt was still installed.
        //
        // Both locks are held across the release, in the same interrupt ->
        // state order `install_interrupt` uses: taking them in that order keeps
        // the two from inverting, and holding the interrupt lock while
        // publishing `thread_exited` stops `install_interrupt` from reinstalling
        // into a slot this call has already drained.
        let mut interrupt_slot = self
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let interrupt = interrupt_slot.take();
        let wakers = {
            let mut state = self.lock();
            if state.terminal.is_running() {
                state.terminal = match outcome {
                    ThreadOutcome::Eof => Terminal::Eof,
                    ThreadOutcome::Error(error) => Terminal::Error(error),
                    ThreadOutcome::Shutdown => Terminal::Shutdown,
                };
            }
            state.io_active = false;
            state.reader_waiting = false;
            state.read_requested = false;
            state.thread_exited = true;
            // Sampled while both locks are held, so it captures exactly what a
            // caller released by this publication can observe.
            #[cfg(test)]
            self.interrupt_released_at_exit_publish
                .store(interrupt_slot.is_none(), Ordering::Release);
            state.take_waiter_wakers()
        };
        drop(interrupt_slot);

        self.space_available.notify_all();
        self.changed.notify_all();
        wake_all(wakers);
        drop(interrupt);
    }

    fn install_interrupt(&self, interrupt: platform::Interrupt) {
        let mut slot = self
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.lock().thread_exited {
            *slot = Some(interrupt);
        }
    }

    fn signal_interrupt(&self) {
        if let Some(interrupt) = self
            .interrupt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            interrupt.signal(&self.io_gate);
        }
    }

    fn read_interrupted(&self) {
        {
            let mut state = self.lock();
            state.io_active = false;
            state.read_requested = !state.waiters.is_empty() && state.buffer.is_empty();
        }
        self.space_available.notify_all();
        self.changed.notify_all();
    }

    fn begin_handoff(&self, reject_active: bool) -> io::Result<()> {
        let interrupt = {
            let mut state = self.lock();
            if reject_active && state.io_active {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "cannot inherit Windows console stdin while a parent read is active",
                ));
            }
            state.handoffs += 1;
            if state.thread_exited || !state.terminal.is_running() {
                return Ok(());
            }
            state.io_active
        };
        if interrupt {
            self.signal_interrupt();
        }

        let mut state = self.lock();
        while state.io_active && !state.thread_exited && state.terminal.is_running() {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        Ok(())
    }

    fn end_handoff(&self) {
        {
            let mut state = self.lock();
            debug_assert!(state.handoffs > 0);
            state.handoffs = state.handoffs.saturating_sub(1);
            if state.handoffs == 0 && !state.waiters.is_empty() && state.buffer.is_empty() {
                state.read_requested = true;
            }
        }
        self.space_available.notify_all();
        self.changed.notify_all();
    }

    #[cfg(test)]
    fn wait_until(&self, timeout: std::time::Duration, predicate: impl Fn(&State) -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.lock();
        loop {
            if predicate(&state) {
                return true;
            }

            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }

            let (next, wait) = self
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if wait.timed_out() && !predicate(&state) {
                return false;
            }
        }
    }
}

#[derive(Default)]
struct State {
    buffer: VecDeque<u8>,
    waiters: Vec<Waiter>,
    terminal: Terminal,
    shutdown_requested: bool,
    thread_exited: bool,
    read_requested: bool,
    io_active: bool,
    reader_waiting: bool,
    handoffs: usize,
    max_buffered: usize,
}

impl State {
    fn take_waiter_wakers(&mut self) -> Vec<Waker> {
        self.waiters
            .iter_mut()
            .filter_map(|waiter| waiter.waker.take())
            .collect()
    }
}

struct Waiter {
    id: u64,
    waker: Option<Waker>,
}

#[derive(Default)]
enum Terminal {
    #[default]
    Running,
    Eof,
    Error(ErrorSnapshot),
    Shutdown,
}

impl Terminal {
    fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

enum ThreadOutcome {
    Eof,
    Error(ErrorSnapshot),
    Shutdown,
}

/// Keeps the owning runtime thread live while a stdin read is outstanding.
///
/// The process-wide reader thread is not a scheduler-visible wake source, so
/// without this a task awaiting stdin lets `run()` reach quiescence, which
/// terminalizes it with `JoinError::Cancelled` while input is still pending.
/// Every other suspended operation accounts for liveness the same way (see
/// `ReadDirShared` and `spawn_blocking_owned`).
///
/// `None` when the future is created off a runtime thread — bare `poll` in unit
/// tests — where there is no runtime to keep alive.
struct StdinReadLiveness {
    thread: ThreadHandle,
}

impl StdinReadLiveness {
    fn acquire() -> Option<Self> {
        let thread = try_current_thread_handle()?;
        thread.begin_async_operation();
        Some(Self { thread })
    }
}

impl Drop for StdinReadLiveness {
    fn drop(&mut self) {
        self.thread.finish_async_operation();
    }
}

struct ReadFuture {
    shared: Arc<Shared>,
    waiter_id: u64,
    capacity: usize,
    stop_at_newline: bool,
    done: bool,
    /// Dropped with the future, releasing the runtime-liveness reference.
    _liveness: Option<StdinReadLiveness>,
}

impl Future for ReadFuture {
    type Output = io::Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done, "stdin read future polled after completion");
        if self.capacity == 0 {
            self.done = true;
            return Poll::Ready(Ok(Vec::new()));
        }

        let candidate = cx.waker().clone();
        let mut discarded_waiter = None;
        let mut discarded_waker = None;
        let mut wake = Vec::new();
        let mut request_reader = false;
        let result = {
            let mut state = self.shared.lock();
            if !state.buffer.is_empty() {
                let read = if self.stop_at_newline {
                    state
                        .buffer
                        .iter()
                        .take(self.capacity)
                        .position(|byte| *byte == b'\n')
                        .map_or_else(
                            || self.capacity.min(state.buffer.len()),
                            |newline| newline + 1,
                        )
                } else {
                    self.capacity.min(state.buffer.len())
                };
                let bytes = state.buffer.drain(..read).collect::<Vec<_>>();
                discarded_waiter = remove_waiter(&mut state, self.waiter_id);
                if !state.buffer.is_empty() || !state.terminal.is_running() {
                    wake = state.take_waiter_wakers();
                }
                Some(Ok(bytes))
            } else {
                match &state.terminal {
                    Terminal::Running => {
                        match state
                            .waiters
                            .iter_mut()
                            .find(|waiter| waiter.id == self.waiter_id)
                        {
                            Some(waiter)
                                if waiter
                                    .waker
                                    .as_ref()
                                    .is_some_and(|waker| waker.will_wake(&candidate)) =>
                            {
                                discarded_waker = Some(candidate);
                            }
                            Some(waiter) => {
                                discarded_waker = waiter.waker.replace(candidate);
                            }
                            None => state.waiters.push(Waiter {
                                id: self.waiter_id,
                                waker: Some(candidate),
                            }),
                        }
                        if !state.io_active {
                            state.read_requested = true;
                            request_reader = true;
                        }
                        None
                    }
                    Terminal::Eof => {
                        discarded_waiter = remove_waiter(&mut state, self.waiter_id);
                        Some(Ok(Vec::new()))
                    }
                    Terminal::Error(error) => {
                        let error = error.to_error();
                        discarded_waiter = remove_waiter(&mut state, self.waiter_id);
                        Some(Err(error))
                    }
                    Terminal::Shutdown => {
                        discarded_waiter = remove_waiter(&mut state, self.waiter_id);
                        Some(Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "stdin reader shut down",
                        )))
                    }
                }
            }
        };

        drop(discarded_waiter);
        drop(discarded_waker);
        if let Some(result) = result {
            self.done = true;
            self.shared.space_available.notify_one();
            self.shared.changed.notify_all();
            wake_all(wake);
            Poll::Ready(result)
        } else {
            if request_reader {
                self.shared.space_available.notify_one();
            }
            Poll::Pending
        }
    }
}

impl Drop for ReadFuture {
    fn drop(&mut self) {
        if !self.done {
            self.shared.abandon(self.waiter_id);
        }
    }
}

fn remove_waiter(state: &mut State, waiter_id: u64) -> Option<Waiter> {
    state
        .waiters
        .iter()
        .position(|waiter| waiter.id == waiter_id)
        .map(|index| state.waiters.swap_remove(index))
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

fn read_loop(
    shared: &Shared,
    source: OwnedFile,
    interrupt: &platform::WorkerInterrupt,
    read: ReadFn,
) -> ThreadOutcome {
    let mut buffer = vec![0; READ_CHUNK_BYTES.min(shared.capacity)];
    loop {
        let Some(capacity) = shared.read_capacity() else {
            return ThreadOutcome::Shutdown;
        };

        match interrupt.wait_for_input(&source) {
            Ok(true) => {}
            Ok(false) => {
                shared.read_interrupted();
                continue;
            }
            Err(error) => return ThreadOutcome::Error(ErrorSnapshot::new(error)),
        }

        let io_guard = shared
            .io_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if shared.is_shutdown_requested() {
            drop(io_guard);
            return ThreadOutcome::Shutdown;
        }
        if !shared.read_is_still_needed() {
            drop(io_guard);
            shared.read_interrupted();
            continue;
        }

        let result = read(&source, &mut buffer[..capacity]);
        drop(io_guard);

        match result {
            Ok(0) => return ThreadOutcome::Eof,
            Ok(read) => {
                if !shared.append(&buffer[..read]) {
                    return ThreadOutcome::Shutdown;
                }
            }
            Err(_) if shared.is_shutdown_requested() => return ThreadOutcome::Shutdown,
            Err(_) if !shared.read_is_still_needed() => {
                shared.read_interrupted();
            }
            Err(error) if super::imp::should_retry_stdin_read(&error) => {
                shared.read_interrupted();
            }
            Err(error) => return ThreadOutcome::Error(ErrorSnapshot::new(error)),
        }
    }
}

#[cfg(unix)]
mod platform {
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::sync::Mutex;

    use crate::sys::handle::{OwnedFile, raw_file};

    pub(super) struct Interrupt(UnixStream);
    pub(super) struct WorkerInterrupt(UnixStream);

    fn interrupt_pair() -> io::Result<(Interrupt, WorkerInterrupt)> {
        let (sender, receiver) = UnixStream::pair()?;
        sender.set_nonblocking(true)?;
        receiver.set_nonblocking(true)?;
        Ok((Interrupt(sender), WorkerInterrupt(receiver)))
    }

    pub(super) fn spawn(
        task: impl FnOnce(WorkerInterrupt) + Send + 'static,
    ) -> io::Result<Interrupt> {
        let (interrupt, worker_interrupt) = interrupt_pair()?;
        std::thread::Builder::new()
            .name("runite-stdin-reader".to_owned())
            .spawn(move || task(worker_interrupt))
            .map(|thread| {
                drop(thread);
                interrupt
            })
    }

    impl Interrupt {
        pub(super) fn signal(&self, _io_gate: &Mutex<()>) {
            let byte = [1u8];
            // SAFETY: the stream remains owned by `Interrupt`, and `byte`
            // contains one initialized byte.
            let _ = unsafe {
                libc::write(
                    self.0.as_raw_fd(),
                    byte.as_ptr().cast::<libc::c_void>(),
                    byte.len(),
                )
            };
        }
    }

    impl WorkerInterrupt {
        fn drain(&self) -> io::Result<()> {
            let mut buffer = [0u8; 64];
            loop {
                // SAFETY: this worker owns the stream, and `buffer` is writable.
                let read = unsafe {
                    libc::read(
                        self.0.as_raw_fd(),
                        buffer.as_mut_ptr().cast::<libc::c_void>(),
                        buffer.len(),
                    )
                };
                if read > 0 {
                    continue;
                }
                if read == 0 {
                    return Ok(());
                }
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }
                return Err(error);
            }
        }

        pub(super) fn wait_for_input(&self, source: &OwnedFile) -> io::Result<bool> {
            let mut descriptors = [
                libc::pollfd {
                    fd: raw_file(source),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: self.0.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];

            loop {
                // SAFETY: `descriptors` contains two valid pollfd values for
                // handles owned by this thread for the duration of the call.
                let result =
                    unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, -1) };
                if result >= 0 {
                    let interrupted = descriptors[1].revents
                        & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)
                        != 0;
                    if interrupted {
                        self.drain()?;
                    }
                    return Ok(!interrupted);
                }

                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::io;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use std::sync::Mutex;

    use windows_sys::Win32::Foundation::{
        DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_NOT_FOUND, GetLastError, HANDLE,
    };
    use windows_sys::Win32::System::IO::CancelSynchronousIo;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetCurrentThread};

    use crate::sys::handle::OwnedFile;

    pub(super) struct Interrupt(OwnedHandle);
    pub(super) struct WorkerInterrupt;

    fn interrupt_pair() -> io::Result<(Interrupt, WorkerInterrupt)> {
        let mut duplicated: HANDLE = std::ptr::null_mut();
        // SAFETY: both pseudo handles refer to the current process/thread and
        // `duplicated` is a valid out-pointer.
        let ok = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                GetCurrentThread(),
                GetCurrentProcess(),
                &mut duplicated,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: `DuplicateHandle` returned a fresh owned thread handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(duplicated) };
        Ok((Interrupt(handle), WorkerInterrupt))
    }

    pub(super) fn spawn(
        task: impl FnOnce(WorkerInterrupt) + Send + 'static,
    ) -> io::Result<Interrupt> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        let thread = std::thread::Builder::new()
            .name("runite-stdin-reader".to_owned())
            .spawn(move || match interrupt_pair() {
                Ok((interrupt, worker)) => {
                    if sender.send(Ok(interrupt)).is_ok() {
                        task(worker);
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error));
                }
            })?;

        match receiver.recv() {
            Ok(Ok(interrupt)) => {
                drop(thread);
                Ok(interrupt)
            }
            Ok(Err(error)) => {
                let _ = thread.join();
                Err(error)
            }
            Err(_) => {
                let _ = thread.join();
                Err(io::Error::other(
                    "stdin reader exited during interrupt setup",
                ))
            }
        }
    }

    impl Interrupt {
        pub(super) fn signal(&self, io_gate: &Mutex<()>) {
            loop {
                if io_gate.try_lock().is_ok() {
                    return;
                }

                // SAFETY: the owned handle identifies the dedicated reader
                // thread for the full lifetime of `Interrupt`.
                if unsafe { CancelSynchronousIo(self.0.as_raw_handle()) } != 0 {
                    return;
                }
                // SAFETY: no other Win32 call intervened.
                if unsafe { GetLastError() } != ERROR_NOT_FOUND {
                    return;
                }
                std::thread::yield_now();
            }
        }
    }

    impl WorkerInterrupt {
        pub(super) fn wait_for_input(&self, _source: &OwnedFile) -> io::Result<bool> {
            Ok(true)
        }
    }
}

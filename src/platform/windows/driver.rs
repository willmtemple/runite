//! Public runtime driver primitives for Windows.
//!
//! The driver is an I/O completion port (IOCP). Overlapped operations are
//! submitted against handles associated with the port; the kernel posts one
//! completion packet per operation, and [`Driver::poll`]/[`Driver::wait`]
//! dequeue and dispatch those packets. Cross-thread wake-ups are completion
//! packets posted with a reserved key ([`PostQueuedCompletionStatus`]), and the
//! runtime timer is a waitable timer whose no-op APC interrupts the alertable
//! [`GetQueuedCompletionStatusEx`] wait (see `docs/WINDOWS.md`).

use std::cell::Cell;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use windows_sys::Win32::Foundation::{
    GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_IO_COMPLETION, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};
use windows_sys::Win32::System::Performance::{QueryPerformanceCounter, QueryPerformanceFrequency};
use windows_sys::Win32::System::Threading::{
    CancelWaitableTimer, CreateWaitableTimerExW, INFINITE, SetWaitableTimer, TIMER_ALL_ACCESS,
};

use crate::platform::runtime_shared::{DriverBackend, Notifier};

pub use crate::platform::runtime_shared::ReadyEvents;

// `RtlNtStatusToDosError` converts the `NTSTATUS` the kernel stores in
// `OVERLAPPED.Internal` into a Win32 error code. It is exported by ntdll and
// documented in the DDK; binding it directly avoids depending on undocumented
// `GetOverlappedResult` behavior with already-closed handles.
#[link(name = "ntdll")]
unsafe extern "system" {
    fn RtlNtStatusToDosError(status: i32) -> u32;
}

/// Completion key for cross-thread wake packets.
const WAKE_KEY: usize = 1;
/// Completion key for overlapped I/O packets carrying an [`OverlappedHeader`].
const IO_KEY: usize = 2;

/// How many completion packets one `GetQueuedCompletionStatusEx` call drains.
const COMPLETION_BATCH: usize = 64;

/// Result of one overlapped operation, as read from the completion packet.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OverlappedResult {
    /// Win32 error code; `0` on success.
    pub(crate) error: u32,
    /// Bytes transferred by the operation.
    pub(crate) bytes: usize,
}

impl OverlappedResult {
    /// Maps the raw completion status into `Ok(bytes)`/`Err`.
    pub(crate) fn into_result(self) -> io::Result<usize> {
        if self.error == 0 {
            Ok(self.bytes)
        } else {
            Err(io::Error::from_raw_os_error(self.error as i32))
        }
    }
}

/// Header every overlapped submission embeds at offset 0.
///
/// The kernel hands back the `OVERLAPPED` pointer in the completion packet; the
/// driver casts it to this header and runs `complete`, which reconstructs the
/// containing allocation (see `sys::windows::overlapped`) and consumes it.
#[repr(C)]
pub(crate) struct OverlappedHeader {
    pub(crate) overlapped: OVERLAPPED,
    /// Consumes the containing allocation and delivers the result.
    ///
    /// # Safety
    ///
    /// Must be called exactly once, with a pointer originally produced by
    /// `Box::into_raw` of the containing allocation.
    pub(crate) complete: unsafe fn(*mut OverlappedHeader, OverlappedResult),
}

impl OverlappedHeader {
    pub(crate) fn new(complete: unsafe fn(*mut OverlappedHeader, OverlappedResult)) -> Self {
        Self {
            // SAFETY: `OVERLAPPED` is a plain C struct for which zeroes are the
            // documented "no offset, no event" initial state.
            overlapped: unsafe { std::mem::zeroed() },
            complete,
        }
    }
}

struct NotifierInner {
    /// The completion port, shared with the [`Driver`]. Sharing the
    /// `OwnedHandle` (rather than storing the raw value) means a racing
    /// `notify` can never target a recycled handle after the driver drops —
    /// the port object stays alive until the last notifier goes away.
    port: Arc<OwnedHandle>,
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

        // SAFETY: the port handle is kept alive by the shared `Arc`; a null
        // OVERLAPPED with the reserved wake key is the documented way to post
        // a user packet.
        let posted = unsafe {
            PostQueuedCompletionStatus(self.port.as_raw_handle(), 0, WAKE_KEY, std::ptr::null_mut())
        };
        if posted == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Cross-thread notifier for a runtime thread's driver.
#[derive(Clone)]
pub struct ThreadNotifier {
    inner: Arc<NotifierInner>,
}

impl Notifier for ThreadNotifier {
    fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

/// Low-level Windows runtime driver backed by an I/O completion port.
pub struct Driver {
    port: Arc<OwnedHandle>,
    timer: OwnedHandle,
    closed: Arc<AtomicBool>,
    timer_deadline: Cell<Option<Duration>>,
    pending_wakes: Cell<u64>,
    pending_timers: Cell<u64>,
}

/// Creates a new driver and its paired [`ThreadNotifier`].
pub fn create_driver() -> io::Result<(Driver, ThreadNotifier)> {
    // SAFETY: passing `INVALID_HANDLE_VALUE` with no existing port creates a
    // fresh completion port; concurrency 1 documents the one-thread-per-loop
    // usage (the value only affects multi-threaded dequeue throttling).
    let raw_port =
        unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
    if raw_port.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `raw_port` was just created and is exclusively owned here.
    let port = Arc::new(unsafe { OwnedHandle::from_raw_handle(raw_port) });

    let timer = create_waitable_timer()?;

    let closed = Arc::new(AtomicBool::new(false));
    let notifier = ThreadNotifier {
        inner: Arc::new(NotifierInner {
            port: Arc::clone(&port),
            closed: Arc::clone(&closed),
        }),
    };

    let driver = Driver {
        port,
        timer,
        closed,
        timer_deadline: Cell::new(None),
        pending_wakes: Cell::new(0),
        pending_timers: Cell::new(0),
    };

    Ok((driver, notifier))
}

/// Creates the runtime's waitable timer.
///
/// A *standard* waitable timer, deliberately: timers created with
/// `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` reject APC completion routines
/// (`SetWaitableTimer` fails with `ERROR_INVALID_PARAMETER`), and the APC is
/// what interrupts the alertable completion-port wait. Expiry precision is
/// therefore bounded by the interrupt period (~15.6 ms worst case), matching
/// mainstream Windows runtimes; marrying the high-resolution timer to the port
/// via `NtAssociateWaitCompletionPacket` is a possible future refinement.
fn create_waitable_timer() -> io::Result<OwnedHandle> {
    // SAFETY: null attributes and name are the documented anonymous-timer form.
    let timer =
        unsafe { CreateWaitableTimerExW(std::ptr::null(), std::ptr::null(), 0, TIMER_ALL_ACCESS) };
    if timer.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: freshly created, exclusively owned.
    Ok(unsafe { OwnedHandle::from_raw_handle(timer) })
}

/// No-op APC completion routine for the runtime timer.
///
/// Its only purpose is to make the alertable `GetQueuedCompletionStatusEx`
/// wait return (`WAIT_IO_COMPLETION`); the driver then re-checks the armed
/// deadline against the monotonic clock. Because the routine touches no state,
/// stale APCs from rearmed or cancelled timers are harmless.
unsafe extern "system" fn timer_apc(
    _context: *const core::ffi::c_void,
    _timer_low: u32,
    _timer_high: u32,
) {
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
    ///
    /// Arms the waitable timer so its APC interrupts a blocked [`Self::wait`];
    /// the millisecond timeout passed to `GetQueuedCompletionStatusEx` (rounded
    /// *up*) is kept as a backstop, so a lost or coalesced APC only degrades
    /// precision, never correctness.
    pub fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()> {
        self.timer_deadline.set(deadline);

        let Some(deadline) = deadline else {
            // SAFETY: the timer handle is owned by `self` and valid.
            unsafe { CancelWaitableTimer(self.timer.as_raw_handle()) };
            return Ok(());
        };

        let relative = deadline.saturating_sub(monotonic_now()?);
        // Negative due time = relative interval in 100 ns units. An
        // already-expired deadline still arms for one tick: the APC must fire
        // so a blocked alertable wait observes the expiry.
        let ticks = (relative.as_nanos() / 100).min(i64::MAX as u128) as i64;
        let due = -ticks.max(1);
        // SAFETY: the timer handle is valid; `due` outlives the call; the APC
        // routine is a `'static` no-op fn queued to this (owning) thread.
        let armed = unsafe {
            SetWaitableTimer(
                self.timer.as_raw_handle(),
                &due,
                0,
                Some(timer_apc),
                std::ptr::null(),
                0,
            )
        };
        if armed == 0 {
            return Err(io::Error::last_os_error());
        }
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

    /// Associates `handle` with this driver's completion port.
    ///
    /// A handle can be associated with exactly one port for its lifetime; the
    /// backends call this once when a file, socket, or pipe handle is created
    /// or adopted on this runtime thread. After association, every overlapped
    /// operation on the handle posts its completion packet to this port.
    pub(crate) fn associate_handle(&self, handle: RawHandle) -> io::Result<()> {
        // SAFETY: `handle` is an open overlapped-capable handle supplied by the
        // backend; associating it with the owned port does not transfer
        // ownership of either handle.
        let result = unsafe {
            CreateIoCompletionPort(handle as HANDLE, self.port.as_raw_handle(), IO_KEY, 0)
        };
        if result.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn process(&self, timeout: Option<Duration>) -> io::Result<Option<ReadyEvents>> {
        let mut ready = ReadyEvents::default();

        // SAFETY: `OVERLAPPED_ENTRY` is a plain C struct; zeroed is a valid
        // uninitialized value for an out-buffer.
        let mut entries = [unsafe { std::mem::zeroed::<OVERLAPPED_ENTRY>() }; COMPLETION_BATCH];
        let mut removed = 0u32;

        // SAFETY: `entries`/`removed` are valid out-pointers; the alertable
        // wait lets the runtime timer's APC interrupt the block.
        let dequeued = unsafe {
            GetQueuedCompletionStatusEx(
                self.port.as_raw_handle(),
                entries.as_mut_ptr(),
                entries.len() as u32,
                &mut removed,
                timeout_to_millis(timeout),
                1,
            )
        };

        let mut saw_any = false;
        if dequeued == 0 {
            // SAFETY: read immediately after the failed call.
            let error = unsafe { GetLastError() };
            match error {
                WAIT_TIMEOUT => {}
                // An APC (the runtime timer) interrupted the wait; the
                // deadline check below picks it up.
                WAIT_IO_COMPLETION => {}
                _ => return Err(io::Error::from_raw_os_error(error as i32)),
            }
        } else {
            let count = removed as usize;
            if count > 0 {
                saw_any = true;
            }
            for entry in entries.iter().take(count) {
                match entry.lpCompletionKey {
                    WAKE_KEY => {
                        ready.wake = true;
                        self.pending_wakes
                            .set(self.pending_wakes.get().saturating_add(1));
                    }
                    IO_KEY => dispatch_io_entry(entry),
                    _ => {}
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
}

/// Reconstructs an I/O packet's [`OverlappedHeader`] and runs its completion.
fn dispatch_io_entry(entry: &OVERLAPPED_ENTRY) {
    let header = entry.lpOverlapped as *mut OverlappedHeader;
    if header.is_null() {
        return;
    }

    // SAFETY: every packet posted with `IO_KEY` carries an `OVERLAPPED` that is
    // the first field of a live, `Box::into_raw`-leaked `OverlappedHeader`
    // allocation, and the kernel delivers exactly one packet per submission —
    // so the pointer is valid and this is its only consumption.
    let (complete, result) = unsafe {
        let status = (*header).overlapped.Internal as i32;
        let error = if status == 0 {
            0
        } else {
            RtlNtStatusToDosError(status)
        };
        (
            (*header).complete,
            OverlappedResult {
                error,
                bytes: entry.dwNumberOfBytesTransferred as usize,
            },
        )
    };
    // SAFETY: forwarded contract — called exactly once with the leaked pointer.
    unsafe { complete(header, result) };
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);

        // SAFETY: the timer handle is valid until `self.timer` drops below.
        unsafe { CancelWaitableTimer(self.timer.as_raw_handle()) };

        // Free any packets that were completed but never dequeued so their
        // payload allocations (buffers, completion handles) are reclaimed. The
        // runtime only tears the driver down once no live operations remain,
        // so this is a leak backstop, not a delivery path — results are
        // discarded.
        loop {
            // SAFETY: as in `process`; zero timeout, non-alertable.
            let mut entries = [unsafe { std::mem::zeroed::<OVERLAPPED_ENTRY>() }; COMPLETION_BATCH];
            let mut removed = 0u32;
            // SAFETY: valid out-pointers; the port handle is still open here.
            let dequeued = unsafe {
                GetQueuedCompletionStatusEx(
                    self.port.as_raw_handle(),
                    entries.as_mut_ptr(),
                    entries.len() as u32,
                    &mut removed,
                    0,
                    0,
                )
            };
            if dequeued == 0 || removed == 0 {
                break;
            }
            for entry in entries.iter().take(removed as usize) {
                if entry.lpCompletionKey == IO_KEY {
                    dispatch_io_entry(entry);
                }
            }
        }

        // Flush any APC the timer queued to this thread before it could be
        // cancelled, so a runtime installed on this thread later can never
        // observe a stale (albeit no-op) timer APC.
        // SAFETY: zero-length alertable sleep only runs already-queued APCs.
        unsafe {
            windows_sys::Win32::System::Threading::SleepEx(0, 1);
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

fn timeout_to_millis(timeout: Option<Duration>) -> u32 {
    match timeout {
        None => INFINITE,
        Some(value) => {
            // Round up so the backstop timeout never fires before the armed
            // deadline; clamp below INFINITE (u32::MAX) which means "forever".
            let millis = value
                .as_millis()
                .saturating_add(u128::from(value.subsec_nanos() % 1_000_000 != 0));
            millis.min(u128::from(INFINITE - 1)) as u32
        }
    }
}

/// Returns the current monotonic clock reading.
///
/// Windows' invariant monotonic clock is the performance counter; the
/// frequency is fixed at boot, so it is queried once.
pub fn monotonic_now() -> io::Result<Duration> {
    use std::sync::OnceLock;
    static FREQUENCY: OnceLock<i64> = OnceLock::new();

    let frequency = *FREQUENCY.get_or_init(|| {
        let mut frequency = 0i64;
        // SAFETY: valid out-pointer; cannot fail on XP and later.
        unsafe { QueryPerformanceFrequency(&mut frequency) };
        frequency.max(1)
    });

    let mut counter = 0i64;
    // SAFETY: valid out-pointer; cannot fail on XP and later.
    let ok = unsafe { QueryPerformanceCounter(&mut counter) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }

    let counter = counter as u128;
    let frequency = frequency as u128;
    let seconds = counter / frequency;
    let nanos = (counter % frequency) * 1_000_000_000 / frequency;
    Ok(Duration::new(seconds as u64, nanos as u32))
}

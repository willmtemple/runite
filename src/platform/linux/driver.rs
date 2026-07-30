//! runite driver for Linux.
//!
//!

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::marker::PhantomData;
#[cfg(test)]
use std::rc::Rc;

use super::uring::{
    IORING_OP_ASYNC_CANCEL, IORING_OP_MSG_RING, IORING_OP_POLL_ADD, IoUring, IoUringCqe,
    IoUringSqe, SupportedOps,
};
use crate::platform::runtime_shared::{DriverBackend, Notifier};
use crate::trace_targets;

pub use crate::platform::runtime_shared::ReadyEvents;

const WAKE_TARGET_TOKEN: u64 = 1;
const TOKEN_KIND_SHIFT: u64 = 56;
const TOKEN_KIND_MASK: u64 = 0xff << TOKEN_KIND_SHIFT;

#[cfg(test)]
thread_local! {
    static TEST_DEFER_SUBMISSIONS: Cell<Option<bool>> = const { Cell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum CompletionKind {
    Timer = 1,
    TimerRemove = 2,
    NotifySend = 3,
    Operation = 4,
    OperationCancel = 5,
    /// CQE produced by the IORING_OP_LINK_TIMEOUT SQE that accompanies a
    /// linked operation.  We register it so the token range is claimed, but
    /// the completion itself carries no useful information and is discarded.
    LinkedTimeout = 6,
}

type CompletionHandler = Box<dyn FnOnce(IoUringCqe) + Send + 'static>;

enum WakeTarget {
    MsgRing(OwnedFd),
    EventFd(OwnedFd),
}

struct NotifierInner {
    target: Mutex<Option<WakeTarget>>,
    closed: AtomicBool,
}

impl NotifierInner {
    fn lock_target(&self) -> MutexGuard<'_, Option<WakeTarget>> {
        match self.target.lock() {
            Ok(target) => target,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn notify(&self) -> io::Result<()> {
        tracing::trace!(
            target: trace_targets::DRIVER,
            event = "notify",
            "sending cross-thread driver wake"
        );
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "target runtime ring is closed",
            ));
        }

        let target = self.lock_target();
        let Some(target) = target.as_ref() else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "target runtime ring is closed",
            ));
        };
        match target {
            WakeTarget::MsgRing(ring_fd) => IoUring::with_submitter(|ring| {
                ring.submit_msg_ring(
                    ring_fd.as_raw_fd(),
                    WAKE_TARGET_TOKEN,
                    1,
                    make_token(CompletionKind::NotifySend, 0),
                )
            }),
            WakeTarget::EventFd(event_fd) => write_eventfd(event_fd.as_raw_fd()),
        }
    }
}

fn write_eventfd(fd: RawFd) -> io::Result<()> {
    let value = 1u64;
    loop {
        // SAFETY: `fd` is an open eventfd and `value` provides the required
        // eight initialized bytes for the duration of the write.
        let written = unsafe {
            libc::write(
                fd,
                (&value as *const u64).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if written == std::mem::size_of::<u64>() as isize {
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

fn drain_eventfd(fd: RawFd) -> u64 {
    let mut total = 0u64;
    loop {
        let mut value = 0u64;
        // SAFETY: `fd` is an open nonblocking eventfd and `value` provides the
        // required eight writable bytes for the duration of the read.
        let read = unsafe {
            libc::read(
                fd,
                (&mut value as *mut u64).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if read == std::mem::size_of::<u64>() as isize {
            total = total.saturating_add(value);
            continue;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if error.kind() != io::ErrorKind::WouldBlock {
            tracing::warn!(
                target: trace_targets::DRIVER,
                event = "eventfd_wake_drain_failed",
                error = %error,
                "failed to drain the runtime wake eventfd"
            );
        }
        return total;
    }
}

/// Duplicates `fd` with `O_CLOEXEC`, returning an owned handle to the copy.
fn dup_cloexec(fd: RawFd) -> io::Result<OwnedFd> {
    // F_DUPFD_CLOEXEC yields the lowest-numbered free fd >= 0, with the
    // close-on-exec flag already set on the new descriptor.
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `duplicated` is a fresh, exclusively-owned fd just returned by
    // `fcntl(F_DUPFD_CLOEXEC)`; wrapping it transfers ownership to the `OwnedFd`.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

#[derive(Clone)]
/// Cross-thread notifier for a runtime thread's driver.
pub struct ThreadNotifier {
    inner: Arc<NotifierInner>,
}

impl Notifier for ThreadNotifier {
    fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

/// Low-level Linux runtime driver backed by `io_uring`.
pub struct Driver {
    /// The `io_uring` instance driving this runtime thread.
    ring: UnsafeCell<Option<IoUring>>,
    /// Process-wide io_uring opcode support snapshot used by this driver.
    supported_ops: Cell<SupportedOps>,
    /// Shared notifier that other threads can use to wake this runtime thread.
    notifier: Arc<NotifierInner>,
    /// Eventfd watched by the ring on kernels predating `IORING_OP_MSG_RING`.
    fallback_wake_fd: Option<OwnedFd>,
    eventfd_wake_active: Cell<bool>,
    /// Enabled by default. Setting `RUNITE_IO_URING_DEFER_SUBMISSIONS=0`
    /// selects the corrected immediate path for controlled benchmark A/Bs.
    defer_submissions: bool,
    shutting_down: Cell<bool>,
    /// Next sequence number for generated completion tokens.
    next_token: Cell<u64>,
    /// The token of the currently active timer, if any timer is armed.
    active_timer_token: Cell<Option<u64>>,
    /// Deadline represented by `active_timer_token`, used to avoid redundant
    /// timeout-update SQEs when the timer heap's earliest deadline is unchanged.
    active_timer_deadline: Cell<Option<Duration>>,
    /// Accumulated count of pending wake notifications that have not yet been triggered.
    pending_wakes: Cell<u64>,
    /// Accumulated count of pending timer expirations that have not yet been triggered.
    pending_timers: Cell<u64>,
    /// Map of active completion tokens to associated handlers. When a CQE is received with a token in this map, the
    /// corresponding handler will be invoked with the CQE and removed from the map. This is the core mechanism by which
    /// async operations are tracked and dispatched to their continuations.
    completions: RefCell<HashMap<u64, CompletionHandler>>,
    /// Guards detached when a future is dropped while its SQE may still touch
    /// memory owned by that future. Entries are keyed by the original operation
    /// token and dropped only when the original CQE proves the kernel released
    /// the referenced storage.
    pending_cancel_tokens: RefCell<HashMap<u64, u64>>,
    /// Submission-queue size this driver was created with. Retained because a
    /// worker's ring is minted on the parent thread and rebuilt on the worker
    /// itself; without it the rebuild would silently fall back to the default
    /// and undo the configuration the worker inherited.
    ring_entries: u32,
}

/// Creates a new driver and its paired [`ThreadNotifier`].
///
/// `ring_entries` has already been validated by
/// [`check_ring_entries`](super::uring::check_ring_entries); callers that have
/// no opinion pass [`DEFAULT_RING_ENTRIES`](super::uring::DEFAULT_RING_ENTRIES).
pub fn create_driver(ring_entries: u32) -> io::Result<(Driver, ThreadNotifier)> {
    let ring = IoUring::new(ring_entries)?;
    tracing::debug!(
        target: trace_targets::DRIVER,
        event = "create_driver",
        ring_fd = ring.ring_fd(),
        "created runtime driver"
    );
    let supported_ops = ring.supported_ops();
    let (target, fallback_wake_fd) = if supported_ops.supports(IORING_OP_MSG_RING) {
        (WakeTarget::MsgRing(dup_cloexec(ring.ring_fd())?), None)
    } else {
        let event_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if event_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `eventfd` returned a fresh descriptor owned by this function.
        let event_fd = unsafe { OwnedFd::from_raw_fd(event_fd) };
        let notifier_fd = dup_cloexec(event_fd.as_raw_fd())?;
        register_eventfd_poll(&ring, event_fd.as_raw_fd())?;
        flush_initial_submissions(&ring)?;
        tracing::info!(
            target: trace_targets::DRIVER,
            event = "eventfd_wake_fallback",
            "kernel lacks IORING_OP_MSG_RING; using eventfd-backed runtime wakes"
        );
        (WakeTarget::EventFd(notifier_fd), Some(event_fd))
    };
    let notifier = Arc::new(NotifierInner {
        target: Mutex::new(Some(target)),
        closed: AtomicBool::new(false),
    });

    Ok((
        Driver {
            ring: UnsafeCell::new(Some(ring)),
            supported_ops: Cell::new(supported_ops),
            notifier: Arc::clone(&notifier),
            eventfd_wake_active: Cell::new(fallback_wake_fd.is_some()),
            fallback_wake_fd,
            defer_submissions: deferred_submissions_enabled(),
            shutting_down: Cell::new(false),
            next_token: Cell::new(1),
            active_timer_token: Cell::new(None),
            active_timer_deadline: Cell::new(None),
            pending_wakes: Cell::new(0),
            pending_timers: Cell::new(0),
            completions: RefCell::new(HashMap::new()),
            pending_cancel_tokens: RefCell::new(HashMap::new()),
            ring_entries,
        },
        ThreadNotifier { inner: notifier },
    ))
}

fn deferred_submissions_enabled() -> bool {
    #[cfg(test)]
    if let Some(enabled) = TEST_DEFER_SUBMISSIONS.with(Cell::get) {
        return enabled;
    }
    std::env::var_os("RUNITE_IO_URING_DEFER_SUBMISSIONS").is_none_or(|value| value != "0")
}

#[cfg(test)]
pub(crate) fn override_deferred_submissions(enabled: bool) -> DeferredSubmissionsOverride {
    let previous = TEST_DEFER_SUBMISSIONS.with(|slot| slot.replace(Some(enabled)));
    DeferredSubmissionsOverride {
        previous,
        _not_send: PhantomData,
    }
}

#[cfg(test)]
pub(crate) struct DeferredSubmissionsOverride {
    previous: Option<bool>,
    _not_send: PhantomData<Rc<()>>,
}

#[cfg(test)]
impl Drop for DeferredSubmissionsOverride {
    fn drop(&mut self) {
        TEST_DEFER_SUBMISSIONS.with(|slot| slot.set(self.previous));
    }
}

fn register_eventfd_poll(ring: &IoUring, event_fd: RawFd) -> io::Result<()> {
    ring.submit_with_token(WAKE_TARGET_TOKEN, |sqe| {
        sqe.opcode = IORING_OP_POLL_ADD;
        sqe.fd = event_fd;
        sqe.op_flags = (libc::POLLIN | libc::POLLERR | libc::POLLHUP) as u32;
    })
}

fn flush_initial_submissions(ring: &IoUring) -> io::Result<()> {
    while ring.has_pending_submissions() {
        let outcome = ring.submit_pending(false)?;
        if outcome.retry {
            std::thread::yield_now();
            continue;
        }
        if let Some(error) = outcome.error {
            return Err(error);
        }
    }
    Ok(())
}

impl Driver {
    pub(crate) fn bind_current_thread(&self) {
        if !self.ring().was_created_on_current_thread() {
            self.recreate_ring_on_current_thread()
                .expect("worker io_uring should initialize on its owner thread");
        }
        self.ring().bind_current_thread();
    }

    pub(crate) fn unbind_current_thread(&self) {
        self.ring().unbind_current_thread();
    }

    fn ring(&self) -> &IoUring {
        // SAFETY: the ring is replaced at most once, during
        // `bind_current_thread`, before the driver is exposed to its runtime
        // loop. Afterwards it remains owner-thread confined. Cross-thread
        // notifiers only access `NotifierInner`, never this cell.
        unsafe {
            (&*self.ring.get())
                .as_ref()
                .expect("driver ring accessed after teardown")
        }
    }

    /// Submission-queue entries the kernel allocated for this thread's ring.
    #[cfg(test)]
    pub(crate) fn ring_sq_entries(&self) -> u32 {
        self.ring().sq_entries()
    }

    #[cfg(test)]
    fn replace_ring_for_test(&mut self, replacement: IoUring) -> IoUring {
        self.ring
            .get_mut()
            .replace(replacement)
            .expect("test driver ring should exist")
    }

    fn recreate_ring_on_current_thread(&self) -> io::Result<()> {
        let replacement = IoUring::new(self.ring_entries)?;
        let (new_target, eventfd_active) = if self.supported_ops.get().supports(IORING_OP_MSG_RING)
        {
            (
                WakeTarget::MsgRing(dup_cloexec(replacement.ring_fd())?),
                false,
            )
        } else {
            let event_fd = self
                .fallback_wake_fd
                .as_ref()
                .expect("MSG_RING support cannot vary between runtime rings");
            register_eventfd_poll(&replacement, event_fd.as_raw_fd())?;
            flush_initial_submissions(&replacement)?;
            (
                WakeTarget::EventFd(dup_cloexec(event_fd.as_raw_fd())?),
                true,
            )
        };

        *self.notifier.lock_target() = Some(new_target);
        self.eventfd_wake_active.set(eventfd_active);
        // SAFETY: see `ring()`. This runs before the owner installs the ring in
        // TLS or submits any operation. The old pre-created ring has no user
        // submissions and can be dropped after the notifier target is switched.
        let old = unsafe {
            (&mut *self.ring.get())
                .replace(replacement)
                .expect("driver ring should exist before rehome")
        };
        drop(old);
        Ok(())
    }

    /// Polls the driver without blocking.
    pub fn poll(&self) -> io::Result<Option<ReadyEvents>> {
        let mut ready = ReadyEvents {
            timer: self.pending_timers.get() != 0,
            wake: self.pending_wakes.get() != 0,
        };
        let had_durable_ready = ready.timer || ready.wake;
        let saw_submission = self.flush_submissions(false, &mut ready)?;
        let saw_completion = self
            .ring()
            .drain_completions(|cqe| self.process_cqe(cqe, &mut ready));
        let saw_any = had_durable_ready || saw_submission || saw_completion;
        if saw_any {
            tracing::trace!(
                target: trace_targets::DRIVER,
                event = "poll_ready",
                timer_ready = ready.timer,
                wake_ready = ready.wake,
                "driver poll produced ready events"
            );
        }
        if saw_any { Ok(Some(ready)) } else { Ok(None) }
    }

    /// Blocks until at least one completion is available.
    pub fn wait(&self) -> io::Result<()> {
        tracing::trace!(
            target: trace_targets::DRIVER,
            event = "wait",
            "waiting for driver completion"
        );
        let mut ready = ReadyEvents::default();
        let _ = self.flush_submissions(true, &mut ready)?;
        Ok(())
    }

    /// Updates the currently armed timer deadline.
    ///
    /// Passing `None` removes any active timer.
    pub fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()> {
        tracing::trace!(
            target: trace_targets::TIMER,
            event = "rearm_timer",
            deadline_ns = deadline.map(|value| value.as_nanos() as u64),
            "rearming driver timer"
        );
        if self.active_timer_deadline.get() == deadline {
            return Ok(());
        }

        match (self.active_timer_token.get(), deadline) {
            (Some(active), Some(deadline)) => {
                self.ring()
                    .submit_timeout_remove(active, self.next_token(CompletionKind::TimerRemove))?;
                let token = self.next_token(CompletionKind::Timer);
                self.ring().submit_timeout(token, deadline)?;
                self.active_timer_token.set(Some(token));
            }
            (Some(active), None) => {
                self.active_timer_token.set(None);
                self.ring()
                    .submit_timeout_remove(active, self.next_token(CompletionKind::TimerRemove))?;
            }
            (None, Some(deadline)) => {
                let token = self.next_token(CompletionKind::Timer);
                self.active_timer_token.set(Some(token));
                self.ring().submit_timeout(token, deadline)?;
            }
            (None, None) => {}
        }
        self.active_timer_deadline.set(deadline);
        self.flush_if_immediate()?;

        Ok(())
    }

    fn flush_submissions(&self, wait_for_cqe: bool, ready: &mut ReadyEvents) -> io::Result<bool> {
        let mut saw_any = false;
        let mut transient_retries = 0usize;

        loop {
            let had_pending = self.ring().has_pending_submissions();
            let combined_wait = wait_for_cqe && had_pending && self.ring().supports_submit_all();
            let outcome = self.ring().submit_pending(wait_for_cqe)?;

            if !outcome.failures.is_empty() {
                saw_any = true;
                for cqe in outcome.failures {
                    self.process_cqe(cqe, ready);
                }
            }

            if outcome.retry {
                let drained = self
                    .ring()
                    .drain_completions(|cqe| self.process_cqe(cqe, ready));
                saw_any |= drained;
                if wait_for_cqe && drained {
                    return Ok(true);
                }
                transient_retries += 1;
                if !wait_for_cqe && !drained && transient_retries >= 2 {
                    return Ok(saw_any);
                }
                std::thread::yield_now();
                continue;
            }

            if let Some(error) = outcome.error {
                self.active_timer_deadline.set(None);
                return Err(error);
            }

            if self.ring().has_pending_submissions() {
                if outcome.submitted == 0 {
                    return Ok(saw_any);
                }
                continue;
            }

            if wait_for_cqe && had_pending && !combined_wait && !saw_any {
                continue;
            }
            return Ok(saw_any);
        }
    }

    fn flush_if_immediate(&self) -> io::Result<()> {
        if self.defer_submissions {
            return Ok(());
        }
        let mut ready = ReadyEvents::default();
        let _ = self.flush_submissions(false, &mut ready)?;
        Ok(())
    }

    pub(crate) fn submit_operation(
        &self,
        fill: impl FnOnce(&mut IoUringSqe),
        on_complete: impl FnOnce(IoUringCqe) + Send + 'static,
    ) -> io::Result<u64> {
        if self.shutting_down.get() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runtime driver is shutting down",
            ));
        }
        let mut prepared = IoUringSqe::default();
        fill(&mut prepared);
        self.validate_opcode(prepared.opcode)?;

        let token = self.next_token(CompletionKind::Operation);
        tracing::trace!(
            target: trace_targets::ASYNC,
            event = "submit_operation",
            token,
            "submitting async driver operation"
        );
        self.completions
            .borrow_mut()
            .insert(token, Box::new(on_complete));

        if let Err(error) = self.ring().submit_with_token(token, |sqe| *sqe = prepared) {
            let _ = self.completions.borrow_mut().remove(&token);
            return Err(error);
        }
        self.flush_if_immediate()?;

        Ok(token)
    }

    /// Submits a main operation linked to a timeout.
    ///
    /// Internally two SQEs are enqueued atomically: the main op (with
    /// `IOSQE_IO_LINK`) and an `IORING_OP_LINK_TIMEOUT` SQE.  If `timeout`
    /// elapses before the main op completes, the kernel cancels the main op and
    /// its CQE will carry `-ECANCELED`.  The timeout's own CQE is silently
    /// discarded in `process_cqe`.
    ///
    /// Returns the token for the main operation, which can be used with
    /// `cancel_operation` to cancel it early.
    pub(crate) fn submit_operation_with_linked_timeout(
        &self,
        fill: impl FnOnce(&mut IoUringSqe),
        timeout: Duration,
        on_complete: impl FnOnce(IoUringCqe) + Send + 'static,
    ) -> io::Result<u64> {
        if self.shutting_down.get() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runtime driver is shutting down",
            ));
        }
        let mut prepared = IoUringSqe::default();
        fill(&mut prepared);
        self.validate_opcode(prepared.opcode)?;

        let main_token = self.next_token(CompletionKind::Operation);
        let timeout_token = self.next_token(CompletionKind::LinkedTimeout);
        tracing::trace!(
            target: trace_targets::ASYNC,
            event = "submit_operation_with_linked_timeout",
            main_token,
            timeout_token,
            timeout_ns = timeout.as_nanos() as u64,
            "submitting async driver operation with linked timeout"
        );
        self.completions
            .borrow_mut()
            .insert(main_token, Box::new(on_complete));

        if let Err(error) = self.ring().submit_linked_with_timeout(
            main_token,
            |sqe| *sqe = prepared,
            timeout_token,
            timeout,
        ) {
            let mut completions = self.completions.borrow_mut();
            let _ = completions.remove(&main_token);
            return Err(error);
        }
        self.flush_if_immediate()?;

        Ok(main_token)
    }

    pub(crate) fn cancel_operation(&self, token: u64) -> io::Result<()> {
        tracing::trace!(
            target: trace_targets::ASYNC,
            event = "cancel_operation",
            token,
            "submitting async driver cancellation"
        );
        self.stage_cancel_operation(token)?;
        self.flush_if_immediate()
    }

    fn stage_cancel_operation(&self, token: u64) -> io::Result<()> {
        let cancel_token = self.next_token(CompletionKind::OperationCancel);
        self.pending_cancel_tokens
            .borrow_mut()
            .insert(cancel_token, token);
        match self.ring().submit_with_token(cancel_token, |sqe| {
            sqe.opcode = IORING_OP_ASYNC_CANCEL;
            sqe.fd = -1;
            sqe.addr = token;
        }) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.pending_cancel_tokens
                    .borrow_mut()
                    .remove(&cancel_token);
                Err(error)
            }
        }
    }

    /// Drains the accumulated wake notification count.
    ///
    /// Returns `Some(count)` with the number of wake notifications collected
    /// since the previous call, or `None` if no wake completions were pending.
    pub fn drain_wake(&self) -> Option<u64> {
        let wakes = self.pending_wakes.replace(0);
        if wakes == 0 { None } else { Some(wakes) }
    }

    /// Drains the accumulated timer-expiration count.
    ///
    /// Returns `Some(count)` with the number of timer expirations collected
    /// since the previous call, or `None` if no timer completions were pending.
    pub fn drain_timer(&self) -> Option<u64> {
        let timers = self.pending_timers.replace(0);
        if timers == 0 { None } else { Some(timers) }
    }

    fn process_cqe(&self, cqe: IoUringCqe, ready: &mut ReadyEvents) {
        tracing::trace!(
            target: trace_targets::DRIVER,
            event = "process_cqe",
            user_data = cqe.user_data,
            result = cqe.res,
            "processing io_uring completion"
        );
        if cqe.user_data == WAKE_TARGET_TOKEN {
            let wakes = if self.eventfd_wake_active.get() {
                let event_fd = self
                    .fallback_wake_fd
                    .as_ref()
                    .expect("active eventfd wake must own its descriptor");
                let wakes = drain_eventfd(event_fd.as_raw_fd());
                if let Err(error) = self.ring().submit_with_token(WAKE_TARGET_TOKEN, |sqe| {
                    sqe.opcode = IORING_OP_POLL_ADD;
                    sqe.fd = event_fd.as_raw_fd();
                    sqe.op_flags = (libc::POLLIN | libc::POLLERR | libc::POLLHUP) as u32;
                }) {
                    tracing::error!(
                        target: trace_targets::DRIVER,
                        event = "eventfd_wake_rearm_failed",
                        error = %error,
                        "failed to rearm the eventfd runtime wake poll"
                    );
                }
                wakes
            } else {
                cqe.res.max(1) as u64
            };
            ready.wake |= wakes != 0;
            self.pending_wakes
                .set(self.pending_wakes.get().saturating_add(wakes));
            return;
        }

        match decode_token_kind(cqe.user_data) {
            Some(CompletionKind::Timer) => {
                if self.active_timer_token.get() == Some(cqe.user_data) {
                    self.active_timer_token.set(None);
                    self.active_timer_deadline.set(None);
                }
                if cqe.res == -libc::ETIME {
                    ready.timer = true;
                    self.pending_timers
                        .set(self.pending_timers.get().saturating_add(1));
                }
            }
            Some(CompletionKind::Operation) => {
                // Dropping the callback drops the staging buffer it owns. This
                // is the only place kernel-visible storage is released, and it
                // is reached only by the original operation's terminal CQE.
                if let Some(callback) = self.completions.borrow_mut().remove(&cqe.user_data) {
                    callback(cqe);
                }
            }
            Some(CompletionKind::OperationCancel) => {
                // A cancel completion only signals that the ASYNC_CANCEL
                // request itself finished; it does NOT prove the original
                // request has released the guarded user buffers. In particular
                // `IORING_OP_ASYNC_CANCEL` can report `-EALREADY` (the target op
                // was already executing, could not be stopped, and will still
                // complete later — potentially writing into those buffers). The
                // buffer guards are therefore released exclusively by the
                // original operation's own completion (the
                // `CompletionKind::Operation` arm above), which the kernel
                // always posts exactly once. Here we only discard the
                // cancel-token bookkeeping.
                self.pending_cancel_tokens
                    .borrow_mut()
                    .remove(&cqe.user_data);
            }
            Some(CompletionKind::TimerRemove)
            | Some(CompletionKind::NotifySend)
            | Some(CompletionKind::LinkedTimeout)
            | None => {}
        }
    }

    fn validate_opcode(&self, opcode: u8) -> io::Result<()> {
        if self.supported_ops.get().supports(opcode) {
            Ok(())
        } else {
            Err(IoUring::unsupported_opcode_error(opcode))
        }
    }

    #[cfg(test)]
    fn supported_ops(&self) -> SupportedOps {
        self.supported_ops.get()
    }

    fn next_token(&self, kind: CompletionKind) -> u64 {
        let seq = self.next_token.get();
        self.next_token.set(seq.wrapping_add(1));
        make_token(kind, seq)
    }

    fn quiesce_operations(&self) -> io::Result<()> {
        if self.completions.borrow().is_empty() {
            return Ok(());
        }
        let tokens = self
            .completions
            .borrow()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for token in tokens {
            let already_canceling = self
                .pending_cancel_tokens
                .borrow()
                .values()
                .any(|target| *target == token);
            if !already_canceling {
                self.stage_cancel_operation(token)?;
            }
        }

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let mut ready = ReadyEvents::default();
            let _ = self.flush_submissions(false, &mut ready)?;
            let drained = self
                .ring()
                .drain_completions(|cqe| self.process_cqe(cqe, &mut ready));
            if self.completions.borrow().is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out quiescing io_uring operations during driver shutdown",
                ));
            }
            if !drained {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    }

    fn leak_kernel_referenced_storage(&mut self) {
        let completions = std::mem::take(self.completions.get_mut());
        std::mem::forget(completions);
        if let Some(ring) = self.ring.get_mut().take() {
            std::mem::forget(ring);
        }
    }

    fn take_notifier_target(&self) -> Option<WakeTarget> {
        self.notifier.lock_target().take()
    }

    fn make_shutdown_storage_safe_on_unwind(&mut self) {
        self.shutting_down.set(true);
        self.notifier.closed.store(true, Ordering::Release);
        drop(self.take_notifier_target());
        self.leak_kernel_referenced_storage();
    }
}

struct DriverDropSafety {
    driver: *mut Driver,
    armed: bool,
}

impl DriverDropSafety {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DriverDropSafety {
    fn drop(&mut self) {
        if self.armed {
            // SAFETY: the guard is created as the first action in
            // `Driver::drop` and cannot outlive that exclusive `&mut Driver`.
            unsafe {
                (&mut *self.driver).make_shutdown_storage_safe_on_unwind();
            }
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let mut safety = DriverDropSafety {
            driver: self as *mut Driver,
            armed: true,
        };
        self.shutting_down.set(true);
        self.notifier.closed.store(true, Ordering::Release);
        drop(self.take_notifier_target());
        let shutdown_error = match self.quiesce_operations() {
            Ok(()) => {
                if let Some(ring) = self.ring.get_mut().take() {
                    drop(ring);
                }
                None
            }
            Err(error) => {
                self.leak_kernel_referenced_storage();
                Some(error)
            }
        };
        safety.disarm();

        if !std::thread::panicking() {
            if let Some(error) = shutdown_error {
                tracing::error!(
                    target: trace_targets::DRIVER,
                    event = "driver_shutdown_leak",
                    error = %error,
                    "io_uring shutdown could not prove buffer quiescence; leaking \
                     kernel-referenced storage"
                );
            } else {
                tracing::debug!(
                    target: trace_targets::DRIVER,
                    event = "drop_driver",
                    "dropped quiescent runtime driver"
                );
            }
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

/// Returns the current monotonic time used by the runtime timer system.
pub fn monotonic_now() -> io::Result<Duration> {
    let mut now = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: `now.as_mut_ptr()` points to writable, properly aligned
    // `timespec` storage for the duration of the syscall.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, now.as_mut_ptr()) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `clock_gettime` returned success, which initializes the
    // `timespec` value in `now`.
    let now = unsafe { now.assume_init() };
    Ok(Duration::new(now.tv_sec as u64, now.tv_nsec as u32))
}

fn make_token(kind: CompletionKind, seq: u64) -> u64 {
    ((kind as u64) << TOKEN_KIND_SHIFT) | (seq & !TOKEN_KIND_MASK)
}

fn decode_token_kind(token: u64) -> Option<CompletionKind> {
    match ((token & TOKEN_KIND_MASK) >> TOKEN_KIND_SHIFT) as u8 {
        1 => Some(CompletionKind::Timer),
        2 => Some(CompletionKind::TimerRemove),
        3 => Some(CompletionKind::NotifySend),
        4 => Some(CompletionKind::Operation),
        5 => Some(CompletionKind::OperationCancel),
        6 => Some(CompletionKind::LinkedTimeout),
        _ => None,
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::super::uring::{
        DEFAULT_RING_ENTRIES, IORING_OP_MSG_RING, IORING_OP_NOP, IORING_OP_TIMEOUT,
        IORING_OP_TIMEOUT_REMOVE, IoUring, IoUringCqe, ScriptedIoUringEnter, SupportedOps,
        override_supported_ops,
    };
    use super::{Notifier as _, ReadyEvents, WakeTarget, create_driver, monotonic_now};
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;
    use tracing::span::{Attributes, Id, Record};
    use tracing::subscriber::Interest;
    use tracing::{Event, Level, Metadata, Subscriber};

    struct PanickingSubscriber;

    impl Subscriber for PanickingSubscriber {
        fn register_callsite(&self, _metadata: &Metadata<'static>) -> Interest {
            Interest::always()
        }

        fn max_level_hint(&self) -> Option<tracing::metadata::LevelFilter> {
            Some(Level::TRACE.into())
        }

        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {
            panic!("injected tracing subscriber panic");
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    #[test]
    fn probe_runs_and_returns_bitmap() {
        let (driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        let ops = driver.supported_ops();

        assert!(
            ops.probe_unavailable() || (ops.probe_supported() && ops.supports(IORING_OP_NOP)),
            "probe should either report NOP support or mark probing unavailable"
        );
    }

    #[test]
    fn unsupported_op_returns_unsupported_error() {
        let _override = override_supported_ops(SupportedOps::only([IORING_OP_NOP]));
        let (driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");

        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_callback = Arc::clone(&completed);
        let token = driver
            .submit_operation(
                |sqe| {
                    sqe.opcode = IORING_OP_NOP;
                    sqe.fd = -1;
                },
                move |_| {
                    completed_for_callback.store(true, Ordering::Release);
                },
            )
            .expect("supported NOP should submit");
        assert_ne!(token, 0);

        for _ in 0..100 {
            let _ = driver.poll().expect("poll should succeed");
            if completed.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(completed.load(Ordering::Acquire));

        let error = driver
            .submit_operation(
                |sqe| {
                    sqe.opcode = 250;
                    sqe.fd = -1;
                },
                |_| {},
            )
            .expect_err("unsupported opcode should be rejected before submission");

        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("opcode 250"));
    }

    #[test]
    fn transient_submit_errors_are_retried_without_losing_the_batch() {
        for errno in [libc::EAGAIN, libc::EBUSY] {
            let _override =
                override_supported_ops(SupportedOps::only([IORING_OP_NOP, IORING_OP_MSG_RING]));
            let (mut driver, _notifier) =
                create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
            driver.defer_submissions = true;
            let script = ScriptedIoUringEnter::new([Err(errno), Ok(2)]);
            let replacement = IoUring::new_with_enter(8, Box::new(script.clone()))
                .expect("scripted ring should initialize");
            let old = driver.replace_ring_for_test(replacement);
            drop(old);

            let mut tokens = Vec::new();
            for _ in 0..2 {
                tokens.push(
                    driver
                        .submit_operation(
                            |sqe| {
                                sqe.opcode = IORING_OP_NOP;
                                sqe.fd = -1;
                            },
                            |_| {},
                        )
                        .expect("NOP should stage"),
                );
            }
            assert_eq!(driver.ring().pending_submission_count(), 2);
            assert!(
                driver
                    .poll()
                    .expect("transient submit should be retried")
                    .is_none()
            );
            assert_eq!(driver.ring().pending_submission_count(), 0);
            assert_eq!(
                script
                    .calls()
                    .into_iter()
                    .map(|call| call.to_submit)
                    .collect::<Vec<_>>(),
                vec![2, 2]
            );
            for token in tokens {
                driver.ring().inject_completion(IoUringCqe {
                    user_data: token,
                    res: 0,
                    flags: 0,
                });
            }
            let mut ready = ReadyEvents::default();
            driver
                .ring()
                .drain_completions(|cqe| driver.process_cqe(cqe, &mut ready));
        }
    }

    #[test]
    fn transient_wait_preserves_drained_timer_readiness() {
        let (mut driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        driver.defer_submissions = true;
        let script = ScriptedIoUringEnter::new([Err(libc::EAGAIN), Ok(1)]);
        let replacement = IoUring::new_with_enter(8, Box::new(script.clone()))
            .expect("scripted ring should initialize");
        let old = driver.replace_ring_for_test(replacement);
        drop(old);

        let deadline = monotonic_now().expect("clock should work") + Duration::from_secs(1);
        driver
            .rearm_timer(Some(deadline))
            .expect("timer should stage");
        let timer_token = driver
            .active_timer_token
            .get()
            .expect("timer token should be active");
        driver.ring().inject_completion(IoUringCqe {
            user_data: timer_token,
            res: -libc::ETIME,
            flags: 0,
        });

        driver
            .wait()
            .expect("wait should return after draining readiness");
        assert_eq!(script.calls().len(), 1);
        assert_eq!(driver.ring().pending_submission_count(), 1);
        assert_eq!(driver.pending_timers.get(), 1);

        let ready = driver
            .poll()
            .expect("poll should retry the staged submission")
            .expect("durable timer readiness should be surfaced");
        assert!(ready.timer);
        assert_eq!(script.calls().len(), 2);
        assert_eq!(driver.drain_timer(), Some(1));
    }

    #[test]
    fn shutdown_flushes_cancel_and_drains_original_terminal_cqe() {
        let (mut driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        driver.defer_submissions = true;
        let script = ScriptedIoUringEnter::new([Ok(2)]);
        let replacement = IoUring::new_with_enter(8, Box::new(script.clone()))
            .expect("scripted ring should initialize");
        let old = driver.replace_ring_for_test(replacement);
        drop(old);

        let completed = Arc::new(AtomicBool::new(false));
        let completed_callback = Arc::clone(&completed);
        let token = driver
            .submit_operation(
                |sqe| {
                    sqe.opcode = IORING_OP_NOP;
                    sqe.fd = -1;
                },
                move |_| completed_callback.store(true, Ordering::Release),
            )
            .expect("operation should stage");
        driver.ring().inject_completion(IoUringCqe {
            user_data: token,
            res: -libc::ECANCELED,
            flags: 0,
        });

        driver
            .quiesce_operations()
            .expect("shutdown should prove terminal completion");
        assert!(completed.load(Ordering::Acquire));
        assert!(
            driver.completions.borrow().is_empty(),
            "the terminal CQE must release the callback, and with it the buffer it owns"
        );
        assert_eq!(
            script
                .calls()
                .into_iter()
                .map(|call| call.to_submit)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn notifier_wakes_target_ring() {
        let (sender, _) =
            create_driver(DEFAULT_RING_ENTRIES).expect("sender driver should initialize");
        sender.bind_current_thread();

        let (target, notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("target driver should initialize");
        notifier.notify().expect("notify should succeed");

        let ready = loop {
            if let Some(ready) = target.poll().expect("poll should succeed") {
                break ready;
            }
            thread::sleep(Duration::from_millis(1));
        };

        assert!(ready.wake);
        assert!(!ready.timer);
        assert_eq!(target.drain_wake(), Some(1));
        sender.unbind_current_thread();
    }

    #[test]
    fn notifier_fd_closes_with_target_driver() {
        let (target, notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("target driver should initialize");
        let target_guard = notifier
            .inner
            .target
            .lock()
            .expect("wake target mutex poisoned");
        assert!(target_guard.is_some());
        drop(target_guard);

        // Teardown must close both the driver's fd and the notifier duplicate
        // before completion-owned buffers are released.
        drop(target);

        assert!(
            notifier
                .inner
                .target
                .lock()
                .expect("wake target mutex poisoned")
                .is_none()
        );

        // notify() short-circuits on the closed flag rather than submitting to
        // a stale/recycled fd.
        assert_eq!(
            notifier
                .notify()
                .expect_err("notify after drop should fail")
                .kind(),
            io::ErrorKind::BrokenPipe,
        );
    }

    #[test]
    fn teardown_is_safe_during_panicking_tracing_subscriber() {
        let (driver, notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::subscriber::with_default(PanickingSubscriber, move || {
                tracing::error!(
                    target: "runite::test",
                    event = "injected_subscriber_panic",
                    "triggering unwind through Driver::drop"
                );
                drop(driver);
            });
        }));
        assert!(result.is_err(), "subscriber panic should be observed");
        assert!(notifier.inner.lock_target().is_none());
        assert_eq!(
            notifier
                .notify()
                .expect_err("closed notifier should reject wakes")
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn teardown_recovers_poisoned_notifier_lock() {
        let (driver, notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        let inner = Arc::clone(&notifier.inner);
        assert!(
            thread::spawn(move || {
                let _target = inner.target.lock().expect("lock should start healthy");
                panic!("poison notifier target");
            })
            .join()
            .is_err()
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(driver)));
        assert!(result.is_ok(), "poison recovery must not panic in teardown");
        assert!(notifier.inner.lock_target().is_none());
        assert_eq!(
            notifier
                .notify()
                .expect_err("closed notifier should reject wakes")
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[test]
    fn notifier_wakes_target_ring_from_plain_thread() {
        let (target, notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("target driver should initialize");

        thread::spawn(move || {
            notifier.notify().expect("notify should succeed");
        })
        .join()
        .expect("notifier thread should exit cleanly");

        let ready = loop {
            if let Some(ready) = target.poll().expect("poll should succeed") {
                break ready;
            }
            thread::sleep(Duration::from_millis(1));
        };

        assert!(ready.wake);
        assert!(!ready.timer);
        assert_eq!(target.drain_wake(), Some(1));
    }

    #[test]
    fn capability_matrix_eventfd_fallback_wakes_target_ring() {
        let _override = override_supported_ops(SupportedOps::all_except([IORING_OP_MSG_RING]));
        let (target, notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("target driver should initialize");
        assert!(matches!(
            notifier
                .inner
                .target
                .lock()
                .expect("wake target mutex poisoned")
                .as_ref(),
            Some(WakeTarget::EventFd(_))
        ));

        thread::spawn(move || {
            notifier.notify().expect("first notify should succeed");
            notifier.notify().expect("second notify should coalesce");
        })
        .join()
        .expect("notifier thread should exit cleanly");

        target.wait().expect("eventfd poll should wake the ring");
        let ready = target
            .poll()
            .expect("poll should succeed")
            .expect("eventfd CQE should be ready");
        assert!(ready.wake);
        assert_eq!(target.drain_wake(), Some(2));
    }

    #[test]
    fn capability_matrix_eventfd_fallback_wakes_parked_runtime() {
        let _override = override_supported_ops(SupportedOps::all_except([IORING_OP_MSG_RING]));
        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_task = Arc::clone(&observed);

        crate::spawn(async move {
            let value = crate::task::spawn_blocking(|| 42)
                .expect("blocking work should queue")
                .await
                .expect("blocking work should complete");
            *observed_task.lock().expect("result mutex poisoned") = Some(value);
        });
        crate::run();

        assert_eq!(*observed.lock().expect("result mutex poisoned"), Some(42));
    }

    #[test]
    fn capability_matrix_eventfd_fallback_survives_worker_ring_rehome() {
        let _override = override_supported_ops(SupportedOps::all_except([IORING_OP_MSG_RING]));
        let observed = Arc::new(AtomicBool::new(false));
        let observed_worker = Arc::clone(&observed);

        let _worker = crate::spawn_worker(
            move || {
                crate::spawn(async move {
                    crate::task::spawn_blocking(|| ())
                        .expect("blocking work should queue")
                        .await
                        .expect("blocking work should complete");
                    observed_worker.store(true, Ordering::Release);
                });
            },
            || {},
        );
        crate::run();

        assert!(observed.load(Ordering::Acquire));
    }

    #[test]
    fn unchanged_timer_deadline_does_not_stage_an_update() {
        let (mut driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        driver.defer_submissions = true;
        let deadline = monotonic_now().expect("clock should work") + Duration::from_secs(1);

        driver
            .rearm_timer(Some(deadline))
            .expect("timer should arm");
        assert_eq!(driver.ring().pending_submission_count(), 1);
        driver
            .rearm_timer(Some(deadline))
            .expect("same deadline should be a no-op");
        assert_eq!(driver.ring().pending_submission_count(), 1);

        driver
            .rearm_timer(Some(deadline + Duration::from_millis(1)))
            .expect("changed deadline should stage remove plus rearm");
        assert_eq!(driver.ring().pending_submission_count(), 3);
        assert_eq!(
            driver.ring().pending_opcodes(),
            vec![
                IORING_OP_TIMEOUT,
                IORING_OP_TIMEOUT_REMOVE,
                IORING_OP_TIMEOUT
            ]
        );
    }

    #[test]
    fn fatal_timer_submission_error_is_propagated() {
        let (mut driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        driver.defer_submissions = true;
        let script = ScriptedIoUringEnter::new([Err(libc::EIO)]);
        let replacement =
            IoUring::new_with_enter(8, Box::new(script)).expect("scripted ring should initialize");
        let old = driver.replace_ring_for_test(replacement);
        drop(old);

        let deadline = monotonic_now().expect("clock should work") + Duration::from_secs(1);
        driver
            .rearm_timer(Some(deadline))
            .expect("timer should stage before the injected enter failure");
        let error = driver
            .poll()
            .expect_err("fatal timer submission must reach the runtime");
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(driver.active_timer_token.get(), None);
        assert_eq!(driver.active_timer_deadline.get(), None);
    }

    #[test]
    fn timeout_reports_deadlines() {
        let (driver, _notifier) =
            create_driver(DEFAULT_RING_ENTRIES).expect("driver should initialize");
        let deadline = monotonic_now().expect("clock should work") + Duration::from_millis(20);
        driver
            .rearm_timer(Some(deadline))
            .expect("timer should arm");

        let ready = loop {
            if let Some(ready) = driver.poll().expect("poll should succeed") {
                break ready;
            }
            thread::sleep(Duration::from_millis(5));
        };

        assert!(ready.timer);
        assert!(!ready.wake);
        assert_eq!(driver.drain_timer(), Some(1));
    }
}

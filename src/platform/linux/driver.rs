//! runite driver for Linux.
//!
//!

use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::uring::{IORING_OP_ASYNC_CANCEL, IoUring, IoUringCqe, IoUringSqe, SupportedOps};
use crate::platform::runtime_shared::{DriverBackend, Notifier};
use crate::trace_targets;

pub use crate::platform::runtime_shared::ReadyEvents;

const WAKE_TARGET_TOKEN: u64 = 1;
const TOKEN_KIND_SHIFT: u64 = 56;
const TOKEN_KIND_MASK: u64 = 0xff << TOKEN_KIND_SHIFT;

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
type CancelGuard = Box<dyn Any + Send + 'static>;

struct NotifierInner {
    ring_fd: RawFd,
    closed: AtomicBool,
}

impl NotifierInner {
    fn notify(&self) -> io::Result<()> {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::DRIVER,
            event = "notify",
            ring_fd = self.ring_fd,
            "sending cross-thread driver wake"
        );
        if self.closed.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "target runtime ring is closed",
            ));
        }

        IoUring::with_submitter(|ring| {
            ring.submit_msg_ring(
                self.ring_fd,
                WAKE_TARGET_TOKEN,
                1,
                make_token(CompletionKind::NotifySend, 0),
            )
        })
    }
}

#[derive(Clone)]
/// Cross-thread notifier for a runtime thread's driver.
pub struct ThreadNotifier {
    inner: Arc<NotifierInner>,
}

impl ThreadNotifier {
    // TODO(roadmap): duplicate of the `Notifier` trait method (which is what
    // callers use); a Linux agent will remove it before release. See ROADMAP.md.
    /// Sends a wake notification to the target runtime thread.
    #[allow(dead_code)]
    pub fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

impl Notifier for ThreadNotifier {
    fn notify(&self) -> io::Result<()> {
        self.inner.notify()
    }
}

/// Low-level Linux runtime driver backed by `io_uring`.
pub struct Driver {
    /// The `io_uring` instance driving this runtime thread.
    ring: IoUring,
    /// Process-wide io_uring opcode support snapshot used by this driver.
    supported_ops: SupportedOps,
    /// Shared notifier that other threads can use to wake this runtime thread.
    notifier: Arc<NotifierInner>,
    /// Next sequence number for generated completion tokens.
    next_token: Cell<u64>,
    /// The token of the currently active timer, if any timer is armed.
    active_timer_token: Cell<Option<u64>>,
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
    /// token and dropped when either the original CQE or cancel CQE arrives.
    pending_cancel_buffers: RefCell<HashMap<u64, Vec<CancelGuard>>>,
    pending_cancel_tokens: RefCell<HashMap<u64, u64>>,
}

/// Creates a new driver and its paired [`ThreadNotifier`].
pub fn create_driver() -> io::Result<(Driver, ThreadNotifier)> {
    let ring = IoUring::new(64)?;
    tracing::debug!(
        target: trace_targets::DRIVER,
        event = "create_driver",
        ring_fd = ring.ring_fd(),
        "created runtime driver"
    );
    let supported_ops = ring.supported_ops();
    let notifier = Arc::new(NotifierInner {
        ring_fd: ring.ring_fd(),
        closed: AtomicBool::new(false),
    });

    Ok((
        Driver {
            ring,
            supported_ops,
            notifier: Arc::clone(&notifier),
            next_token: Cell::new(1),
            active_timer_token: Cell::new(None),
            pending_wakes: Cell::new(0),
            pending_timers: Cell::new(0),
            completions: RefCell::new(HashMap::new()),
            pending_cancel_buffers: RefCell::new(HashMap::new()),
            pending_cancel_tokens: RefCell::new(HashMap::new()),
        },
        ThreadNotifier { inner: notifier },
    ))
}

impl Driver {
    pub(crate) fn bind_current_thread(&self) {
        self.ring.bind_current_thread();
    }

    pub(crate) fn unbind_current_thread(&self) {
        self.ring.unbind_current_thread();
    }

    /// Polls the driver without blocking.
    pub fn poll(&self) -> io::Result<Option<ReadyEvents>> {
        let mut ready = ReadyEvents::default();
        let saw_any = self
            .ring
            .drain_completions(|cqe| self.process_cqe(cqe, &mut ready));
        #[cfg(debug_assertions)]
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
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::DRIVER,
            event = "wait",
            "waiting for driver completion"
        );
        self.ring.wait_for_cqe()
    }

    /// Updates the currently armed timer deadline.
    ///
    /// Passing `None` removes any active timer.
    pub fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()> {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::TIMER,
            event = "rearm_timer",
            deadline_ns = deadline.map(|value| value.as_nanos() as u64),
            "rearming driver timer"
        );
        match (self.active_timer_token.get(), deadline) {
            (Some(active), Some(deadline)) => {
                self.ring.submit_timeout_update(active, deadline)?;
            }
            (Some(active), None) => {
                self.active_timer_token.set(None);
                self.ring
                    .submit_timeout_remove(active, self.next_token(CompletionKind::TimerRemove))?;
            }
            (None, Some(deadline)) => {
                let token = self.next_token(CompletionKind::Timer);
                self.active_timer_token.set(Some(token));
                self.ring.submit_timeout(token, deadline)?;
            }
            (None, None) => {}
        }

        Ok(())
    }

    pub(crate) fn submit_operation(
        &self,
        fill: impl FnOnce(&mut IoUringSqe),
        on_complete: impl FnOnce(IoUringCqe) + Send + 'static,
    ) -> io::Result<u64> {
        let mut prepared = IoUringSqe::default();
        fill(&mut prepared);
        self.validate_opcode(prepared.opcode)?;

        let token = self.next_token(CompletionKind::Operation);
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::ASYNC,
            event = "submit_operation",
            token,
            "submitting async driver operation"
        );
        self.completions
            .borrow_mut()
            .insert(token, Box::new(on_complete));

        if let Err(error) = self.ring.submit_with_token(token, |sqe| *sqe = prepared) {
            let _ = self.completions.borrow_mut().remove(&token);
            return Err(error);
        }

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
        let mut prepared = IoUringSqe::default();
        fill(&mut prepared);
        self.validate_opcode(prepared.opcode)?;

        let main_token = self.next_token(CompletionKind::Operation);
        let timeout_token = self.next_token(CompletionKind::LinkedTimeout);
        #[cfg(debug_assertions)]
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

        if let Err(error) = self.ring.submit_linked_with_timeout(
            main_token,
            |sqe| *sqe = prepared,
            timeout_token,
            timeout,
        ) {
            let mut completions = self.completions.borrow_mut();
            let _ = completions.remove(&main_token);
            return Err(error);
        }

        Ok(main_token)
    }

    pub(crate) fn cancel_operation(&self, token: u64) -> io::Result<()> {
        self.cancel_operation_with_guard(token, None)
    }

    pub(crate) fn cancel_operation_with_guard(
        &self,
        token: u64,
        guard: Option<CancelGuard>,
    ) -> io::Result<()> {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::ASYNC,
            event = "cancel_operation",
            token,
            "submitting async driver cancellation"
        );
        if let Some(guard) = guard {
            self.pending_cancel_buffers
                .borrow_mut()
                .entry(token)
                .or_default()
                .push(guard);
        }
        let cancel_token = self.next_token(CompletionKind::OperationCancel);
        self.pending_cancel_tokens
            .borrow_mut()
            .insert(cancel_token, token);
        match self.ring.submit_with_token(cancel_token, |sqe| {
            sqe.opcode = IORING_OP_ASYNC_CANCEL;
            sqe.fd = -1;
            sqe.addr = token;
        }) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.pending_cancel_tokens
                    .borrow_mut()
                    .remove(&cancel_token);
                let _ = self.pending_cancel_buffers.borrow_mut().remove(&token);
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
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::DRIVER,
            event = "process_cqe",
            user_data = cqe.user_data,
            result = cqe.res,
            "processing io_uring completion"
        );
        if cqe.user_data == WAKE_TARGET_TOKEN {
            ready.wake = true;
            let wakes = cqe.res.max(1) as u64;
            self.pending_wakes
                .set(self.pending_wakes.get().saturating_add(wakes));
            return;
        }

        match decode_token_kind(cqe.user_data) {
            Some(CompletionKind::Timer) => {
                if self.active_timer_token.get() == Some(cqe.user_data) {
                    self.active_timer_token.set(None);
                }
                if cqe.res == -libc::ETIME {
                    ready.timer = true;
                    self.pending_timers
                        .set(self.pending_timers.get().saturating_add(1));
                }
            }
            Some(CompletionKind::Operation) => {
                let _guards = self
                    .pending_cancel_buffers
                    .borrow_mut()
                    .remove(&cqe.user_data);
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
        if self.supported_ops.supports(opcode) {
            Ok(())
        } else {
            Err(IoUring::unsupported_opcode_error(opcode))
        }
    }

    #[cfg(test)]
    fn supported_ops(&self) -> SupportedOps {
        self.supported_ops
    }

    fn next_token(&self, kind: CompletionKind) -> u64 {
        let seq = self.next_token.get();
        self.next_token.set(seq.wrapping_add(1));
        make_token(kind, seq)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        tracing::debug!(
            target: trace_targets::DRIVER,
            event = "drop_driver",
            "dropping runtime driver"
        );
        self.notifier.closed.store(true, Ordering::Release);
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

#[cfg(test)]
mod tests {
    use super::super::uring::{IORING_OP_NOP, SupportedOps, override_supported_ops};
    use super::{create_driver, monotonic_now};
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn probe_runs_and_returns_bitmap() {
        let (driver, _notifier) = create_driver().expect("driver should initialize");
        let ops = driver.supported_ops();

        assert!(
            ops.probe_unavailable() || (ops.probe_supported() && ops.supports(IORING_OP_NOP)),
            "probe should either report NOP support or mark probing unavailable"
        );
    }

    #[test]
    fn unsupported_op_returns_unsupported_error() {
        let _override = override_supported_ops(SupportedOps::only([IORING_OP_NOP]));
        let (driver, _notifier) = create_driver().expect("driver should initialize");

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
    fn notifier_wakes_target_ring() {
        let (sender, _) = create_driver().expect("sender driver should initialize");
        sender.bind_current_thread();

        let (target, notifier) = create_driver().expect("target driver should initialize");
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
    fn notifier_wakes_target_ring_from_plain_thread() {
        let (target, notifier) = create_driver().expect("target driver should initialize");

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
    fn timeout_reports_deadlines() {
        let (driver, _notifier) = create_driver().expect("driver should initialize");
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

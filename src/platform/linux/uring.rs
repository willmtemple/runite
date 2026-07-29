use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
#[cfg(test)]
use std::marker::PhantomData;
use std::os::fd::{FromRawFd, OwnedFd, RawFd};
use std::ptr;
#[cfg(test)]
use std::rc::Rc;
#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::{Ordering, fence};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const IORING_OFF_SQ_RING: libc::off_t = 0;
const IORING_OFF_CQ_RING: libc::off_t = 0x0800_0000;
const IORING_OFF_SQES: libc::off_t = 0x1000_0000;

const IORING_ENTER_GETEVENTS: u32 = 1 << 0;
const IORING_REGISTER_PROBE: u32 = 8;
const IORING_SETUP_CLAMP: u32 = 1 << 4;
const IORING_SETUP_SUBMIT_ALL: u32 = 1 << 7;
const IORING_SETUP_COOP_TASKRUN: u32 = 1 << 8;
const IORING_SETUP_SINGLE_ISSUER: u32 = 1 << 12;
const IORING_SETUP_DEFER_TASKRUN: u32 = 1 << 13;

const IORING_FEAT_SINGLE_MMAP: u32 = 1 << 0;
/// Linux 5.5+ preserves overflowed CQEs for the next `io_uring_enter`.
const IORING_FEAT_NODROP: u32 = 1 << 1;
const IORING_FEAT_CQE_SKIP: u32 = 1 << 11;

#[cfg(test)]
pub(crate) const IORING_OP_NOP: u8 = 0;
pub(crate) const IORING_OP_FSYNC: u8 = 3;
pub(crate) const IORING_OP_POLL_ADD: u8 = 6;
pub(crate) const IORING_OP_SENDMSG: u8 = 9;
pub(crate) const IORING_OP_RECVMSG: u8 = 10;
pub(crate) const IORING_OP_TIMEOUT: u8 = 11;
pub(crate) const IORING_OP_LINK_TIMEOUT: u8 = 15;
pub(crate) const IORING_OP_TIMEOUT_REMOVE: u8 = 12;
pub(crate) const IORING_OP_ACCEPT: u8 = 13;
pub(crate) const IORING_OP_ASYNC_CANCEL: u8 = 14;
pub(crate) const IORING_OP_CONNECT: u8 = 16;
pub(crate) const IORING_OP_OPENAT: u8 = 18;
pub(crate) const IORING_OP_STATX: u8 = 21;
pub(crate) const IORING_OP_READ: u8 = 22;
pub(crate) const IORING_OP_WRITE: u8 = 23;
pub(crate) const IORING_OP_SEND: u8 = 26;
pub(crate) const IORING_OP_RECV: u8 = 27;
pub(crate) const IORING_OP_SHUTDOWN: u8 = 34;
pub(crate) const IORING_OP_RENAMEAT: u8 = 35;
pub(crate) const IORING_OP_UNLINKAT: u8 = 36;
pub(crate) const IORING_OP_MKDIRAT: u8 = 37;
pub(crate) const IORING_OP_MSG_RING: u8 = 40;
pub(crate) const IORING_OP_SOCKET: u8 = 45;
pub(crate) const IORING_OP_FTRUNCATE: u8 = 55;
pub(crate) const IORING_OP_BIND: u8 = 56;
pub(crate) const IORING_OP_LISTEN: u8 = 57;

const IORING_MSG_DATA: u64 = 0;
const IORING_OP_SUPPORTED: u16 = 1 << 0;
pub(crate) const IORING_FSYNC_DATASYNC: u32 = 1 << 0;
pub(crate) const IORING_TIMEOUT_ABS: u32 = 1 << 0;
pub(crate) const IOSQE_IO_LINK: u8 = 1 << 2;
pub(crate) const IOSQE_CQE_SKIP_SUCCESS: u8 = 1 << 6;

thread_local! {
    static CURRENT_SUBMITTER: Cell<*const IoUring> = const { Cell::new(ptr::null()) };
}

static GLOBAL_SUBMITTER: OnceLock<Mutex<Option<IoUring>>> = OnceLock::new();
static SUPPORTED_OPS: OnceLock<SupportedOps> = OnceLock::new();
static SINGLE_ISSUER_SETUP_FLAGS: OnceLock<u32> = OnceLock::new();
static SHARED_SETUP_FLAGS: OnceLock<u32> = OnceLock::new();

#[cfg(test)]
thread_local! {
    static TEST_SUPPORTED_OPS_OVERRIDE: Cell<Option<SupportedOps>> = const { Cell::new(None) };
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoSqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    user_addr: u64,
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoCqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    user_addr: u64,
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: IoSqringOffsets,
    cq_off: IoCqringOffsets,
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub(crate) struct IoUringSqe {
    pub(crate) opcode: u8,
    pub(crate) flags: u8,
    pub(crate) ioprio: u16,
    pub(crate) fd: i32,
    pub(crate) off: u64,
    pub(crate) addr: u64,
    pub(crate) len: u32,
    pub(crate) op_flags: u32,
    pub(crate) user_data: u64,
    pub(crate) buf_index: u16,
    pub(crate) personality: u16,
    pub(crate) file_index: i32,
    pub(crate) pad2: [u64; 2],
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub(crate) struct IoUringCqe {
    pub(crate) user_data: u64,
    pub(crate) res: i32,
    pub(crate) flags: u32,
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoUringProbeOp {
    op: u8,
    resv: u8,
    flags: u16,
    resv2: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IoUringProbe {
    last_op: u8,
    ops_len: u8,
    resv: u16,
    resv2: [u32; 3],
    ops: [IoUringProbeOp; 256],
}

impl Default for IoUringProbe {
    fn default() -> Self {
        Self {
            last_op: 0,
            ops_len: 0,
            resv: 0,
            resv2: [0; 3],
            ops: [IoUringProbeOp::default(); 256],
        }
    }
}

/// Process-wide, cached io_uring operation support for the running kernel.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SupportedOps {
    ops: [bool; 256],
    probe_supported: bool,
}

impl SupportedOps {
    pub(crate) fn supports(self, opcode: u8) -> bool {
        !self.probe_supported || self.ops[opcode as usize]
    }

    #[cfg(test)]
    pub(crate) fn probe_supported(self) -> bool {
        self.probe_supported
    }

    #[cfg(test)]
    pub(crate) fn probe_unavailable(self) -> bool {
        !self.probe_supported
    }

    fn from_probe(probe: &IoUringProbe) -> Self {
        let mut ops = [false; 256];
        let count = probe.ops_len as usize;
        for probe_op in probe.ops.iter().take(count.min(probe.ops.len())) {
            if probe_op.flags & IORING_OP_SUPPORTED != 0 {
                ops[probe_op.op as usize] = true;
            }
        }
        Self {
            ops,
            probe_supported: true,
        }
    }

    fn permissive_after_probe_failure() -> Self {
        Self {
            ops: [true; 256],
            probe_supported: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn only(opcodes: impl IntoIterator<Item = u8>) -> Self {
        let mut ops = [false; 256];
        for opcode in opcodes {
            ops[opcode as usize] = true;
        }
        Self {
            ops,
            probe_supported: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn all_except(opcodes: impl IntoIterator<Item = u8>) -> Self {
        let mut ops = [true; 256];
        for opcode in opcodes {
            ops[opcode as usize] = false;
        }
        Self {
            ops,
            probe_supported: true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct UnsupportedIoUringOpcode {
    opcode: u8,
}

impl UnsupportedIoUringOpcode {
    fn new(opcode: u8) -> Self {
        Self { opcode }
    }
}

impl fmt::Display for UnsupportedIoUringOpcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "io_uring opcode {} is not supported by this kernel",
            self.opcode
        )
    }
}

impl std::error::Error for UnsupportedIoUringOpcode {}

pub(crate) fn is_unsupported_operation(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Unsupported
        || matches!(
            error.raw_os_error(),
            Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
        )
}

#[derive(Debug)]
pub(crate) struct IoUringUnavailable {
    message: std::borrow::Cow<'static, str>,
}

impl IoUringUnavailable {
    fn kernel_too_old_or_disabled() -> Self {
        Self {
            message: std::borrow::Cow::Borrowed(
                "io_uring is not available on this kernel (CONFIG_IO_URING not enabled or kernel too old)",
            ),
        }
    }

    fn blocked_by_seccomp() -> Self {
        Self {
            message: std::borrow::Cow::Borrowed(
                "io_uring is not available because io_uring_setup was blocked (likely by seccomp)",
            ),
        }
    }

    /// `ENOMEM` from `io_uring_setup` almost never means the machine is out of
    /// memory. The rings are pinned against `RLIMIT_MEMLOCK`, and anything else
    /// in the process charged to the same budget — a profiler's sample buffers
    /// are the common case — can exhaust it on a machine with tens of gigabytes
    /// free. The raw errno renders as "Cannot allocate memory", which sends the
    /// reader to look at free RAM, which is the wrong place entirely.
    fn locked_memory_exhausted() -> Self {
        let limit = match locked_memory_limit() {
            Some(u64::MAX) => "RLIMIT_MEMLOCK is unlimited, so the limit is elsewhere".to_string(),
            Some(bytes) => format!("RLIMIT_MEMLOCK is {} KiB", bytes / 1024),
            None => "RLIMIT_MEMLOCK could not be read".to_string(),
        };
        Self {
            message: std::borrow::Cow::Owned(format!(
                "could not initialize the io_uring driver: locked-memory limit reached ({limit}). \
                 Another tool in this process may hold part of it; profilers charge their sample \
                 buffers to the same budget, so `perf record` with default settings is a common \
                 cause and `perf record -m 32` leaves room. Raise the limit, or reduce the \
                 requested ring size."
            )),
        }
    }
}

/// Reads the soft `RLIMIT_MEMLOCK` in bytes, or `None` if it cannot be read.
fn locked_memory_limit() -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is valid writable storage for one `rlimit`, and
    // `RLIMIT_MEMLOCK` is a valid resource identifier.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
    (result == 0).then_some(limit.rlim_cur)
}

impl fmt::Display for IoUringUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for IoUringUnavailable {}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct KernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

struct OwnedSqe {
    sqe: IoUringSqe,
    _timespec: Option<Box<KernelTimespec>>,
    _fd: Option<OwnedFd>,
    fd_until_completion: bool,
}

struct SubmissionBatch {
    sqes: Vec<OwnedSqe>,
}

#[derive(Clone, Copy)]
enum SetupProfile {
    SingleIssuer,
    Shared,
}

pub(crate) struct SubmitOutcome {
    pub(crate) submitted: u32,
    pub(crate) failures: Vec<IoUringCqe>,
    pub(crate) error: Option<io::Error>,
    pub(crate) retry: bool,
}

impl SubmitOutcome {
    fn complete(submitted: u32) -> Self {
        Self {
            submitted,
            failures: Vec::new(),
            error: None,
            retry: false,
        }
    }
}

pub(crate) struct IoUring {
    ring_fd: RawFd,
    supported_ops: SupportedOps,
    setup_flags: u32,
    cqe_skip: bool,
    creator: std::thread::ThreadId,
    sq_ring_ptr: *mut u8,
    cq_ring_ptr: *mut u8,
    sqes_ptr: *mut IoUringSqe,
    sq_ring_size: usize,
    cq_ring_size: usize,
    sqes_size: usize,
    single_mmap: bool,
    sq_head: *mut u32,
    sq_tail: *mut u32,
    sq_ring_mask: *mut u32,
    sq_ring_entries: *mut u32,
    sq_array: *mut u32,
    cq_head: *mut u32,
    cq_tail: *mut u32,
    cq_ring_mask: *mut u32,
    cq_overflow: *mut u32,
    cqes: *mut IoUringCqe,
    /// Whether overflowed CQEs are preserved for the next enter.
    nodrop: bool,
    /// Highest CQ-overflow count observed so far. Used to warn at most once per
    /// new overflow event rather than on every drain.
    overflow_seen: Cell<u32>,
    /// Turn-local owned batches; short submits leave the unaccepted suffix.
    pending: RefCell<VecDeque<SubmissionBatch>>,
    inflight_fds: RefCell<HashMap<u64, Vec<OwnedFd>>>,
    submission_poisoned: Cell<bool>,
    enter: Box<dyn IoUringEnter>,
}

pub(crate) trait IoUringEnter: Send + 'static {
    fn enter(
        &self,
        ring_fd: RawFd,
        to_submit: u32,
        min_complete: u32,
        flags: u32,
    ) -> io::Result<u32>;

    #[cfg(test)]
    fn advances_sq_head(&self) -> bool {
        false
    }
}

struct SystemIoUringEnter;

impl IoUringEnter for SystemIoUringEnter {
    fn enter(
        &self,
        ring_fd: RawFd,
        to_submit: u32,
        min_complete: u32,
        flags: u32,
    ) -> io::Result<u32> {
        // SAFETY: `ring_fd` is an open io_uring descriptor. The final sigset
        // pointer is null with size 0, so the kernel reads no user memory
        // beyond the by-value syscall arguments.
        cvt_long(unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                ring_fd,
                to_submit as libc::c_uint,
                min_complete as libc::c_uint,
                flags as libc::c_uint,
                ptr::null::<libc::c_void>(),
                0usize,
            )
        })
        .map(|value| value as u32)
    }

    #[cfg(test)]
    fn advances_sq_head(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IoUringEnterCall {
    pub(crate) ring_fd: RawFd,
    pub(crate) to_submit: u32,
    pub(crate) min_complete: u32,
    pub(crate) flags: u32,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ScriptedIoUringEnter {
    inner: Arc<Mutex<ScriptedIoUringEnterState>>,
}

#[cfg(test)]
struct ScriptedIoUringEnterState {
    results: VecDeque<Result<u32, i32>>,
    calls: Vec<IoUringEnterCall>,
}

#[cfg(test)]
impl ScriptedIoUringEnter {
    pub(crate) fn new(results: impl IntoIterator<Item = Result<u32, i32>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ScriptedIoUringEnterState {
                results: results.into_iter().collect(),
                calls: Vec::new(),
            })),
        }
    }

    pub(crate) fn calls(&self) -> Vec<IoUringEnterCall> {
        self.inner
            .lock()
            .expect("io_uring enter script poisoned")
            .calls
            .clone()
    }
}

#[cfg(test)]
impl IoUringEnter for ScriptedIoUringEnter {
    fn enter(
        &self,
        ring_fd: RawFd,
        to_submit: u32,
        min_complete: u32,
        flags: u32,
    ) -> io::Result<u32> {
        let mut state = self.inner.lock().expect("io_uring enter script poisoned");
        state.calls.push(IoUringEnterCall {
            ring_fd,
            to_submit,
            min_complete,
            flags,
        });
        match state
            .results
            .pop_front()
            .expect("io_uring enter script exhausted")
        {
            Ok(submitted) => Ok(submitted),
            Err(errno) => Err(io::Error::from_raw_os_error(errno)),
        }
    }

    fn advances_sq_head(&self) -> bool {
        false
    }
}

impl IoUring {
    pub(crate) fn new(entries: u32) -> io::Result<Self> {
        Self::new_with_profile(
            entries,
            Box::new(SystemIoUringEnter),
            SetupProfile::SingleIssuer,
        )
    }

    fn new_shared(entries: u32) -> io::Result<Self> {
        Self::new_with_profile(entries, Box::new(SystemIoUringEnter), SetupProfile::Shared)
    }

    #[cfg(test)]
    pub(crate) fn new_with_enter(entries: u32, enter: Box<dyn IoUringEnter>) -> io::Result<Self> {
        Self::new_with_profile(entries, enter, SetupProfile::SingleIssuer)
    }

    fn new_with_profile(
        entries: u32,
        enter: Box<dyn IoUringEnter>,
        profile: SetupProfile,
    ) -> io::Result<Self> {
        let (ring_fd, params) = setup_ring(entries, profile)?;
        let supported_ops = supported_ops_for_ring(ring_fd);

        let nodrop = params.features & IORING_FEAT_NODROP != 0;
        if !nodrop {
            tracing::warn!(
                target: "runite::driver",
                event = "cq_nodrop_unsupported",
                "this kernel lacks IORING_FEAT_NODROP (needs Linux 5.5+); completions may \
                 be dropped on completion-queue overflow",
            );
        }

        let sq_ring_size =
            params.sq_off.array as usize + params.sq_entries as usize * std::mem::size_of::<u32>();
        let cq_ring_size = params.cq_off.cqes as usize
            + params.cq_entries as usize * std::mem::size_of::<IoUringCqe>();
        let single_mmap = params.features & IORING_FEAT_SINGLE_MMAP != 0;

        let sq_ring_ptr = mmap_ring(
            if single_mmap {
                sq_ring_size.max(cq_ring_size)
            } else {
                sq_ring_size
            },
            ring_fd,
            IORING_OFF_SQ_RING,
        )?;
        let cq_ring_ptr = if single_mmap {
            sq_ring_ptr
        } else {
            mmap_ring(cq_ring_size, ring_fd, IORING_OFF_CQ_RING)?
        };
        let sqes_size = params.sq_entries as usize * std::mem::size_of::<IoUringSqe>();
        let sqes_ptr = mmap_ring(sqes_size, ring_fd, IORING_OFF_SQES)? as *mut IoUringSqe;

        Ok(Self {
            ring_fd,
            supported_ops,
            setup_flags: params.flags,
            cqe_skip: params.features & IORING_FEAT_CQE_SKIP != 0,
            creator: std::thread::current().id(),
            sq_ring_ptr,
            cq_ring_ptr,
            sqes_ptr,
            sq_ring_size,
            cq_ring_size,
            sqes_size,
            single_mmap,
            sq_head: offset_ptr(sq_ring_ptr, params.sq_off.head),
            sq_tail: offset_ptr(sq_ring_ptr, params.sq_off.tail),
            sq_ring_mask: offset_ptr(sq_ring_ptr, params.sq_off.ring_mask),
            sq_ring_entries: offset_ptr(sq_ring_ptr, params.sq_off.ring_entries),
            sq_array: offset_ptr(sq_ring_ptr, params.sq_off.array),
            cq_head: offset_ptr(cq_ring_ptr, params.cq_off.head),
            cq_tail: offset_ptr(cq_ring_ptr, params.cq_off.tail),
            cq_ring_mask: offset_ptr(cq_ring_ptr, params.cq_off.ring_mask),
            cq_overflow: offset_ptr(cq_ring_ptr, params.cq_off.overflow),
            cqes: offset_ptr(cq_ring_ptr, params.cq_off.cqes),
            nodrop,
            overflow_seen: Cell::new(0),
            pending: RefCell::new(VecDeque::new()),
            inflight_fds: RefCell::new(HashMap::new()),
            submission_poisoned: Cell::new(false),
            enter,
        })
    }

    pub(crate) fn ring_fd(&self) -> RawFd {
        self.ring_fd
    }

    pub(crate) fn supported_ops(&self) -> SupportedOps {
        self.supported_ops
    }

    pub(crate) fn supports_submit_all(&self) -> bool {
        self.setup_flags & IORING_SETUP_SUBMIT_ALL != 0
    }

    pub(crate) fn was_created_on_current_thread(&self) -> bool {
        self.creator == std::thread::current().id()
    }

    pub(crate) fn unsupported_opcode_error(opcode: u8) -> io::Error {
        io::Error::new(
            io::ErrorKind::Unsupported,
            UnsupportedIoUringOpcode::new(opcode),
        )
    }

    pub(crate) fn bind_current_thread(&self) {
        CURRENT_SUBMITTER.with(|submitter| submitter.set(self as *const Self));
    }

    pub(crate) fn unbind_current_thread(&self) {
        CURRENT_SUBMITTER.with(|submitter| {
            if ptr::eq(submitter.get(), self) {
                submitter.set(ptr::null());
            }
        });
    }

    pub(crate) fn with_submitter<T>(f: impl FnOnce(&IoUring) -> io::Result<T>) -> io::Result<T> {
        CURRENT_SUBMITTER.with(|submitter| {
            let ptr = submitter.get();
            if !ptr.is_null() {
                // SAFETY: `bind_current_thread` stores a pointer to an
                // `IoUring` owned by the installed driver for this thread, and
                // `unbind_current_thread` clears it before the driver drops.
                // The reference is used only for the duration of this call.
                let ring = unsafe { &*ptr };
                // Never mix a notification with the driver's deferred user
                // batch: this immediate path has no access to Driver's
                // completion table if submission fails. Fall back to the
                // process-wide notification ring whenever user SQEs are staged.
                if !ring.has_pending_submissions() {
                    return f(ring).and_then(|value| {
                        ring.flush_immediate()?;
                        Ok(value)
                    });
                }
            }

            let mut ring = global_submitter()
                .lock()
                .expect("global io_uring submitter should not be poisoned");
            if ring.is_none() {
                *ring = Some(IoUring::new_shared(64)?);
            }

            let ring = ring
                .as_ref()
                .expect("global submitter ring should initialize");
            let result = f(ring).and_then(|value| {
                ring.flush_immediate()?;
                Ok(value)
            });

            // Nothing polls the global fallback ring's completion queue, so
            // drain it here to keep it from overflowing over the process
            // lifetime. Only failures land here — every op submitted through the
            // fallback (currently just MSG_RING wakes) uses CQE_SKIP_SUCCESS — so
            // a CQE means a cross-thread wake failed; surface it.
            ring.drain_completions(|cqe| {
                if cqe.res < 0 {
                    tracing::warn!(
                        target: "runite::driver",
                        event = "fallback_submitter_op_failed",
                        errno = -cqe.res,
                        "an io_uring op on the global fallback submitter failed \
                         (likely a cross-thread MSG_RING wake to a closed ring)",
                    );
                }
            });

            result
        })
    }

    pub(crate) fn submit_timeout(&self, token: u64, deadline: Duration) -> io::Result<()> {
        self.push_sqe_with_timespec(deadline, |sqe, timespec| {
            sqe.opcode = IORING_OP_TIMEOUT;
            sqe.fd = -1;
            sqe.off = 0;
            sqe.user_data = token;
            sqe.addr = timespec as u64;
            sqe.len = 1;
            sqe.op_flags = IORING_TIMEOUT_ABS;
        })
    }

    pub(crate) fn submit_timeout_remove(
        &self,
        token_to_remove: u64,
        completion: u64,
    ) -> io::Result<()> {
        let skip_success = self.cqe_skip;
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_TIMEOUT_REMOVE;
            sqe.fd = -1;
            if skip_success {
                sqe.flags = IOSQE_CQE_SKIP_SUCCESS;
            }
            sqe.user_data = completion;
            sqe.addr = token_to_remove;
        })
    }

    pub(crate) fn submit_msg_ring(
        &self,
        target_ring_fd: RawFd,
        target_user_data: u64,
        value: u32,
        completion: u64,
    ) -> io::Result<()> {
        let skip_success = self.cqe_skip;
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_MSG_RING;
            if skip_success {
                sqe.flags = IOSQE_CQE_SKIP_SUCCESS;
            }
            sqe.fd = target_ring_fd;
            sqe.off = target_user_data;
            sqe.addr = IORING_MSG_DATA;
            sqe.len = value;
            sqe.user_data = completion;
        })
    }

    pub(crate) fn submit_with_token(
        &self,
        token: u64,
        fill: impl FnOnce(&mut IoUringSqe),
    ) -> io::Result<()> {
        self.push_sqe(|sqe| {
            fill(sqe);
            sqe.user_data = token;
        })
    }

    /// Submits a main SQE atomically with an `IORING_OP_LINK_TIMEOUT` SQE.
    pub(crate) fn submit_linked_with_timeout(
        &self,
        main_token: u64,
        fill: impl FnOnce(&mut IoUringSqe),
        timeout_token: u64,
        timeout: Duration,
    ) -> io::Result<()> {
        let main = self.prepare_sqe(None, |sqe, _| {
            fill(sqe);
            sqe.flags |= IOSQE_IO_LINK;
            sqe.user_data = main_token;
        })?;
        let timespec = Box::new(duration_to_kernel_timespec(timeout));
        let timespec_ptr = (&*timespec) as *const KernelTimespec;
        let timeout = self.prepare_sqe(Some(timespec), |sqe, _| {
            sqe.opcode = IORING_OP_LINK_TIMEOUT;
            sqe.fd = -1;
            sqe.addr = timespec_ptr as u64;
            sqe.len = 1;
            sqe.user_data = timeout_token;
        })?;
        self.pending.borrow_mut().push_back(SubmissionBatch {
            sqes: vec![main, timeout],
        });
        Ok(())
    }

    pub(crate) fn drain_completions(&self, mut f: impl FnMut(IoUringCqe)) -> bool {
        let mut head = load_u32(self.cq_head);
        let tail = load_u32(self.cq_tail);
        if head == tail {
            return false;
        }
        let mask = load_u32(self.cq_ring_mask);

        while head != tail {
            let index = (head & mask) as usize;
            // SAFETY: `cqes` points into the kernel-created CQE array mapping.
            // The ring mask comes from the same mapping and confines `index` to
            // the CQ ring entry range; `head != tail` means this slot has been
            // published by the kernel and can be copied out with a volatile read.
            let cqe = unsafe { ptr::read_volatile(self.cqes.add(index)) };
            let _fd_guards = self.inflight_fds.borrow_mut().remove(&cqe.user_data);
            f(cqe);
            head = head.wrapping_add(1);
        }

        store_u32(self.cq_head, head);
        self.check_cq_overflow();
        true
    }

    /// Reports each newly observed completion-queue overflow once.
    fn check_cq_overflow(&self) {
        // SAFETY: `cq_overflow` points into the CQ ring mapping at the
        // kernel-provided overflow offset; a volatile read observes the kernel's
        // latest store.
        let overflow = unsafe { ptr::read_volatile(self.cq_overflow) };
        if overflow > self.overflow_seen.get() {
            self.overflow_seen.set(overflow);
            // `rings->cq_overflow` is only incremented by the kernel's
            // `io_account_cq_overflow`, which runs on the path where allocating
            // the overflow CQE failed. Completions preserved by FEAT_NODROP go
            // onto the backlog list and never touch this counter, so any
            // increase here means completions were genuinely lost, whether or
            // not the kernel advertises FEAT_NODROP.
            let cause = if self.nodrop {
                "the kernel could not allocate overflow entries"
            } else {
                "this kernel lacks FEAT_NODROP"
            };
            tracing::error!(
                target: "runite::driver",
                event = "cq_overflow_dropped",
                overflow,
                nodrop = self.nodrop,
                "io_uring dropped {overflow} completion(s) because {cause}; the operations \
                 they belonged to cannot complete and their futures will hang",
            );
        }
    }

    fn push_sqe(&self, fill: impl FnOnce(&mut IoUringSqe)) -> io::Result<()> {
        let owned = self.prepare_sqe(None, |sqe, _| fill(sqe))?;
        self.pending
            .borrow_mut()
            .push_back(SubmissionBatch { sqes: vec![owned] });
        Ok(())
    }

    fn push_sqe_with_timespec(
        &self,
        duration: Duration,
        fill: impl FnOnce(&mut IoUringSqe, *const KernelTimespec),
    ) -> io::Result<()> {
        let timespec = Box::new(duration_to_kernel_timespec(duration));
        let timespec_ptr = (&*timespec) as *const KernelTimespec;
        let owned = self.prepare_sqe(Some(timespec), |sqe, _| fill(sqe, timespec_ptr))?;
        self.pending
            .borrow_mut()
            .push_back(SubmissionBatch { sqes: vec![owned] });
        Ok(())
    }

    fn prepare_sqe(
        &self,
        timespec: Option<Box<KernelTimespec>>,
        fill: impl FnOnce(&mut IoUringSqe, Option<*const KernelTimespec>),
    ) -> io::Result<OwnedSqe> {
        let timespec_ptr = timespec
            .as_deref()
            .map(|value| value as *const KernelTimespec);
        let mut sqe = IoUringSqe::default();
        fill(&mut sqe, timespec_ptr);
        let (fd, fd_until_completion) = duplicate_sqe_fd(&mut sqe)?;
        Ok(OwnedSqe {
            sqe,
            _timespec: timespec,
            _fd: fd,
            fd_until_completion,
        })
    }

    pub(crate) fn has_pending_submissions(&self) -> bool {
        !self.pending.borrow().is_empty()
    }

    fn flush_immediate(&self) -> io::Result<()> {
        while self.has_pending_submissions() {
            let outcome = self.submit_pending(false)?;
            if let Some(error) = outcome.error {
                return Err(error);
            }
            if outcome.submitted == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "io_uring accepted none of the pending submissions",
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_submission_count(&self) -> usize {
        self.pending
            .borrow()
            .iter()
            .map(|batch| batch.sqes.len())
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn pending_opcodes(&self) -> Vec<u8> {
        self.pending
            .borrow()
            .iter()
            .flat_map(|batch| batch.sqes.iter().map(|owned| owned.sqe.opcode))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn inject_completion(&self, cqe: IoUringCqe) {
        let tail = load_u32(self.cq_tail);
        let index = (tail & load_u32(self.cq_ring_mask)) as usize;
        // SAFETY: tests inject at the current free CQ tail in the live mapping.
        unsafe {
            ptr::write_volatile(self.cqes.add(index), cqe);
        }
        store_u32(self.cq_tail, tail.wrapping_add(1));
    }

    pub(crate) fn submit_pending(&self, wait_for_cqe: bool) -> io::Result<SubmitOutcome> {
        if self.submission_poisoned.get() {
            return Err(io::Error::other(
                "io_uring submission state is poisoned after a partial atomic batch",
            ));
        }
        let head = load_u32(self.sq_head);
        let tail = load_u32(self.sq_tail);
        let already_published = tail.wrapping_sub(head);
        if already_published != 0 {
            return Err(io::Error::other(
                "io_uring retained an unexpected published SQE prefix",
            ));
        }
        let entries = load_u32(self.sq_ring_entries);
        let available = entries.saturating_sub(already_published);
        let (batch_count, to_publish, all_pending_selected) = {
            let pending = self.pending.borrow();
            let mut batch_count = 0usize;
            let mut sqe_count = 0u32;
            for batch in pending.iter() {
                let batch_len = u32::try_from(batch.sqes.len()).unwrap_or(u32::MAX);
                if batch_len > entries {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "an io_uring submission batch exceeds ring capacity",
                    ));
                }
                if sqe_count.saturating_add(batch_len) > available {
                    break;
                }
                sqe_count += batch_len;
                batch_count += 1;
            }
            (batch_count, sqe_count, batch_count == pending.len())
        };

        if to_publish == 0 {
            if !wait_for_cqe && self.setup_flags & IORING_SETUP_DEFER_TASKRUN == 0 {
                return Ok(SubmitOutcome::complete(0));
            }
            return self.enter_without_submission(u32::from(wait_for_cqe));
        }

        let mask = load_u32(self.sq_ring_mask);
        {
            let pending = self.pending.borrow();
            let mut offset = 0u32;
            for batch in pending.iter().take(batch_count) {
                for owned in &batch.sqes {
                    let position = tail.wrapping_add(offset);
                    let index = (position & mask) as usize;
                    // SAFETY: `index` is confined by the kernel-provided ring
                    // mask, and complete batches are selected only while they
                    // fit in the free SQ capacity.
                    unsafe {
                        ptr::write_volatile(self.sqes_ptr.add(index), owned.sqe);
                        ptr::write_volatile(self.sq_array.add(index), index as u32);
                    }
                    offset += 1;
                }
            }
        }
        fence(Ordering::Release);
        store_u32(self.sq_tail, tail.wrapping_add(to_publish));

        let combine_wait = wait_for_cqe && all_pending_selected && self.supports_submit_all();
        let min_complete = u32::from(combine_wait);
        loop {
            match self.enter(to_publish, min_complete, IORING_ENTER_GETEVENTS) {
                Ok(submitted) => {
                    if submitted > to_publish {
                        store_u32(self.sq_tail, head);
                        self.submission_poisoned.set(true);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "io_uring_enter reported {submitted} submissions for \
                                 {to_publish} published SQEs"
                            ),
                        ));
                    }

                    let accepted_tail = head.wrapping_add(submitted);
                    if submitted != to_publish {
                        store_u32(self.sq_tail, accepted_tail);
                    }
                    let (accepted_batches, split_batch) = {
                        let pending = self.pending.borrow();
                        let mut boundary = 0u32;
                        let mut accepted_batches = 0usize;
                        let mut split_batch = false;
                        for batch in pending.iter().take(batch_count) {
                            let next = boundary + batch.sqes.len() as u32;
                            if submitted >= next {
                                boundary = next;
                                accepted_batches += 1;
                            } else {
                                split_batch = submitted > boundary;
                                break;
                            }
                        }
                        (accepted_batches, split_batch)
                    };
                    if split_batch {
                        self.submission_poisoned.set(true);
                        return Err(io::Error::other(
                            "kernel split an atomic linked io_uring submission batch",
                        ));
                    }
                    let mut pending = self.pending.borrow_mut();
                    for _ in 0..accepted_batches {
                        if let Some(batch) = pending.pop_front() {
                            for mut owned in batch.sqes {
                                if owned.fd_until_completion
                                    && let Some(fd) = owned._fd.take()
                                {
                                    self.inflight_fds
                                        .borrow_mut()
                                        .entry(owned.sqe.user_data)
                                        .or_default()
                                        .push(fd);
                                }
                            }
                        }
                    }
                    return Ok(SubmitOutcome::complete(submitted));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if matches!(error.raw_os_error(), Some(libc::EBUSY | libc::EAGAIN)) => {
                    store_u32(self.sq_tail, head);
                    return Ok(SubmitOutcome {
                        submitted: 0,
                        failures: Vec::new(),
                        error: Some(error),
                        retry: true,
                    });
                }
                Err(error) => {
                    store_u32(self.sq_tail, head);
                    let errno = error.raw_os_error().unwrap_or(libc::EIO);
                    let failures = self
                        .pending
                        .borrow_mut()
                        .drain(..)
                        .flat_map(|batch| {
                            batch.sqes.into_iter().map(move |owned| IoUringCqe {
                                user_data: owned.sqe.user_data,
                                res: -errno,
                                flags: 0,
                            })
                        })
                        .collect();
                    return Ok(SubmitOutcome {
                        submitted: 0,
                        failures,
                        error: Some(error),
                        retry: false,
                    });
                }
            }
        }
    }

    fn enter_without_submission(&self, min_complete: u32) -> io::Result<SubmitOutcome> {
        loop {
            match self.enter(0, min_complete, IORING_ENTER_GETEVENTS) {
                Ok(_) => return Ok(SubmitOutcome::complete(0)),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) if matches!(error.raw_os_error(), Some(libc::EBUSY | libc::EAGAIN)) => {
                    return Ok(SubmitOutcome {
                        submitted: 0,
                        failures: Vec::new(),
                        error: Some(error),
                        retry: true,
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn enter(&self, to_submit: u32, min_complete: u32, flags: u32) -> io::Result<u32> {
        let result = self
            .enter
            .enter(self.ring_fd, to_submit, min_complete, flags);

        #[cfg(test)]
        if !self.enter.advances_sq_head()
            && let Ok(&submitted) = result.as_ref()
        {
            let head = load_u32(self.sq_head);
            let pending = load_u32(self.sq_tail).wrapping_sub(head);
            assert!(
                submitted <= to_submit && submitted <= pending,
                "scripted io_uring_enter submitted {submitted} SQEs with only \
                 {to_submit} requested and {pending} pending"
            );
            store_u32(self.sq_head, head.wrapping_add(submitted));
        }

        result
    }
}

impl Drop for IoUring {
    fn drop(&mut self) {
        // Close first: once the last duplicate is gone, the kernel quiesces
        // operations before their completion-owned storage can be dropped.
        // The mappings are then unmapped exactly once.
        unsafe {
            libc::close(self.ring_fd);
            libc::munmap(self.sqes_ptr.cast(), self.sqes_size);
            if self.single_mmap {
                libc::munmap(
                    self.sq_ring_ptr.cast(),
                    self.sq_ring_size.max(self.cq_ring_size),
                );
            } else {
                libc::munmap(self.sq_ring_ptr.cast(), self.sq_ring_size);
                libc::munmap(self.cq_ring_ptr.cast(), self.cq_ring_size);
            }
        }
    }
}

// SAFETY: a runtime ring moves to its owner before use and remains confined
// there. The global notification ring is mutex-serialized. Cross-ring messages
// use only integer fds, never a foreign ring's mapped pointers.
unsafe impl Send for IoUring {}

fn duplicate_sqe_fd(sqe: &mut IoUringSqe) -> io::Result<(Option<OwnedFd>, bool)> {
    let uses_descriptor = matches!(
        sqe.opcode,
        IORING_OP_FSYNC
            | IORING_OP_POLL_ADD
            | IORING_OP_SENDMSG
            | IORING_OP_RECVMSG
            | IORING_OP_ACCEPT
            | IORING_OP_CONNECT
            | IORING_OP_OPENAT
            | IORING_OP_STATX
            | IORING_OP_READ
            | IORING_OP_WRITE
            | IORING_OP_SEND
            | IORING_OP_RECV
            | IORING_OP_SHUTDOWN
            | IORING_OP_RENAMEAT
            | IORING_OP_UNLINKAT
            | IORING_OP_MKDIRAT
            | IORING_OP_MSG_RING
            | IORING_OP_FTRUNCATE
            | IORING_OP_BIND
            | IORING_OP_LISTEN
    );
    if !uses_descriptor || sqe.fd < 0 {
        return Ok((None, false));
    }

    // F_DUPFD_CLOEXEC retains the same open file description while reserving a
    // distinct descriptor number until io_uring_enter accepts this SQE.
    let duplicated = unsafe { libc::fcntl(sqe.fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `duplicated` is a fresh descriptor returned by fcntl and is
    // transferred exactly once into OwnedFd.
    let owned = unsafe { OwnedFd::from_raw_fd(duplicated) };
    sqe.fd = duplicated;
    Ok((Some(owned), sqe.opcode != IORING_OP_MSG_RING))
}

fn offset_ptr<T>(base: *mut u8, offset: u32) -> *mut T {
    // SAFETY: callers pass offsets provided by `io_uring_setup` for fields
    // inside the mmap region referenced by `base`; the target kernel ABI gives
    // these fields the alignment of `T`.
    unsafe { base.add(offset as usize).cast::<T>() }
}

fn mmap_ring(length: usize, fd: RawFd, offset: libc::off_t) -> io::Result<*mut u8> {
    // SAFETY: `fd` is an open io_uring descriptor and `offset` is one of the
    // io_uring mmap offsets. A null address lets the kernel choose an aligned
    // mapping, and `length` is computed from the kernel-reported ring sizes.
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            length,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd,
            offset,
        )
    };
    if ptr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr.cast())
    }
}

fn load_u32(ptr: *const u32) -> u32 {
    // SAFETY: `ptr` points at a 32-bit ring control field within a live
    // io_uring mmap. The kernel may update it asynchronously, so it must be
    // read with volatile semantics.
    let value = unsafe { ptr::read_volatile(ptr) };
    // Pair with the kernel's release store of the same head/tail/mask cell.
    // `compiler_fence` would only restrain reordering by the compiler; we need
    // a real CPU fence to acquire shared-memory updates produced by another
    // executor (kernel or another core).
    fence(Ordering::Acquire);
    value
}

fn store_u32(ptr: *mut u32, value: u32) {
    // Publish prior shared-memory writes to the kernel before advancing the
    // head/tail value it observes.  See note in `load_u32`.
    fence(Ordering::Release);
    // SAFETY: `ptr` points at a writable 32-bit ring control field within a
    // live io_uring mmap owned by this `IoUring`. The kernel observes this
    // shared memory, so the store must be volatile.
    unsafe {
        ptr::write_volatile(ptr, value);
    }
}

fn cvt_long(result: libc::c_long) -> io::Result<libc::c_long> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn normalize_setup_error(error: io::Error) -> io::Error {
    match error.raw_os_error() {
        Some(libc::ENOSYS) => io::Error::new(
            io::ErrorKind::Unsupported,
            IoUringUnavailable::kernel_too_old_or_disabled(),
        ),
        Some(libc::EPERM) => io::Error::new(
            io::ErrorKind::Unsupported,
            IoUringUnavailable::blocked_by_seccomp(),
        ),
        // Deliberately not `OutOfMemory`: the kind is what a caller matches on,
        // and reporting a resource-limit failure as memory exhaustion is the
        // half of this that misdirects hardest.
        Some(libc::ENOMEM) => io::Error::new(
            io::ErrorKind::QuotaExceeded,
            IoUringUnavailable::locked_memory_exhausted(),
        ),
        _ => error,
    }
}

fn setup_ring(entries: u32, profile: SetupProfile) -> io::Result<(RawFd, IoUringParams)> {
    let cached = match profile {
        SetupProfile::SingleIssuer => SINGLE_ISSUER_SETUP_FLAGS.get().copied(),
        SetupProfile::Shared => SHARED_SETUP_FLAGS.get().copied(),
    };
    if let Some(flags) = cached {
        match setup_ring_once(entries, flags) {
            Ok(ring) => return Ok(ring),
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {}
            Err(error) => return Err(normalize_setup_error(error)),
        }
    }

    let mut candidates = Vec::new();
    let optional = match profile {
        SetupProfile::SingleIssuer => vec![
            IORING_SETUP_SUBMIT_ALL,
            IORING_SETUP_COOP_TASKRUN,
            IORING_SETUP_SINGLE_ISSUER,
            IORING_SETUP_DEFER_TASKRUN,
        ],
        SetupProfile::Shared => vec![IORING_SETUP_SUBMIT_ALL, IORING_SETUP_COOP_TASKRUN],
    };
    for subset in 0..(1usize << optional.len()) {
        let mut flags = IORING_SETUP_CLAMP;
        for (index, flag) in optional.iter().enumerate() {
            if subset & (1 << index) != 0 {
                flags |= *flag;
            }
        }
        if flags & IORING_SETUP_DEFER_TASKRUN != 0 && flags & IORING_SETUP_SINGLE_ISSUER == 0 {
            continue;
        }
        candidates.push(flags);
    }
    candidates.sort_unstable_by(|left, right| {
        right
            .count_ones()
            .cmp(&left.count_ones())
            .then_with(|| right.cmp(left))
    });
    candidates.dedup();

    for flags in candidates {
        if cached == Some(flags) {
            continue;
        }
        match setup_ring_once(entries, flags) {
            Ok((fd, params)) => {
                let cache = match profile {
                    SetupProfile::SingleIssuer => &SINGLE_ISSUER_SETUP_FLAGS,
                    SetupProfile::Shared => &SHARED_SETUP_FLAGS,
                };
                let _ = cache.set(flags);
                tracing::debug!(
                    target: "runite::driver",
                    event = "io_uring_setup_flags",
                    flags,
                    entries,
                    "created io_uring with probed setup flags"
                );
                return Ok((fd, params));
            }
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => continue,
            Err(error) => return Err(normalize_setup_error(error)),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "the kernel rejected every supported io_uring setup flag combination",
    ))
}

fn setup_ring_once(entries: u32, flags: u32) -> io::Result<(RawFd, IoUringParams)> {
    let mut params = IoUringParams {
        flags,
        ..IoUringParams::default()
    };
    // SAFETY: `params` points to writable ABI-compatible storage for the
    // duration of the syscall, and `entries` is passed by value.
    let result = unsafe {
        libc::syscall(
            libc::SYS_io_uring_setup,
            entries as libc::c_uint,
            &mut params as *mut IoUringParams,
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok((result as RawFd, params))
    }
}

fn supported_ops_for_ring(ring_fd: RawFd) -> SupportedOps {
    #[cfg(test)]
    if let Some(ops) = TEST_SUPPORTED_OPS_OVERRIDE.with(Cell::get) {
        return ops;
    }

    *SUPPORTED_OPS.get_or_init(|| probe_supported_ops(ring_fd))
}

fn probe_supported_ops(ring_fd: RawFd) -> SupportedOps {
    let mut probe = IoUringProbe::default();
    // SAFETY: `ring_fd` is the valid file descriptor returned by a successful
    // `io_uring_setup` call and remains open for the duration of this syscall.
    // `probe` points to writable memory large enough for exactly 256
    // `io_uring_probe_op` entries, and `nr_args` is set to the same length.
    let result = unsafe {
        libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd,
            IORING_REGISTER_PROBE as libc::c_uint,
            &mut probe as *mut IoUringProbe,
            probe.ops.len() as libc::c_uint,
        )
    };

    if result == -1 {
        let error = io::Error::last_os_error();
        tracing::warn!(
            target: crate::trace_targets::DRIVER,
            event = "io_uring_probe_unavailable",
            error = %error,
            "kernel is too old to probe io_uring opcode support; assuming all opcodes are supported"
        );
        SupportedOps::permissive_after_probe_failure()
    } else {
        SupportedOps::from_probe(&probe)
    }
}

#[cfg(test)]
pub(crate) fn override_supported_ops(ops: SupportedOps) -> SupportedOpsOverride {
    TEST_SUPPORTED_OPS_OVERRIDE.with(|slot| {
        let previous = slot.replace(Some(ops));
        SupportedOpsOverride {
            previous,
            _not_send: PhantomData,
        }
    })
}

#[cfg(test)]
pub(crate) struct SupportedOpsOverride {
    previous: Option<SupportedOps>,
    _not_send: PhantomData<Rc<()>>,
}

#[cfg(test)]
impl Drop for SupportedOpsOverride {
    fn drop(&mut self) {
        TEST_SUPPORTED_OPS_OVERRIDE.with(|slot| {
            slot.set(self.previous);
        });
    }
}

fn global_submitter() -> &'static Mutex<Option<IoUring>> {
    GLOBAL_SUBMITTER.get_or_init(|| Mutex::new(None))
}

fn duration_to_kernel_timespec(duration: Duration) -> KernelTimespec {
    KernelTimespec {
        tv_sec: duration.as_secs() as i64,
        tv_nsec: duration.subsec_nanos() as i64,
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::{
        IORING_ENTER_GETEVENTS, IORING_OP_MSG_RING, IORING_OP_NOP, IORING_OP_POLL_ADD,
        IORING_SETUP_DEFER_TASKRUN, IORING_SETUP_SINGLE_ISSUER, IOSQE_CQE_SKIP_SUCCESS, IoUring,
        IoUringEnter, IoUringEnterCall, ScriptedIoUringEnter, SupportedOps,
        is_unsupported_operation, load_u32, normalize_setup_error, override_supported_ops,
        supported_ops_for_ring,
    };
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    struct PausedEnter {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl IoUringEnter for PausedEnter {
        fn enter(
            &self,
            _ring_fd: RawFd,
            to_submit: u32,
            _min_complete: u32,
            _flags: u32,
        ) -> io::Result<u32> {
            self.entered.wait();
            self.release.wait();
            Ok(to_submit)
        }
    }

    #[test]
    fn older_kernel_simulation_uses_permissive_bitmap() {
        let ops = SupportedOps::permissive_after_probe_failure();

        assert!(ops.probe_unavailable());
        assert!(ops.supports(IORING_OP_NOP));
        assert!(ops.supports(u8::MAX));
    }

    #[test]
    fn fallback_recognizes_probe_and_kernel_unsupported_signals() {
        assert!(is_unsupported_operation(
            &IoUring::unsupported_opcode_error(IORING_OP_NOP)
        ));
        for errno in [libc::EINVAL, libc::ENOSYS, libc::EOPNOTSUPP] {
            assert!(is_unsupported_operation(
                &std::io::Error::from_raw_os_error(errno)
            ));
        }
        assert!(!is_unsupported_operation(
            &std::io::Error::from_raw_os_error(libc::EBADF)
        ));
    }

    #[test]
    fn supported_opcode_override_is_scoped_and_nestable() {
        let _outer = override_supported_ops(SupportedOps::only([IORING_OP_NOP]));
        assert!(supported_ops_for_ring(-1).supports(IORING_OP_NOP));
        assert!(!supported_ops_for_ring(-1).supports(IORING_OP_MSG_RING));

        {
            let _inner = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
            assert!(!supported_ops_for_ring(-1).supports(IORING_OP_NOP));
            assert!(supported_ops_for_ring(-1).supports(IORING_OP_MSG_RING));
        }

        assert!(supported_ops_for_ring(-1).supports(IORING_OP_NOP));
        assert!(!supported_ops_for_ring(-1).supports(IORING_OP_MSG_RING));
    }

    #[test]
    fn probed_setup_flags_preserve_kernel_dependencies() {
        let ring = IoUring::new(8).expect("ring should initialize");
        if ring.setup_flags & IORING_SETUP_DEFER_TASKRUN != 0 {
            assert_ne!(ring.setup_flags & IORING_SETUP_SINGLE_ISSUER, 0);
        }
    }

    #[test]
    fn capability_matrix_old_timer_remove_avoids_cqe_skip() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([]);
        let mut ring =
            IoUring::new_with_enter(8, Box::new(script)).expect("ring should initialize");
        ring.cqe_skip = false;
        ring.submit_timeout_remove(1, 2)
            .expect("timeout remove should stage");
        assert_eq!(ring.pending.borrow()[0].sqes[0].sqe.flags, 0);

        ring.pending.borrow_mut().clear();
        ring.cqe_skip = true;
        ring.submit_timeout_remove(1, 2)
            .expect("timeout remove should stage");
        assert_eq!(
            ring.pending.borrow()[0].sqes[0].sqe.flags,
            IOSQE_CQE_SKIP_SUCCESS
        );
    }

    #[test]
    fn staged_sqe_retains_duplicated_fd_until_enter_accepts_it() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let ring = IoUring::new_with_enter(
            8,
            Box::new(PausedEnter {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            }),
        )
        .expect("ring should initialize");

        let mut fds = [0; 2];
        // SAFETY: pipe2 initializes both descriptor slots on success.
        assert_eq!(
            unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) },
            0
        );
        // SAFETY: pipe2 returned fresh descriptors owned by this test.
        let original = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        let original_raw = original.as_raw_fd();

        ring.submit_with_token(91, |sqe| {
            sqe.opcode = IORING_OP_POLL_ADD;
            sqe.fd = original_raw;
            sqe.op_flags = libc::POLLIN as u32;
        })
        .expect("poll should stage");
        let staged_fd = ring.pending.borrow()[0].sqes[0].sqe.fd;
        assert_ne!(staged_fd, original_raw);

        let submitter = thread::spawn(move || {
            let outcome = ring.submit_pending(false).expect("submit should resume");
            (ring, outcome.submitted)
        });
        entered.wait();

        let replacement = File::open("/dev/null").expect("replacement should open");
        // SAFETY: dup2 atomically closes the pipe descriptor and installs a
        // duplicate of replacement at the same number. `original` continues to
        // own that number, now referring to /dev/null.
        assert_eq!(
            unsafe { libc::dup2(replacement.as_raw_fd(), original_raw) },
            original_raw
        );
        let reused = original;

        let byte = 7u8;
        // SAFETY: writer is the live pipe write end and byte is initialized.
        assert_eq!(
            unsafe {
                libc::write(
                    writer.as_raw_fd(),
                    (&byte as *const u8).cast::<libc::c_void>(),
                    1,
                )
            },
            1
        );
        let mut observed = 0u8;
        // SAFETY: staged_fd is the ring-owned duplicate of the pipe read end.
        assert_eq!(
            unsafe {
                libc::read(
                    staged_fd,
                    (&mut observed as *mut u8).cast::<libc::c_void>(),
                    1,
                )
            },
            1
        );
        assert_eq!(observed, byte);

        release.wait();
        let (ring, submitted) = submitter.join().expect("submitter should exit");
        assert_eq!(submitted, 1);
        assert!(!ring.has_pending_submissions());
        drop(reused);
    }

    #[test]
    fn transient_msg_ring_retains_target_only_until_acceptance() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        for errno in [libc::EAGAIN, libc::EBUSY] {
            let script = ScriptedIoUringEnter::new([Err(errno), Ok(1)]);
            let ring =
                IoUring::new_with_enter(8, Box::new(script)).expect("ring should initialize");
            let mut fds = [0; 2];
            // SAFETY: pipe2 initializes both descriptor slots on success.
            assert_eq!(
                unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) },
                0
            );
            // SAFETY: pipe2 returned fresh descriptors owned by this test.
            let target = unsafe { OwnedFd::from_raw_fd(fds[0]) };
            let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
            let target_raw = target.as_raw_fd();

            ring.submit_msg_ring(target_raw, 1, 1, 93)
                .expect("MSG_RING should stage");
            let staged_fd = ring.pending.borrow()[0].sqes[0]
                ._fd
                .as_ref()
                .expect("MSG_RING must retain a target duplicate")
                .as_raw_fd();
            let retry = ring
                .submit_pending(false)
                .expect("transient enter should remain retryable");
            assert!(retry.retry);

            let replacement = File::open("/dev/null").expect("replacement should open");
            // SAFETY: dup2 atomically rehomes the target descriptor number while
            // `target` remains its sole Rust owner.
            assert_eq!(
                unsafe { libc::dup2(replacement.as_raw_fd(), target_raw) },
                target_raw
            );
            let byte = 9u8;
            // SAFETY: writer is the original pipe write end.
            assert_eq!(
                unsafe {
                    libc::write(
                        writer.as_raw_fd(),
                        (&byte as *const u8).cast::<libc::c_void>(),
                        1,
                    )
                },
                1
            );
            let mut observed = 0u8;
            // SAFETY: staged_fd is the retained duplicate of the original target.
            assert_eq!(
                unsafe {
                    libc::read(
                        staged_fd,
                        (&mut observed as *mut u8).cast::<libc::c_void>(),
                        1,
                    )
                },
                1
            );
            assert_eq!(observed, byte);

            assert_eq!(
                ring.submit_pending(false)
                    .expect("retry should accept MSG_RING")
                    .submitted,
                1
            );
            assert!(ring.pending.borrow().is_empty());
            assert!(
                ring.inflight_fds.borrow().is_empty(),
                "MSG_RING guard must drop at acceptance because success may skip CQE"
            );
            drop(target);
        }
    }

    #[test]
    fn scripted_enter_advances_sq_head_after_short_submission() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Ok(1), Err(libc::EAGAIN), Ok(1)]);
        let ring =
            IoUring::new_with_enter(8, Box::new(script.clone())).expect("ring should initialize");

        ring.push_sqe(|sqe| sqe.opcode = IORING_OP_NOP)
            .expect("first SQE should queue");
        ring.push_sqe(|sqe| sqe.opcode = IORING_OP_NOP)
            .expect("second SQE should queue");

        let short = ring
            .submit_pending(false)
            .expect("short submission should succeed");
        assert_eq!(short.submitted, 1);
        assert!(ring.has_pending_submissions());
        assert_eq!(load_u32(ring.sq_head), 1);
        assert_eq!(
            load_u32(ring.sq_tail),
            1,
            "the unaccepted suffix must be unpublished"
        );

        let retry = ring
            .submit_pending(false)
            .expect("EAGAIN should preserve the pending suffix");
        assert!(retry.retry);
        assert_eq!(
            retry.error.as_ref().and_then(std::io::Error::raw_os_error),
            Some(libc::EAGAIN)
        );
        assert_eq!(load_u32(ring.sq_head), 1);
        assert_eq!(load_u32(ring.sq_tail), 1);

        let remainder = ring
            .submit_pending(false)
            .expect("the preserved suffix should retry");
        assert_eq!(remainder.submitted, 1);
        assert!(!ring.has_pending_submissions());
        assert_eq!(load_u32(ring.sq_head), 2);
        assert_eq!(load_u32(ring.sq_tail), 2);
        assert_eq!(
            script.calls(),
            vec![
                IoUringEnterCall {
                    ring_fd: ring.ring_fd(),
                    to_submit: 2,
                    min_complete: 0,
                    flags: IORING_ENTER_GETEVENTS,
                },
                IoUringEnterCall {
                    ring_fd: ring.ring_fd(),
                    to_submit: 1,
                    min_complete: 0,
                    flags: IORING_ENTER_GETEVENTS,
                },
                IoUringEnterCall {
                    ring_fd: ring.ring_fd(),
                    to_submit: 1,
                    min_complete: 0,
                    flags: IORING_ENTER_GETEVENTS,
                },
            ]
        );
    }

    #[test]
    fn public_submission_paths_observe_scripted_consumption_and_error() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Ok(1), Err(libc::EIO)]);
        let ring =
            IoUring::new_with_enter(8, Box::new(script.clone())).expect("ring should initialize");

        ring.submit_with_token(11, |sqe| sqe.opcode = IORING_OP_NOP)
            .expect("first public submission should succeed");
        ring.submit_with_token(12, |sqe| sqe.opcode = IORING_OP_NOP)
            .expect("second public submission should stage");
        assert_eq!(load_u32(ring.sq_head), 0);
        assert_eq!(load_u32(ring.sq_tail), 0);

        let first = ring
            .submit_pending(false)
            .expect("the first SQE should be accepted");
        assert_eq!(first.submitted, 1);
        let failed = ring
            .submit_pending(false)
            .expect("a submission error should become a deferred completion");
        assert_eq!(
            failed.error.as_ref().and_then(std::io::Error::raw_os_error),
            Some(libc::EIO)
        );
        assert_eq!(failed.failures.len(), 1);
        assert_eq!(failed.failures[0].user_data, 12);
        assert_eq!(failed.failures[0].res, -libc::EIO);
        assert_eq!(load_u32(ring.sq_head), 1);
        assert_eq!(load_u32(ring.sq_tail), 1);
        assert_eq!(
            script
                .calls()
                .into_iter()
                .map(|call| call.to_submit)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn submit_and_wait_combines_enter_when_submit_all_is_available() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Ok(1)]);
        let ring =
            IoUring::new_with_enter(8, Box::new(script.clone())).expect("ring should initialize");
        ring.submit_with_token(31, |sqe| sqe.opcode = IORING_OP_NOP)
            .expect("SQE should stage");

        let outcome = ring
            .submit_pending(true)
            .expect("combined submit/wait should succeed");
        assert_eq!(outcome.submitted, 1);
        assert_eq!(
            script.calls()[0].min_complete,
            u32::from(ring.supports_submit_all())
        );
        assert_eq!(script.calls()[0].flags, IORING_ENTER_GETEVENTS);
    }

    #[test]
    fn linked_batch_faults_preserve_exact_suffix_ownership() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Err(libc::EIO)]);
        let ring = IoUring::new_with_enter(8, Box::new(script)).expect("ring should initialize");
        ring.submit_linked_with_timeout(
            41,
            |sqe| sqe.opcode = IORING_OP_NOP,
            42,
            Duration::from_millis(1),
        )
        .expect("linked batch should stage");
        let failure = ring
            .submit_pending(false)
            .expect("an error before acceptance should fail the whole batch");
        assert_eq!(
            failure
                .failures
                .iter()
                .map(|cqe| cqe.user_data)
                .collect::<Vec<_>>(),
            vec![41, 42]
        );
        assert!(!ring.has_pending_submissions());
    }

    #[test]
    fn linked_batch_retries_only_before_any_sqe_is_accepted() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));

        for scripted in [
            vec![Ok(0), Ok(2)],
            vec![Err(libc::EAGAIN), Ok(2)],
            vec![Err(libc::EBUSY), Ok(2)],
        ] {
            let script = ScriptedIoUringEnter::new(scripted);
            let ring =
                IoUring::new_with_enter(8, Box::new(script)).expect("ring should initialize");
            ring.submit_linked_with_timeout(
                51,
                |sqe| sqe.opcode = IORING_OP_NOP,
                52,
                Duration::from_millis(1),
            )
            .expect("linked batch should stage");

            for _ in 0..3 {
                if !ring.has_pending_submissions() {
                    break;
                }
                let _ = ring
                    .submit_pending(false)
                    .expect("short/transient submission should remain retryable");
            }

            assert!(!ring.has_pending_submissions());
            assert_eq!(load_u32(ring.sq_head), 2);
            assert_eq!(load_u32(ring.sq_tail), 2);
        }
    }

    #[test]
    fn linked_batch_short_submission_is_fatal_and_retains_ownership() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Ok(1), Ok(1)]);
        let ring =
            IoUring::new_with_enter(8, Box::new(script.clone())).expect("ring should initialize");
        ring.submit_linked_with_timeout(
            61,
            |sqe| sqe.opcode = IORING_OP_NOP,
            62,
            Duration::from_millis(1),
        )
        .expect("linked batch should stage");

        let error = match ring.submit_pending(false) {
            Err(error) => error,
            Ok(_) => panic!("the kernel must not split a linked batch"),
        };
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let pending = ring.pending.borrow();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].sqes.len(), 2);
        assert!(pending[0].sqes[1]._timespec.is_some());
        drop(pending);
        assert!(
            ring.submit_pending(false).is_err(),
            "a split atomic batch must poison further submissions"
        );
        assert_eq!(script.calls().len(), 1, "timeout must not retry standalone");
        assert_eq!(load_u32(ring.sq_head), 1);
        assert_eq!(load_u32(ring.sq_tail), 1);
    }

    #[test]
    fn linked_batch_is_not_split_at_ring_capacity_boundary() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Ok(7), Ok(2)]);
        let ring =
            IoUring::new_with_enter(8, Box::new(script.clone())).expect("ring should initialize");
        for token in 1..=7 {
            ring.submit_with_token(token, |sqe| sqe.opcode = IORING_OP_NOP)
                .expect("NOP should stage");
        }
        ring.submit_linked_with_timeout(
            71,
            |sqe| sqe.opcode = IORING_OP_NOP,
            72,
            Duration::from_millis(1),
        )
        .expect("linked batch should stage");

        assert_eq!(ring.submit_pending(false).unwrap().submitted, 7);
        assert_eq!(ring.submit_pending(false).unwrap().submitted, 2);
        assert_eq!(
            script
                .calls()
                .into_iter()
                .map(|call| call.to_submit)
                .collect::<Vec<_>>(),
            vec![7, 2]
        );
    }

    #[test]
    fn submit_wait_does_not_block_before_pending_linked_batch_is_published() {
        let _ops = override_supported_ops(SupportedOps::only([IORING_OP_MSG_RING]));
        let script = ScriptedIoUringEnter::new([Ok(255), Ok(2)]);
        let ring =
            IoUring::new_with_enter(256, Box::new(script.clone())).expect("ring should initialize");
        let source = File::open("/dev/null").expect("poll source should open");

        for token in 1..=255 {
            ring.submit_with_token(token, |sqe| {
                sqe.opcode = IORING_OP_POLL_ADD;
                sqe.fd = source.as_raw_fd();
                sqe.op_flags = libc::POLLIN as u32;
            })
            .expect("poll should stage");
        }
        ring.submit_linked_with_timeout(
            301,
            |sqe| {
                sqe.opcode = IORING_OP_POLL_ADD;
                sqe.fd = source.as_raw_fd();
                sqe.op_flags = libc::POLLIN as u32;
            },
            302,
            Duration::from_secs(1),
        )
        .expect("linked poll should stage");

        assert_eq!(ring.submit_pending(true).unwrap().submitted, 255);
        assert_eq!(ring.pending_submission_count(), 2);
        assert_eq!(
            script.calls()[0].min_complete,
            0,
            "prefix submission must not wait before the linked timeout is submitted"
        );

        assert_eq!(ring.submit_pending(true).unwrap().submitted, 2);
        assert_eq!(script.calls()[1].to_submit, 2);
        assert_eq!(
            script.calls()[1].min_complete,
            u32::from(ring.supports_submit_all())
        );
    }

    /// `ENOMEM` from `io_uring_setup` is a locked-memory limit, not memory
    /// exhaustion. The raw errno renders as "Cannot allocate memory", which
    /// sends the reader to look at free RAM on a machine that has plenty.
    #[test]
    fn enomem_setup_failure_names_the_locked_memory_limit() {
        let normalized = normalize_setup_error(io::Error::from_raw_os_error(libc::ENOMEM));

        assert_eq!(
            normalized.kind(),
            io::ErrorKind::QuotaExceeded,
            "a resource-limit failure should not present as memory exhaustion"
        );

        let message = normalized.to_string();
        assert!(
            message.contains("RLIMIT_MEMLOCK"),
            "the message should name the limit that was hit, got {message:?}"
        );
        assert!(
            message.contains("perf record"),
            "the message should name the common cause, got {message:?}"
        );
        assert!(
            !message.contains("Cannot allocate memory"),
            "the misleading raw errno text should not survive, got {message:?}"
        );
    }

    /// The other mapped errnos keep their diagnosis, and an errno with no known
    /// cause is passed through rather than guessed at.
    #[test]
    fn other_setup_errors_keep_their_mapping() {
        let unsupported = normalize_setup_error(io::Error::from_raw_os_error(libc::ENOSYS));
        assert_eq!(unsupported.kind(), io::ErrorKind::Unsupported);
        assert!(unsupported.to_string().contains("kernel"));

        let blocked = normalize_setup_error(io::Error::from_raw_os_error(libc::EPERM));
        assert_eq!(blocked.kind(), io::ErrorKind::Unsupported);
        assert!(blocked.to_string().contains("seccomp"));

        let untouched = normalize_setup_error(io::Error::from_raw_os_error(libc::EIO));
        assert_eq!(untouched.raw_os_error(), Some(libc::EIO));
    }
}

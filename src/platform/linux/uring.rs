use std::cell::Cell;
use std::fmt;
use std::io;
use std::os::fd::RawFd;
use std::ptr;
use std::sync::atomic::{Ordering, fence};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const IORING_OFF_SQ_RING: libc::off_t = 0;
const IORING_OFF_CQ_RING: libc::off_t = 0x0800_0000;
const IORING_OFF_SQES: libc::off_t = 0x1000_0000;

const IORING_ENTER_GETEVENTS: u32 = 1 << 0;
const IORING_REGISTER_PROBE: u32 = 8;
const IORING_SETUP_CLAMP: u32 = 1 << 4;

const IORING_FEAT_SINGLE_MMAP: u32 = 1 << 0;

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
// TODO(roadmap): only referenced by the unwired io_uring close path. See ROADMAP.md.
#[allow(dead_code)]
pub(crate) const IORING_OP_CLOSE: u8 = 19;
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
pub(crate) const IORING_TIMEOUT_UPDATE: u32 = 1 << 1;
pub(crate) const IOSQE_IO_LINK: u8 = 1 << 2;
pub(crate) const IOSQE_CQE_SKIP_SUCCESS: u8 = 1 << 6;

thread_local! {
    static CURRENT_SUBMITTER: Cell<*const IoUring> = const { Cell::new(ptr::null()) };
}

static GLOBAL_SUBMITTER: OnceLock<Mutex<Option<IoUring>>> = OnceLock::new();
static SUPPORTED_OPS: OnceLock<SupportedOps> = OnceLock::new();

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

/// Process-wide io_uring operation support discovered from the running kernel.
///
/// The probe result is cached once per process because the kernel's supported
/// opcode set cannot change while the process is running, and probing once per
/// runtime thread would waste a registration syscall.
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

#[derive(Debug)]
pub(crate) struct IoUringUnavailable {
    message: &'static str,
}

impl IoUringUnavailable {
    fn kernel_too_old_or_disabled() -> Self {
        Self {
            message: "io_uring is not available on this kernel (CONFIG_IO_URING not enabled or kernel too old)",
        }
    }

    fn blocked_by_seccomp() -> Self {
        Self {
            message: "io_uring is not available because io_uring_setup was blocked (likely by seccomp)",
        }
    }
}

impl fmt::Display for IoUringUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for IoUringUnavailable {}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct KernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

pub(crate) struct IoUring {
    ring_fd: RawFd,
    supported_ops: SupportedOps,
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
    cqes: *mut IoUringCqe,
}

impl IoUring {
    pub(crate) fn new(entries: u32) -> io::Result<Self> {
        let mut params = IoUringParams {
            flags: IORING_SETUP_CLAMP,
            ..IoUringParams::default()
        };

        // SAFETY: `params` points to a valid, writable `IoUringParams` for the
        // duration of the syscall, and `entries` is passed by value.
        let ring_fd = cvt_setup(unsafe {
            libc::syscall(
                libc::SYS_io_uring_setup,
                entries as libc::c_uint,
                &mut params as *mut IoUringParams,
            )
        })? as RawFd;
        let supported_ops = supported_ops_for_ring(ring_fd);

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
            cqes: offset_ptr(cq_ring_ptr, params.cq_off.cqes),
        })
    }

    pub(crate) fn ring_fd(&self) -> RawFd {
        self.ring_fd
    }

    pub(crate) fn supported_ops(&self) -> SupportedOps {
        self.supported_ops
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
                return f(ring);
            }

            let mut ring = global_submitter()
                .lock()
                .expect("global io_uring submitter should not be poisoned");
            if ring.is_none() {
                *ring = Some(IoUring::new(64)?);
            }

            f(ring
                .as_ref()
                .expect("global submitter ring should initialize"))
        })
    }

    pub(crate) fn submit_timeout(&self, token: u64, deadline: Duration) -> io::Result<()> {
        // SAFETY: `timespec` is stack-allocated and its pointer is passed to
        // the kernel via the SQE's `addr` field. This is safe because
        // `submit_pending` calls `io_uring_enter` synchronously within this
        // stack frame, before `timespec` is dropped. The kernel copies the
        // timespec value during submission of IORING_OP_TIMEOUT, so no
        // lifetime extension beyond this call is required.
        let timespec = duration_to_kernel_timespec(deadline);
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_TIMEOUT;
            sqe.fd = -1;
            sqe.off = 0;
            sqe.user_data = token;
            sqe.addr = (&timespec as *const KernelTimespec) as u64;
            sqe.len = 1;
            sqe.op_flags = IORING_TIMEOUT_ABS;
        })?;
        self.submit_pending().map(|_| ())
    }

    pub(crate) fn submit_timeout_remove(
        &self,
        token_to_remove: u64,
        completion: u64,
    ) -> io::Result<()> {
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_TIMEOUT_REMOVE;
            sqe.fd = -1;
            sqe.flags = IOSQE_CQE_SKIP_SUCCESS;
            sqe.user_data = completion;
            sqe.addr = token_to_remove;
        })?;
        self.submit_pending().map(|_| ())
    }

    pub(crate) fn submit_timeout_update(
        &self,
        token_to_update: u64,
        deadline: Duration,
    ) -> io::Result<()> {
        // SAFETY: Same stack-pointer contract as submit_timeout — the kernel
        // copies the timespec during the synchronous io_uring_enter call made
        // by submit_pending before this function returns.
        let timespec = duration_to_kernel_timespec(deadline);
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_TIMEOUT_REMOVE;
            sqe.fd = -1;
            sqe.off = (&timespec as *const KernelTimespec) as u64;
            sqe.addr = token_to_update;
            sqe.op_flags = IORING_TIMEOUT_UPDATE | IORING_TIMEOUT_ABS;
        })?;
        self.submit_pending().map(|_| ())
    }

    pub(crate) fn submit_msg_ring(
        &self,
        target_ring_fd: RawFd,
        target_user_data: u64,
        value: u32,
        completion: u64,
    ) -> io::Result<()> {
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_MSG_RING;
            sqe.flags = IOSQE_CQE_SKIP_SUCCESS;
            sqe.fd = target_ring_fd;
            sqe.off = target_user_data;
            sqe.addr = IORING_MSG_DATA;
            sqe.len = value;
            sqe.user_data = completion;
        })?;
        self.submit_pending().map(|_| ())
    }

    pub(crate) fn submit_with_token(
        &self,
        token: u64,
        fill: impl FnOnce(&mut IoUringSqe),
    ) -> io::Result<()> {
        self.push_sqe(|sqe| {
            fill(sqe);
            sqe.user_data = token;
        })?;
        self.submit_pending().map(|_| ())
    }

    /// Submits a main SQE linked to an `IORING_OP_LINK_TIMEOUT` SQE.
    ///
    /// The main SQE is submitted with `IOSQE_IO_LINK` set so that if `timeout`
    /// elapses before the main op completes, the kernel cancels the main op
    /// (`-ECANCELED`) and the timeout CQE arrives with `-ETIME`.  If the main op
    /// completes first, the timeout CQE arrives with `-ECANCELED`.
    ///
    /// # Safety of the stack-allocated timespec
    ///
    /// `KernelTimespec` is stack-allocated and its address is passed to the kernel
    /// in the timeout SQE.  This is safe for the same reason as `submit_timeout`:
    /// both SQEs are flushed via the synchronous `io_uring_enter` call inside
    /// `submit_pending` before this function returns.  The kernel copies the
    /// timespec value at submission time; no lifetime extension beyond this call
    /// is required.
    pub(crate) fn submit_linked_with_timeout(
        &self,
        main_token: u64,
        fill: impl FnOnce(&mut IoUringSqe),
        timeout_token: u64,
        timeout: Duration,
    ) -> io::Result<()> {
        let timespec = duration_to_kernel_timespec(timeout);
        self.push_sqe(|sqe| {
            fill(sqe);
            sqe.flags |= IOSQE_IO_LINK;
            sqe.user_data = main_token;
        })?;
        self.push_sqe(|sqe| {
            sqe.opcode = IORING_OP_LINK_TIMEOUT;
            sqe.fd = -1;
            sqe.addr = (&timespec as *const KernelTimespec) as u64;
            sqe.len = 1;
            sqe.user_data = timeout_token;
        })?;
        self.submit_pending().map(|_| ())
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
            f(cqe);
            head = head.wrapping_add(1);
        }

        store_u32(self.cq_head, head);
        true
    }

    pub(crate) fn wait_for_cqe(&self) -> io::Result<()> {
        loop {
            match self.enter(0, 1, IORING_ENTER_GETEVENTS) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn push_sqe(&self, fill: impl FnOnce(&mut IoUringSqe)) -> io::Result<()> {
        let head = load_u32(self.sq_head);
        let tail = load_u32(self.sq_tail);
        let entries = load_u32(self.sq_ring_entries);
        if tail.wrapping_sub(head) >= entries {
            self.submit_pending()?;
            let head = load_u32(self.sq_head);
            let tail = load_u32(self.sq_tail);
            if tail.wrapping_sub(head) >= entries {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "io_uring submission queue is full",
                ));
            }
        }

        let tail = load_u32(self.sq_tail);
        let mask = load_u32(self.sq_ring_mask);
        let index = (tail & mask) as usize;
        // SAFETY: `sqes_ptr` points to the SQE array mapping sized for
        // `sq_entries`; the kernel-provided ring mask confines `index` to that
        // array, and the queue-full check above guarantees this tail slot is not
        // concurrently owned by the kernel.
        let sqe = unsafe { &mut *self.sqes_ptr.add(index) };
        *sqe = IoUringSqe::default();
        fill(sqe);
        // SAFETY: `sq_array` points to the submission array mapping and `index`
        // is in bounds for the same ring-mask reason as the SQE pointer above.
        // A volatile write is required because the kernel observes this memory.
        unsafe {
            ptr::write_volatile(self.sq_array.add(index), index as u32);
        }
        // Publish the new SQE (the volatile write above and any prior writes
        // through `sqe`) before advancing the SQ tail that the kernel reads.
        fence(Ordering::Release);
        store_u32(self.sq_tail, tail.wrapping_add(1));
        Ok(())
    }

    fn submit_pending(&self) -> io::Result<u32> {
        let head = load_u32(self.sq_head);
        let tail = load_u32(self.sq_tail);
        let to_submit = tail.wrapping_sub(head);
        if to_submit == 0 {
            return Ok(0);
        }
        self.enter(to_submit, 0, 0)
    }

    fn enter(&self, to_submit: u32, min_complete: u32, flags: u32) -> io::Result<u32> {
        // SAFETY: `ring_fd` is an open io_uring descriptor owned by `self`.
        // The final sigset pointer is null with size 0, so the kernel reads no
        // user memory beyond the by-value syscall arguments.
        cvt_long(unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                self.ring_fd,
                to_submit as libc::c_uint,
                min_complete as libc::c_uint,
                flags as libc::c_uint,
                ptr::null::<libc::c_void>(),
                0usize,
            )
        })
        .map(|value| value as u32)
    }
}

impl Drop for IoUring {
    fn drop(&mut self) {
        // SAFETY: each pointer/length pair was returned by `mmap_ring` during
        // construction and is unmapped exactly once here. In single-mmap mode
        // SQ and CQ ring pointers alias the same mapping, so only the maximum
        // shared size is unmapped. `ring_fd` is the owned descriptor for these
        // mappings and is closed after unmapping.
        unsafe {
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
            libc::close(self.ring_fd);
        }
    }
}

// SAFETY: `IoUring` owns kernel-shared memory mapped at construction time.
// Cross-thread access is restricted to two narrow paths:
//   1. The `IoUring` is built on one thread and then moved into a per-thread
//      `Driver`; after `Driver::bind_current_thread` the ring stays on its
//      owning thread for the rest of its life, so the mapped SQ/CQ pointers
//      are only dereferenced on a single thread.
//   2. The `with_submitter` global-fallback path stores its `IoUring` inside
//      a `Mutex<Option<IoUring>>` and serializes all SQ tail mutation through
//      that lock.
// Cross-ring messaging (`IORING_OP_MSG_RING`) only uses the integer `ring_fd`
// field; no shared-memory pointers are dereferenced from a foreign thread.
// The `Send` bound is therefore required only to satisfy the type-system
// requirement that `IoUring` be moved through these initialization paths.
unsafe impl Send for IoUring {}

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

fn cvt_setup(result: libc::c_long) -> io::Result<libc::c_long> {
    if result != -1 {
        return Ok(result);
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ENOSYS) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            IoUringUnavailable::kernel_too_old_or_disabled(),
        )),
        Some(libc::EPERM) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            IoUringUnavailable::blocked_by_seccomp(),
        )),
        _ => Err(error),
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
        SupportedOpsOverride { previous }
    })
}

#[cfg(test)]
pub(crate) struct SupportedOpsOverride {
    previous: Option<SupportedOps>,
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

#[cfg(test)]
mod tests {
    use super::{IORING_OP_NOP, SupportedOps};

    #[test]
    fn older_kernel_simulation_uses_permissive_bitmap() {
        let ops = SupportedOps::permissive_after_probe_failure();

        assert!(ops.probe_unavailable());
        assert!(ops.supports(IORING_OP_NOP));
        assert!(ops.supports(u8::MAX));
    }
}

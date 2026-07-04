# Roadmap

Post-0.1 work and tracked follow-ups. (GitHub Issues are currently disabled on the
repository, so near-term tracking lives here.)

## Performance

### io_uring registered / fixed buffers

runite does **not** currently use io_uring fixed/registered buffers. Only
`IORING_REGISTER_PROBE` is used (for opcode capability detection); every read/write
passes an ordinary user-space pointer, so the kernel pins the buffer per operation.

Adopting `IORING_REGISTER_BUFFERS` (plus the fixed-buffer read/write opcodes) would
remove per-op buffer pinning cost on hot I/O paths. This is a 0.2 performance
optimization — it requires a buffer-pool/registration lifecycle that interacts with the
existing buffer-ownership-across-Drop model, so it needs careful design and benchmarking.

## Runtime fairness

### Macrotask-queue starvation detection

Task continuations run as *microtasks*, fully drained before the runtime services
*macrotask* / driver events (timers, I/O completions). A task that keeps resolving
always-ready continuations without yielding can starve the macrotask queue and delay
timers/I-O.

Two approaches under consideration:

1. **Hard cooperative budget** (tokio-style): cap consecutive poll steps per loop turn,
   forcing a yield. Deterministic but imposes a global policy and is hard to tune.
2. **`tracing` warning on starvation** (preferred direction): emit a warning when driver
   events are pending but microtask draining has run for an unusually long stretch.
   Non-intrusive, but the detection heuristic is the open design problem — measuring
   "the macrotask queue is being starved" cheaply and without false positives.

No hard budget is implemented for 0.1. Design a reliable, low-overhead starvation signal
before committing to either approach.

## Platform / hardening (from the 0.1 productionization pass)

- Windows follow-ups (see `docs/WINDOWS.md`):
  - Unix-domain sockets via `AF_UNIX` (Windows 10 1803+, stream-only) so
    `runite::net::unix` can exist on Windows.
  - High-resolution runtime timers by associating a
    `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` timer with the completion port via
    `NtAssociateWaitCompletionPacket` (the high-resolution timer kind rejects the
    APC route used today, which is bounded by the ~15.6 ms interrupt period).
  - `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS` on sockets to elide completion packets
    for synchronously-completed operations (needs the documented non-IFS-LSP
    caveat handled).
- Validate the macOS kqueue backend's `unsafe` on a real runner; run the macOS CI job.
- Sanitizers (ASan/TSan) and Miri (needs a mock driver) for the logic crates.
- Linux net data-path readiness fallback (epoll/io_uring hybrid for
  connect/accept/send/recv on kernels with limited io_uring opcode support) — needs CI on
  an io_uring-limited kernel to exercise the fallback.

### Linux dead-code cleanup (pre-release, Linux agent)

The earlier "Tighten public API" pass orphaned some Linux-only internals that are
currently silenced with `#[allow(dead_code)]`. A Linux agent should wire these up or
remove them before release (the `#[allow]` attributes and `TODO(roadmap)` comments mark
every site):

- **Unwired io_uring async-close path.** `FsOp::Close` / `NetOp::Close` are never
  constructed; `sys::linux::{fs,net}::close` (and `close_sync`, `IORING_OP_CLOSE`) are
  never called. Every fd is closed synchronously via `libc::close` in `Drop`. Decide
  whether closing through io_uring (ordered relative to in-flight SQEs on the fd) is
  needed, then either wire it or delete the path. *Consideration:* on io_uring, closing
  an fd with `libc::close` while operations referencing it are still in flight can race;
  `IORING_OP_CLOSE` exists to order the close — confirm the ownership model makes the
  synchronous close safe before deleting.
- **Unused op classifier.** `sys::linux::{fs,net}::execution_path` / `ExecutionPath`
  classify ops into io_uring/offload/inline but are never called (routing is inlined at
  the dispatch sites). Remove, or adopt as the single routing source of truth.
- **Duplicate notifier method.** `ThreadNotifier::notify` (inherent) duplicates the
  `Notifier` trait method that all callers use; delete the inherent method.

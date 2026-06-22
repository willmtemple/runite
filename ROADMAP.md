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

- Validate the macOS kqueue backend's `unsafe` on a real runner; run the macOS CI job.
- Sanitizers (ASan/TSan) and Miri (needs a mock driver) for the logic crates.
- Linux net data-path readiness fallback (epoll/io_uring hybrid for
  connect/accept/send/recv on kernels with limited io_uring opcode support) — needs CI on
  an io_uring-limited kernel to exercise the fallback.

# runite Roadmap

Forward-looking design intent for work deferred past 0.1. Items here are not
bugs and not commitments — they record the direction and the open design
questions so future work starts from a documented position. (The 0.1
release-readiness effort itself is recorded in `docs/RELEASE_PLAN.md`, which
tracks the remaining pre-release performance items.)

## Performance (0.2 focus)

### Batched / deferred io_uring submission

Today every operation submits its SQE immediately (one `io_uring_enter` per
op). Deferring submission and flushing once per loop turn — combining submit
and wait in a single `enter(to_submit, min_complete, GETEVENTS)` — removes a
syscall per operation on hot paths. This carries two correctness follow-ups
that the current immediate-submit design sidesteps: partial-submission handling
(roll back only the un-consumed SQE suffix while keeping accepted operations'
completions alive) and owned timespec storage for timer SQEs that outlive the
submitting stack frame.

### Registered / fixed buffers

runite does not use io_uring fixed buffers; every read/write passes an
ordinary user-space pointer, so the kernel pins the buffer per operation.
`IORING_REGISTER_BUFFERS` plus the fixed-buffer opcodes would remove that
per-op cost, but requires a buffer-pool lifecycle that composes with the
buffer-ownership-across-`Drop` model — careful design and benchmarking first.
Beyond that, in rough order: `buf_ring` + multishot recv/accept, an
owned-buffer API (tokio-uring style, so hot paths can opt out of the staging
copy), and `SEND_ZC`.

### Ring setup flags

`SINGLE_ISSUER`, `COOP_TASKRUN`, `DEFER_TASKRUN`, and `SUBMIT_ALL` all match
runite's one-ring-per-thread model; adopt them (with kernel-version probing),
consider larger rings, and skip redundant timer rearms.

## API growth

### Explicit async close

Both backends close file descriptors synchronously via `OwnedFd`'s `Drop`. An
*awaitable* close (surfacing close errors; on Linux, ordered through
`IORING_OP_CLOSE` relative to in-flight SQEs on the fd) is deferred until the
`Drop`-vs-`await` ordering story is designed. The operation-enum scaffolding
that once reserved space for this was removed pre-0.1; re-adding a `Close`
variant when the design lands is trivial.

### Timer handles and `Drop`

`set_timeout`/`set_interval` return cloneable cancellation *tokens*: dropping
a handle does **not** stop the timer, mirroring JavaScript's
`setInterval`/`clearInterval` (and `JoinHandle`'s detach-on-drop). This
diverges from Rust RAII guard expectations. Open question: keep the token
model only (documented today), or additionally offer an opt-in RAII wrapper
(`cancel_on_drop()`-style) for scope-bound timers.

### Completeness

- `AsyncBufRead`, vectored I/O, and `AsyncSeek` traits.
- Remaining `fs`/`net`/`process`/`sync`/`time` surface gaps relative to mature
  runtimes, added as concrete needs arise rather than speculatively.
- `select!` v2: `else` branches, `biased` mode, more than 16 arms. (Note for
  the redesign: the current macro expands to `async move`, so selecting on
  methods of loop-owned resources requires creating the futures first.)
- `CancellationToken` and `WorkerHandle::join()`.
- Cancel-safe buffered stdin: a dedicated long-lived reader thread feeding a
  buffer, so a dropped `Stdin::read` on the blocking-offload path (macOS, or
  Linux without io_uring stdin support) no longer loses bytes or pins a pool
  worker.

## Scheduler policy

### Microtask starvation: warning vs. budget

A task chain that keeps resolving ready continuations without yielding starves
the macrotask queue (timers, I/O). runite currently emits a `tracing` warning
from inside the drain loop when a single checkpoint crosses 1000 microtasks —
observability without a policy. The open question is whether to also add a
tokio-style hard cooperative budget (cap consecutive polls per turn, forcing a
yield): deterministic, but a global policy that is hard to tune and changes
JS-equivalent ordering. Design a reliable, low-overhead starvation signal
before committing.

## Platform & hardening

- **Windows backend (IOCP)**, with thread offload where IOCP does not apply.
  Descriptor interop for it lands as `AsSocket`/`AsHandle` under
  `#[cfg(windows)]`, parallel to the existing `#[cfg(unix)]` fd interop.
- Sanitizers (ASan/TSan) in CI, and Miri for the logic crates (requires a mock
  driver).
- CI on an io_uring-limited kernel, so the readiness/synchronous fallbacks are
  exercised end-to-end rather than only by unit tests.
- macOS child-process wait currently busy-polls; move it to a kqueue
  `EVFILT_PROC` watch.
- `ReadDir`'s blocking-pool producer is unbounded; bound it.

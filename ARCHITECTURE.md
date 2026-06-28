*This document describes both current and intended future behavior; intended behavior is marked with `[future]`.*

# Overview

`runite` is a single-threaded async runtime with explicit worker-thread support,
JavaScript-style microtask/macrotask scheduling, and platform-specific I/O drivers. It is meant for
UI front-ends, embedded event loops, and fine-grained reactive systems that need deterministic flush
points between input and rendering. It is deliberately not a general-purpose high-throughput server
runtime: it prefers simple per-thread event loops, local state, and predictable scheduling over
work-stealing, `Send`-future ergonomics, and maximum I/O throughput.

# Threading model

- A runtime thread owns one `ThreadState`, one scheduler, one timer heap, and one driver.
- `spawn_worker` creates a new OS thread, installs a separate runtime state on it, queues the
  initial macrotask, and runs that worker loop (`src/platform/linux/runtime.rs`).
- Linux: each runtime thread owns its own `io_uring` ring and `ThreadNotifier`; the
  notifier wakes the ring with `IORING_OP_MSG_RING`
  (`src/platform/linux/driver.rs`).
- macOS `aarch64`: each runtime thread owns a `kqueue` plus a nonblocking wake pipe
  (`src/platform/macos_aarch64/driver.rs`).
- `ThreadHandle::queue_task` is the cross-thread boundary. It accepts only `Send` closures, pushes
  into the target thread's remote macrotask queue, and wakes the target driver
  (`src/platform/linux/runtime.rs`).

What `!Send` means here:

- Most reactive values are intended to stay on their originating runtime thread.
- Timer and interval handles carry `Rc<()>` plus a raw owner pointer, so they are `!Send`
  (`src/platform/linux/runtime.rs`).
- `JoinHandle<T>` stores `Rc<JoinState<T>>`, so it is `!Send`
  (`src/platform/linux/runtime.rs`).
- `current_thread()` returns thread-local runtime state; callers must not move that state or values
  tied to it across threads.
- Cross-thread work must be expressed as `FnOnce() + Send + 'static` and submitted through
  `ThreadHandle::queue_task`.

Lazy initialization contract:

- Today, `current_thread()` lazily creates a driver, notifier, shared state, and `ThreadState` when
  called on a thread with no installed runtime (`src/platform/linux/runtime.rs`).
- Any helper that calls `current_thread()` from a non-runtime thread can therefore instantiate a
  full runtime state and, on Linux, an `io_uring` ring on that thread.
- `current_thread_handle()` also initializes state because it calls `current_thread().handle()`
  (`src/platform/linux/runtime.rs`).
- **[future]** Runtime state will be held as `Rc<ThreadState>` and accessed through scoped accessors
  such as `with_current_thread(|state| ...)`. The runtime will not auto-initialize from arbitrary
  helper calls, and `current_thread_handle` will return `Option<ThreadHandle>` when no runtime is
  installed.

## Scaling across cores

`runite` scales by running more independent event loops, not by making one scheduler shared. Each
runtime thread owns its own scheduler queues, timer heap, and platform I/O driver (`io_uring` ring on
Linux, `kqueue` on macOS). There is no work-stealing layer between runtime threads.

Futures are thread-local and deliberately `!Send`: a task queued on one runtime thread is always
polled, woken, aborted, and joined on that same thread. Tasks never migrate between threads. This is a
trade-off: applications do not get cross-thread future ergonomics or automatic load balancing, but the
hot path stays free of scheduler synchronization and scheduling remains predictable.

To use multiple cores, the application starts one event loop per core with `spawn_worker`. Each worker
installs its own runtime state and runs independently. Cross-thread coordination is explicit and goes
through the `Send`-only boundary: `ThreadHandle::queue_task` accepts `FnOnce() + Send + 'static`,
enqueues a macrotask on the target worker, and wakes that worker's driver.

For network servers, the idiomatic multi-core shape is `SO_REUSEPORT`: start one worker per core, let
each worker bind its own listener to the same address, and run an independent accept loop on each
worker. The kernel then load-balances incoming connections across those per-core listeners without a
shared accept lock or a userspace dispatch thread.

# Task scheduling — micro vs macro tasks

Microtasks:

- Are strictly thread-local; remote threads cannot enqueue them
  (`src/platform/linux/runtime.rs`).
- Run to completion between macrotasks.
- Drive future continuations: `FutureTask::schedule` calls `queue_microtask`, and the waker vtable
  reschedules the same local task (`src/platform/linux/runtime.rs`).
- Match JavaScript Promise microtasks.

Macrotasks:

- May be local (`queue_macrotask`) or remote (`ThreadHandle::queue_task`).
- Carry I/O completion callbacks, timer expirations, worker-exit callbacks, and host/event callbacks.
- Remote macrotasks are swapped into the local macrotask queue during `drain_remote_tasks`
  (`src/platform/linux/runtime.rs`).
- Expired timers dispatch by pushing timer callbacks into the macrotask queue
  (`src/platform/linux/runtime.rs`).
- Worker exits enqueue their `on_exit` callback as a macrotask
  (`src/platform/linux/runtime.rs`).

## Backpressure

Local microtask and macrotask queues are intentionally unbounded: they are owned by one runtime
thread, and runaway local production is a scheduler bug rather than a cross-thread backpressure
problem. The cross-thread remote macrotask queue is bounded to 65,536 entries by default, tunable
with `RUNITE_REMOTE_QUEUE_CAPACITY` (parsed as `usize`, minimum 1, capped at a sane maximum). When it
is full, `ThreadHandle::queue_task` returns `Err(QueueError::Full)` immediately. The runtime does
not block the producer and does not silently drop the task; the caller must choose whether to retry,
drop, or fail. The first overflow per runtime thread emits a scheduler warning for observability
without spamming logs during overload.

Per-turn ordering in `run()`:

1. Drain driver events.
2. Drain remote tasks.
3. Flush completed workers.
4. Run all microtasks until empty, draining driver/remote/worker events after each microtask.
5. Warn if a microtask flush reaches 1000 tasks.
6. Run one macrotask.
7. Repeat.

This is implemented by `drain_all()`, the microtask loop, and the single `pop_macrotask()` in
`run()` (`src/platform/linux/runtime.rs`). The same starvation threshold is shared in
`runtime_shared` (`src/platform/runtime_shared/mod.rs`).

Why this shape exists:

- It gives a deterministic flush point between input handling and rendering.
- It lets a fine-grained reactive UI settle all same-turn future continuations before one external
  event callback mutates the world again.
- It prevents cross-thread senders from injecting microtasks into the middle of a local microtask
  flush.

Guideline:

- Schedule continuations of in-thread work as microtasks.
- Schedule everything originating from I/O or another thread as a macrotask.
- Local-thread I/O completions wake as microtasks even though they originate in
  the driver, because they have a same-thread observer
  (`CompletionState::queue_wake` short-circuits via `ThreadHandle::is_current`
  to `queue_microtask`). Cross-thread completions stay macrotasks.

Minimal shape:

```rust
queue_microtask(|| poll_same_thread_future());
thread_handle.queue_task(|| handle_remote_or_io_event());
```

# Run lifecycle

`run()`:

- Is the canonical runtime loop.
- Runs until no ready tasks, timers, live child workers, or pending async operations remain
  (`src/platform/linux/runtime.rs`).
- Blocks in `driver.wait()` when timers, children, or async operations still exist but no work is
  ready (`src/platform/linux/runtime.rs`).

`run_until_stalled()`:

- Drains all currently ready work.
- Does not block for future driver events.
- Returns when no immediately runnable work remains, and clears `closing`
  (`src/platform/linux/runtime.rs`).
- Intended for tests and host-loop integrations that own the outer wait.

`run_ready_tasks()`:

- Drains remote tasks, completed workers, microtasks, and local macrotasks.
- Does not poll the driver and therefore does not re-enter timers or I/O readiness
  (`src/platform/linux/runtime.rs`).
- Intended for host callbacks that need to flush application work without re-entering timer or I/O
  callbacks.

## Shutdown commit protocol

`run()` uses a two-phase commit so no remote task can be accepted and then stranded after the event
loop exits:

1. Drain everything currently observable.
2. If ready work exists, keep running.
3. Set `closing = true` with a CAS via `try_begin_shutdown`
   (`src/platform/linux/runtime.rs`).
4. Drain again.
5. If any ready work appeared, clear `closing` and continue
   (`src/platform/linux/runtime.rs`).
6. If timers, child workers, or async operations remain, clear `closing`, wait on the driver, and
   continue (`src/platform/linux/runtime.rs`).
7. Otherwise take the remote-queue lock.
8. While holding that lock, if the remote queue is still empty, set `closed = true`
   (`src/platform/linux/runtime.rs`).
9. `ThreadShared::enqueue_macro` checks `closed` under the same lock before accepting a task, so the
   exit path and the remote enqueue path are mutually ordered
   (`src/platform/linux/runtime.rs`).
10. If the queue was not empty, clear `closing` and process the newly arrived work.
11. If the thread is a worker, mark its completion finished and notify the parent
    (`src/platform/linux/runtime.rs`).
12. Notify the local driver, tear down TLS state, and return
    (`src/platform/linux/runtime.rs`).

# Cancellation semantics

Drop is the I/O cancellation primitive today. Aborting a queued future uses the same mechanism:
`JoinHandle::abort` marks the task aborted, drops the task future on its owning runtime thread, and
wakes any joiner. Awaiting a `JoinHandle<T>` yields `Result<T, JoinError>`; an aborted task resolves to
`Err(JoinError::Aborted)` (`src/platform/runtime_shared/future_task.rs`,
`src/platform/runtime_shared/handles.rs`).

Dropping a `CompletionFuture`:

- Sets `interested = false` so later completions do not store a result or queue a wake
  (`src/op/completion.rs`).
- Clears any stored result and waker.
- If the operation is unfinished and has a cancel callback, runs that callback.
- Linux cancel callbacks submit `IORING_OP_ASYNC_CANCEL` through the driver
  (`src/platform/linux/driver.rs`,
  `src/sys/linux/fs.rs`, `src/sys/linux/net.rs`).
- If no cancel callback exists because submission failed before registration, the future decrements
  the pending operation count directly (`src/op/completion.rs`).

There is no separate cancellation plumbing for in-flight I/O. Dropping an async task cascades normal
Rust `Drop` through the suspended future tree; any in-flight operation future dropped by that cascade
runs the same completion cancel callback and, on Linux, submits `IORING_OP_ASYNC_CANCEL`.

What Drop does not mean:

- It does not guarantee the kernel or OS operation stopped.
- The operation may still complete after the user-visible future is dropped.
- The runtime discards that result because `interested` is false.

Implication for buffer ownership:

- Any buffer the kernel may read from or write into must outlive the I/O even after Drop returns.
- The current implementation satisfies this by moving owned staging buffers into completion/cancel
  guards as described below.

**[future]** An explicit `CancellationToken` plus `select!`/`with_cancellation`-style APIs will be
added in phase 9 so cancellation can be expressed without relying only on Drop.

# I/O buffer ownership rules

The public API uses borrowed buffers:

- `File::read(&mut [u8])`, `TcpStream::read(&mut [u8])`, `UdpSocket::recv(&mut [u8])`, and
  `Stdin::read(&mut [u8])` return the byte count written into the caller's buffer.
- `File::write(&[u8])`, `TcpStream::write(&[u8])`, and UDP send APIs return the byte count consumed.
- `File::read_to_end` and `File::read_to_string` are convenience helpers that allocate internally.

For v1, Linux does **not** point io_uring SQEs at arbitrary caller-owned `&mut [u8]` memory. That
would be unsound on Drop: after the future is dropped, the caller's borrow ends, but the kernel may
still write until the original CQE or an acknowledged `IORING_OP_ASYNC_CANCEL`. Instead, the runtime
uses the conservative "option A" staging model:

- Read operations allocate an internal boxed byte buffer, submit that stable allocation to io_uring,
  and copy the completed bytes into the caller's slice before returning.
- Write operations copy the caller's slice into an internal owned buffer before submission so the
  kernel can keep reading from it after the user-visible future is dropped.
- On normal completion, the operation callback drops the internal buffer after mapping the CQE.
- On Drop before completion, the cancel callback submits `IORING_OP_ASYNC_CANCEL` and detaches a
  guard into the Linux driver's `pending_cancel_buffers` map, keyed by the original operation token.
  `pending_cancel_tokens` maps the cancel SQE token back to that original token. The driver drops
  guards when either the original operation CQE or the cancel CQE arrives.

This is sound for arbitrary borrowed buffers, but it is not zero-copy. A future hot-path API can add
an `OwnedBuf`/registered-buffer style (tokio-uring-like) option that transfers ownership of a stable
allocation to the operation and returns it on completion. Registered buffers remain outside the
current phase scope.

# Subprocesses

On Linux, child process exit is represented as fd readiness. `Command::spawn` opens a pidfd for the
child, and `Child::wait` parks on `wait_readable(pidfd)`, which submits `IORING_OP_POLL_ADD` through
the thread's io_uring driver before collecting the status with `waitpid`
(`src/sys/linux/process.rs`, `src/sys/linux/fd.rs`). There is no SIGCHLD handler and no blocking-pool
offload for child exit.

Pipes attached to child stdin/stdout/stderr use the same platform byte-stream paths as other fds:
Linux goes through the runtime-owned-buffer I/O path plus readiness where needed, while macOS uses the
existing kqueue/blocking-pool split for its backend.

# Driver abstraction (current and [future])

Current:

- Linux and macOS have parallel `Driver` types with similar surfaces but different internals.
- Linux uses `io_uring` for timers, wake notifications, fs ops, network ops, fd readiness, and close
  where supported (`src/platform/linux/driver.rs`).
- macOS uses `kqueue` for the runtime wait/wake path, timers, and fd readiness; filesystem work is
  offloaded to a blocking pool (`src/platform/macos_aarch64/driver.rs`,
  `src/sys/macos/fs.rs`).
- Both platform `runtime.rs` files duplicate scheduler, timer, JoinHandle, FutureTask, ThreadShared,
  and waker-vtable logic.

**[future]** A `DriverBackend` trait will define only the platform-specific surface:

```rust
trait DriverBackend {
    type Notifier: Clone + Send + Sync + 'static;

    fn poll(&self) -> io::Result<Option<ReadyEvents>>;
    fn wait(&self) -> io::Result<()>;
    fn rearm_timer(&self, deadline: Option<Duration>) -> io::Result<()>;
    fn drain_wake(&self) -> io::Result<u64>;
    fn drain_timer(&self) -> io::Result<u64>;
    fn submit_operation(&self, op: Operation) -> io::Result<OperationToken>;
    fn cancel_operation(&self, token: OperationToken) -> io::Result<()>;
}
```

**[future]** The scheduler, timer heap, `JoinHandle`, `FutureTask`, `ThreadShared`, macrotask queue,
and waker vtable will live once in `runtime_shared` and be generic over `DriverBackend`.
Per-platform `runtime.rs` files will become thin re-exports that pick the backend.

## Feature probing

On Linux x86_64, `Driver::create_driver` initializes an `io_uring` ring and records the
process-wide `IORING_REGISTER_PROBE` result from `src/platform/linux/uring.rs`.
The supported-op bitmap is cached behind a `OnceLock` because kernel opcode support cannot change
under a running process, and probing once per runtime thread would waste syscalls.

If `io_uring_setup(2)` reports that io_uring is unavailable (`ENOSYS`) or blocked (`EPERM`, usually
seccomp), driver creation returns an `io::ErrorKind::Unsupported` with a named error value so callers
can choose a fallback backend. If `IORING_REGISTER_PROBE` itself is unavailable on very old kernels,
the driver logs one warning during cache initialization and uses a permissive bitmap; submission is
then allowed to preserve compatibility with kernels that support io_uring but not probing. When a
probe bitmap is available, `submit_operation` rejects unsupported `IORING_OP_*` values with
`io::ErrorKind::Unsupported` before the SQE reaches the kernel.

# Platform parity matrix

| Capability | Linux io_uring path | macOS path |
| --- | --- | --- |
| open | `IORING_OP_OPENAT` | blocking pool (`std::fs::OpenOptions`) |
| read | `IORING_OP_READ` into guarded internal buffer, then copy into caller slice | blocking pool (`read`/`pread`) |
| write | `IORING_OP_WRITE` from guarded internal buffer | blocking pool (`write`/`pwrite`) |
| metadata | `IORING_OP_STATX` | blocking pool (`metadata`/`fstat`) |
| sync | `IORING_OP_FSYNC` | blocking pool (`fsync`/`F_FULLFSYNC`) |
| set_len | `IORING_OP_FTRUNCATE` | blocking pool (`ftruncate`) |
| try_clone | inline `fcntl(F_DUPFD_CLOEXEC)` (never blocks) | blocking pool |
| read_dir | offloaded streaming producer (`getdents` can block, no io_uring opcode) | blocking pool producer |
| close | `IORING_OP_CLOSE`, inline `close(2)` fallback | blocking pool / synchronous close helper |
| network ops | `io_uring` first; non-blocking control ops fall back inline, data-path ops fall back to a non-blocking readiness path (`IORING_OP_POLL_ADD`) on unsupported kernels — never the blocking pool | `kqueue` readiness plus synchronous nonblocking socket calls |
| Unix domain sockets | stream/datagram APIs reuse guarded send/recv paths plus readiness for path-addressed ops | stream/datagram APIs use the same guarded send/recv and readiness path |
| stdin | Linux tries `IORING_OP_READ`, then per-call blocking fallback | blocking fallback path |
| child exit | pidfd readiness via `IORING_OP_POLL_ADD`; no SIGCHLD handler or blocking-pool offload | process wait backend |
| wait_readable | `IORING_OP_POLL_ADD` | `kqueue` `EVFILT_READ` one-shot |

Notes:

- Linux fs `read_dir` streams from the blocking pool (`getdents` can block on disk and has no
  io_uring opcode); `try_clone` runs the non-blocking `fcntl(F_DUPFD_CLOEXEC)` inline on the
  event-loop thread rather than offloading (`src/sys/linux/fs.rs`).
- Linux network operations use io_uring first. When an opcode is unsupported on the running
  kernel, the *non-blocking* control operations (socket, bind, listen, shutdown, close, dup) run
  inline on the event loop, and the *data-path* operations that can block (connect, accept, send,
  recv, datagram recv) fall back to a **readiness path** — they mark the fd non-blocking and park
  on `IORING_OP_POLL_ADD` readiness (`wait_readable`/`wait_writable`) rather than offloading to
  the blocking pool, mirroring the macOS and Unix-domain-socket model (`src/sys/linux/net.rs`).
  The blocking pool is reserved for genuinely synchronous-only work (DNS resolution via
  `getaddrinfo`, `read_dir`/`getdents`). On a modern kernel the io_uring completion path always
  wins; the readiness fallback is validated by a direct unit test that exercises it explicitly.
- macOS has no io_uring equivalent. Its filesystem backend is entirely blocking-pool-based
  (`src/sys/macos/fs.rs`).
- macOS network behavior is readiness-driven, not completion-driven; performance characteristics
  differ substantially from Linux io_uring (`src/sys/macos/net.rs`).

# Cross-thread completion path

Current path:

1. A backend creates a `CompletionFuture`/`CompletionHandle` pair for the current thread
   (`src/op/completion.rs`).
2. The owner is captured as a `ThreadHandle`.
3. On completion, `CompletionHandle::finish` stores the result if `interested` is still true and
   calls `CompletionState::queue_wake` (`src/op/completion.rs`).
4. `queue_wake` queues a macrotask on the owner with `ThreadHandle::queue_task`
   (`src/op/completion.rs`).
5. `ThreadHandle::queue_task` locks the target remote queue, pushes the task, and calls
   `ThreadShared::notify` (`src/platform/linux/runtime.rs`).
6. On Linux, the notifier submits `IORING_OP_MSG_RING` to the target ring
   (`src/platform/linux/driver.rs`,
   `src/platform/linux/uring.rs`).
7. The target driver receives the wake CQE, records a pending wake, and the scheduler drains remote
   tasks (`src/platform/linux/driver.rs`,
   `src/platform/linux/runtime.rs`).

This adds a syscall per wake on Linux. That is acceptable for cross-thread completions, where there
is no cheaper way to wake the owner reliably.

Same-thread completions short-circuit this path: `CompletionState::queue_wake` calls
`ThreadHandle::is_current` (`src/platform/runtime_shared/handles.rs`) — which
`Arc::ptr_eq`s the handle's `ThreadShared` against the current thread's installed state — and on a
match enqueues a microtask via `queue_microtask` directly. `is_current` returns `false` when the
caller has no runtime state installed (e.g. a blocking-pool worker), so non-runtime threads safely
fall through to the macrotask path. Only true cross-thread completions use `queue_macrotask` and
`MSG_RING`.

# Safety invariants

Current, precarious invariants:

- Runtime TLS uses `thread_local!(static CURRENT_THREAD: ...)` via the scoped
  accessor `with_current_thread` (`src/platform/runtime_shared/state.rs`).
- Accessors are scoped: `with_current_thread(|state| ...)` and
  `try_with_installed_thread`. No `'static` references to `ThreadState` escape
  the runtime crate; downstream code never holds a borrow across an `await`.
- The runtime crate itself is **stable-Rust clean** — no `#![feature(...)]`. `runite` does
  not depend on any nightly-only crate; it builds on stable Rust.
- `TimeoutHandle`/`IntervalHandle` carry a process-wide generation token
  (`NEXT_GENERATION: AtomicU64`) so stale handles from a destroyed
  `ThreadState` can never false-match a freshly-installed one
  (`src/platform/runtime_shared/handles.rs`).
- `IoUring` has `unsafe impl Send` with a precise safety comment
  (`src/platform/linux/uring.rs`): the ring is moved
  into `Driver` during construction and remains pinned to that runtime thread;
  the submitter TLS pointer is per-thread.
- io_uring SQ tail and CQ head publication use real `atomic::fence(Release)`
  and `atomic::fence(Acquire)` around the kernel-visible boundaries
  (`src/platform/linux/uring.rs`).

# What this runtime is NOT (and the reasons)

- Not a general-purpose server runtime.
  - No registered buffers.
  - No fixed files.
  - No vectored I/O.
  - No splice.
  - These are intentionally outside the current top-10 refactor scope.
- Not a multi-threaded work-stealing runtime.
  - Each runtime thread owns its own queues and driver.
  - Cross-thread work is explicit through `ThreadHandle::queue_task`.
  - This preserves deterministic UI/reactive ordering.
- Not `Send`-future friendly.
  - Futures queued with `spawn` only need `Future + 'static`, not `Send`.
  - Handles such as `JoinHandle`, `TimeoutHandle`, and `IntervalHandle` are `!Send` by design.
- Nightly-toolchain-free in `runite` itself.
  - The runtime crate compiles on stable Rust with no `#![feature(...)]`.

# Glossary

microtask
: A thread-local continuation queue drained fully before one macrotask runs. Used for same-thread
  future wakeups and Promise-like continuation semantics.

macrotask
: A local or remote event callback queue. Used for I/O completions today, timer callbacks, worker
  exits, host callbacks, and cross-thread work.

driver
: The per-runtime-thread platform event backend. Linux uses `io_uring`; macOS uses `kqueue` plus a
  wake pipe and blocking pools for fs.

ring
: A Linux `io_uring` instance with submission and completion queues. Each Linux runtime thread owns
  its own ring.

notifier
: A cloneable handle that wakes another runtime thread's driver. Linux uses `MSG_RING`; macOS writes
  to a wake pipe.

ThreadHandle
: A cloneable, `Send`-capable handle for queueing a `Send` macrotask onto a specific runtime
  thread.

JoinHandle
: The `!Send` future returned by `spawn`; awaiting it yields `Result<T, JoinError>`, with
  `Err(JoinError::Aborted)` after `abort`.

completion
: The `CompletionFuture`/`CompletionHandle` pair that bridges backend operation completion to a
  future waker on the owning runtime thread.

CQE
: Completion Queue Entry. Kernel/driver result record for a submitted operation.

SQE
: Submission Queue Entry. Kernel-visible request record for an operation.

MSG_RING
: Linux io_uring operation used to send a wake message to another ring.

parking/wait
: Blocking the runtime thread in its driver (`driver.wait()`) because no work is ready but timers,
  children, or async operations remain.

# Async I/O traits

The runtime exposes crate-local `io::AsyncRead` and `io::AsyncWrite` traits as the byte-stream
polling primitives for borrowed-buffer I/O. `File`, `TcpStream`, and `Stdin` implement `AsyncRead`;
`File` and `TcpStream` implement `AsyncWrite`. `UdpSocket` intentionally does not implement these
traits because datagram sockets preserve message boundaries rather than stream semantics. The
crate-local `io::Stream` trait provides the same poll-based shape for asynchronous item streams;
directory iterators and MPSC receivers implement it, and `AsyncReadExt::lines` adapts any
`AsyncRead` into a `Stream<Item = io::Result<String>>`.

The extension traits in `io` provide concrete future adapters (`Read`, `ReadExact`, `ReadToEnd`,
`Write`, `WriteAll`, `Flush`, `Close`, `Next`, `Collect`, and `ForEach`) and stream adapters (`Map`,
`Filter`, `Take`, and `Skip`) that repeatedly drive the poll methods. Implementations submit
operations with runtime-owned buffers, preserving the phase-7 cancellation rule: after a
borrowed-buffer operation returns `Pending`, the kernel-visible buffer is owned by the runtime and is
kept alive by the existing completion/cancel guard path until the original completion or cancel CQE
arrives.

With the optional `futures-compat` feature, `io::compat::Compat<T>` adapts runtime async I/O types to
`futures_io` traits, and `io::compat::FuturesCompat<T>` adapts `futures_io` types back to the runtime
traits.

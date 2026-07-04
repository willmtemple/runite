*This document describes the runtime as implemented for 0.1. Forward-looking design intent lives in `ROADMAP.md`.*

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
  initial macrotask, and runs that worker loop (`src/platform/runtime_shared/scheduler.rs`).
- Linux: each runtime thread owns its own `io_uring` ring and `ThreadNotifier`; the
  notifier wakes the ring with `IORING_OP_MSG_RING`
  (`src/platform/linux/driver.rs`).
- macOS `aarch64`: each runtime thread owns a `kqueue` plus a nonblocking wake pipe
  (`src/platform/macos_aarch64/driver.rs`).
- Windows: each runtime thread owns an I/O completion port plus a waitable timer; the
  notifier wakes the port with `PostQueuedCompletionStatus`
  (`src/platform/windows/driver.rs`, `docs/WINDOWS.md`).
- `ThreadHandle::queue_macrotask` is the cross-thread boundary. It accepts only `Send` closures,
  pushes into the target thread's remote macrotask queue, and wakes the target driver
  (`src/platform/runtime_shared/handles.rs`, `src/platform/runtime_shared/state.rs`).

What `!Send` means here:

- Most reactive values are intended to stay on their originating runtime thread.
- `JoinHandle<T>` and `AbortHandle` store `Rc`s, so they are `!Send`
  (`src/platform/runtime_shared/handles.rs`).
- `TimeoutHandle`/`IntervalHandle` are plain `{id, generation}` cancellation tokens and *are*
  sendable; cancelling from a thread other than the creator is a documented no-op (the generation
  check fails), not a panic.
- A task's `Waker`, by `std` contract, **is** `Send + Sync`: it carries an `Arc<TaskWaker>` holding
  only the owning `ThreadHandle` and a numeric task id, and resolves the `!Send` task through the
  owner thread's registry (`src/platform/runtime_shared/future_task.rs`). Leaf futures may therefore
  move `cx.waker()` to other threads freely; the task itself still never migrates.
- Cross-thread work must be expressed as `FnOnce() + Send + 'static` and submitted through
  `ThreadHandle::queue_macrotask`.

Lazy initialization contract:

- Runtime state is reached only through scoped accessors — `with_current_thread(|state| ...)`
  (lazy-initializing) and `with_installed_thread`/`try_with_installed_thread` (non-initializing) —
  in `src/platform/runtime_shared/state.rs`; no `'static` reference to `ThreadState` escapes.
- Public entry points use the lazy accessor, so any of them (including `current_thread_handle()`)
  called on a thread with no installed runtime creates a driver, notifier, shared state, and
  `ThreadState` — on Linux, that includes an `io_uring` ring. Whether auto-initialization should
  be narrowed (e.g. `current_thread_handle` returning `Option`) remains an open design question.

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
through the `Send`-only boundary: `ThreadHandle::queue_macrotask` accepts
`FnOnce() + Send + 'static`, enqueues a macrotask on the target worker, and wakes that worker's
driver.

For network servers, the idiomatic multi-core shape on Linux and macOS is `SO_REUSEPORT`: start one
worker per core, let each worker bind its own listener to the same address, and run an independent
accept loop on each worker. The kernel then load-balances incoming connections across those per-core
listeners without a shared accept lock or a userspace dispatch thread. Windows has no `SO_REUSEPORT`
(`TcpSocket::set_reuseport` reports `Unsupported` there); a single accept loop that distributes
connections to workers over `ThreadHandle::queue_macrotask` is the portable alternative.

# Task scheduling — micro vs macro tasks

Microtasks:

- Are strictly thread-local; remote threads cannot enqueue them
  (`src/platform/runtime_shared/state.rs`).
- Run to completion between macrotasks.
- Drive future continuations: a wake looks the task up by id in the owner thread's registry and
  `FutureTask::schedule` enqueues one poll microtask, deduplicated while a poll is already queued
  (`src/platform/runtime_shared/future_task.rs`).
- Match JavaScript Promise microtasks.

Macrotasks:

- May be local (`queue_macrotask`) or remote (`ThreadHandle::queue_macrotask`).
- Carry I/O completion callbacks, timer expirations, worker-exit callbacks, and host/event callbacks.
- Remote macrotasks are swapped into the local macrotask queue during `drain_remote_tasks`
  (`src/platform/runtime_shared/scheduler.rs`).
- Expired timers dispatch by pushing timer callbacks into the macrotask queue; worker exits enqueue
  their `on_exit` callback as a macrotask (`src/platform/runtime_shared/scheduler.rs`).

## Backpressure

Local microtask and macrotask queues are intentionally unbounded: they are owned by one runtime
thread, and runaway local production is a scheduler bug rather than a cross-thread backpressure
problem. The cross-thread remote macrotask queue is bounded to 65,536 entries by default, tunable
with `RUNITE_REMOTE_QUEUE_CAPACITY` (parsed as `usize`, minimum 1, capped at a sane maximum). When it
is full, `ThreadHandle::queue_macrotask` returns `Err(QueueError::Full)` immediately. The runtime
does not block the producer and does not silently drop the task; the caller must choose whether to
retry, drop, or fail. The first overflow per runtime thread emits a scheduler warning for
observability without spamming logs during overload.

The capacity limit applies to **user** tasks only. Internal wakes — an I/O/channel completion whose
result is already stored, or a spawned task's waker firing from another thread — route through a
capacity-bypassing path (`ThreadShared::enqueue_internal_wake`), because dropping one would strand
its target forever (a lost wakeup). Their count is naturally bounded: one coalesced pending wake per
in-flight operation or live task, so the bypass cannot grow the queue without matching outstanding
work.

Per-turn ordering in `run()`:

1. Drain driver events (I/O completions, expired timers).
2. Drain remote tasks.
3. Flush completed workers.
4. Run all microtasks until the queue is empty (one checkpoint). The driver is **not** re-polled
   between microtasks: everything the driver produces is a macrotask, so it cannot run
   mid-checkpoint anyway, and polling once per turn instead of once per microtask removes a
   syscall per microtask without changing observable ordering.
5. Run one macrotask.
6. Repeat.

If a single checkpoint crosses 1000 microtasks, a starvation warning is emitted from *inside* the
drain loop — a checkpoint that never empties (the pathology the warning exists for) would never
reach an after-the-loop check. Implemented by `drain_all()`, `drain_microtasks()`, and the single
`pop_macrotask()` per turn (`src/platform/runtime_shared/scheduler.rs`).

Why this shape exists:

- It gives a deterministic flush point between input handling and rendering.
- It lets a fine-grained reactive UI settle all same-turn future continuations before one external
  event callback mutates the world again.
- It prevents cross-thread senders from injecting microtasks into the middle of a local microtask
  flush.

Guideline:

- Schedule continuations of in-thread work as microtasks.
- Schedule everything originating from I/O or another thread as a macrotask.
- Each completion carries an explicit wake class chosen by its creator
  (`WakeClass::Microtask` for in-process resolutions — channel sends, `Notify` — and
  `WakeClass::Macrotask` for I/O completions). A same-thread I/O completion therefore wakes as a
  **local macrotask** (a poll-phase callback, like Node), never mid-checkpoint; same-thread channel
  resolutions join the current microtask checkpoint (the `Promise.resolve` analog); cross-thread
  completions are always macrotasks (`src/op/completion.rs`).

Minimal shape:

```rust
queue_microtask(|| resolve_same_thread_promise());
thread_handle.queue_macrotask(|| handle_remote_or_io_event());
```

# Run lifecycle

`run()`:

- Is the canonical run-to-idle loop.
- Runs until no ready tasks, timers, live child workers, or pending async operations remain
  (`src/platform/runtime_shared/scheduler.rs`).
- Blocks in `driver.wait()` when timers, children, or async operations still exist but no work is
  ready.

`block_on(future)`:

- Drives the same per-turn machinery but exits as soon as the given future resolves, returning its
  output; other spawned tasks stay queued for a later driver call.
- Polls the future in place (stack-pinned) with a `Send + Sync` waker, so the future may borrow
  locals and need not be `Send` or `'static` (`src/platform/runtime_shared/scheduler.rs`).

`run_until_stalled()`:

- Drains all currently ready work.
- Does not block for future driver events.
- Returns when no immediately runnable work remains, and clears `closing`
  (`src/platform/runtime_shared/scheduler.rs`).
- Intended for tests and host-loop integrations that own the outer wait.

`run_ready_tasks()`:

- Drains remote tasks, completed workers, microtasks, and local macrotasks.
- Does not poll the driver and therefore does not re-enter timers or I/O readiness
  (`src/platform/runtime_shared/scheduler.rs`).
- Intended for host callbacks that need to flush application work without re-entering timer or I/O
  callbacks.

All four drivers set an `in_event_loop` flag for their duration; re-entering any of them from inside
a task or callback already running on the loop panics rather than double-driving the same queues.
For `run`/`run_until_stalled`/`run_ready_tasks` that panic is absorbed by the per-task firewall
(the offending task resolves to `JoinError::Panicked`); `block_on` is a direct driver, so it
propagates to the caller.

## Panic isolation

A panic must never tear down the event loop that observes it:

- `FutureTask::poll` wraps the future poll in `catch_unwind`; a panicking task is dropped,
  deregistered, and its joiner resolves to `Err(JoinError::Panicked)`
  (`src/platform/runtime_shared/future_task.rs`).
- Every other scheduled closure (timer/interval callbacks, `queue_*` closures, worker `on_exit`)
  runs through the `run_guarded` firewall; a panic is logged and the loop continues.
- Blocking-pool jobs are firewalled in the worker loop so one panicking job cannot retire a pool
  thread; `spawn_blocking` additionally converts its closure's panic into `JoinError::Panicked`
  for the awaiter.
- A worker thread whose loop somehow unwinds still marks its `WorkerCompletion` finished and
  notifies the parent, so a dead worker can never hang the thread waiting on it.
- The shutdown probe in `run()` holds a scope guard that restores the `closing` flag on any
  non-committed exit, including a panic unwind.

In all cases the panic is still reported through the process panic hook.

## Shutdown commit protocol

`run()` uses a two-phase commit so no remote task can be accepted and then stranded after the event
loop exits:

1. Drain everything currently observable.
2. If ready work exists, keep running.
3. Set `closing = true` with a CAS via `try_begin_shutdown`
   (`src/platform/runtime_shared/state.rs`).
4. Drain again.
5. If any ready work appeared, clear `closing` and continue
   (`src/platform/runtime_shared/scheduler.rs`).
6. If timers, child workers, or async operations remain, clear `closing`, wait on the driver, and
   continue (`src/platform/runtime_shared/scheduler.rs`).
7. Otherwise take the remote-queue lock.
8. While holding that lock, if the remote queue is still empty, set `closed = true`
   (`src/platform/runtime_shared/scheduler.rs`).
9. `ThreadShared::enqueue_macro` checks `closed` under the same lock before accepting a task, so the
   exit path and the remote enqueue path are mutually ordered
   (`src/platform/runtime_shared/scheduler.rs`).
10. If the queue was not empty, clear `closing` and process the newly arrived work.
11. If the thread is a worker, mark its completion finished and notify the parent
    (`src/platform/runtime_shared/scheduler.rs`).
12. Notify the local driver, tear down TLS state, and return
    (`src/platform/runtime_shared/scheduler.rs`).

# Cancellation semantics

Drop is the I/O cancellation primitive today. Aborting a queued future uses the same mechanism:
`JoinHandle::abort` marks the task aborted, drops the task future on its owning runtime thread, and
wakes any joiner. Awaiting a `JoinHandle<T>` yields `Result<T, JoinError>`; an aborted task resolves
to `Err(JoinError::Aborted)` and a task that panicked while polled resolves to
`Err(JoinError::Panicked)` (`src/platform/runtime_shared/future_task.rs`,
`src/platform/runtime_shared/handles.rs`).

Dropping a `CompletionFuture`:

- Sets `interested = false` so later completions do not store a result or queue a wake
  (`src/op/completion.rs`).
- Clears any stored result and waker.
- If the operation is unfinished and has a cancel callback, runs that callback.
- Linux cancel callbacks submit `IORING_OP_ASYNC_CANCEL` through the driver
  (`src/platform/linux/driver.rs`,
  `src/sys/linux/fs.rs`, `src/sys/linux/net.rs`).
- Windows cancel callbacks call `CancelIoEx(handle, overlapped)`; the aborted operation's
  completion packet still arrives and reclaims the operation context
  (`src/sys/windows/overlapped.rs`).
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

An explicit `CancellationToken` is a candidate post-0.1 addition so cancellation can be expressed
without relying only on Drop; see `ROADMAP.md`.

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

On top of the staging model, the concrete I/O types make **reads cancel-safe** by stashing the
in-flight operation on the object rather than in the transient future: `TcpStream`, `UnixStream`,
`File`, `Stdin`, and the child pipes hold their `pending_read`, and a read that completes with more
bytes than the current caller buffer retains the surplus in a `ReadOverflow` buffer served to the
next read — a dropped read future therefore loses nothing. Writes stash `pending_write` the same way
but record the submitted buffer's identity; a re-poll presenting a *different* buffer while a write
is in flight is rejected rather than misreporting that write's byte count against the wrong buffer
(`src/io/mod.rs`, `src/net/mod.rs`).

This is sound for arbitrary borrowed buffers, but it is not zero-copy. A hot-path API that transfers
ownership of a stable allocation to the operation (tokio-uring style) and registered buffers are
roadmap items (`ROADMAP.md`).

# Subprocesses

On Linux, child process exit is represented as fd readiness. `Command::spawn` opens a pidfd for the
child, and `Child::wait` parks on `wait_readable(pidfd)`, which submits `IORING_OP_POLL_ADD` through
the thread's io_uring driver before collecting the status with `waitpid`
(`src/sys/linux/process.rs`, `src/sys/linux/fd.rs`). There is no SIGCHLD handler and no blocking-pool
offload for child exit.

Pipes attached to child stdin/stdout/stderr use the same platform byte-stream paths as other fds:
Linux goes through the runtime-owned-buffer I/O path plus readiness where needed, while macOS uses the
existing kqueue/blocking-pool split for its backend.

# Driver abstraction

The scheduler, timer heap, `JoinHandle`, `FutureTask`, `ThreadShared`, macrotask queues, and the
task waker live **once** in `src/platform/runtime_shared/` and are platform-agnostic. Platform state
is fully type-erased: `ThreadState` holds the driver as `Box<dyn DriverBackend>` and the
cross-thread notifier as `Box<dyn Notifier>` (`src/platform/runtime_shared/driver_backend.rs`).
Each platform's `runtime.rs` is a thin shim that implements the tiny `Runtime` glue trait (mint a
driver/notifier pair; read the monotonic clock) on a marker type and re-exports the generic public
functions with that type fixed.

The backends behind the trait:

- Linux: one `io_uring` ring per runtime thread, used for timers, cross-thread wake notifications
  (`IORING_OP_MSG_RING`), fs ops, network ops, and fd readiness (`src/platform/linux/driver.rs`).
- macOS `aarch64`: `kqueue` for the wait/wake path, timers, and fd readiness, plus a wake pipe;
  filesystem work is offloaded to the blocking pool (`src/platform/macos_aarch64/driver.rs`,
  `src/sys/macos/fs.rs`).
- Windows: an I/O completion port for the wait/wake path and for dispatching overlapped
  operation completions; the runtime timer is a waitable-timer APC that interrupts the
  alertable port wait, and work with no overlapped form is offloaded to the blocking pool
  (`src/platform/windows/driver.rs`, `src/sys/windows/`, `docs/WINDOWS.md`).
- Driver-specific entry points that the shared scheduler does not know about (e.g. Linux
  `cancel_operation`) are reached by downcasting through `DriverBackend::as_any` in the platform
  shim.

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

| Capability | Linux io_uring path | macOS path | Windows path |
| --- | --- | --- | --- |
| open | `IORING_OP_OPENAT` | blocking pool (`std::fs::OpenOptions`) | blocking pool (`CreateFileW` + `FILE_FLAG_OVERLAPPED`), then port association |
| read | `IORING_OP_READ` into guarded internal buffer, then copy into caller slice | blocking pool (`read`/`pread`) | overlapped `ReadFile` at explicit offsets through the port |
| write | `IORING_OP_WRITE` from guarded internal buffer | blocking pool (`write`/`pwrite`) | overlapped `WriteFile` at explicit offsets through the port |
| metadata | `IORING_OP_STATX` | blocking pool (`metadata`/`fstat`) | blocking pool (`GetFileInformationByHandle`) |
| sync | `IORING_OP_FSYNC` | blocking pool (`fsync`/`F_FULLFSYNC`) | blocking pool (`FlushFileBuffers`) |
| set_len | `IORING_OP_FTRUNCATE` (6.9+), inline `ftruncate(2)` fallback | blocking pool (`ftruncate`) | blocking pool (`SetFileInformationByHandle`) |
| try_clone | inline `fcntl(F_DUPFD_CLOEXEC)` (never blocks) | blocking pool | inline `DuplicateHandle` (never blocks) |
| read_dir | offloaded streaming producer (`getdents` can block, no io_uring opcode) | blocking pool producer | blocking pool producer |
| close | synchronous `close(2)` via `OwnedFd` `Drop` | synchronous `close(2)` via `OwnedFd` `Drop` | synchronous `CloseHandle`/`closesocket` via owned-handle `Drop` (an awaitable close is a roadmap item on every platform) |
| network ops | `io_uring` first; non-blocking control ops fall back inline, data-path ops fall back to a non-blocking readiness path (`IORING_OP_POLL_ADD`) on unsupported kernels — never the blocking pool | `kqueue` readiness plus synchronous nonblocking socket calls | overlapped `ConnectEx`/`AcceptEx`/`WSASend`/`WSARecv`/`WSASendTo`/`WSARecvFrom`; control ops inline |
| Unix domain sockets | stream/datagram APIs reuse guarded send/recv paths plus readiness for path-addressed ops | stream/datagram APIs use the same guarded send/recv and readiness path | not provided (tracked in ROADMAP) |
| stdin | Linux tries `IORING_OP_READ`, then per-call blocking fallback | blocking fallback path | blocking pool (console handles cannot overlap) |
| child exit | pidfd readiness via `IORING_OP_POLL_ADD`; no SIGCHLD handler or blocking-pool offload | process wait backend | `RegisterWaitForSingleObject` on the process handle (OS wait-thread pool) |
| wait_readable | `IORING_OP_POLL_ADD` | `kqueue` `EVFILT_READ` one-shot | not provided (readiness has no IOCP analogue; `runite::fd` is Unix-only) |

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
- Windows is completion-driven like Linux: every overlapped submission produces exactly one
  completion packet, and the packet context owns the staging buffers, which is what makes the
  buffer-ownership-across-Drop rules hold without a separate cancel-guard map
  (`src/sys/windows/overlapped.rs`, `docs/WINDOWS.md`).
- Windows child stdio pipes are the overlapped named-pipe parent ends std creates for
  `Stdio::piped`; the backend adopts them and drives them like any other overlapped handle
  (`src/sys/windows/process.rs`).

# Cross-thread completion path

Current path:

1. A backend creates a `CompletionFuture`/`CompletionHandle` pair for the current thread
   (`src/op/completion.rs`).
2. The owner is captured as a `ThreadHandle`.
3. On completion, `CompletionHandle::finish` stores the result if `interested` is still true and
   calls `CompletionState::queue_wake` (`src/op/completion.rs`).
4. `queue_wake` queues the wake on the owner with `ThreadHandle::queue_internal_wake` — the
   capacity-bypassing enqueue, because a wake whose result is already stored must not be droppable
   by user-queue backpressure (`src/op/completion.rs`).
5. The enqueue locks the target remote queue, pushes the task, and calls
   `ThreadShared::notify` (`src/platform/runtime_shared/state.rs`).
6. On Linux, the notifier submits `IORING_OP_MSG_RING` to the target ring
   (`src/platform/linux/driver.rs`,
   `src/platform/linux/uring.rs`).
7. The target driver receives the wake CQE, records a pending wake, and the scheduler drains remote
   tasks (`src/platform/linux/driver.rs`,
   `src/platform/runtime_shared/scheduler.rs`).

This adds a syscall per wake on Linux. That is acceptable for cross-thread completions, where there
is no cheaper way to wake the owner reliably.

Same-thread completions short-circuit this path: `CompletionState::queue_wake` calls
`ThreadHandle::is_current` (`src/platform/runtime_shared/handles.rs`) — which
`Arc::ptr_eq`s the handle's `ThreadShared` against the current thread's installed state — and on a
match enqueues directly per the completion's wake class: a *local macrotask* for I/O completions, a
microtask for in-process resolutions. `is_current` returns `false` when the caller has no runtime
state installed (e.g. a blocking-pool worker), so non-runtime threads safely fall through to the
remote path. Only true cross-thread completions pay for the remote queue and `MSG_RING`.

# Safety invariants

Load-bearing invariants:

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
  and `atomic::fence(Acquire)` around the kernel-visible boundaries, and a failed
  `io_uring_enter` rolls the SQ tail back so a stale SQE can never reference freed buffers
  (`src/platform/linux/uring.rs`).
- The task `Waker` honors `std`'s `Send + Sync` contract: it carries an `Arc` payload (owner
  handle + task id) and never touches the `!Send` task from a foreign thread; wakes resolve
  through the owner thread's task registry (`src/platform/runtime_shared/future_task.rs`).
- Cross-thread notifiers own a **dup** of their target fd (`OwnedFd`), so a notify racing the
  target driver's teardown can only ever reach the original, draining ring/pipe — never a
  recycled fd number (`src/platform/linux/driver.rs`, `src/platform/macos_aarch64/driver.rs`).
- The Linux driver watches the CQ overflow counter and the `FEAT_NODROP`/`MSG_RING` capability
  bits, warning (or erroring) instead of silently losing completions
  (`src/platform/linux/uring.rs`).

# What this runtime is NOT (and the reasons)

- Not a general-purpose high-throughput server runtime.
  - No registered buffers, fixed files, vectored I/O, or splice; these are roadmap items
    (`ROADMAP.md`), not 0.1 goals.
  - The design optimizes for interactive applications and deterministic scheduling first.
- Not a multi-threaded work-stealing runtime.
  - Each runtime thread owns its own queues and driver.
  - Cross-thread work is explicit through `ThreadHandle::queue_macrotask`.
  - This preserves deterministic UI/reactive ordering.
- Not `Send`-future based.
  - Futures queued with `spawn` only need `Future + 'static`, not `Send`.
  - `JoinHandle`/`AbortHandle` are `!Send` by design; a task is joined on its own thread. (Timer
    handles, by contrast, are sendable tokens whose cross-thread cancel is a documented no-op.)
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
  wake pipe and blocking pools for fs; Windows uses an I/O completion port plus a waitable timer.

ring
: A Linux `io_uring` instance with submission and completion queues. Each Linux runtime thread owns
  its own ring.

notifier
: A cloneable handle that wakes another runtime thread's driver. Linux uses `MSG_RING`; macOS writes
  to a wake pipe; Windows posts a reserved-key packet with `PostQueuedCompletionStatus`.

ThreadHandle
: A cloneable, `Send`-capable handle for queueing a `Send` macrotask onto a specific runtime
  thread.

JoinHandle
: The `!Send` handle returned by `spawn`; awaiting it yields `Result<T, JoinError>`, with
  `Err(JoinError::Aborted)` after `abort` and `Err(JoinError::Panicked)` if the task's poll
  unwound.

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

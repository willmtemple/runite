# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-07-27

This release hardens the runtime's lifecycle and completion ownership, adds the
portable I/O traits from issue #9, and makes Windows resource affinity explicit.
See the [0.1 → 0.2 migration guide](docs/MIGRATING-0.2.md) for required source
changes.

### Breaking

- Owned resource adoption is now fallible everywhere. `From<OwnedFd>`,
  `From<OwnedHandle>`, and `From<OwnedSocket>` become `TryFrom`, and the
  inherent `from_owned` and `from_std` constructors return `io::Result`. This
  covers `File` and the TCP/UDP types, and also the Unix-domain stream,
  listener, and datagram extensions, which gain `from_owned` alongside
  `try_from`. Their infallible `From` adopted a descriptor without applying the
  non-blocking mode the driver requires, so a blocking socket could be adopted
  into a readiness-based backend and stall the event loop on its first read.
- Windows adoption rejects synchronous resources, resources suppressing
  success completion packets, and resources already associated with another
  I/O completion port. Operations validate the creating runtime's
  process-unique port identity; internal duplicates preserve proven affinity.
- `run()` now terminalizes spawned tasks when the loop reaches quiescence.
  In 0.1 a pending task simply outlived the `run()` call and could be resumed
  by a later one; it is now resolved to `JoinError::Cancelled` and its future
  is dropped. A task whose only wake source is invisible to the scheduler — a
  bare `Waker` clone handed to a foreign thread, rather than a runite channel,
  timer, or I/O operation — is therefore cancelled instead of resumed. The
  `Cancelled` variant itself is unchanged from 0.1, so no `match` needs
  updating; what changed is when it is produced.
- The channel error enums `mpsc::TrySendError`, `mpsc::TryRecvError`,
  `oneshot::TryRecvError`, and `broadcast::RecvError` are now
  `#[non_exhaustive]`, matching every other public enum in the crate. Matches on
  them need a `_` arm. Done now because 0.2 is already a breaking release —
  deferring it would only move the same break to 0.3.
- `select!` rotates its starting arm rather than always polling in lexical
  order. **This change is invisible to the compiler**: existing code keeps
  building and silently changes behavior. Add `biased;` to any `select!` that
  depended on lexical priority.

### Added

- `io::AsyncBufRead` (poll-only, with async helpers on `BufReader`),
  `io::AsyncSeek` with `AsyncSeekExt::seek`, and portable
  vectored methods on `AsyncRead`/`AsyncWrite`, addressing the trait gaps in
  issue #9. `BufReader`, `File`, and both `futures-compat` directions implement
  the applicable buffered and seek contracts. The vectored methods are API
  surface with sound scalar-backed defaults — every current backend writes the
  first non-empty slice rather than issuing `writev`/`readv` or a multi-buffer
  io_uring operation, so issue #9's native scatter/gather work remains open.
- `select!` branch guards (`pattern = future, if condition => ...`), `else`,
  output-pattern matching, async/control-flow handlers, and unbounded arm
  counts. Losing futures are dropped before the handler runs.
- `sync::RwLockReadFuture` and `sync::RwLockWriteFuture` are exported. They were
  already returned by `RwLock::read`/`write` but not nameable, so callers could
  not store or bound on them.
- `WorkerHandle::join`, `WorkerJoin`, and `WorkerJoinError`. Completion is
  published only after a non-runtime reaper joins the worker OS thread, and
  repeated joins observe the stored result.
- Windows console event streams under `signal::windows`, backed by durable,
  coalesced internal wakes; portable `signal::ctrl_c` routes to them.
- Per-target reporting for default, `hyper`, `futures-compat`, and combined
  feature sets, plus compile contracts for re-exported handle methods and
  Windows-only modules.

### Changed

- Ordinary runtime threads retain a stable driver across sequential `run()` /
  `block_on()` entries. Worker threads atomically close their remote queue,
  explicitly tear down runtime state, and report setup/runtime teardown
  panics. Every driver entry rejects re-entry.
- Idle shutdown first terminalizes all stranded spawned tasks, then invokes
  wakers/destructors. Accepted I/O and blocking jobs remain runtime-live until
  terminal result publication; notification failures retain accepted internal
  wakes and retry durably.
- macOS child-exit waits are event-driven: the wait registers `EVFILT_PROC`
  with `NOTE_EXIT` on the runtime's own kqueue instead of polling a private
  kqueue on a 1 ms timer (issue #20). A child that has already exited when the
  registration runs is reaped through `waitpid` rather than hanging.
- Callback timeout cancellation suppresses an expired callback still queued as
  a macrotask. A panicking interval is cancelled, and timer teardown is
  panic-isolated.
- Reads remain cancel-safe and resource-owned. Logical writes now carry live
  identities: shared `File` clones queue them FIFO, preserve completions for
  the owning future, and remove cancelled generations without crediting a
  later buffer. Shared close/shutdown results retain raw OS errors.
- `Compat<T>` retains one accepted scalar or vectored write buffer through
  cancellation; `FuturesCompat<T>` short-circuits empty vectored operations.
- Stdin uses one lazily started, demand-driven process-wide reader and a
  bounded 64 KiB buffer on every platform. Cancelling a waiter loses no bytes;
  inherited child stdin pauses the reader. Windows returns `WouldBlock` when a
  child tries to inherit a console while its parent read is active.
- `read_dir` uses shared resumable 32-entry blocking-pool batches on every
  platform. Workers never wait for consumer capacity, and drop releases
  buffered/stored iterator state cooperatively.
- The Linux hard kernel floor is 5.6 for single- and multithreaded runtimes.
  `MSG_RING` is optional from 5.18; older kernels use `eventfd`. Missing socket
  data opcodes use nonblocking readiness, and `FTRUNCATE` uses `ftruncate(2)`.
  Linux 6.1 remains the recommended baseline. CI does not pin a kernel version,
  so fallback paths are covered by opcode-capability injection tests.
- `#[runite::test]` preserves harness attributes on the generated wrapper and
  keeps lint, cfg, and documentation attributes scoped over the generated
  implementation.

### Fixed

- Removed completion-vs-idle races that could cancel a successful blocking
  result, strand a task wake, or let a parent observe a worker before its TLS
  destructors finished.
- Made oneshot result publication atomic and persistent, restored MPSC FIFO
  waiter behavior, removed a watch lock-order cycle, and moved user wakes out
  of `Notify`, `Mutex`, and `Semaphore` critical sections.
- Unified pending read/write/shutdown ownership so cloned file cursor
  reconciliation cannot lose or resubmit an accepted operation.
- A completion resolved on its own runtime thread no longer notifies that
  thread. The notification exists to wake a *parked* thread, so notifying the
  thread already dispatching the completion cost a wake round trip per
  completion — on Linux an `IORING_OP_MSG_RING` to the ring's own descriptor
  plus the `io_uring_enter` to submit it, which collapsed deferred-submission
  batches back to one operation each.
- Hardened io_uring batching, short-submit rollback, linked-SQE ownership,
  CQ-overflow handling, old-kernel opcode dispatch, cancellation storage, and
  panic-safe teardown. If kernel quiescence cannot be proven, storage is
  deliberately leaked rather than freed while referenced.
- macOS writable `EV_EOF` now reports the socket error carried in
  `kevent.fflags` instead of a generic `BrokenPipe`, so a refused connect
  surfaces `ECONNREFUSED`. (Readable `EV_EOF` already deferred to the next
  `recv` so buffered bytes drain first; that behavior is unchanged.)
- Linux `create_dir`, `rename`, `remove_file`, and `remove_dir` fall back to
  `mkdirat(2)`/`renameat(2)`/`unlinkat(2)` when the kernel lacks the
  corresponding io_uring opcode. `MKDIRAT` (5.15), `RENAMEAT` (5.11), and
  `UNLINKAT` (5.11) are all newer than the documented 5.6 floor, so these
  operations previously failed outright on a kernel that met it.
- `AsyncWrite::poll_flush` and `poll_close` wait for writes the resource still
  owns, across TCP and Unix-domain sockets, `File`, child pipes, and
  stdout/stderr. `poll_flush` previously returned `Ok(())` unconditionally on
  all six, reporting bytes as visible while their operation was still in
  flight; `poll_close` did the same on `File` and the stdio writers, and could
  shut a socket or pipe down out from under an in-flight write.
  A failure from a raw `poll_write` the resource still owns is now reported by
  the flush rather than discarded (a failed write started through
  `AsyncWriteExt` is still reported only to that future). Draining a successful
  raw write no longer consumes it either — a caller re-polling its buffer, as
  `AsyncWrite` requires, resolves to that operation instead of submitting the
  same bytes a second time.
- `read_dir` no longer ends a partly delivered scan when a refill hits a
  transient blocking-pool queue-full condition; the refill is retried from a
  later poll while buffered entries remain. Its shared state also tolerates a
  poisoned lock, which previously turned a panic under that lock into a process
  abort during unwinding.
- Dropped io_uring completions are reported as data loss. `rings->cq_overflow`
  counts entries the kernel failed to allocate, never ones FEAT_NODROP
  preserved, so the previous "held and flushed, ring undersized" warning
  described the opposite of what had happened.
- macOS no longer discards readiness events when draining the wake pipe fails.
  Registrations are `EV_ONESHOT`, so events already dequeued in that batch
  exist nowhere else and their waiters would hang; the batch is now dispatched
  before the error is surfaced.
- Windows reports a failed `CancelIoEx` instead of discarding it. Such an
  operation is neither cancelled nor completing, so its owning reference is
  never released and the waiting task cannot progress.
- Raw `AsyncWrite::poll_write` callers no longer receive an abandoned write's
  byte count for a different buffer. A raw write is now identified by its
  buffer as well as its generation, so a new logical write started after a
  previous future was dropped consumes the stale completion and submits its own
  bytes instead of reporting them as already written.
- Windows IOCP packets retain owning file/socket references; cursor clones
  share serialized state; socket deadline cancellation waits for the terminal
  packet; console reads distinguish interruption from EOF.

### Packaging and release

- The main crate archive includes `docs/WINDOWS.md` and the migration guide.
  The proc-macro archive includes both MIT and Apache-2.0 license texts.
- `xtask release-verify` packages and unpacks both crates, patches the packaged
  main crate to the packaged proc-macro crate locally, checks archive contents,
  and builds/tests/checks/docs the actual artifacts without requiring a
  previously published dependency.
- Release publication now requires successful CI for the exact tag commit,
  verifies both artifacts before the first upload, compares exact crates.io
  checksums on rerun, skips already-published matching artifacts, recovers a
  proc-macro-only partial publish, and rejects inconsistent states.

## [0.1.0] — 2026-07-04

Initial public release of runite's event-loop-per-thread runtime: io_uring on
Linux, kqueue on macOS aarch64, IOCP on Windows, JavaScript-style
microtask/macrotask scheduling, local `!Send` futures, explicit worker
runtimes, async filesystem/network/process/stdio services, timers, channels,
and synchronization primitives.

[0.2.0]: https://github.com/willmtemple/runite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/willmtemple/runite/releases/tag/v0.1.0

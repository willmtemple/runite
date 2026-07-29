# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

See the [0.2 → 0.3 migration guide](docs/MIGRATING-0.3.md) for required source
changes.

### Added

- A child process can be started on a descriptor the caller already owns.
  `Stdio` gains `From<OwnedFd>` on Unix and `From<OwnedHandle>` on Windows, so
  any of the three standard streams can be wired to a pseudoterminal, a socket
  accepted elsewhere, or a preopened file. The descriptor is duplicated at each
  spawn, so one `Command` can start several children and the caller keeps its
  original. ([#39](https://github.com/willmtemple/runite/issues/39))
- `runite::os::unix::process::CommandExt::pre_exec` runs a hook in the child
  between `fork` and `exec`, mirroring
  `std::os::unix::process::CommandExt::pre_exec`. This is the window in which
  a process acquires a controlling terminal (`setsid` then `TIOCSCTTY`), changes
  process group, or drops privileges. It takes `Fn` rather than `FnMut` because
  a runite `Command` may be spawned more than once. An error from the hook
  aborts the spawn. ([#39](https://github.com/willmtemple/runite/issues/39))

  Together these remove the last reason for an otherwise all-runite application
  to reach for `std::process` — a terminal multiplexer can now start a shell on
  its own pseudoterminal without a second process API.

- `Child::from_pid` adopts an already-running process, so a process started
  through some other API can have its exit awaited through the reactor instead
  of polled. Exit notification is event-driven on every platform — a pidfd on
  Linux, a `kqueue` process filter on macOS, a registered wait on the process
  handle on Windows — so no thread is parked for the process's lifetime, and
  a signal-escalation ladder can be an ordinary task built from `wait` and
  `time::timeout` rather than a sequence of blocking sleeps.
  ([#44](https://github.com/willmtemple/runite/issues/44))

  On Unix the process must be a direct child, since reading an exit status
  requires being its parent; Windows has no such restriction. Adopting a
  process that does not exist fails at adoption rather than producing a handle
  whose `wait` never completes.

- `Child` implements `Debug`, reporting the process id and which standard
  streams are piped. ([#31](https://github.com/willmtemple/runite/issues/31))

- `signal::unix::signals` watches several signal kinds on one stream, yielding
  the `SignalKind` that produced each event. An application whose SIGINT,
  SIGTERM and SIGHUP all mean "shut down" needed one spawned task per kind,
  each duplicating the teardown call. `Signals` implements `io::Stream`,
  deduplicates repeated kinds, rejects an empty set rather than returning a
  stream that is never ready, and rotates which kind it polls first so a
  frequent signal cannot starve the others.
  ([#45](https://github.com/willmtemple/runite/issues/45))

- `try_block_on`, a fallible counterpart to `block_on`. Creating a thread's
  platform driver can fail for reasons that are about the machine rather than
  the program — `io_uring` disabled by a container or hardening policy, or the
  locked-memory budget exhausted — and `block_on` treats those as
  unrecoverable. A panic there gives the user a backtrace through the runtime
  and no way to act; an error lets an application explain itself, fall back to
  a synchronous path, or choose its own exit status. Only startup is fallible:
  an error produced by the future is returned inside `Ok`.
  ([#40](https://github.com/willmtemple/runite/issues/40))
- `current_turn()` and `TurnId`: a stable, process-wide, monotonic key for one
  iteration of the event loop, readable from a task poll or a microtask
  callback and `None` outside a turn. A consumer with its own diagnostics
  stamps its records with it and joins on equality, so a reactive flush and the
  runtime turn that drove it become the same row rather than two entries
  correlated by wall-clock order. Every entry point that drives the loop
  produces turns, including `run_until_stalled` and `run_ready_tasks`, so a
  host embedding the runtime sees them too. The identifier carries no
  information about what the turn did; per-turn statistics belong to
  [#43](https://github.com/willmtemple/runite/issues/43).
  ([#52](https://github.com/willmtemple/runite/issues/52))
- `AsyncReadExt::read_to_string`, which had no trait-level equivalent — it
  existed only as an inherent method on `File`. Validation happens once at end
  of input rather than per chunk, so a multi-byte character split across two
  reads is not rejected.
- `AsyncRead`, `AsyncBufRead`, `AsyncWrite`, and `AsyncSeek` are implemented for
  `&mut T`, `Box<T>`, and `Pin<P>`. Only `Stream` had these before, so wrapping
  a borrowed reader did not work — `BufReader::new(&mut file)` failed to
  compile, and code that only held a `&mut` had to give up ownership or
  restructure. Every method is forwarded explicitly, including the vectored
  methods and the internal cancellation-generation hooks, so wrapping a
  runtime-backed writer in a pointer cannot silently make its writes
  cancellation-unsafe. ([#35](https://github.com/willmtemple/runite/issues/35))

### Fixed

- `Command::spawn` no longer blocks its runtime thread indefinitely when stdin
  is inherited. It waits for the process-wide stdin reader to release the
  terminal, and that wait was unbounded — an interrupt frees a reader parked in
  `poll`, but cannot un-issue a `read(2)` the reader has already entered, which
  on an interactive terminal returns only when the user types. Spawning a child
  could therefore hang the whole event loop until a keypress. The wait is now
  bounded and reports `ErrorKind::WouldBlock` past that point, matching what
  Windows already did, and the caller may retry.
  ([#28](https://github.com/willmtemple/runite/issues/28))

- `watch::Sender::send` could report success with no receivers. It checked the
  receiver count under the book lock, released it, then wrote the value, so the
  last `Receiver` dropping in that window left `send` consuming the value,
  advancing the version, and returning `Ok(())` — contradicting its documented
  contract. The check and the write now happen under one book lock. The
  previous value is moved out rather than assigned over, so `T::drop` runs
  after both locks are released: dropping it in place would run user code under
  the book lock, which is the self-deadlock the 0.2 lock-order fix removed.
  ([#26](https://github.com/willmtemple/runite/issues/26))

- `Debug` on the 67 public types that lacked it, and
  `missing_debug_implementations` is now denied in `Cargo.toml` so the gap
  cannot reopen. Coverage was inconsistent within single modules —
  `fs::Metadata` and `DirEntry` derived it while `File`, `OpenOptions` and
  `ReadDir` did not; every channel error derived it while no channel `Sender`
  or `Receiver` did — which poisoned `#[derive(Debug)]` on any downstream type
  holding one. The impls are deliberately opaque
  (`debug_struct(..).finish_non_exhaustive()`): most of these are futures and
  guards holding `&mut R` where `R: ?Sized`, so a derive would demand `Debug`
  on type parameters that frequently cannot have it.
  ([#31](https://github.com/willmtemple/runite/issues/31))

- `fd::read_chunks`, which encapsulates the readiness loop that `wait_readable`
  otherwise asks every caller to write. The loop carries three pieces of
  load-bearing knowledge the raw API does not express, each a bug when missed
  rather than an inefficiency: chunks must be delivered *before* the loop
  parks, or a burst ending mid-frame stays invisible until the next write;
  `Interrupted` must retry rather than wait for readiness already reported; and
  the caller needs a way to stop, or a descriptor producing faster than it
  consumes starves everything else sharing the loop.

  `on_chunk` runs for each read as it completes, so delivery-before-parking is
  structural rather than advisory, and returning `ControlFlow::Break` ends the
  drain. The returned `fd::Drain` says which of the two ended it: a consumer
  draining a pseudoterminal keys its teardown on end of input — that is how it
  learns the child exited — and must be able to tell that from its own byte
  budget running out. One-shot readiness is deliberately kept rather than replaced by an
  `AsyncRead` for descriptors: a consumer sharing its thread with a frame clock
  needs the yield point that reading to completion inside a single stream call
  would take away. ([#42](https://github.com/willmtemple/runite/issues/42))

- `metrics::snapshot()`, returning a `Snapshot` of `Gauges` and `Counters`.
  Gauges are levels read at an instant — live tasks, ready tasks, microtask and
  macrotask queue depths, remote queue depth, armed timers, outstanding driver
  operations. Counters are monotonic totals — turns, task polls, task wakes,
  coalesced wakes, microtasks and macrotasks run, operations completed, tasks
  cancelled, remote tasks rejected — whose useful quantity is the difference
  between two snapshots.

  They are two types rather than one flat struct on purpose: a flat struct
  invites subtracting a gauge or reading a counter as a level, and a consumer
  that cannot tell them apart will misreport. High-water marks are a third kind
  and will be a third type.

  Reading a snapshot walks nothing. Gauges come from state the runtime already
  maintains, counters are incremented at the mutation sites that do the work,
  and taking a snapshot is a handful of loads — because the act of measuring an
  idle runtime must not itself be work. A thread with no runtime installed
  reads zeroes rather than panicking or installing one, so a benchmark or test
  harness that drives application logic without a reactor can still call it.

  `task_wakes` counts only wakes that actually schedule a poll; one coalescing
  into an already-queued poll lands in `coalesced_wakes` instead, so neither
  number misleads and the work coalescing avoids stays visible. Comparing
  `task_wakes` against `task_polls` is how a spurious-wake problem surfaces,
  and `operations_completed` against the `outstanding_operations` gauge is how
  a leaked operation does. A snapshot spans whatever interval the reader
  chooses, so it carries no `TurnId`. ([#43](https://github.com/willmtemple/runite/issues/43))

- `sync::CancellationToken`: cloneable, hierarchical cooperative cancellation
  that `!Send` tasks can await. It complements `AbortHandle` rather than
  replacing it — an abort is done *to* a task at its next suspension point, a
  token is something a task chooses to observe, so work that must flush a
  buffer or release a lock before stopping can do so. `child_token` derives a
  token cancelled when its parent is, letting a subsystem cancel its own work
  without touching its siblings; cancellation flows down only.
  ([#11](https://github.com/willmtemple/runite/issues/11))

- `TimeoutHandle::cancel_on_drop` and `IntervalHandle::cancel_on_drop`, which
  wrap a timer token in a `CancelOnDrop` guard that stops the timer when it
  falls out of scope. The plain handles keep their token semantics — dropping
  one leaves the timer running, matching `setInterval`/`clearInterval` and this
  crate's own `JoinHandle` — so this is opt-in and changes nothing by default.
  It matters most for intervals, where an uncancelled timer keeps the runtime
  alive and a leaked one stops `run()` from ever returning. The guard is not
  `Clone`, and `into_inner` releases the timer to a longer-lived owner without
  cancelling. ([#8](https://github.com/willmtemple/runite/issues/8))

- `task::is_retryable`, which reports whether a `spawn_blocking` refusal is
  worth retrying: `true` only for a momentarily full queue, `false` for a
  stopped or uncreatable pool. Deliberately takes `io::Error` rather than
  introducing a dedicated error type — the kinds already carry the
  distinction, and keeping `spawn_blocking` in `io::Result` lets it compose
  with the rest of the crate without a conversion at every seam.
  ([#46](https://github.com/willmtemple/runite/issues/46))

- `#[must_use]` on the futures and guards that were missing it: `time::Sleep`,
  `YieldNow`, `RwLockReadFuture`, `RwLockWriteFuture`, `MutexGuard`,
  `RwLockReadGuard`, `RwLockWriteGuard`, `SemaphorePermit` and `watch::Ref`.
  `sleep(d);` and `let _ = semaphore.acquire().await;` were silent no-ops that
  compiled without a warning.

  `JoinHandle` and `BlockingJoinHandle` are deliberately **not** marked.
  Dropping a join handle detaches the task, which is a documented and intended
  operation rather than a mistake — unlike an unawaited future, which does
  nothing at all. Marking them flagged 141 call sites across this repository's
  own tests and examples, essentially all of them correct.
  ([#30](https://github.com/willmtemple/runite/issues/30))

### Fixed

- `release-verify` no longer passes `--allow-dirty` to `cargo package`
  unconditionally. That flag writes `"dirty": true` into
  `.cargo_vcs_info.json`, which changes the bytes of the produced `.crate` and
  therefore its checksum — while the release workflow's whole idempotency
  argument rests on that checksum being reproducible, and a mismatch there
  wedges a version permanently. Verifying from a dirty tree was therefore
  verifying an artifact the real release could never produce. The flag is now
  opt-in via `release-verify --allow-dirty`; without it, a dirty packaged file
  fails loudly instead. ([#29](https://github.com/willmtemple/runite/issues/29))

### Documented

- The scheduling guarantee that layers built on runite batch on — *a microtask
  queued during a turn runs before the next macrotask* — is now stated as a
  contract in the crate documentation, along with the fact that runite imposes
  no scheduling budget and that cooperative yielding is the only mechanism.
  Tests pin the guarantee (`tests/event_loop_order.rs`), so it cannot be
  removed silently. ([#14](https://github.com/willmtemple/runite/issues/14))

- `spawn_blocking`'s refusals now say which are retryable. The two failures
  already carried distinct `io::ErrorKind`s — `WouldBlock` for a full queue,
  `BrokenPipe` for a stopped pool — but nothing said so, so callers discarded
  the error and lost the distinction. Both are now also reported as `tracing`
  warnings on the `runite::runtime` target, so a caller that drops the error
  still leaves evidence; the downstream symptom is work that silently stops
  happening, which is otherwise hard to trace back.
  ([#46](https://github.com/willmtemple/runite/issues/46))

- `ChildStdin`'s close can deadlock against a child waiting for end of input,
  and the hazard is now on the type rather than absent. Because `poll_close`
  drains pending writes first, a write cancelled with the pipe buffer full
  cannot complete while the child will not read again until it sees EOF — so
  the close waits for the write, the write waits for the child, and the child
  waits for the close. This is inherent to "close flushes what you already
  wrote"; a bounded drain would replace a visible hang with silent truncation
  the caller cannot detect. Dropping the handle closes immediately and abandons
  the pending write, and is now documented as the escape, with a test pinning
  it. ([#27](https://github.com/willmtemple/runite/issues/27))

### Changed

- Fixed two intra-doc links on `TcpStream` that pointed at inherent
  `read_exact`/`write_all` methods removed in this release; they now name the
  extension-trait methods.
- Removed `FuturesCompat`'s `poll_write_vectored_operation` override, which was
  identical to its `poll_write_vectored` and to what the trait default already
  forwards to, plus two `#[allow(dead_code)]` attributes that had gone stale as
  their targets became unconditionally used.
  ([#32](https://github.com/willmtemple/runite/issues/32))
- Removed the Linux driver's `pending_cancel_buffers` map, the `CancelGuard`
  type, and the guard parameter threaded down to four call sites that all
  passed `None`. It was a second, always-empty home for the staging buffers
  that survive a cancelled operation. The invariant it was meant to enforce is
  real and unchanged: the buffer is owned by the operation's completion
  callback, the driver holds that callback until the *original* operation's
  terminal CQE, and the cancel path never touches it — because
  `IORING_OP_ASYNC_CANCEL` can report `-EALREADY`, meaning the target may still
  write. Internal only; no API change.
  ([#32](https://github.com/willmtemple/runite/issues/32))

- `io_uring` setup failing with `ENOMEM` now says what actually went wrong. The
  rings are pinned against `RLIMIT_MEMLOCK`, so this is a locked-memory limit
  rather than memory exhaustion — but the raw errno renders as "Cannot allocate
  memory", which sends the reader to look at free RAM on a machine with tens of
  gigabytes of it. The error now reports the current `RLIMIT_MEMLOCK`, names
  profilers as the usual competitor for the same budget (`perf record` with
  default settings is the common case; `-m 32` leaves room), and uses
  `ErrorKind::QuotaExceeded` rather than `OutOfMemory` so a caller matching on
  the kind is not misled either.
  ([#40](https://github.com/willmtemple/runite/issues/40))

### Breaking

- `sync::Permit` is renamed `sync::SemaphorePermit`. It was the only guard type
  in the crate without an owner prefix, beside `MutexGuard`,
  `RwLockReadGuard` and `RwLockWriteGuard`.
  ([#35](https://github.com/willmtemple/runite/issues/35))

- `Stdin::read_line` is renamed `Stdin::next_line`. It shared a name with
  `BufReader::read_line` while having an incompatible signature and a different
  end-of-input convention — one allocates and returns `Option<String>` with
  `None` at end of input, the other appends to a caller-supplied `String` and
  reports `Ok(0)`. `BufReader::read_line` keeps its name because it follows
  `std`; the divergent one is the one that moved.
  ([#35](https://github.com/willmtemple/runite/issues/35))

- `Stdin::read`, `Stdout::write` and `Stderr::write` are removed, completing
  the sweep below. These were not duplicates: `Stdin::read` wrapped its poll in
  a guard that discarded the pending operation and unregistered the waiter on
  cancellation, while the `AsyncRead` path retained both — so the documented
  cancel-safety sentence held on one path and not the other, and generic code
  over `AsyncRead` silently got the undocumented one. Both now share a single
  read path with retention as the contract, matching every other runite reader:
  a cancelled read leaves its operation claimable by the next one.
  ([#53](https://github.com/willmtemple/runite/issues/53))

- The inherent `read`/`write`-family methods on `File`, `TcpStream` and
  `UnixStream` are removed in favour of `AsyncReadExt`, `AsyncWriteExt` and
  `AsyncSeekExt`. `File` had eight of them, `TcpStream` four, `UnixStream`
  three, while `ChildStdin`, `BufReader` and the owned split halves had none.
  Inherent methods win name resolution, so identical-looking calls dispatched
  to different code depending on the concrete type, and refactoring a concrete
  type into `fn f<R: AsyncRead>(r: &mut R)` silently changed which
  implementation ran. The bodies were already thin wrappers over the same
  `poll_read`/`poll_write_operation` paths the ext traits use, so behaviour —
  including cancel safety and write-operation identity — is unchanged; only the
  import is new. Add `use runite::io::{AsyncReadExt, AsyncWriteExt};` (and
  `AsyncSeekExt` for `File::seek`).

  The positional methods (`read_at`, `read_exact_at`, `write_at`,
  `write_all_at`) are **not** affected: they take an explicit offset, do not
  shadow a trait method, and remain inherent.
  ([#35](https://github.com/willmtemple/runite/issues/35))

- `net::unix::Incoming` no longer borrows its listener. It was `Incoming<'a>`
  holding `&'a UnixListener` while the TCP `net::Incoming` held an owned
  listener, so the same method name produced a movable stream for TCP and a
  borrowed one for Unix — `spawn(async move { listener.incoming()... })`
  compiled for one and not the other, and generic code over both could not be
  written once. `UnixListener` now reference-counts its descriptor internally,
  exactly as `TcpListener` already did, and `incoming()` returns an owned
  `Incoming`. Code that named the lifetime (`Incoming<'_>`, `Incoming<'a>`)
  drops it. ([#35](https://github.com/willmtemple/runite/issues/35))

- `Stdio` is no longer `Clone`, `Copy`, `PartialEq`, or `Eq`, and `Command` is
  no longer `Clone`. A `Stdio` can now own a descriptor, and neither copying one
  implicitly nor comparing one for equality is meaningful; `std::process::Stdio`
  and `std::process::Command` are not `Clone` for the same reason. Code that
  relied on passing a `Stdio` by copy should construct one per call, and code
  that cloned a `Command` should build it twice or wrap it.

  Note that the public API report does not track derived trait impls, so this
  change does not appear in `docs/public-api.md`.

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

[Unreleased]: https://github.com/willmtemple/runite/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/willmtemple/runite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/willmtemple/runite/releases/tag/v0.1.0

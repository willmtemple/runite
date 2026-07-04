# runite 0.1 Release Plan

Tracking checklist derived from the pre-0.1 deep review (2026-07-03). Ordered by
tier: Tier 0 (soundness) → Tier 1 (data corruption/loss) → Tier 2 (hangs/panics)
→ Tier 3 (API shape) → Tier 4 (release mechanics), then post-0.1.

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` deferred post-0.1

---

## Tier 0 — Soundness (unreleasable until fixed)

- [x] **0.1 Waker is `Rc`-based; `Waker` is `Send+Sync` by contract.**
  `future_task.rs:61-111`. Cross-thread wake races the non-atomic refcount (UB);
  waking from another runite worker migrates a `!Send` future to the wrong thread.
  Fix: atomic waker header (`owner: ThreadHandle`, `AtomicBool queued`); `wake`
  checks `owner.is_current()` → local microtask, else remote macrotask.
  **Done:** replaced the `Rc<FutureTask>` waker with a `Send+Sync`
  `Arc<TaskWaker>` (owner handle + numeric task id + `scheduled` flag); tasks now
  live in a per-thread `ThreadState::tasks` registry keyed by id, so a wake from
  any thread resolves the `!Send` task on its owner. Regression tests in
  `tests/waker.rs` (foreign-thread wake resolves; concurrent clone/drop is race-
  free). Cross-thread wake under a full remote queue was best-effort here;
  **resolved in 2.3** (task wakes now route through the capacity-bypassing
  internal-wake path).
- [x] **0.2 Stale-SQE use-after-free on `io_uring_enter` failure.**
  `uring.rs:580-627` + `driver.rs:246-248`. Advances SQ tail, then on enter failure
  drops the buffer-owning completion while the SQE stays queued → next submit hands
  the kernel freed memory.
  **Done (minimal, standalone):** `submit_pending` now retries `EINTR` and, on any
  other `enter` error (which consumes 0 SQEs), rolls the SQ tail back to `head`,
  un-publishing the stale SQE(s) so dropping the completion/buffer is safe. Sound
  under the current immediate-submit design (every un-consumed SQE belongs to the
  failing op). **Remaining, folded into P-1/2.5:** partial-submission handling
  (roll back only the un-consumed suffix, keep accepted ops alive) and
  `EBUSY`/`EAGAIN` drain-then-retry. Not unit-tested (needs syscall fault
  injection); change is small and local.

## Tier 1 — Data corruption & loss

- [x] **1.1 BufReader replays consumed bytes after inner `Pending`.**
  `io/buf.rs:253-254` (and `fill_buf` error/drop path `178-185`).
  **Done:** both `poll_read` and `fill_buf` now assign `pos`/`filled` only after
  the inner read resolves, so a `Pending`/cancelled/errored read leaves the
  cursor invariant intact. Added a `ParkingReader` (self-waking `Pending` between
  chunks) + two regression tests (byte-wise `poll_read`, and `read_line`);
  verified they fail against the pre-fix code.
- [x] **1.2 Pending-op/buffer mismatch across polls.**
  `net/mod.rs`, `fs.rs`, `stdio.rs`, `process/pipe.rs`, `hyper_impl.rs`.
  **Done:** added a shared `pub(crate) io::ReadOverflow` (boxed, cursor-drained)
  and wired it into every completion-based reader — `TcpStream` (covers the split
  halves), `File`, `Stdin`, `ChildStdout`/`ChildStderr`, and the hyper adapter —
  so a read completing with more bytes than the current buffer keeps the surplus
  instead of erroring/discarding (and the hyper `put_slice` panic is gone). Writes
  on `TcpStream` now record the in-flight buffer identity and reject a re-poll
  presenting a different buffer rather than misreporting the count. Regression test
  covers the read buffer-shrink path.
- [x] **1.3 `read(true).truncate(true)` truncates on Linux.**
  `sys/linux/fs.rs:542-573`. **Done:** split `open_flags` into std-mirroring
  `access_mode` + `creation_mode`; invalid access combinations (truncate/create
  without write, truncate+append without create_new) now fail `EINVAL` instead of
  silently opening `O_RDONLY | O_TRUNC`. Regression test in `tests/fs.rs` (asserts
  the open errors and the file is preserved); verified it fails without the guard.
- [x] **1.4 Mixing `mpsc::recv()` with the `Stream` impl reorders / aborts process.**
  `channel/mpsc.rs:686-711`. **Done:** `recv()` now uses the receiver's persistent
  `stream_wait` slot (shared with `poll_next`) instead of a local one, so the two
  can't register two waiters (no assert/abort) and values are never reordered.
  This also makes `mpsc::recv` cancel-safe (covers the mpsc half of 1.6): a value
  delivered to a dropped `recv` future is retained for the next call. Regression
  test asserts FIFO across an abandoned stream poll; verified it hangs (strands
  the value) against the pre-fix code.
- [x] **1.5 `watch::changed()` version regression after cancelled wait.**
  `channel/watch.rs:450-462`. **Done:** the stale-completion arm now only accepts
  `version > self.version` and otherwise re-registers instead of regressing the
  receiver's version. Regression test in `tests/channel.rs` (verified it fails
  against the pre-fix code). Writing the test uncovered the pre-existing
  completion-drop deadlock (D-2), fixed alongside.
- [x] **1.6 `mpsc::recv`/`oneshot::recv` not cancel-safe.**
  `mpsc.rs:551-576`, `oneshot.rs:167-169`. **Done:** mpsc covered by 1.4 (recv now
  shares the persistent `stream_wait` slot). oneshot gained a persistent `wait`
  slot on the receiver (poll_recv refactored to an associated fn with disjoint
  field borrows), so a delivered-but-unpolled value survives a cancelled `recv`.
  Cancel-safety documented on both; regression tests for each; verified they fail
  against the pre-fix code.
- [x] **1.7 Inherent async I/O methods lose data on cancellation.**
  `net/mod.rs`, `fs.rs`, `stdio.rs`. **Done:** `TcpStream::{read,write}`,
  `File::{read,write}` (sequential), and `Stdin::read` now delegate to their
  `AsyncRead`/`AsyncWrite` trait paths, so the in-flight op is stashed on the
  object — a dropped inherent read retains its op (cancel-safe via the overflow
  buffer) and can no longer race a concurrent trait read (two recvs in flight).
  Regression test locks the stash-through-inherent-read behavior. **Remaining
  (documented):** UDP datagram recv and the stdin blocking-offload fallback still
  lose on cancel (offload can't cancel `read(2)`) — folded into 2.10; UnixStream
  gains this once it implements the traits in 3.4.
- [x] **1.8 macOS `sync_all`/`sync_data` durability inverted.**
  `sys/macos/fs.rs:124-138`. **Done:** both now go through a `full_fsync` helper
  that uses `F_FULLFSYNC` (the only real drive-cache flush on macOS) with an
  `ENOTSUP`/`EINVAL` fallback to `fsync`, matching `std::fs`. `sync_all` no longer
  gets the weaker plain `fsync`. Cross-checked with `cargo check --target
  aarch64-apple-darwin`; runtime behavior exercised on macOS CI.
- [x] **1.9 Accepted sockets missing `SOCK_CLOEXEC` on uring path.**
  `sys/linux/net.rs:201-207`. **Done:** set `sqe.op_flags = SOCK_CLOEXEC` on the
  ACCEPT SQE (matching the `accept_sync` fallback). Regression test drives the
  production io_uring accept and asserts `FD_CLOEXEC`; verified it fails without
  the flag.

## Tier 2 — Hangs, panics, robustness

- [x] **2.1 No panic isolation.** `catch_unwind` per task/macrotask/microtask and per
  blocking job; `Drop` guard on worker completion (notify parent on unwind); scope-guard
  reset of `closing` in `run()`; add `JoinError::Panicked` (coordinate with 3.6).
  **Done:** added `JoinError::Panicked` + `is_panicked()`. `FutureTask::poll` now
  `catch_unwind`s the future poll: a panicking task is dropped, deregistered, and its
  joiner resolved to `Panicked` (new `TaskState::Panicked`) instead of unwinding the
  loop. A `run_guarded` firewall wraps every scheduled macrotask/microtask closure in
  all three drivers (`run`, `run_until_stalled`, `run_ready_tasks`) so a panicking timer/
  interval/`queue_*`/`on_exit` closure is isolated too. `spawn_worker` wraps the whole
  worker body in `catch_unwind` and, on unwind, marks the `WorkerCompletion` finished +
  notifies the parent (fulfils the "notify parent on unwind" guard) so a dead worker can
  never hang its parent. `run()` gained a `ClosingResetGuard` that restores `closing` on
  any non-committed exit from the shutdown probe, including a panic. The blocking pool's
  `worker_loop` catches job panics (keeps pool threads alive for every internal caller),
  and `spawn_blocking` catches the closure's panic to deliver `Panicked` (vs `Cancelled`)
  to the awaiter. Regression tests in `tests/panic_isolation.rs` (task panic → joinable +
  loop survives; blocking panic → `Panicked` + pool survives; macrotask panic → loop
  survives; sibling tasks unaffected). Public API snapshot regenerated. **Deferred to
  3.6:** `#[non_exhaustive]` on `JoinError` (this commit adds the variant only). The panic
  payload is intentionally not carried, keeping `JoinError: Copy`.
- [x] **2.2 No reentrancy guard on `run()`/`run_until_stalled()`.**
  Add `in_event_loop` flag; panic on nested entry (tokio-style). **Done:** added
  `ThreadState::in_event_loop` and an `EventLoopGuard` set at the top of all three
  drivers (`run`, `run_until_stalled`, `run_ready_tasks`); constructing it panics if a
  driver is already active on the thread (re-entry from inside a task/callback). The
  panic is caught by the 2.1 per-task firewall, so an offending task resolves to
  `JoinError::Panicked` and the outer loop survives. Sequential (non-nested) calls are
  unaffected. Regression tests in `tests/reentrancy.rs` (nested `run` and
  `run_until_stalled` both rejected; outer loop continues).
- [x] **2.3 Cross-thread completion wakes dropped when remote queue full.**
  `op/completion.rs:118-127`. Give internal completion wakes a reserved/unbounded
  internal queue (bounded by `pending_ops`). **Done:** split the remote-enqueue
  path into `enqueue_macro` (user tasks, capacity-limited → `QueueError::Full`) and
  `enqueue_internal_wake` (capacity-bypassing), exposed as
  `ThreadHandle::queue_internal_wake`. Both the completion machinery
  (`op/completion.rs`) and the spawned-task waker (`future_task.rs` cross-thread
  path) now route through it, so a wake is never dropped on a full queue — dropping
  either stranded the target (a completion whose result is already stored, or a
  task with no other scheduling signal = lost-wakeup hang). The internal wakes stay
  in the same single remote queue, so the atomic close-commit protocol is unchanged;
  they are bounded by in-flight ops + live tasks (one coalesced wake each) rather
  than user input. Unit tests assert the capacity bypass and that `closed` still
  rejects. This also closes the 0.1 waker follow-up.
- [x] **2.4 Notifier TOCTOU on raw fds.** `linux/driver.rs:42-71` (+ macOS). Notifier holds
  a dup'd `OwnedFd` of the ring / pipe write end. **Done:** both notifiers now hold a
  dup'd `OwnedFd` (Linux: `F_DUPFD_CLOEXEC` of the ring fd; macOS: dup of the wake-pipe
  write end, `Arc<OwnedFd>` so the notifier stays `Clone`, plus `F_SETNOSIGPIPE` so a
  racing write to a torn-down pipe yields `EPIPE` not `SIGPIPE`). The dup keeps the fd
  number reserved (and the kernel object alive) for as long as any cross-thread handle
  exists, so a `notify` that races the target's teardown can only ever reach the
  original — now draining — ring/pipe, never a recycled fd pointing at an unrelated
  resource. The `closed` flag still short-circuits the common case. Linux regression
  test asserts the notifier's dup fd stays valid after the target driver drops (fails
  on the old bare-`RawFd` copy); macOS cross-checked with
  `cargo check/clippy --target aarch64-apple-darwin`.
- [x] **2.5 io_uring robustness cluster.** Check CQ-overflow flags / `FEAT_NODROP`; honor
  partial-submission return; log/ handle MSG_RING failure CQEs (kernel <5.18 has no
  cross-thread wake); drain the global fallback submitter CQ; give `TIMEOUT_UPDATE` a real
  token + pre-5.11 remove/re-add fallback. **Done (most):**
  - **CQ overflow / FEAT_NODROP:** map the CQ `overflow` counter and check it after each
    drain; warn once per new overflow event (NODROP → backpressure/undersized-ring), error
    if the kernel lacks `FEAT_NODROP` (dropped CQEs → possible hangs). Warn at startup if
    `FEAT_NODROP` is missing.
  - **MSG_RING unsupported:** warn at ring creation if `IORING_OP_MSG_RING` is unsupported
    (<5.18), so a runtime whose cross-thread wakes will silently never arrive is flagged
    instead of hanging mysteriously.
  - **Global fallback submitter CQ:** `with_submitter` now drains the global ring's CQ
    after each use (nobody else polls it), preventing lifetime CQ overflow, and logs any
    failure CQE — which for the fallback ring means a cross-thread MSG_RING wake failed
    (covers "log/handle MSG_RING failure CQEs").
  - **TIMEOUT_UPDATE token:** the update SQE now carries a real, decodable completion token
    and `IOSQE_CQE_SKIP_SUCCESS` (matching `submit_timeout_remove`) instead of leaving
    `user_data` at 0 and emitting an unattributable token-0 CQE.
  **Deferred:** partial-submission handling (roll back only the un-consumed suffix, keep
  accepted ops alive) → folded into **P-1** batched submission as the plan already notes
  (the current immediate-submit design makes the 0.2 all-or-nothing rollback sound). The
  pre-5.11 `TIMEOUT_UPDATE` remove/re-add fallback is unnecessary at the documented kernel
  floor (5.11 < the 5.18 multi-thread floor and `TIMEOUT_UPDATE` sits above the 5.6 base;
  recommended floor is 6.1), so it is left out with this note.
- [x] **2.6 `IORING_OP_SOCKET` flags in wrong SQE field.** `sys/linux/net.rs:89-99`.
  `sqe.off = (type | flags); sqe.op_flags = 0`. **Done:** the socket-creation flags
  (`SOCK_CLOEXEC`/`SOCK_NONBLOCK`) now OR into the type field (`sqe.off`), matching
  `socket(2)`/`socket_sync`, with `op_flags = 0`. Empirically confirmed: pre-fix the
  flags sat in `op_flags` (`rw_flags`), which kernels ≥5.19 reject with `EINVAL`, so
  every uring `socket()` silently fell back to `socket_sync` (dead fast path; cloexec
  stayed correct only via the fallback) — and would drop `SOCK_CLOEXEC` outright on
  non-validating kernels. Post-fix the uring path succeeds and produces a cloexec
  socket (verified by disabling the fallback: pre-fix panics with `EINVAL`, post-fix
  succeeds). Regression test `uring_socket_is_cloexec` locks the property.
- [x] **2.7 Signal reader consumes a shared blocking-pool worker.**
  `signal/unix.rs:319-324`. Move to a dedicated `std::thread`. **Done:** the
  process-lifetime `reader_loop` now runs on a dedicated `std::thread`
  (`runite-signal`) instead of `spawn_blocking`, so it no longer permanently parks one
  of the bounded (2–32) blocking-pool workers — which starved `spawn_blocking`, fs
  offload, and DNS of a slot (worst case 1 of 2 workers). fd cleanup on spawn failure
  preserved. macOS cross-checked.
- [x] **2.8 `watch::Ref` holds the channel `Mutex`; two same-thread borrows deadlock.**
  `watch.rs:99-101` etc. RwLock the value or split the value lock; harden docs.
  **Done:** split the single `Mutex<State<T>>` into `Shared<T> { value: RwLock<T>,
  version: AtomicU64, book: Arc<Mutex<Book>> }`. `Ref` now holds a `RwLockReadGuard<T>`,
  so multiple borrows (including several on one thread) coexist instead of deadlocking
  on the second. The version is bumped under the value **write** lock and read under the
  value **read** lock, so `borrow_and_update` always sees a consistent (value, version)
  pair; `changed`'s check+enqueue stays under the `book` lock that `send` wakes under, so
  no wakeup is lost. Keeping the value split from the bookkeeping (and giving `book` its
  own `Arc<Mutex<_>>`) lets the `Send` cancel closure capture only the bookkeeping — so
  the public bound stays `T: Send` (a straight `RwLock<State<T>>` would have forced
  `T: Send + Sync`, dropping thread-local `!Sync` values). Handles are `Send`/`Sync` only
  when `T: Send + Sync` (auto). `Ref` docs now warn that holding a borrow across a
  same-thread `send`/`.await` deadlocks. Regression test
  `two_borrows_on_same_thread_do_not_deadlock` (hangs on the old design); public API
  unchanged.
- [x] **2.9 fs ops have no older-kernel fallback; min kernel undocumented.**
  Decide floor (~5.19 full / 5.18 multi-thread) and document, or add blocking-pool fallbacks.
  **Done (both):** documented a precise kernel floor in `lib.rs` and `README.md` —
  recommended **6.1 LTS** (what CI tests), with hard lower bounds of **5.6**
  (single-threaded base ring) and **5.18** (`MSG_RING`, for `spawn_worker`
  multithreading / cross-thread wakes). Audited the opcodes actually used: net socket
  lifecycle ops (`socket` 5.19, `bind`/`listen` 6.11, connect/accept/send/recv/shutdown)
  already fall back to blocking syscalls; the only no-fallback fs op newer than the base
  ring was `IORING_OP_FTRUNCATE` (6.9), so `set_len` now falls back to an inline
  `ftruncate(2)` (a fast metadata op, matching the net `*_sync` fallbacks). Net result:
  the recommended 6.1 floor exercises every feature; older core fs ops (openat/read/
  write/fsync/statx 5.6, mkdirat/renameat/unlinkat 5.15) are covered by the floor.
- [x] **2.10 Smaller robustness:** mpsc bounded-send cancel-registration Arc-cycle leak
  (`mpsc.rs:448-489`); accept fd leak on address-parse failure; stdin inherent-read offload
  steals input + leaks a pool thread after cancel. **Done:**
  - **Accept fd leak (fixed):** the io_uring accept completion handler now wraps the
    accepted fd in an `OwnedFd` immediately, so a failure parsing the peer address
    (`socket_addr_from_storage`) closes the live connection via RAII instead of leaking
    it; the fd is released into `AcceptedSocket` only on success.
  - **mpsc bounded-send cycle (analyzed, no change):** the cancel closure forms a
    channel-state ↔ completion-state reference cycle, but it is broken on **every**
    terminating path — a successful send removes the waiter and `finish()` clears the
    cancel slot; a dropped send future runs the cancel (which removes the waiter and
    drops the closure). The 200-iteration channel stress tests do not leak, confirming
    it self-breaks. Left the (delicate, cancel-safety-critical) code untouched rather
    than risk a regression.
  - **stdin offload (documented):** the blocking-offload fallback cannot cancel an
    in-flight `read(2)`, so a dropped stdin read loses the consumed bytes and pins a
    pool worker until input arrives. This is now clearly documented on `Stdin::read`
    (cancel-safe on the io_uring path, not on the fallback). The proper fix — a dedicated
    buffered stdin reader thread — is deferred to post-0.1 (added below).

## Tier 3 — API shape (breaking window closes at 0.1)

- [x] **3.1 Add `block_on`.** Return a value from async code. **Done:** added
  `runite::block_on(future) -> F::Output`, a value-returning driver alongside `run`. It
  drives the current thread's loop (reusing the same turn machinery + panic firewall) and
  returns as soon as the given future resolves, leaving other spawned tasks queued. The
  future is driven **in place** (stack-pinned via a `std::task::Wake` waker), so it need
  not be `Send` or `'static` and may borrow locals — matching Tokio's `block_on`
  ergonomics. Trips the 2.2 reentrancy guard when nested (panic propagates, since
  `block_on` is a direct driver). Wired through both platform shims. Tests in
  `tests/block_on.rs` (value + borrow-locals, drives timers, returns before an unfinished
  background task, nested-reject). Public API snapshot updated.
- [x] **3.2 `#[runite::main]` discards `Result` → exits 0 on error.** Honor `Termination`
  or reject signature. De-hardcode `::runite` (crate-rename support). Add `#[runite::test]`.
  **Done:** rewrote the proc-macro. `#[runite::main]` now preserves the original return
  type on the generated `fn main` and drives an async body with `block_on`, so
  `async fn main() -> Result<…>` reports a non-zero exit via `Termination` instead of
  silently exiting 0 (sync `main` runs its body, drains the loop with `run`, returns its
  value). Added `crate = "…"` support (parsing the `crate` keyword) on both macros so a
  renamed `runite` dependency works. Added `#[runite::test]`: generates a `#[test]`
  wrapper that `block_on`s the async body, preserves the return type (so tests can `?`),
  and forwards user attributes (`#[ignore]`, `#[should_panic]`) to the wrapper. Tests in
  `tests/macros.rs` (async body, `Result` return, spawn, `should_panic` forwarding,
  `crate =` path); `examples/main_result.rs` compiles the `main -> Result` path; all
  existing examples still build under the new `block_on`-based codegen. **Note:** switching
  `main` to `block_on` means a `#[runite::main]` returns when its future resolves
  (detached tasks abandoned), matching `std`/Tokio `main` semantics rather than the old
  run-to-idle behavior.
- [x] **3.3 fd interop.** `AsFd`/`AsRawFd`/`From<OwnedFd>`/`from_std` on fd-backed types;
  `fd::wait_writable`; `wait_readable` take `impl AsFd`. **Done, all `#[cfg(unix)]`-gated**
  (fds are a Unix concept; the Windows/IOCP backend will expose `AsSocket`/`AsHandle`
  separately). `AsFd` + `AsRawFd` + `From<OwnedFd>` on `TcpStream`, `TcpListener`,
  `UdpSocket`, `TcpSocket`, `UnixStream`, `UnixListener`, `UnixDatagram`, and `File`.
  `from_std` adopts the matching std type and switches sockets to non-blocking mode
  (runite's own sockets are created `SOCK_NONBLOCK` on both backends; `File` needs no
  mode change) — `TcpStream`/`TcpListener`/`UdpSocket` from `std::net`, the Unix types
  from `std::os::unix::net`, `File` from `std::fs::File` (infallible). `set_nonblocking`
  exposed through `sys::current::net`. `runite::fd` is now `#[cfg(unix)]`; `wait_readable`
  and the new `wait_writable` take `impl AsFd` (holding the borrow across the wait so the
  descriptor can't be closed underneath it). Tests in `tests/fd_interop.rs` (adopt a std
  listener + accept, `From<OwnedFd>`, adopt a std `File` + read); public API snapshot
  updated (+93 items).
- [x] **3.4 `UnixStream` implement `AsyncRead`/`AsyncWrite` + `shutdown` + split.**
  **Done:** brought `UnixStream` to parity with `TcpStream`. Re-shaped it to share the fd
  via `Arc<UnixStreamInner>` (so split halves reference-count the same socket) and gave it
  the same stashed pending-op fields (`pending_read`/`read_overflow`/`pending_write`/
  `pending_write_ident`/`pending_shutdown`). Implemented `AsyncRead` + `AsyncWrite` with
  the Tier-1 cancel-safety machinery (overflow retention on read, in-flight write-buffer
  identity guard), added `shutdown(how)`, and `into_split` → `OwnedReadHalf`/
  `OwnedWriteHalf` + `reunite`/`ReuniteError` (with `Display`+`Error`). The inherent
  `read`/`write` now delegate to the trait poll paths (cancel-safe, like 1.7). Manual
  `Debug` impl (the future fields can't derive it). So `copy`, `BufReader`, and the
  `AsyncReadExt`/`AsyncWriteExt` combinators now work on `UnixStream`. Tests in
  `tests/unix_stream.rs` (trait read/write + shutdown-to-EOF, concurrent split halves,
  reunite origin check); public API snapshot updated. Closes the 1.7 note that UnixStream
  gains cancel-safe inherent I/O once it implements the traits.
- [x] **3.5 `Command::output()` → `Output { status, stdout, stderr }`; don't error on
  non-zero exit; close child stdin.** **Done:** `output()` now returns
  `io::Result<Output>` where `Output { status, stdout, stderr }` mirrors
  `std::process::Output` (derives `Clone/Debug/Eq/PartialEq`, re-exported as
  `runite::process::Output`). It forces stdout+stderr to piped, redirects **stdin to
  null** (a stdin-reading child sees EOF instead of hanging — matches std), reads stdout
  and stderr **concurrently** (spawned stderr reader) to avoid pipe-buffer deadlock, and
  **no longer treats a non-zero exit as an error** — the caller inspects `output.status`.
  Updated the process tests (the old "false errors" assertion now checks
  `status.code() == Some(1)`, and the stderr-drain test asserts the 200KB is captured)
  and the doc examples. Public API snapshot updated.
- [x] **3.6 Panic story + `#[non_exhaustive]`.** `JoinError::Panicked`; mark `SignalKind`
  (or make opaque w/ `from_raw`), `JoinError`, `QueueError`, `MissedTickBehavior`.
  **Done:** `JoinError::Panicked` landed in 2.1 (the panic story). Added
  `#[non_exhaustive]` to the four growable public enums — `JoinError`, `QueueError`,
  `SignalKind`, `MissedTickBehavior` — so future variants (new signal kinds, new join
  failure modes, new tick behaviors) are additive rather than breaking. Internal
  exhaustive matches are unaffected; no external test needed a wildcard arm. API snapshot
  updated. Kept `SignalKind` as an enum (rather than opaque + `from_raw`) since it now
  grows non-breakingly.
- [x] **3.7 Conventions sweep.** `Debug` on all public types; `#[must_use]` on guards/
  lazy futures/handles; `Display`+`Error` on mpsc/oneshot errors; export `io::ext` future
  types; drop `TcpListener: Clone` for `try_clone`. **Done:**
  - **`Display` + `Error`** on all six channel error types (`mpsc::{SendError, TrySendError,
    TryRecvError}`, `oneshot::{SendError, RecvError, TryRecvError}`), so they satisfy
    `std::error::Error` and compose with `?`/`Box<dyn Error>`.
  - **`TcpListener: Clone` dropped** — the derived `Clone` silently shared the fd via `Arc`
    (misleading vs std). Replaced with a private `share()` for the internal `incoming()`
    use; the public path to an independent listener is `try_clone` (dups the fd), matching
    `std::net`.
  - **Exported the `io::ext` future types** (`Read`, `ReadExact`, `ReadToEnd`, `Write`,
    `WriteAll`, `Flush`, `Close`) — they were `pub` inside a private module and therefore
    unnameable return types.
  - **`#[must_use]`** on every lazy future/stream combinator in `io::ext`/`io::stream`
    (they do nothing unless awaited/polled).
  - **`Debug`** filled in for the primary I/O types that couldn't derive it (future fields):
    manual impls for `TcpStream` and both `Incoming` streams, derives on the `TcpStream`
    split halves (`UnixStream` + its halves already got theirs in 3.4).
  Public API snapshot updated.
- [x] **3.8 fs semantic gaps.** `symlink_metadata` (so `is_symlink` can be true); `File::seek`;
  document/drop implicit `SO_REUSEADDR`; unify `Metadata::mode()` across platforms.
  **Done (all four):**
  - **`SO_REUSEADDR`** — flipped `TcpListener::bind` to std's default-off, opt-in via the
    existing cross-platform `TcpSocket` (committed separately).
  - **`symlink_metadata`** — added `fs::symlink_metadata(path)` (the backend already
    threaded `follow_symlinks`; it now passes `false`), so `Metadata::is_symlink()` can be
    `true`. Regression test with a real symlink.
  - **`File::seek`** — added `File::seek(SeekFrom) -> u64` (inline `lseek(2)`; both backends
    drive sequential I/O off the kernel fd cursor, so seek composes with `read`/`write`).
    Test covers Start/Current/End.
  - **`Metadata::mode()` unification** — macOS was masking to `0o7777` (permission bits
    only) while Linux returned the full `st_mode`; now both return the full `st_mode`
    (file-type + permission bits, widened to `u32`) matching
    `std::os::unix::fs::MetadataExt::mode`. Test asserts the `S_IFREG` type bits are
    present.
  Public API snapshot updated.
- [x] **3.9 Pin proc-macro dep `=0.1.0`.** Done: `runite-proc-macros` dependency pinned to `=0.1.0` so the lockstep-versioned macro crate can never resolve to a mismatched version.
- [x] **3.10 Per-method cancel-safety docs.** **Done:** added explicit `# Cancel safety`
  sections to the primary await points, documenting the facts established in Tier 1.
  Cancel-safe: `TcpStream::read`, `File::read`, `broadcast::recv`, `watch::changed`
  (+ `mpsc::recv`/`oneshot::recv`/`Stdin::read` already documented in 1.6/2.10). Explicitly
  **not** cancel-safe: `TcpStream::write`, `File::write` (a completion-based write dropped
  mid-flight may have committed bytes without reporting the count). `time::timeout`
  documented as inheriting its inner future's cancel-safety. `UnixStream` read/write carry
  the same note from 3.4.

## Tier 4 — Release mechanics

- [ ] **4.1 CI builds default (empty) feature set** + gate `docs/public-api.md` drift.
  Also add example smoke-runs: every self-driving example plus `command_center --demo`
  and `chat_server --demo` run to completion in CI (all 13 verified locally when the
  example suite landed).
- [ ] **4.2 Merge the two ROADMAP files; rewrite CHANGELOG to a `0.1.0` narrative.**
- [ ] **4.3 Packaging:** exclude `mise.lock`; `doc_auto_cfg` feature badges;
  `cargo publish --dry-run -p runite` step in release.yml.
- [ ] **4.4 Fix ARCHITECTURE.md drift** (4 stale claims) and move the microtask-starvation
  warning inside the drain loop (`scheduler.rs:376-388`).
- [ ] **4.5 Regression tests** for 1.1/1.2/1.3/1.7 + a non-`#[ignore]` signal e2e test.

## Performance (0.1-eligible)

- [ ] **P-1 Batched/deferred submission** (also fixes 0.2). Flush once per turn via
  `enter(to_submit, min_complete, GETEVENTS)`; owned timespec storage; atomic linked pairs.
- [ ] **P-2 Staging-model cleanup, no API change.** Stop zeroing; return the staging buffer
  directly; drop `Arc<Mutex<Box<[u8]>>>` for a closure-owned `Box<[u8]>`.
- [ ] **P-3 Setup flags** `SINGLE_ISSUER`/`COOP_TASKRUN`/`DEFER_TASKRUN`/`SUBMIT_ALL`;
  larger rings; skip redundant timer rearms.

## Discovered during work (not in the original review)

- [ ] **D-1 `spawn_blocking` doctests flake (pre-existing, confirmed on baseline).**
  The merged doctest binary intermittently fails `src/task.rs` blocking examples
  with `JoinError::Cancelled` (~50% on a clean checkout). Root cause: `run()` can
  reach its exit commit while a blocking task is still in flight / before its
  oneshot result is delivered, and the dropped sender surfaces as `Cancelled`
  (conflating shutdown with cancellation). Belongs with the blocking-pool work in
  2.1 (panic isolation) and the "run() exits while blocking in flight" note; also
  a CI-stability issue. Fix: keep `run()` alive while blocking tasks are
  outstanding (register a pending op for the duration) and give the blocking pool
  a distinct panic vs. shutdown outcome.

- [x] **D-2 Completion-drop self-deadlock in the cancel path (pre-existing).**
  `op/completion.rs`. `CompletionFuture::drop` held the `cancel` mutex guard
  across the `cancel()` call (`if let Some(cancel) = mutex.lock().take()` keeps
  the guard alive for the whole body). Channel waiter cancels call `finish()`
  synchronously, which re-locks the same mutex → self-deadlock on drop; io_uring
  cancels finish asynchronously so they never hit it. Result: dropping any
  channel receiver (watch/mpsc/broadcast/oneshot) with a pending waiter — e.g.
  a `timeout(dur, rx.changed())` that elapses — froze the runtime thread.
  **Done:** take the callback out and release the guard before invoking it.
  Confirmed on baseline; full suite green after the fix.

## Post-0.1 (deferred)

- [-] Perf: buf_ring + multishot recv/accept → registered buffers (files) → owned-buffer
  API → `SEND_ZC`.
- [-] Completeness: `AsyncBufRead`, vectored I/O, `AsyncSeek`; fs/net/process/sync/time gaps.
- [-] `select!` v2 (`else`/`biased`/>16 arms).
- [-] `CancellationToken`; `WorkerHandle::join()`.
- [-] Hardening: Miri (mock driver), ASAN, io_uring-limited-kernel CI, macOS runner;
  macOS child-wait busy-poll; `ReadDir` unbounded producer.
- [-] Cancel-safe buffered stdin: a dedicated long-lived reader thread feeding a buffer,
  so a dropped `Stdin::read` on the blocking-offload path (macOS / pre-uring-stdin Linux)
  no longer loses bytes or pins a pool worker (see 2.10).
- [-] Windows IOCP backend.
- [-] Dead-code cleanup (fold into P-1 driver refactor).

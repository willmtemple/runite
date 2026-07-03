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
  free). Cross-thread wake under a full remote queue is still best-effort — folds
  into task 2.3's reserved-capacity work.
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
- [ ] **1.2 Pending-op/buffer mismatch across polls.**
  `net/mod.rs:731-780`, `fs.rs:400-404`, `stdio.rs:453-457`, `hyper_impl.rs:48-51`.
  Smaller re-poll buffer destroys received bytes; changed write buffer misreports;
  hyper adapter panics. Stash surplus in a per-object overflow buffer / clamp writes.
- [x] **1.3 `read(true).truncate(true)` truncates on Linux.**
  `sys/linux/fs.rs:542-573`. **Done:** split `open_flags` into std-mirroring
  `access_mode` + `creation_mode`; invalid access combinations (truncate/create
  without write, truncate+append without create_new) now fail `EINVAL` instead of
  silently opening `O_RDONLY | O_TRUNC`. Regression test in `tests/fs.rs` (asserts
  the open errors and the file is preserved); verified it fails without the guard.
- [ ] **1.4 Mixing `mpsc::recv()` with the `Stream` impl reorders / aborts process.**
  `channel/mpsc.rs:686-711`. Unify both on the persistent `stream_wait` slot.
- [ ] **1.5 `watch::changed()` version regression after cancelled wait.**
  `channel/watch.rs:450-462`. Only accept `version > self.version`; never regress.
- [ ] **1.6 `mpsc::recv`/`oneshot::recv` not cancel-safe.**
  `mpsc.rs:551-576`, `oneshot.rs:167-169`. Adopt the persistent-slot pattern (park the
  delivered value, pick it up on next poll) that broadcast/watch already use.
- [ ] **1.7 Inherent async I/O methods lose data on cancellation.**
  `net/mod.rs:451-468` etc. Route inherent read/recv through the same pending-op stash;
  document cancel-safety per method.
- [ ] **1.8 macOS `sync_all`/`sync_data` durability inverted.**
  `sys/macos/fs.rs:124-138`. Swap so `sync_all` → `F_FULLFSYNC`.
- [ ] **1.9 Accepted sockets missing `SOCK_CLOEXEC` on uring path.**
  `sys/linux/net.rs:201-207`. Set `sqe.op_flags = SOCK_CLOEXEC` (consider `SOCK_NONBLOCK`).

## Tier 2 — Hangs, panics, robustness

- [ ] **2.1 No panic isolation.** `catch_unwind` per task/macrotask/microtask and per
  blocking job; `Drop` guard on worker completion (notify parent on unwind); scope-guard
  reset of `closing` in `run()`; add `JoinError::Panicked` (coordinate with 3.6).
- [ ] **2.2 No reentrancy guard on `run()`/`run_until_stalled()`.**
  Add `in_event_loop` flag; panic on nested entry (tokio-style).
- [ ] **2.3 Cross-thread completion wakes dropped when remote queue full.**
  `op/completion.rs:118-127`. Give internal completion wakes a reserved/unbounded
  internal queue (bounded by `pending_ops`).
- [ ] **2.4 Notifier TOCTOU on raw fds.** `linux/driver.rs:42-71` (+ macOS). Notifier holds
  a dup'd `OwnedFd` of the ring / pipe write end.
- [ ] **2.5 io_uring robustness cluster.** Check CQ-overflow flags / `FEAT_NODROP`; honor
  partial-submission return; log/ handle MSG_RING failure CQEs (kernel <5.18 has no
  cross-thread wake); drain the global fallback submitter CQ; give `TIMEOUT_UPDATE` a real
  token + pre-5.11 remove/re-add fallback.
- [ ] **2.6 `IORING_OP_SOCKET` flags in wrong SQE field.** `sys/linux/net.rs:89-99`.
  `sqe.off = (type | flags); sqe.op_flags = 0`.
- [ ] **2.7 Signal reader consumes a shared blocking-pool worker.**
  `signal/unix.rs:319-324`. Move to a dedicated `std::thread`.
- [ ] **2.8 `watch::Ref` holds the channel `Mutex`; two same-thread borrows deadlock.**
  `watch.rs:99-101` etc. RwLock the value or split the value lock; harden docs.
- [ ] **2.9 fs ops have no older-kernel fallback; min kernel undocumented.**
  Decide floor (~5.19 full / 5.18 multi-thread) and document, or add blocking-pool fallbacks.
- [ ] **2.10 Smaller robustness:** mpsc bounded-send cancel-registration Arc-cycle leak
  (`mpsc.rs:448-489`); accept fd leak on address-parse failure; stdin inherent-read offload
  steals input + leaks a pool thread after cancel.

## Tier 3 — API shape (breaking window closes at 0.1)

- [ ] **3.1 Add `block_on`.** Return a value from async code.
- [ ] **3.2 `#[runite::main]` discards `Result` → exits 0 on error.** Honor `Termination`
  or reject signature. De-hardcode `::runite` (crate-rename support). Add `#[runite::test]`.
- [ ] **3.3 fd interop.** `AsFd`/`AsRawFd`/`From<OwnedFd>`/`from_std` on fd-backed types;
  `fd::wait_writable`; `wait_readable` take `impl AsFd`.
- [ ] **3.4 `UnixStream` implement `AsyncRead`/`AsyncWrite` + `shutdown` + split.**
- [ ] **3.5 `Command::output()` → `Output { status, stdout, stderr }`; don't error on
  non-zero exit; close child stdin.**
- [ ] **3.6 Panic story + `#[non_exhaustive]`.** `JoinError::Panicked`; mark `SignalKind`
  (or make opaque w/ `from_raw`), `JoinError`, `QueueError`, `MissedTickBehavior`.
- [ ] **3.7 Conventions sweep.** `Debug` on all public types; `#[must_use]` on guards/
  lazy futures/handles; `Display`+`Error` on mpsc/oneshot errors; export `io::ext` future
  types; drop `TcpListener: Clone` for `try_clone`.
- [ ] **3.8 fs semantic gaps.** `symlink_metadata` (so `is_symlink` can be true); `File::seek`;
  document/drop implicit `SO_REUSEADDR`; unify `Metadata::mode()` across platforms.
- [ ] **3.9 Pin proc-macro dep `=0.1.0`.**
- [ ] **3.10 Per-method cancel-safety docs.**

## Tier 4 — Release mechanics

- [ ] **4.1 CI builds default (empty) feature set** + gate `docs/public-api.md` drift.
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

## Post-0.1 (deferred)

- [-] Perf: buf_ring + multishot recv/accept → registered buffers (files) → owned-buffer
  API → `SEND_ZC`.
- [-] Completeness: `AsyncBufRead`, vectored I/O, `AsyncSeek`; fs/net/process/sync/time gaps.
- [-] `select!` v2 (`else`/`biased`/>16 arms).
- [-] `CancellationToken`; `WorkerHandle::join()`.
- [-] Hardening: Miri (mock driver), ASAN, io_uring-limited-kernel CI, macOS runner;
  macOS child-wait busy-poll; `ReadDir` unbounded producer.
- [-] Windows IOCP backend.
- [-] Dead-code cleanup (fold into P-1 driver refactor).

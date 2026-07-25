# Migrating from runite 0.1 to 0.2

runite 0.2 keeps the event-loop-per-thread model, but tightens resource
affinity, runtime shutdown, and cancellation contracts. Update both crates in
lockstep; applications normally depend only on `runite`:

```toml
[dependencies]
runite = "0.2"
```

## Required source changes

This section covers the change the compiler will force you to make. The
[silent behavior changes](#silent-behavior-changes) below produce no diagnostic
at all and need a deliberate audit.

### Resource adoption is fallible

The portable `File` and TCP/UDP socket conversions from owned
standard-library resources now use `TryFrom`, and their
`from_owned`/`from_std` constructors return `std::io::Result`:

```rust
#[cfg(unix)]
fn adopt(owned: std::os::fd::OwnedFd) -> std::io::Result<runite::fs::File> {
    let file = runite::fs::File::try_from(owned)?;
    // Equivalent: runite::fs::File::from_owned(owned)?
    Ok(file)
}
```

Replace `File::from(...)`, `TcpStream::from(...)`, and corresponding
TCP/UDP conversions with `try_from(...)` or `from_owned(...)?`; add `?`/error
handling to `from_std` calls. Unix-domain socket types remain platform
extensions with their existing Unix conversion surface.

On Unix, adoption can fail while applying the nonblocking/runtime setup. On
Windows it additionally rejects synchronous handles, file objects configured
with `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`, and objects already associated
with another runtime thread's completion port. A Windows handle cannot be
rebound after association: create it on the intended runtime thread or pass
data across threads instead. `try_clone` preserves the proven affinity and,
for files, the shared serialized cursor.

## Silent behavior changes

Neither change below produces a compiler diagnostic. Existing code keeps
building and behaves differently, so these need an explicit audit.

### `run()` now cancels tasks that are still pending at quiescence

`JoinError::Cancelled` is **not** a new variant — it existed in 0.1 and no
`match` needs updating. What changed is when it is produced. In 0.1, `run()`
returned and left pending spawned tasks alive for a later `run()`. In 0.2,
reaching quiescence resolves every still-pending task to `Cancelled` and drops
its future.

A task is safe if the runtime can see its wake source: runite channels, timers,
I/O operations, `spawn_blocking`, signals, and `WorkerHandle::join` all register
runtime liveness. A task whose only wake source is a bare `Waker` clone handed
to a foreign thread — a third-party leaf future, a `futures` combinator over a
non-runite source, or a hand-written `poll_fn` — is *not* visible, and will be
cancelled rather than resumed:

```rust
match handle.await {
    Ok(value) => consume(value),
    Err(error) if error.is_aborted() => {}
    Err(error) if error.is_cancelled() => {}
    Err(error) if error.is_panicked() => {}
    Err(_) => {}
}

fn consume<T>(_: T) {}
```

Exposure depends on the entry point: an `async fn main` under
`#[runite::main]` is driven by `block_on`, which has no quiescence probe. A
synchronous `#[runite::main] fn main`, and any explicit `runite::run()`, do
reach quiescence.

Dropping a `JoinHandle` still detaches; it does not itself abort the task.
`block_on` still returns as soon as its input future resolves and leaves other
ready tasks for a later driver call.

### `select!` no longer polls in lexical order

The 0.1 macro always polled in lexical order. The 0.2 default rotates the
starting arm. Audit every existing `select!` — `grep -rn 'select!' src/` — and
add `biased;` wherever lexical priority was intentional (a shutdown arm written
first, for example, is no longer checked first):

```rust
async fn choose() {
    let value = runite::select! {
        biased;
        value = async { 1 }, if true => value,
        value = async { 2 } => value,
        else => 0,
    };
    let _ = value;
}
```

0.2 also accepts branch guards, `else`, output patterns, and more than sixteen
arms. Guards are evaluated once in lexical order before futures are created; a
disabled future is created but never polled. Pattern mismatch disables that
branch. All branch futures are dropped before the winning handler runs, so
handlers may safely `.await`, `return`, or `break`.

## New APIs to adopt

Issue #9's portable traits are now available:

- `AsyncBufRead::{poll_fill_buf, consume}`;
- `AsyncSeek::poll_seek` and `AsyncSeekExt::seek`;
- `AsyncRead::poll_read_vectored` / `AsyncReadExt::read_vectored`;
- `AsyncWrite::poll_write_vectored` / `AsyncWriteExt::write_vectored`.

The vectored trait defaults use the first non-empty slice, so existing custom
`AsyncRead`/`AsyncWrite` implementations continue to compile. Implementors can
override the methods with native vectored I/O. Note that runite's own backends
do not yet do so: every built-in type writes the first non-empty slice rather
than issuing `writev`/`readv`, so a vectored call currently costs one round trip
per slice. Treat the methods as forward-compatible API, not as a scatter/gather
optimization. `BufReader` implements `AsyncBufRead` and conditionally
`AsyncSeek`; `File` implements `AsyncSeek`. The `futures-compat` adapters cover
all four traits.

Workers can now be joined without blocking a runtime thread:

```rust
fn example() {
    let worker = runite::spawn_worker(|| {}, || {});
    runite::block_on(worker.join()).expect("worker should exit cleanly");
    assert!(worker.is_finished());
}
```

`WorkerJoin` completes only after the worker OS thread and TLS destructors have
exited. Setup and runtime/teardown panics become `WorkerJoinError`; the result
is retained for later join futures. Dropping a pending join future only
unregisters that waiter.

## Behavioral changes

### Runtime and timers

- Ordinary threads keep their runtime driver installed across sequential
  `run()` and `block_on()` calls. Runtime-owned workers close atomically,
  explicitly tear down, and publish completion only after an OS-thread join.
- `run`, `block_on`, `run_until_stalled`, and `run_ready_tasks` all reject
  re-entry.
- Accepted blocking jobs and I/O operations keep the runtime live through
  terminal publication, closing completion-vs-quiescence races.
- Cancelling a callback timeout after expiry but before its queued macrotask
  starts now suppresses it. A panicking interval is cancelled.

### Reads, writes, and shutdown

Reads remain cancel-safe: the resource owns an accepted read and retains any
completed bytes for a later caller. Writes now have a distinct logical
operation identity. Dropping a write future does not imply the OS write was
undone, but its eventual byte count cannot be credited to a later buffer.
Shared `File` clones serialize live write generations FIFO; cancelled
generations are removed without losing another future's completion.

After a write returns `Pending`, direct poll callers must continue polling the
same logical buffer. Prefer the extension futures for cancellation-capable
code. `Compat<T>` similarly owns one accepted scalar or vectored write buffer
until it drains. Read/write shutdown is shared and terminal results, including
raw OS error codes, are replayed consistently to every waiter.

### Standard input and directories

All `Stdin` handles now consume from one demand-driven process-wide reader with
a bounded 64 KiB buffer. Cancelling a read removes only its waiter; bytes read
by the dedicated thread remain available. Multiple handles compete for the
same stream. Spawning a child with inherited stdin pauses the reader for a
lossless handoff. On Windows, inheriting an active console read returns
`WouldBlock`; retry after that read completes or avoid inherited stdin.

`read_dir` now uses resumable 32-entry blocking-pool batches on every platform.
No worker waits for queue capacity. Dropping the stream discards buffered
entries and stored iterator state, although the current OS directory call may
finish before cancellation is observed.

## Linux compatibility floor

The hard kernel floor is Linux 5.6 for both single-threaded and worker
runtimes. `MSG_RING` (5.18) is optional; older kernels use an `eventfd`
notifier. Linux 6.1 is the recommended baseline, not a promise that newer
opcodes are present: `FTRUNCATE` (6.9) falls back to `ftruncate(2)`, the
directory opcodes `MKDIRAT` (5.15), `RENAMEAT` (5.11), and `UNLINKAT` (5.11)
fall back to the corresponding `*at(2)` syscall, and missing socket data-path
opcodes use nonblocking readiness rather than a blocking-pool call.

CI does not pin or assert a kernel version — it runs on GitHub-hosted Ubuntu
runners — so these fallbacks are verified by opcode-capability injection tests
rather than against an actual older kernel.

See the [0.2 changelog](../CHANGELOG.md) and
[Windows backend notes](WINDOWS.md) for the complete release details.

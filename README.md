# runite

`runite` is an **event-loop-per-thread**, non-work-stealing async runtime for
Rust. Each runtime thread owns its own scheduler, timer heap, and platform I/O
driver. `io_uring` on Linux, `kqueue` on macOS, and IOCP on Windows. It uses
JavaScript-style microtask/macrotask scheduling to give deterministic flush
points.

It is built for UI front-ends, embedded event loops, and fine-grained reactive
systems rather than as a general-purpose high-throughput server runtime. It
deliberately prefers simple per-thread event loops, thread-local state, and
predictable scheduling over work-stealing, `Send`-future ergonomics, and maximum
I/O throughput.

> **Status:** early 0.x release. APIs may change before 1.0. Breaking changes
> are documented per release in the migration guides:
> [0.1 → 0.2](./docs/MIGRATING-0.2.md), [0.2 → 0.3](./docs/MIGRATING-0.3.md).

## Platform support

| Platform         | Backend            | Status    |
| ---------------- | ------------------ | --------- |
| Linux `x86_64`   | `io_uring`         | Primary   |
| Linux `aarch64`  | `io_uring`         | Supported |
| macOS `aarch64`  | `kqueue` + offload | Supported |
| Windows `x86_64` | IOCP + offload     | Supported |

The Windows backend drives sockets, files, and child-process pipes with
overlapped I/O through one I/O completion port per runtime thread, offloading
to the blocking pool only where Windows has no asynchronous form (open,
metadata, directory scans, DNS, console output). Stdin instead uses the same
demand-driven, bounded process-wide dedicated reader as the Unix backends. See
[docs/WINDOWS.md](./docs/WINDOWS.md) for the design. Windows-only differences:
`runite::fd` (descriptor readiness) and `runite::net::unix` (Unix-domain
sockets) are not available, `TcpSocket::set_reuseport` reports
`ErrorKind::Unsupported`, and `runite::signal::windows` replaces
`runite::signal::unix` (the portable `runite::signal::ctrl_c` works on all
platforms). Unsupported targets fail to compile with a clear error.

### Minimum Linux kernel

The io_uring backend recommends **Linux 6.1+**. The hard floor is 5.6; newer
opcodes are used opportunistically. CI runs on GitHub-hosted Ubuntu runners
(currently 6.8 and newer) and does not pin or assert a kernel version, so the
older-kernel paths below are covered by opcode-capability injection tests rather
than by running against an actual older kernel.

- **5.6** — base ring operations (`openat`/`read`/`write`/`fsync`/`statx`/…);
  required.
- **5.18** — `MSG_RING`, preferred for cross-thread wakeups. Older kernels use
  a nonblocking `eventfd` fallback, including for `spawn_worker` and blocking
  completions.
- `File::set_len` uses `FTRUNCATE` (6.9) and falls back to `ftruncate(2)` on
  older kernels. `OpenOptions::truncate` uses `OPENAT | O_TRUNC`.
- Directory operations use `MKDIRAT` (5.15), `RENAMEAT` (5.11), and `UNLINKAT`
  (5.11), falling back to `mkdirat(2)`/`renameat(2)`/`unlinkat(2)` on the
  blocking pool below those versions.
- Socket operations (`socket` 5.19, `bind`/`listen` 6.11, and later
  `connect`/`accept`/`send`/`recv`/`shutdown` opcodes) transparently fall back.
  Control operations run inline on nonblocking sockets; data operations wait
  for io_uring poll readiness before retrying, so the event loop and blocking
  pool are not parked.

Thus 6.1 is the tested/recommended baseline, not a requirement for every newer
native opcode. The hard lower bound is 5.6 for both single- and
multithreaded runtimes.

## Installation

```toml
[dependencies]
runite = "0.2"
```

## Quick start

```rust
#[runite::main]
async fn main() {
    let entries = runite::fs::read_dir(".").await.unwrap();
    // ... drive async work on the current runtime thread
}
```

You can also use a synchronous entry point and drive the loop yourself:

```rust
#[runite::main]
fn main() {
    runite::spawn(async {
        runite::time::sleep(std::time::Duration::from_millis(10)).await;
    });
}
```

Both of those start the thread's runtime implicitly and panic if it cannot be
started. `Builder` makes startup explicit and returns the failure instead:

```rust
fn main() -> std::io::Result<()> {
    let runtime = runite::Builder::new().build()?;
    runtime.run();
    Ok(())
}
```

A runtime is configured by the call that creates it, so `build()` must be the
thread's first runtime call — inside a `#[runite::main]` body it reports
`AlreadyExists`, because the attribute has already started one. Pass the
settings to the attribute instead:

```rust,ignore
// Linux only: `ring_entries` sizes the io_uring submission queue, which kqueue
// and IOCP have no equivalent of. On macOS or Windows this is a compile error
// naming the platform, not a setting that quietly does nothing.
#[runite::main(ring_entries = 32)]
async fn main() {
    // 32 submission entries instead of the default 256, to leave locked memory
    // for a profiler attached to the same process.
}
```

The same setting on a hand-built runtime, where a startup failure can be
handled rather than raised:

```rust,ignore
use runite::os::linux::BuilderExt; // Linux only, as above.

fn main() -> std::io::Result<()> {
    let runtime = runite::Builder::new().ring_entries(32).build()?;
    runtime.run();
    Ok(())
}
```

## What you get

- **Entry points:** `#[runite::main]` (works on `fn main` or `async fn main`)
  and `#[runite::test]`, either of which can carry the runtime's settings —
  `#[runite::main(ring_entries = 32)]`; `block_on` for driving one future to
  completion; and `try_block_on`/`Builder::build` when a startup failure should
  be reported rather than raised. The attributes start the runtime before your
  body runs, so `Builder::build` inside one is refused; it is for a
  hand-written `fn main`.
- **Event loop:** `run`, `run_until_stalled`, `run_ready_tasks`, `queue_macrotask`,
  `queue_microtask`, `spawn`, `yield_now`, and `current_turn` for a key that joins
  your own diagnostics to the loop iteration that produced them.
- **Workers:** `spawn_worker`, nonblocking `WorkerHandle::join`, and the
  `Send`-only cross-thread `ThreadHandle::queue_macrotask`.
- **Tasks:** spawned futures return `JoinHandle<T>` that awaits to `Result<T, JoinError>`;
  use `abort`, `abort_handle`, `is_finished`, and cloneable `AbortHandle`s for cancellation,
  and `task::JoinSet` for structured ownership of a group of local tasks.
  `JoinError::Cancelled` identifies tasks terminalized when `run()` reaches
  quiescence without a scheduler-visible wake source.
- **Timers:** `time::set_timeout` and `time::set_interval` (each returns a
  handle with `.cancel()`), plus `time::{sleep, timeout, interval}` where
  `time::interval` is the awaitable interval.
- **I/O:** async `fs`, `net` (TCP/UDP everywhere; Unix-domain sockets on Unix), `stdio`, and crate-local
  `AsyncRead`/`AsyncBufRead`/`AsyncWrite`/`AsyncSeek`/`Stream` traits with vectored
  method surface (scalar-backed today — no backend issues `readv`/`writev` yet) and
  future adapters; TCP split/reunite, listener `incoming()` streams, async
  stdin/stdout/stderr, and `BufReader`/`BufWriter`.
- **Control flow:** fair-by-default `select!` with `biased;`, branch guards,
  `else`, output patterns, and handlers that can await or leave the surrounding
  control-flow context.
- **Processes:** `process::{Command, Child}` with piped async stdio, `kill`, and `wait`;
  standard streams can be wired to a descriptor you already own (`Stdio::from(OwnedFd)`),
  a Unix `pre_exec` hook runs between fork and exec, and `Child::from_pid` adopts a
  process started elsewhere.
- **Channels & sync:** `channel::{mpsc, oneshot, broadcast, watch}`,
  `sync::{Mutex, RwLock, Semaphore, Notify, OnceCell}`.
- **Blocking offload:** `spawn_blocking` onto a bounded shared OS-thread pool.
- **Signals:** portable `signal::ctrl_c`, async Unix signal handling (including SIGWINCH
  via `SignalKind::WindowChange`) with `signal::unix::signals` for watching several
  kinds on one stream, and Windows console control events (`signal::windows`).

Accepted reads are resource-owned and cancel-safe: bytes remain available to a
later caller. Cancelling an accepted write does not promise that the OS write
was undone, but its completion is tied to that logical write and is never
credited to a later buffer. Stdin uses one bounded, demand-driven process
reader; `read_dir` uses bounded resumable blocking-pool batches.

### Scaling across cores

`runite` is event-loop-per-thread: each runtime thread drives its own local scheduler and
accepts `!Send` futures. To scale CPU-bound or server workloads across cores, start one
event loop per core with `spawn_worker`; on Linux and macOS, servers should bind per-core accept
loops with `SO_REUSEPORT` so the OS distributes inbound connections. See [ARCHITECTURE.md](./ARCHITECTURE.md)
for the full threading and scaling model.

## Feature flags

| Feature          | Default | Description                                                           |
| ---------------- | ------- | --------------------------------------------------------------------- |
| `hyper`          | off     | `hyper` 1.x integration: transport impls for `TcpStream` (and `UnixStream` on Unix) plus the `hyper_rt` executor/timer for server and HTTP/2 use. |
| `futures-compat` | off     | `io::compat` adapters to/from the `futures-io` traits.                |
| `rustls`         | off     | `tls::TlsConnector`/`TlsAcceptor`/`TlsStream`: TLS client and server sessions over any runite transport, and over `hyper` when that feature is on too. runite depends on `rustls` with **no** provider feature — the application chooses `ring` or `aws-lc-rs` — and re-exports it as `runite::tls::rustls`, so a `rustls` major release is a breaking change for runite. |

## Configuration

| Environment variable           | Effect                                                                |
| ------------------------------ | --------------------------------------------------------------------- |
| `RUNITE_BLOCKING_THREADS`      | Size of the shared blocking-task pool (clamped 1..=32).               |
| `RUNITE_REMOTE_QUEUE_CAPACITY` | Bound on the per-thread cross-thread macrotask queue (default 65536). |
| `RUNITE_IO_URING_DEFER_SUBMISSIONS` | Linux only. Set to `0` to submit each io_uring operation immediately instead of batching per loop turn. Diagnostic; batching is the default. |

## Examples

Start with these — each one demonstrates a reason the event-loop-per-thread
model exists, not just an API:

| Example                                                      | What it shows                                                                                                                                                                                       |
| ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`command_center`](./examples/command_center.rs)             | An interactive terminal app: async stdin, background jobs, and shared `Rc<RefCell>` state on one loop that never blocks on you. Interactive, or `-- --demo`.                                        |
| [`chat_server`](./examples/chat_server.rs)                   | A collaborative-session backend whose entire room state is `Rc<RefCell<HashMap>>` — no `Arc`, no `Mutex`, no `Send` bounds — plus Ctrl-C graceful shutdown. Interactive (`nc` in!), or `-- --demo`. |
| [`background_workers`](./examples/background_workers.rs)     | The Web-Workers discipline: CPU work on the blocking pool while a heartbeat _measures_ that the loop stayed responsive. Run with `-- --blocking` to see the jank, quantified.                       |
| [`frame_loop_embedding`](./examples/frame_loop_embedding.rs) | runite as a guest inside a host frame loop (GUI/game shape): `run_until_stalled()` per frame, `requestAnimationFrame`-style tasks, render from settled state.                                       |
| [`build_pipeline`](./examples/build_pipeline.rs)             | Dev-tool process orchestration: bounded-concurrency subprocess fan-out with `Command::output`, where a failing step is data, not a crash.                                                           |

Feature tours of specific APIs:

```sh
cargo run --example runtime_loop_showcase   # scheduling rules, asserted in order
cargo run --example channel_showcase        # mpsc/oneshot, on and across threads
cargo run --example broadcast_watch         # broadcast + watch channels
cargo run --example async_fs_showcase       # async filesystem API
cargo run --example tcp_echo_server         # TcpStream split halves
cargo run --example subprocess_pipeline     # piped child stdin/stdout
cargo run --example main_result             # #[runite::main] with Result
cargo run --example hyper_http_client --features hyper
```

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the threading model, micro/macro task
scheduling, run lifecycle, cancellation and buffer-ownership rules, the driver abstraction,
the platform parity matrix, and the documented safety invariants. Upgrading?
Read [Migrating to 0.2](./docs/MIGRATING-0.2.md) or
[Migrating to 0.3](./docs/MIGRATING-0.3.md).

## Development

The toolchain is pinned with [mise](https://mise.jdx.dev/). Install it, then:

```sh
mise install            # fetch the pinned Rust toolchain and dev tools
mise run check          # fmt + clippy + tests + workflow lint (the full local gate)
```

Individual tasks:

| Task                | Command                                                                | Purpose                            |
| ------------------- | ---------------------------------------------------------------------- | ---------------------------------- |
| `mise run build`    | `cargo build --workspace --all-targets`                                | Build the workspace.               |
| `mise run test`     | `cargo test --workspace --all-features`                                | Unit, integration, and doctests.   |
| `mise run lint`     | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Lint with warnings denied.         |
| `mise run bench`    | `cargo bench --workspace --all-features`                               | Criterion benchmarks (`benches/`). |
| `mise run coverage` | `cargo llvm-cov --workspace --all-features ...`                        | HTML + lcov coverage report.       |
| `mise run api-report-check` | `cargo run -p xtask -- api-report --check`                    | Check target/feature API surfaces. |
| `mise run package-verify` | `cargo run -p xtask -- release-verify`                         | Verify unpacked release artifacts. |
| `mise run miri` / `asan` / `tsan` | Pinned-nightly focused safety suites.                  | Driver-free Miri/TSan; Linux ASan. |
| `mise run capability-matrix` | Injected constrained-opcode dispatch tests, then the io-facing tests under two masked opcode profiles. | Verify old-kernel fallbacks.  |
| `mise run stress-issue-6` | Repeated doctest and blocking-runtime liveness tests.             | Guard the former intermittent race. |
| `mise run ci-lint` | `actionlint .github/workflows/*.yml`                                    | Validate workflow YAML.            |

### Testing

Integration tests live in `tests/` and drive the public API end to end (TCP/UDP echo,
filesystem round trips, cross-thread workers and channels) via a `block_on` helper that
runs each future on a dedicated event-loop thread.

### Benchmarks

`benches/runtime.rs` measures executor mechanics (task spawn, yield, channels, timers) and
`benches/io.rs` measures loopback TCP and filesystem throughput, using
[criterion](https://github.com/bheisler/criterion.rs). Run a single benchmark with:

```sh
cargo bench --bench runtime -- spawn_join
```

### Profiling and observability

`runite` emits [`tracing`](https://docs.rs/tracing) spans/events on these targets, usable for
latency investigation with any `tracing` subscriber:

| Target              | Covers                                                            |
| ------------------- | ----------------------------------------------------------------- |
| `runite::driver`    | wake drains and driver failures; io_uring submission and completions (Linux only) |
| `runite::runtime`   | runtime and worker lifecycle                                      |
| `runite::scheduler` | task scheduling and cross-thread queueing                         |
| `runite::timer`     | timer arming/firing                                               |
| `runite::async`     | future polling and cancellation                                   |
| `runite::signal`    | signal delivery (Windows only)                                    |

The kqueue and IOCP backends do not instrument individual operations, so `runite::driver` is
much sparser there. The other targets emit the same events on every platform they exist on.

Every event is emitted in release builds as well as debug. Steady-state events used to be
compiled out of release entirely, which meant the builds you would actually profile were the
ones with nothing to see. With no subscriber installed, a `tracing` event costs a relaxed load
of a shared static and a not-taken branch — field expressions are never evaluated — so the
events are present without being paid for.

Two consequences worth knowing:

- **Queue-wait timing starts when you start collecting.** `macrotask_dequeued` carries
  `wait_ns`, which requires stamping the clock on every macrotask push. That stamp is taken
  only while a subscriber is accepting `runite::scheduler` at `TRACE`, so tasks already queued
  when the subscriber is installed are dequeued without it.
- **Installing a subscriber is not free for the runtime.** Once anything sets a global default,
  each event site consults its interest cache; if your filter yields "sometimes" rather than a
  definite no, hot sites like `queue_microtask` pay a thread-local read and a virtual call per
  emission. Filter runite's targets off explicitly if you are collecting something else.

#### Identity: what a record is about

Task ids and timer ids restart at 1 on every runtime thread, and driver tokens are per-driver
and wrapping, so `timer_id = 3` names nothing on its own — in a timeline merged from several
threads it is as many timers as there are threads. Every event on `runite::scheduler`,
`runite::timer` and `runite::async` therefore carries both a `runtime_id` and a `turn_id`.
Either is absent (`None`) when there is no honest answer: work queued from a foreign thread has
no `runtime_id`, and anything outside a turn has no `turn_id`. A cross-thread post
(`queue_remote_task`, `remote_queue_full`) additionally carries `to_runtime_id`, because that is
the one case where "which runtime" has two answers.

`runite::runtime` and `runite::driver` are not covered by that rule, so read each event's fields
rather than assuming. The turn record and `run_wait` carry both identities; `run_enter`,
`run_exit` and `spawn_worker` name a runtime but no turn, since they bracket turns rather than
happen inside one; the teardown events name neither, because the state they report on is already
being dismantled.

The same identities are readable from application code, so your own records can join against
runite's on equality:

| Function                                  | Returns                                            |
| ----------------------------------------- | -------------------------------------------------- |
| [`current_runtime_id()`]                  | which runtime — one thread's event loop            |
| [`current_turn()`]                        | which turn of that loop                            |
| [`time::monotonic_now()`]                 | the clock runite arms its own deadlines on         |

`monotonic_now` has a documented epoch: unspecified origin, so only differences mean anything,
but every thread in the process and every process on the same running system reads the same
clock, and runite's deadlines are expressed against it. It does not survive a reboot and has no
relationship to wall-clock time.

[`current_runtime_id()`]: https://docs.rs/runite/latest/runite/fn.current_runtime_id.html
[`current_turn()`]: https://docs.rs/runite/latest/runite/fn.current_turn.html
[`time::monotonic_now()`]: https://docs.rs/runite/latest/runite/time/fn.monotonic_now.html

#### Per-turn records

`runite::runtime` at `TRACE` emits one `event = "turn"` record per iteration of the event loop.
It is the cheapest useful unit of attribution — one record for a whole turn rather than one per
task or per completion — and it answers the question an idle process raises: what woke this
loop, and what did it then do?

| Field                                                    | Meaning                                                                     |
| -------------------------------------------------------- | --------------------------------------------------------------------------- |
| `runtime_id`, `turn_id`                                  | which loop, which iteration                                                 |
| `entry`                                                  | `run`, `block_on`, `run_until_stalled`, or `run_ready_tasks`                |
| `wake`                                                   | `timer`, `io`, `notify`, `spurious`, or `queued`                            |
| `wait_ns`                                                | time parked in the driver before this turn; `0` means the loop never parked |
| `runnable_ns`, `microtask_ns`                            | time on work, and how much of it went to the microtask checkpoint           |
| `microtasks`, `macrotasks`, `task_polls`                 | units of work run                                                           |
| `timers`, `remote_adopted`, `worker_exits`, `notifications` | what the turn drained from the driver and the cross-thread queue         |
| `operations_completed`                                   | async operations of this runtime that finished during the turn              |
| `microtask_bound`, `microtask_starvation`                | whether the checkpoint dominated the turn, and whether the guard fired      |
| `*_depth_before` / `*_depth_after`                       | microtask, local macrotask and cross-thread queue depths either side        |

Three details matter when reading these.

A park belongs to the turn its wake *begins*, not to the turn that performed it, so `wait_ns`
and `wake` describe the same event and `runnable_ns` never includes a park.

`wake` is derived from what the turn observed and nothing else. A turn that did not park was not
woken by anything and reads `queued`, however much else happened to be moving at the time — that
covers a turn continuing existing work, a host driving the loop with `run_ready_tasks`, and the
first turn after entering. Given a park, the cause comes from the driver's own readiness bits;
since a wake can carry more than one, `wake` names the narrowest — a timer expiry beats I/O,
which beats a bare notification — with the counts on the same record saying what else arrived.
`spurious` means the loop parked and the driver gave it nothing.

`operations_completed` is a difference of a cumulative counter, so it counts every async
operation of this runtime that reached a terminal result inside the turn's wall-clock window,
including ones finished on a blocking-pool thread. That makes it useful for accounting and
useless for attribution, which is why `wake` does not read it.

The record costs nothing when nothing is collecting: the queue depths are not sampled, the
cross-thread queue is not locked, and the driver park is not timed. All of that sits behind the
same interest check every other event site makes — which means the check answers on target and
level only. Select turn records with `runite::runtime` at `TRACE`; a filter that decides by
field name or value can accept the record's callsite while declining the guard's, and then
no record is produced at all.

For CPU profiling, build with `--release` and use `perf` / `cargo flamegraph` against an
example or benchmark binary.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))
- MIT license ([LICENSE-MIT](./LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.

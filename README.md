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

## What you get

- **Entry points:** `#[runite::main]` (works on `fn main` or `async fn main`),
  `#[runite::test]`, and `block_on` for driving one future to completion.
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

| Target              | Covers                                              |
| ------------------- | --------------------------------------------------- |
| `runite::driver`    | io_uring / kqueue / IOCP submission and completions |
| `runite::runtime`   | runtime and worker lifecycle                        |
| `runite::scheduler` | task scheduling and cross-thread queueing           |
| `runite::timer`     | timer arming/firing                                 |
| `runite::async`     | future polling and cancellation                     |
| `runite::signal`    | signal delivery (Windows only)                      |

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

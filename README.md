# runite

`runite` is an **event-loop-per-thread**, non-work-stealing async runtime for Rust. Each
runtime thread owns its own scheduler, timer heap, and platform I/O driver — `io_uring` on
Linux, `kqueue` on macOS, and IOCP on Windows. It uses JavaScript-style microtask/macrotask
scheduling to give deterministic flush points.

It is built for UI front-ends, embedded event loops, and fine-grained reactive systems — not
as a general-purpose high-throughput server runtime. It deliberately prefers simple per-thread
event loops, thread-local state, and predictable scheduling over work-stealing, `Send`-future
ergonomics, and maximum I/O throughput.

> **Status:** pre-release (0.1). APIs may change before 1.0.

## Platform support

| Platform        | Backend            | Status    |
| --------------- | ------------------ | --------- |
| Linux `x86_64`  | `io_uring`         | Primary   |
| Linux `aarch64` | `io_uring`         | Supported |
| macOS `aarch64` | `kqueue` + offload | Supported |
| Windows `x86_64`| IOCP + offload     | Supported |

The Windows backend drives sockets, files, and child-process pipes with
overlapped I/O through one I/O completion port per runtime thread, offloading
to the blocking pool only where Windows has no asynchronous form (open,
metadata, directory scans, DNS, console stdio). See
[docs/WINDOWS.md](./docs/WINDOWS.md) for the design. Windows-only differences:
`runite::fd` (descriptor readiness) and `runite::net::unix` (Unix-domain
sockets) are not available, `TcpSocket::set_reuseport` reports
`ErrorKind::Unsupported`, and `runite::signal::windows` replaces
`runite::signal::unix` (the portable `runite::signal::ctrl_c` works on all
platforms). Unsupported targets fail to compile with a clear error.

### Minimum Linux kernel

The io_uring backend targets **Linux 6.1+** (current LTS), which is what CI
tests. Older kernels may work subject to these io_uring feature requirements:

- **5.6** — base ring operations (`openat`/`read`/`write`/`fsync`/`statx`/…);
  required.
- **5.18** — `MSG_RING`, used for cross-thread wakeups; required for
  multithreaded runtimes (`spawn_worker`), optional for a single event loop.
- File truncation (`OpenOptions::truncate`, `File::set_len`) uses `FTRUNCATE`
  (6.9) and falls back to `ftruncate(2)` on older kernels.
- Socket operations (`socket` 5.19, `bind`/`listen` 6.11, and
  `connect`/`accept`/`send`/`recv`/`shutdown`) transparently fall back to
  blocking syscalls on kernels that lack the opcode, so networking works below
  these versions with reduced native-io_uring coverage.

So the recommended 6.1 floor exercises every feature; the only hard lower bounds
are 5.6 (single-threaded) and 5.18 (multithreaded).

## Installation

```toml
[dependencies]
runite = "0.1"
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

- **Entry points:** `#[runite::main]` (works on `fn main` or `async fn main`).
- **Event loop:** `run`, `run_until_stalled`, `run_ready_tasks`, `queue_macrotask`,
  `queue_microtask`, `spawn`, `yield_now`.
- **Workers:** `spawn_worker` plus the `Send`-only cross-thread `ThreadHandle::queue_task`.
- **Tasks:** spawned futures return `JoinHandle<T>` that awaits to `Result<T, JoinError>`;
  use `abort`, `abort_handle`, `is_finished`, and cloneable `AbortHandle`s for cancellation.
- **Timers:** `time::set_timeout` and `time::set_interval` (each returns a
  handle with `.cancel()`), plus `time::{sleep, timeout, interval}` where
  `time::interval` is the awaitable interval.
- **I/O:** async `fs`, `net` (TCP/UDP everywhere; Unix-domain sockets on Unix), `stdio`, and crate-local
  `AsyncRead`/`AsyncWrite`/`Stream` traits with extension adapters; TCP split/reunite,
  listener `incoming()` streams, async stdin/stdout/stderr, and `BufReader`/`BufWriter`.
- **Processes:** `process::{Command, Child}` with piped async stdio, `kill`, and `wait`.
- **Channels & sync:** `channel::{mpsc, oneshot, broadcast, watch}`,
  `sync::{Mutex, Semaphore, Notify, OnceCell}`.
- **Blocking offload:** `spawn_blocking` onto a bounded shared OS-thread pool.
- **Signals:** portable `signal::ctrl_c`, async Unix signal handling (including SIGWINCH
  via `SignalKind::WindowChange`), and Windows console control events (`signal::windows`).

### Scaling across cores

`runite` is event-loop-per-thread: each runtime thread drives its own local scheduler and
accepts `!Send` futures. To scale CPU-bound or server workloads across cores, start one
event loop per core with `spawn_worker`; on Linux and macOS, servers should bind per-core accept
loops with `SO_REUSEPORT` so the OS distributes inbound connections. See [ARCHITECTURE.md](./ARCHITECTURE.md)
for the full threading and scaling model.

## Feature flags

| Feature          | Default | Description                                                           |
| ---------------- | ------- | --------------------------------------------------------------------- |
| `hyper`          | off     | `hyper` 1.x client integration (see `examples/hyper_http_client.rs`). |
| `futures-compat` | off     | `io::compat` adapters to/from the `futures-io` traits.                |

## Configuration

| Environment variable           | Effect                                                                |
| ------------------------------ | --------------------------------------------------------------------- |
| `RUNITE_BLOCKING_THREADS`      | Size of the shared blocking-task pool (clamped 1..=32).               |
| `RUNITE_REMOTE_QUEUE_CAPACITY` | Bound on the per-thread cross-thread macrotask queue (default 65536). |

## Examples

Start with these — each one demonstrates a reason the event-loop-per-thread
model exists, not just an API:

| Example | What it shows |
| --- | --- |
| [`reactive_state`](./examples/reactive_state.rs) | **The flagship.** Model→update→render with dirty-flag coalescing at the microtask checkpoint — why deterministic flush points make reactive UIs tractable. |
| [`command_center`](./examples/command_center.rs) | An interactive terminal app: async stdin, background jobs, and shared `Rc<RefCell>` state on one loop that never blocks on you. Interactive, or `-- --demo`. |
| [`chat_server`](./examples/chat_server.rs) | A collaborative-session backend whose entire room state is `Rc<RefCell<HashMap>>` — no `Arc`, no `Mutex`, no `Send` bounds — plus Ctrl-C graceful shutdown. Interactive (`nc` in!), or `-- --demo`. |
| [`background_workers`](./examples/background_workers.rs) | The Web-Workers discipline: CPU work on the blocking pool while a heartbeat *measures* that the loop stayed responsive. Run with `-- --blocking` to see the jank, quantified. |
| [`frame_loop_embedding`](./examples/frame_loop_embedding.rs) | runite as a guest inside a host frame loop (GUI/game shape): `run_until_stalled()` per frame, `requestAnimationFrame`-style tasks, render from settled state. |
| [`build_pipeline`](./examples/build_pipeline.rs) | Dev-tool process orchestration: bounded-concurrency subprocess fan-out with `Command::output`, where a failing step is data, not a crash. |

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
the platform parity matrix, and the documented safety invariants.

## Development

The toolchain is pinned with [mise](https://mise.jdx.dev/). Install it, then:

```sh
mise install            # fetch the pinned Rust toolchain and Agent Cop
mise run check          # fmt + clippy + tests + cop (the full local gate)
```

Individual tasks:

| Task                | Command                  | Purpose                                        |
| ------------------- | ------------------------ | ---------------------------------------------- |
| `mise run build`    | `cargo build --workspace --all-targets` | Build the workspace.             |
| `mise run test`     | `cargo test --workspace --all-features` | Unit, integration, and doctests. |
| `mise run lint`     | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Lint with warnings denied. |
| `mise run bench`    | `cargo bench --workspace --all-features` | Criterion benchmarks (`benches/`). |
| `mise run coverage` | `cargo llvm-cov --workspace --all-features ...` | HTML + lcov coverage report. |
| `mise run cop`      | `cop cop-checks/main.cop -t .` | Agent Cop static-analysis checks.       |

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

| Target              | Covers                                       |
| ------------------- | -------------------------------------------- |
| `runite::driver`    | io_uring / kqueue / IOCP submission and completions |
| `runite::runtime`   | runtime and worker lifecycle                 |
| `runite::scheduler` | task scheduling and cross-thread queueing    |
| `runite::timer`     | timer arming/firing (debug builds)           |
| `runite::async`     | future polling and cancellation (debug builds) |

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

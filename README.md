# runite

`runite` is an **event-loop-per-thread**, non-work-stealing async runtime for Rust. Each
runtime thread owns its own scheduler, timer heap, and platform I/O driver — `io_uring` on
Linux and `kqueue` on macOS. It uses JavaScript-style microtask/macrotask scheduling to give
deterministic flush points.

It is built for UI front-ends, embedded event loops, and fine-grained reactive systems — not
as a general-purpose high-throughput server runtime. It deliberately prefers simple per-thread
event loops, thread-local state, and predictable scheduling over work-stealing, `Send`-future
ergonomics, and maximum I/O throughput.

> **Status:** pre-release (0.1). APIs may change before 1.0.

## Platform support

| Platform        | Backend            | Status    |
| --------------- | ------------------ | --------- |
| Linux `x86_64`  | `io_uring`         | Primary   |
| macOS `aarch64` | `kqueue` + offload | Supported |
| Windows `x86_64`| IOCP + offload     | In progress (not yet available) |

A Windows backend built on IOCP (with thread offload where IOCP does not apply)
is in progress. It is **not** part of the 0.1 release; on Windows `runite` does
not yet compile. The other unsupported targets fail to compile with a clear
error.

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
    runite::queue_future(async {
        runite::time::sleep(std::time::Duration::from_millis(10)).await;
    });
}
```

## What you get

- **Entry points:** `#[runite::main]` (works on `fn main` or `async fn main`).
- **Event loop:** `run`, `run_until_stalled`, `run_ready_tasks`, `queue_task`,
  `queue_microtask`, `queue_future`, `yield_now`.
- **Workers:** `spawn_worker` plus the `Send`-only cross-thread `ThreadHandle::queue_task`.
- **Tasks:** spawned futures return `JoinHandle<T>` that awaits to `Result<T, JoinError>`;
  use `abort`, `abort_handle`, `is_finished`, and cloneable `AbortHandle`s for cancellation.
- **Timers:** `timeout` and `interval` (each returns a handle with `.cancel()`),
  and `time::{sleep, deadline}`.
- **I/O:** async `fs`, `net` (TCP/UDP/Unix-domain), `stdio`, and crate-local
  `AsyncRead`/`AsyncWrite`/`Stream` traits with extension adapters; TCP split/reunite,
  listener `incoming()` streams, async stdin/stdout/stderr, and `BufReader`/`BufWriter`.
- **Processes:** `process::{Command, Child}` with piped async stdio, `kill`, and `wait`.
- **Channels & sync:** `channel::{mpsc, oneshot, broadcast, watch}`,
  `sync::{Mutex, Semaphore, Notify, OnceCell}`.
- **Blocking offload:** `spawn_blocking` onto a bounded shared OS-thread pool.
- **Signals:** async Unix signal handling, including SIGWINCH (`SignalKind::WindowChange`).

### Scaling across cores

`runite` is event-loop-per-thread: each runtime thread drives its own local scheduler and
accepts `!Send` futures. To scale CPU-bound or server workloads across cores, start one
event loop per core with `spawn_worker`; servers should bind per-core accept loops with
`SO_REUSEPORT` so the OS distributes inbound connections. See [ARCHITECTURE.md](./ARCHITECTURE.md)
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

```sh
cargo run --example runtime_loop_showcase
cargo run --example async_fs_showcase
cargo run --example channel_showcase
cargo run --example tcp_echo_server
cargo run --example subprocess_pipeline
cargo run --example broadcast_watch
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
| `runite::driver`    | io_uring / kqueue submission and completions |
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

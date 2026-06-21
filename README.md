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

Other targets fail to compile with a clear error.

## Installation

```toml
[dependencies]
runite = "0.1"
```

## Quick start

```rust
#[runite::async_main]
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

- **Entry points:** `#[runite::main]`, `#[runite::async_main]`.
- **Event loop:** `run`, `run_until_stalled`, `run_ready_tasks`, `queue_task`,
  `queue_microtask`, `queue_future`, `yield_now`.
- **Workers:** `spawn_worker` plus the `Send`-only cross-thread `ThreadHandle::queue_task`.
- **Timers:** `set_timeout`, `clear_timeout`, `set_interval`, `clear_interval`,
  and `time::{sleep, timeout}`.
- **I/O:** async `fs`, `net` (TCP/UDP/Unix-domain), `stdio`, and crate-local
  `AsyncRead`/`AsyncWrite`/`Stream` traits with extension adapters.
- **Channels & sync:** `channel::{mpsc, oneshot}`, `sync::{Mutex, Semaphore, Notify, OnceCell}`.
- **Blocking offload:** `spawn_blocking` onto a bounded shared OS-thread pool.
- **Signals:** async Unix signal handling.

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
cargo run --example hyper_http_client --features hyper
```

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the threading model, micro/macro task
scheduling, run lifecycle, cancellation and buffer-ownership rules, the driver abstraction,
the platform parity matrix, and the documented safety invariants.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))
- MIT license ([LICENSE-MIT](./LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] — 0.1.0

Initial public release of `runite`: an event-loop-per-thread async runtime for
Rust, built on io_uring (Linux `x86_64`/`aarch64`), kqueue (macOS `aarch64`),
and I/O completion ports (Windows `x86_64`), with JavaScript-style
deterministic microtask/macrotask scheduling. Futures on a runtime thread are
`!Send` and never migrate; explicit worker threads and channels provide
parallelism.

### Runtime & scheduling

- JS-style event loop per thread: microtask checkpoints drain fully between
  macrotasks (timers, I/O completions, cross-thread work), giving deterministic
  flush points for reactive code. A `tracing` warning fires if a single
  checkpoint runs 1000 microtasks without yielding while macrotasks wait.
- Entry points: `block_on` (drive a borrowed, non-`Send` future to completion
  and return its value), `run` (drive the loop to idle), `run_until_stalled` /
  `run_ready_tasks` (pump the loop from a host frame loop without blocking),
  and the `#[runite::main]` / `#[runite::test]` attribute macros. `main` and
  `test` honor `Termination`, so an `async fn main() -> Result` that errors
  exits non-zero; both accept `crate = "..."` for renamed dependencies.
- `spawn` for `!Send` futures with `JoinHandle` (detach-on-drop), `abort` /
  `AbortHandle`, `task::JoinSet` for structured ownership of local tasks, and
  `task::spawn_blocking` for offloading blocking work to a bounded pool.
- `spawn_worker` starts additional runtime threads (each with its own loop and
  driver); `ThreadHandle::queue_macrotask` is the explicit, `Send`-bounded
  cross-thread boundary, with a bounded remote queue that reports backpressure
  instead of blocking or dropping. Internal completion and task wakes use a
  reserved path that a full queue cannot starve.
- Panic isolation: a panic in a spawned task resolves its `JoinHandle` to
  `JoinError::Panicked` instead of unwinding the event loop; scheduled
  callbacks and blocking-pool jobs are likewise firewalled, and a worker
  thread that dies always notifies its parent. Re-entering the event loop
  (`run` inside a task) is rejected.
- Cancellation is `Drop`-driven and documented per method: reads on files,
  sockets, and stdin are cancel-safe (bytes received by an in-flight operation
  are retained for the next read); write cancellation semantics are documented
  where they differ.

### I/O services

- `net`: async TCP (`TcpStream`/`TcpListener` with owned split halves,
  `TcpSocket` builder for `SO_REUSEADDR`/`SO_REUSEPORT`), UDP, and Unix-domain
  sockets (stream, listener, datagram) implementing the crate's
  `AsyncRead`/`AsyncWrite` traits. `TcpListener::bind` matches `std` (no
  implicit `SO_REUSEADDR`). Optional Hyper integration (`hyper` feature):
  transport impls for `TcpStream` **and** `UnixStream`, plus `hyper_rt`'s
  `RuniteExecutor`/`RuniteTimer` runtime glue — hyper client *and server*,
  http1 *and http2*, over TCP or Unix-domain sockets.
- `fs`: async file open/read/write (cursor and positional), metadata and
  `symlink_metadata`, `seek`, `set_len`, sync (`F_FULLFSYNC` on macOS),
  directory create/remove/rename, and a streaming `read_dir`.
- `process`: async `Command`/`Child` with piped async stdio, `kill`, `wait`,
  and `std`-shaped `output()` returning `Output { status, stdout, stderr }`
  (non-zero exit is data, not an error; stdout and stderr are drained
  concurrently). Child exit on Linux is awaited via `pidfd` with io_uring —
  no `SIGCHLD` handler.
- `io`: poll-based `AsyncRead`/`AsyncWrite`/`Stream` traits with extension
  combinators, `BufReader`/`BufWriter`, `copy`/`copy_bidirectional`, and
  optional `futures-io` adapters (`futures-compat` feature).
- `time`: `sleep`, `timeout`, awaitable `interval` with `MissedTickBehavior`,
  and JS-style callback timers (`set_timeout`/`set_interval` returning
  cloneable cancellation tokens).
- `channel`: bounded/unbounded `mpsc`, `oneshot`, `broadcast` (with lag
  reporting), and `watch` — all with cancel-safe `recv` and `Send + Sync`
  wakers, usable from any thread while values resolve on their owning loop.
- `sync`: async `Mutex`, `RwLock` (FIFO-fair), `Semaphore`, `Notify`, and
  `OnceCell`; `signal`: Unix signal streams plus a cross-platform `ctrl_c`;
  `stdio`: async stdin/stdout/stderr.
- Descriptor/handle interop: on Unix, `AsFd`/`AsRawFd`/`From<OwnedFd>` and
  `from_std` adopters on every fd-backed type plus
  `fd::wait_readable`/`wait_writable` readiness helpers taking `impl AsFd`
  (all `#[cfg(unix)]`); on Windows, the parallel
  `AsHandle`/`AsRawHandle`/`AsSocket`/`AsRawSocket` impls with
  `From<OwnedHandle>`/`From<OwnedSocket>`/`from_std` adoption (which associates
  the handle with the completion port).

### Platform notes

- Linux: one io_uring ring per runtime thread; cross-thread wakes via
  `IORING_OP_MSG_RING`. Recommended kernel floor is the 6.1 LTS line; hard
  minimums are 5.6 (single-threaded) and 5.18 (multithreaded), with
  synchronous fallbacks for newer opcodes (socket lifecycle ops, `ftruncate`).
  CQ overflow and missing-feature conditions are detected and reported.
- macOS `aarch64`: kqueue readiness plus a blocking pool for filesystem work;
  `sync_all`/`sync_data` use `F_FULLFSYNC` for real durability.
- Windows `x86_64` (MSVC): one I/O completion port per runtime thread;
  overlapped `ReadFile`/`WriteFile` for files and child pipes; overlapped
  Winsock (`ConnectEx`/`AcceptEx`/`WSASend`/`WSARecv`/`WSASendTo`/`WSARecvFrom`)
  for TCP/UDP; `CancelIoEx`-based drop-cancellation; blocking-pool offload for
  open/metadata/directory scans/DNS/console stdio; `RegisterWaitForSingleObject`
  child-exit waits; a waitable-timer APC for runtime timers;
  `runite::signal::windows` console control events backing the portable
  `signal::ctrl_c`; and `runite::os::windows::fs` extensions. See
  `docs/WINDOWS.md` for the design. `runite::fd` and `runite::net::unix` are
  Unix-only, and `TcpSocket::set_reuseport` reports `Unsupported` on Windows.
- Stable Rust only (MSRV 1.88); no nightly features.

[Unreleased]: https://github.com/willmtemple/runite/commits/main

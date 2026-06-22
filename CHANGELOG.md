# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial public release of `runite`, an event-loop-per-thread async runtime built on
  io_uring (Linux `x86_64`) and kqueue (macOS `aarch64`).
- `#[runite::main]` entry-point macro (supports both `fn main` and `async fn main`).
- Async `fs`, `net` (TCP/UDP/Unix-domain sockets), `time`, and `channel` services.
- Cross-thread worker spawning and task queueing.
- Optional `hyper` client integration and `futures-io` compatibility adapters.
- Reproducible toolchain via `mise`, Agent Cop static-analysis checks, GitHub CI
  (Linux + macOS), code-coverage and benchmark jobs, and a tag-triggered crates.io
  release workflow.
- Integration test suites and criterion benchmarks for the runtime and I/O paths.
- Task cancellation: `JoinHandle::abort`, `JoinHandle::is_finished`, and a cloneable
  `AbortHandle` (via `JoinHandle::abort_handle`). Aborting drops the task's future, which
  cancels any in-flight driver operation it is parked on.
- Async subprocess support in `runite::process`: `Command`/`Child` with piped async stdio
  (`ChildStdin`/`ChildStdout`/`ChildStderr`), `kill`, and `wait`. On Linux child exit is
  awaited via a `pidfd` registered with io_uring readiness (no `SIGCHLD` handler, no
  blocking-pool offload); the same `Command`/`Child` interface is provided on macOS.
- `broadcast` and `watch` channels in `runite::channel`: multi-producer/multi-consumer
  fan-out (with `RecvError::Lagged`) and a latest-value cell with `changed()` notifications.
- Async `BufReader`/`BufWriter` adapters in `runite::io` to amortize syscalls over the
  underlying `AsyncRead`/`AsyncWrite`.
- Async `stdout()`/`stderr()` writers (alongside `stdin()`), and a `SignalKind::WindowChange`
  (`SIGWINCH`) signal for terminal-resize-aware TUIs.
- `TcpStream::into_split` producing owned `OwnedReadHalf`/`OwnedWriteHalf` (recombine with
  `TcpStream::reunite`; `ReuniteError` on mismatch) so reads and writes can run in separate tasks.
- `TcpListener::incoming` and `UnixListener::incoming`, returning a `Stream` of inbound
  connections.

### Changed

- Awaiting a `JoinHandle<T>` now yields `Result<T, JoinError>` instead of `T`, resolving to
  `Err(JoinError::Aborted)` when the task is aborted. `JoinError` gained an `Aborted` variant
  alongside `Cancelled`.
- Non-blocking control syscalls (socket/bind/listen/shutdown/close, fd duplication) now run
  inline on the event loop instead of being offloaded to the blocking thread pool when their
  io_uring opcode is unsupported.
- Linux network data-path operations (connect/accept/send/recv/datagram recv) now fall back to a
  non-blocking readiness path (`IORING_OP_POLL_ADD`) instead of the blocking thread pool when an
  io_uring opcode is unsupported, so socket I/O is never offloaded.

### Security

- Fixed a latent use-after-free window in the Drop-cancellation path where a detached I/O
  buffer guard could be released on the `IORING_OP_ASYNC_CANCEL` completion before the
  original operation had finished with the buffer. Buffers are now released solely on the
  original operation's completion.

[Unreleased]: https://github.com/willmtemple/runite/commits/main

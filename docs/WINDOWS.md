# Windows backend — 0.2 design

*This document describes the design of the Windows backend: an IOCP-based driver plus a
`sys/windows` operation backend. It parallels the Linux (`io_uring`) and macOS (`kqueue` +
blocking offload) backends described in `ARCHITECTURE.md`. Applications
upgrading resource-adoption code should also read the
[0.1 → 0.2 migration guide](MIGRATING-0.2.md).*

## Why IOCP (and not readiness emulation or IoRing)

Windows asynchronous I/O is **completion-based**: an operation is submitted with an
`OVERLAPPED` context and the kernel reports *completion* (not readiness) through an
I/O Completion Port (IOCP). This is the same shape as `io_uring`'s SQE/CQE model, so the
runite driver contract (submit → completion callback → wake owner thread) maps directly.

Alternatives considered and rejected:

- **Readiness emulation (`select`/`WSAPoll`/AFD)**: this is what `wepoll`/`mio` do to
  present an epoll-like surface. It exists to serve readiness-oriented runtimes; runite's
  op layer is already completion-oriented, so emulating readiness on top of a completion
  kernel just to re-derive completions would add latency and complexity for nothing.
- **Windows IoRing (`NtSubmitIoRing`, Windows 11+)**: Microsoft's io_uring analogue. As of
  2026 it supports only a small set of file operations (read/write/flush), no socket
  operations, and requires very recent builds. IOCP remains the canonical, fully-supported
  substrate for general async I/O. IoRing can later slot in as a file-op fast path behind
  the same driver, exactly like io_uring opcode probing on Linux.

## Threading model

Identical to the other platforms: **one driver per runtime thread**.

- Each runtime thread owns one completion port (`CreateIoCompletionPort`, concurrency 1).
  The port remains installed for the OS thread's lifetime, including across sequential
  `run()` and `block_on()` calls.
- `wait()` blocks in `GetQueuedCompletionStatusEx` (alertable — see Timers).
- The cross-thread `ThreadNotifier` posts a packet with a reserved completion key via
  `PostQueuedCompletionStatus`. The notifier shares the port through an
  `Arc<OwnedHandle>`, so a racing `notify` can never target a recycled handle after the
  driver drops (the same TOCTOU hazard the macOS wake pipe closes with a dup'd fd).
- The monotonic clock is `QueryPerformanceCounter` scaled by the boot-constant
  `QueryPerformanceFrequency`.
- Runtime-owned workers explicitly tear down their port outside the Windows
  loader lock. A non-runtime reaper publishes `WorkerJoin`/`is_finished` only
  after the OS thread and its TLS destructors exit. The last-resort TLS path on
  an arbitrary Windows thread marks handles closed but retains state that
  cannot be safely destroyed under loader lock.

### Handle association

A HANDLE/SOCKET can be associated with **exactly one** completion port for its lifetime.
The Windows backend associates every file, socket, and pipe handle with the *current*
runtime thread's port at creation/adoption time and records that port's process-unique
identity on the owned resource. Every operation validates the identity, and the public
resource remains `!Send`, so submission, dispatch, and completion ownership cannot migrate.
External adoption is strict: synchronous handles, handles configured with
`FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`, and handles already bound to another port are
rejected. Internal `try_clone` paths propagate a proven affinity without retrying an
ambiguous association call. Skip-on-success remains a deferred optimization (issue #17)
because the current packet context is reclaimed only by its terminal completion packet.
The public `from_owned`/`from_std` constructors and `TryFrom<OwnedHandle>` /
`TryFrom<OwnedSocket>` conversions therefore return `io::Result`; there is no
infallible adoption path.

## Timers: waitable timer APC + alertable wait

The runtime timer is a **waitable timer delivered as an APC**, backstopped by the
millisecond timeout of `GetQueuedCompletionStatusEx`:

- `rearm_timer(deadline)` calls `SetWaitableTimer` with a **no-op** APC completion routine.
  APCs queue to the *setting* thread and run only while it waits alertably; `rearm_timer`
  is always called by the owning scheduler thread, which is also the thread that blocks in
  `GetQueuedCompletionStatusEx(..., fAlertable = TRUE)`.
- The APC body is intentionally empty: its only job is to make the alertable wait return
  (`WAIT_IO_COMPLETION`). After any wake-up the driver re-checks the armed deadline against
  the monotonic clock — the same pattern the macOS driver uses after `kevent` returns — so
  stale APCs from a rearmed/cancelled timer are harmless spurious wake-ups.
- `wait()` also passes the armed deadline (rounded *up* to milliseconds) as the
  `GetQueuedCompletionStatusEx` timeout, so a lost or coalesced APC only degrades
  precision, never correctness.
- Driver teardown cancels the timer and drains stray queued APCs with `SleepEx(0, TRUE)`
  before the thread can ever host another runtime.

The timer is deliberately a *standard* waitable timer: timers created with
`CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` reject APC completion routines
(`SetWaitableTimer` fails with `ERROR_INVALID_PARAMETER`). Expiry precision is therefore
bounded by the system interrupt period (~15.6 ms worst case), matching mainstream Windows
runtimes; marrying the high-resolution timer kind to the port via
`NtAssociateWaitCompletionPacket` is a possible future refinement, tracked in the project's
GitHub issues.

## Operation submission and buffer ownership

The Linux staging rules (the buffer-ownership model in `ARCHITECTURE.md`) carry over
unchanged, and Windows makes them mandatory:
the kernel writes into the `WSABUF`/`ReadFile` buffer until the completion packet arrives,
so the buffer must be runtime-owned and pinned for the life of the operation.

Every overlapped submission heap-allocates one packet context:

```text
#[repr(C)] OverlappedOp<T> {
    OVERLAPPED,                               // must be at offset 0
    complete: unsafe fn(*mut header, ...),    // thin dispatch fn (per op kind)
    owner: Arc<OwnedHandle/OwnedSocket>,       // kernel object live through terminal packet
    data: T,                                  // owned buffer(s), CompletionHandle, addrs
}
```

- The box is leaked into the kernel at submit (`Box::into_raw` → `lpOverlapped`).
- The port returns the same pointer in the `OVERLAPPED_ENTRY`; the driver reconstructs the
  box and runs its completion function, which maps the result and calls
  `CompletionHandle::complete`. The buffer dies with the box — after the packet, never
  before.
- If the submitting call fails synchronously (not `ERROR_IO_PENDING`/`WSA_IO_PENDING`), no
  packet will ever arrive; the box is reclaimed immediately and the error is surfaced
  inline.
- Synchronous *success* still posts a completion packet (the backend does not enable
  `FILE_SKIP_COMPLETION_PORT_ON_SUCCESS`), so there is exactly one code path per op.
  Skip-on-success is a future optimization with documented caveats (non-IFS LSPs).
- Operation status is read from `OVERLAPPED.Internal` (an `NTSTATUS`) translated with
  `RtlNtStatusToDosError`, the same technique libuv uses; this avoids needing a live
  handle in `GetOverlappedResult` after the resource may have closed.

### Cancellation and logical writes

Every low-level `CompletionFuture` registers a cancel callback that calls
`CancelIoEx(handle, lpOverlapped)`. Dropping a future that directly owns that
completion runs the callback:

- If the op is still in flight it completes with `ERROR_OPERATION_ABORTED`; the packet
  still arrives and frees the context — this is the IOCP analogue of Linux's
  `pending_cancel_buffers` guard map, but the port gives it to us for free because *every*
  submitted op produces exactly one packet.
- If the op already completed (packet dequeued, `finished` set), the future's Drop skips
  the cancel callback entirely; dispatch and drop share a thread, so there is no race.
- Closing a handle with in-flight I/O also cancels it; the packets are still delivered.
- Socket deadlines issue `CancelIoEx` for the exact `OVERLAPPED` and continue awaiting its
  terminal packet. A successful completion wins if it was already visible at the deadline;
  only the terminal `ERROR_OPERATION_ABORTED` is translated to `TimedOut`.

Public byte-stream reads and writes retain an accepted low-level future in the
resource's `ReadState`/`WriteState`, so dropping only the transient caller
future does not discard it. Completed read bytes remain available. Each
extension/adapter write has a live generation; completion is retained for that
generation and can never be credited to a later caller's buffer. Cloned files
share one FIFO cursor/write state, while direct poll callers must continue the
same logical write after `Pending`.

## Platform parity

| Capability | Windows path |
| --- | --- |
| open | blocking pool (`std::fs::OpenOptions` + `FILE_FLAG_OVERLAPPED`), then port association |
| read / write | overlapped `ReadFile`/`WriteFile` at explicit offsets through IOCP |
| cursor I/O | one shared serialized cursor state across `try_clone` handles, with explicit-offset overlapped ops and checked `SetFilePointerEx` updates |
| metadata / sync / set_len / try_clone | blocking pool (no overlapped form), mirroring macOS |
| read_dir | shared bounded, demand-driven 32-entry blocking-pool batches; no worker waits for buffer capacity |
| TCP connect | `ConnectEx` (wildcard-bind first) + `SO_UPDATE_CONNECT_CONTEXT` |
| TCP accept | `AcceptEx` + `SO_UPDATE_ACCEPT_CONTEXT`, address parsed from the accept buffer |
| send / recv / send_to / recv_from | overlapped `WSASend`/`WSARecv`/`WSASendTo`/`WSARecvFrom` with staged buffers |
| socket control ops | inline non-blocking Winsock calls (`bind`/`listen`/`shutdown`/`getsockopt`…) |
| DNS | blocking pool `to_socket_addrs` (same as Linux/macOS) |
| child exit | `RegisterWaitForSingleObject` on the process handle (OS wait-thread pool, no runtime thread parked) |
| child stdio | overlapped **named-pipe** pairs (anonymous pipes cannot overlap); child end is a plain inheritable handle |
| stdin | one demand-driven process-wide dedicated blocking reader feeding a bounded 64 KiB shared buffer; handles compete for one stream, synchronous console/file/pipe handles are supported, overlapped handles are rejected, and inherited-console spawn returns `WouldBlock` while a parent console read is active |
| stdout/stderr | blocking-pool offload (console handles do not support overlapped I/O) |
| signals | `SetConsoleCtrlHandler` → `signal::windows::{ctrl_c, ctrl_break, …}`; `runite::signal::ctrl_c()` routes here |
| fd readiness (`runite::fd`) | intentionally absent — readiness is a descriptor concept with no IOCP analogue |
| Unix domain sockets | not yet provided (Windows AF_UNIX is stream-only; tracked in the project's GitHub issues) |
| `SO_REUSEPORT` | unsupported; `TcpSocket::set_reuseport` returns `ErrorKind::Unsupported` |

## The handle façade

POSIX backends speak `RawFd`/`OwnedFd`; Windows separates file **handles** from
**sockets** (different types, different close functions, and `RawHandle` is a non-`Send`
pointer). Rather than scattering `#[cfg]` through the op and public layers, a single
façade module — `src/sys/handle.rs` — defines the platform's I/O handle vocabulary once:

- Unix: `RawFile`/`RawSock` alias `RawFd`; `OwnedFile`/`OwnedSock` alias `OwnedFd`.
- Windows: operation references clone an `Arc<OwnedHandle>`/`Arc<OwnedSocket>` together
  with IOCP affinity, so accepted blocking jobs and overlapped packets never retain only a
  reusable raw value.

`op::fs`, `op::net`, `fs.rs`, `net/`, `process/pipe.rs`, and `stdio.rs` are written
against the façade; only `sys/handle.rs` and the per-platform interop `impl` blocks
(`AsFd`/`AsRawFd` on Unix, `AsHandle`/`AsSocket`/`AsRawHandle`/`AsRawSocket` on Windows)
know which world they are in.

## Windows-only public surface

- `runite::os::windows::fs::OpenOptionsExt` — `access_mode`, `share_mode`,
  `custom_flags`, `attributes`, `security_qos_flags` (mirrors
  `std::os::windows::fs::OpenOptionsExt`).
- `runite::os::windows::fs::MetadataExt` — `file_attributes`.
- `runite::signal::windows` — console control events.
- `Metadata::mode()` returns a synthesized POSIX-style mode on Windows (directory/file
  type bits plus `0o444`/`0o666`-style permission bits derived from `FILE_ATTRIBUTE_READONLY`),
  documented as an emulation.
- Interop impls: `File: AsHandle + AsRawHandle + TryFrom<OwnedHandle>`, `TcpStream`/
  `TcpListener`/`UdpSocket`/`TcpSocket`: `AsSocket + AsRawSocket + TryFrom<OwnedSocket>`,
  plus fallible inherent `from_owned` and `from_std` constructors.

## Known deltas vs. Unix backends

- `sys::windows` has no `fd` module and `runite::fd` does not exist on Windows.
- `ExitStatus::signal()` remains Unix-only; `Child::kill` maps to `TerminateProcess`.
- Sockets are left in blocking mode (overlapped ops never block the submitting thread;
  mixing `FIONBIO` with overlapped I/O is discouraged). `from_std` adoption therefore does
  not toggle non-blocking mode on Windows — it associates the socket with the port instead.
- Reads at end-of-file complete with `ERROR_HANDLE_EOF` (files) or `ERROR_BROKEN_PIPE`
  (pipes) rather than a 0-byte success; the backend maps both to the Unix "read returns 0"
  convention.

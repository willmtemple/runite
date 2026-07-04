# Windows port — design

*This document describes the design of the Windows backend: an IOCP-based driver plus a
`sys/windows` operation backend. It parallels the Linux (`io_uring`) and macOS (`kqueue` +
blocking offload) backends described in `ARCHITECTURE.md`.*

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
- `wait()` blocks in `GetQueuedCompletionStatusEx` (alertable — see Timers).
- The cross-thread `ThreadNotifier` posts a packet with a reserved completion key via
  `PostQueuedCompletionStatus`. The notifier shares the port through an
  `Arc<OwnedHandle>`, so a racing `notify` can never target a recycled handle after the
  driver drops (the same TOCTOU hazard the macOS wake pipe closes with a dup'd fd).
- The monotonic clock is `QueryPerformanceCounter` scaled by the boot-constant
  `QueryPerformanceFrequency`.

### Handle association

A HANDLE/SOCKET can be associated with **exactly one** completion port for its lifetime.
The Windows backend associates every file, socket, and pipe handle with the *current*
runtime thread's port at creation/adoption time (all creation paths run on a runtime
thread). Because the public I/O types hold type-erased pending futures (`Pin<Box<dyn
Future>>`, which is `!Send`), a resource is created, used, polled, and dropped on one
thread — submission thread, dispatch thread, and completion owner always coincide. This is
the IOCP analogue of "each Linux thread owns its own ring".

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
`NtAssociateWaitCompletionPacket` is a possible future refinement, tracked in the roadmap.

## Operation submission and buffer ownership

The Linux "option A" staging rules carry over unchanged, and Windows makes them mandatory:
the kernel writes into the `WSABUF`/`ReadFile` buffer until the completion packet arrives,
so the buffer must be runtime-owned and pinned for the life of the operation.

Every overlapped submission heap-allocates one packet context:

```text
#[repr(C)] OverlappedOp<T> {
    OVERLAPPED,                               // must be at offset 0
    complete: unsafe fn(*mut header, ...),    // thin dispatch fn (per op kind)
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

### Cancellation

Drop remains the cancellation primitive. The cancel callback registered on each
`CompletionFuture` calls `CancelIoEx(handle, lpOverlapped)`:

- If the op is still in flight it completes with `ERROR_OPERATION_ABORTED`; the packet
  still arrives and frees the context — this is the IOCP analogue of Linux's
  `pending_cancel_buffers` guard map, but the port gives it to us for free because *every*
  submitted op produces exactly one packet.
- If the op already completed (packet dequeued, `finished` set), the future's Drop skips
  the cancel callback entirely; dispatch and drop share a thread, so there is no race.
- Closing a handle with in-flight I/O also cancels it; the packets are still delivered.

## Platform parity

| Capability | Windows path |
| --- | --- |
| open | blocking pool (`std::fs::OpenOptions` + `FILE_FLAG_OVERLAPPED`), then port association |
| read / write | overlapped `ReadFile`/`WriteFile` at explicit offsets through IOCP |
| cursor I/O | explicit-offset overlapped ops around the shared file-object cursor (`SetFilePointerEx`); dup'd handles share the cursor like Unix `dup` |
| metadata / sync / set_len / read_dir / try_clone | blocking pool (no overlapped form), mirroring macOS |
| TCP connect | `ConnectEx` (wildcard-bind first) + `SO_UPDATE_CONNECT_CONTEXT` |
| TCP accept | `AcceptEx` + `SO_UPDATE_ACCEPT_CONTEXT`, address parsed from the accept buffer |
| send / recv / send_to / recv_from | overlapped `WSASend`/`WSARecv`/`WSASendTo`/`WSARecvFrom` with staged buffers |
| socket control ops | inline non-blocking Winsock calls (`bind`/`listen`/`shutdown`/`getsockopt`…) |
| DNS | blocking pool `to_socket_addrs` (same as Linux/macOS) |
| child exit | `RegisterWaitForSingleObject` on the process handle (OS wait-thread pool, no runtime thread parked) |
| child stdio | overlapped **named-pipe** pairs (anonymous pipes cannot overlap); child end is a plain inheritable handle |
| stdin/stdout/stderr | blocking-pool offload (console handles do not support overlapped I/O) |
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
- Windows: `RawFile` is a `Send`/`Copy` newtype over the handle value, `OwnedFile` is
  `OwnedHandle`, `RawSock`/`OwnedSock` are `RawSocket`/`OwnedSocket`.

`op::fs`, `op::net`, `fs.rs`, `net/`, `process/pipe.rs`, and `stdio.rs` are written
against the façade; only `sys/handle.rs` and the per-platform interop `impl` blocks
(`AsFd`/`AsRawFd` on Unix, `AsHandle`/`AsSocket`/`AsRawHandle`/`AsRawSocket` on Windows)
know which world they are in.

## Windows-only public surface

- `runite::os::windows::fs::OpenOptionsExt` — `access_mode`, `share_mode`,
  `custom_flags`, `attributes` (mirrors `std::os::windows::fs::OpenOptionsExt`).
- `runite::os::windows::fs::MetadataExt` — `file_attributes`.
- `runite::signal::windows` — console control events.
- `Metadata::mode()` returns a synthesized POSIX-style mode on Windows (directory/file
  type bits plus `0o444`/`0o666`-style permission bits derived from `FILE_ATTRIBUTE_READONLY`),
  documented as an emulation.
- Interop impls: `File: AsHandle + AsRawHandle + From<OwnedHandle>`, `TcpStream`/
  `TcpListener`/`UdpSocket`/`TcpSocket`: `AsSocket + AsRawSocket + From<OwnedSocket>`,
  plus the matching `from_std` constructors.

## Known deltas vs. Unix backends

- `sys::windows` has no `fd` module and `runite::fd` does not exist on Windows.
- `ExitStatus::signal()` remains Unix-only; `Child::kill` maps to `TerminateProcess`.
- Sockets are left in blocking mode (overlapped ops never block the submitting thread;
  mixing `FIONBIO` with overlapped I/O is discouraged). `from_std` adoption therefore does
  not toggle non-blocking mode on Windows — it associates the socket with the port instead.
- Reads at end-of-file complete with `ERROR_HANDLE_EOF` (files) or `ERROR_BROKEN_PIPE`
  (pipes) rather than a 0-byte success; the backend maps both to the Unix "read returns 0"
  convention.

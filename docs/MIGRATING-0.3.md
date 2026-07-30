# Migrating from runite 0.2 to 0.3

runite 0.3 keeps the event-loop-per-thread model and every architectural
property 0.2 established. What it changes is naming and shape: five breaking
changes, spread across `sync`, `stdio`, `io`, `net::unix` and `process`, that
remove name collisions and asymmetries the 0.2 API had accumulated. Nothing
about scheduling, cancellation, or buffer ownership moves.

```toml
[dependencies]
runite = "0.3"
```

## Required source changes

This section covers changes the compiler will force you to make. The
[silent behavior changes](#silent-behavior-changes) below produce no diagnostic
at all and need a deliberate audit.

### `sync::Permit` is now `sync::SemaphorePermit`

A plain rename. It was the only guard in the crate without an owner prefix,
beside `MutexGuard`, `RwLockReadGuard` and `RwLockWriteGuard`.

### `Stdin::read_line` is now `Stdin::next_line`

`Stdin::read_line` and `BufReader::read_line` shared a name while behaving
differently in both signature and end-of-input convention:

```rust
// Allocates, returns the line, `None` at end of input.
let line: Option<String> = stdin.read_line().await?;   // 0.2
let line: Option<String> = stdin.next_line().await?;   // 0.3

// Appends to your `String`, `Ok(0)` at end of input. Unchanged: this is the
// `std` shape, so it keeps the `std` name.
let read: usize = buf_reader.read_line(&mut line).await?;
```

Only the divergent one moved. Call sites need the new name; behaviour is
identical.

### Inherent `read`/`write` methods moved to the extension traits

`File`, `TcpStream` and `UnixStream` each carried inherent copies of methods the
`AsyncReadExt`/`AsyncWriteExt`/`AsyncSeekExt` traits already provide — unevenly,
so `ChildStdin`, `BufReader` and the owned split halves had none. Because
inherent methods win name resolution, two identical-looking calls dispatched to
different code depending on the concrete type, and turning a concrete type into
a generic silently changed which implementation ran.

The fix is an import:

```rust
// 0.2
let mut file = runite::fs::File::open("input.txt").await?;
file.read_to_end(&mut bytes).await?;

// 0.3
use runite::io::AsyncReadExt;          // add this

let mut file = runite::fs::File::open("input.txt").await?;
file.read_to_end(&mut bytes).await?;   // unchanged
```

This also covers `Stdin::read`, `Stdout::write` and `Stderr::write`.

The affected names are `read`, `read_exact`, `read_to_end`, `read_to_string`,
`write`, `write_all`, `flush` and `seek`. **Behaviour is unchanged** — the
inherent bodies were thin wrappers over the same `poll_read` and
`poll_write_operation` paths the traits use, so cancel safety and
write-operation identity are exactly as documented before.

One behaviour change rides along, on stdin only. `Stdin::read` used to discard
its pending operation and unregister its waiter when cancelled, while the
`AsyncRead` path kept both — so the two disagreed, and only one matched the
documentation. They now share one path, and retention is the contract: a
cancelled stdin read leaves its operation for the next read to claim, exactly
as `File`, `TcpStream` and `UnixStream` already behaved. Buffered input was
preserved under either rule, so this is visible only to code inspecting waiter
registration.

The **positional** methods are unaffected and stay inherent: `read_at`,
`read_exact_at`, `write_at`, and `write_all_at` take an explicit offset and
shadow nothing.

`read_to_string` previously existed only on `File`; it is now on
`AsyncReadExt`, so every reader has it.

### `net::unix::Incoming` lost its lifetime parameter

`UnixListener::incoming()` used to borrow the listener, while the TCP
equivalent returned an owned stream. The two are now the same shape, so drop
the lifetime wherever the type is named:

```rust
// 0.2
fn serve(incoming: runite::net::unix::Incoming<'_>) { /* ... */ }

// 0.3
fn serve(incoming: runite::net::unix::Incoming) { /* ... */ }
```

This is mostly a fix rather than a cost: an owned `Incoming` can be built in
one place and moved into a spawned task, which the borrowed form could not
express, and code generic over both listener kinds can now be written once.

### `Stdio` and `Command` are no longer `Clone`

`process::Stdio` loses `Clone`, `Copy`, `PartialEq` and `Eq`; `process::Command`
loses `Clone`. A `Stdio` can now own a file descriptor (see
[below](#a-child-can-be-started-on-a-descriptor-you-own)), so copying one
implicitly would hide a `dup`, and comparing two for equality is not meaningful.
`std::process::Stdio` and `std::process::Command` are not `Clone` for the same
reason.

Passing a `Stdio` by copy no longer works:

```rust
// 0.2
let piped = Stdio::piped();
command.stdout(piped);
command.stderr(piped);        // relied on `Copy`

// 0.3 — construct one per stream
command.stdout(Stdio::piped());
command.stderr(Stdio::piped());
```

Comparisons must go, and a cloned `Command` becomes two builders:

```rust
// 0.2
let base = Command::new("git");
let mut status = base.clone();
let mut diff = base.clone();

// 0.3 — build each, or factor the shared setup into a function
fn git() -> Command {
    let mut command = Command::new("git");
    command.env("GIT_CONFIG_GLOBAL", "/dev/null");
    command
}
let mut status = git();
let mut diff = git();
```

Derived impls are tracked from 0.3 on, in `docs/public-api-traits.md`; this
particular removal predates that file, so it shows up in neither report.

## Silent behavior changes

Nothing below produces a compiler error. Existing code keeps building and
behaves differently, so these need an explicit audit.

### `tracing` events are now emitted in release builds

Twenty-one steady-state trace sites on `runite::driver`, `runite::runtime`,
`runite::scheduler`, `runite::timer` and `runite::async` were
`#[cfg(debug_assertions)]` in 0.2, so a release build emitted no per-turn,
per-task, per-timer or per-operation event at all. They are unconditional now.

If your release deployment installs a `tracing` subscriber that accepts
everything, it will start receiving events it has never seen. Two things follow:

- **Filter runite's targets off explicitly if you are not collecting them.**
  Once anything installs a global default, every event site consults its
  interest cache. A filter that answers "sometimes" rather than a definite no
  makes hot sites like `queue_microtask` pay a thread-local read and a virtual
  call per emission. With no subscriber installed at all, an event costs a
  relaxed load and a not-taken branch, and its fields are never evaluated.
- **Queue-wait timing starts when you start collecting.** `macrotask_dequeued`
  carries `wait_ns`, which needs a clock stamp on every macrotask push; that
  stamp is taken only while a subscriber is accepting `runite::scheduler` at
  `TRACE`, so tasks already queued when the subscriber arrives are dequeued
  without it.

README.md's "Profiling and observability" section documents the full target
list.

### `watch::Sender::send` reports the receivers it actually had

`send` checked the receiver count, released the book lock, then wrote the value.
The last `Receiver` dropping in that window left `send` consuming the value,
advancing the version, and returning `Ok(())` — contradicting its documented
contract. The check and the write now happen under one lock, so that race
returns `Err` and the value comes back to you. Code that treated `Ok(())` as
"nothing to handle" was relying on a bug in the narrow case; code that already
handled `Err` is unaffected.

### An inherited-stdin spawn can now report `WouldBlock`

`Command::spawn` waits for the process-wide stdin reader to release the
terminal before handing it to a child. That wait was unbounded: a reader already
inside `read(2)` on an interactive terminal returns only when the user types, so
spawning could hang the whole event loop. It is bounded now and reports
`ErrorKind::WouldBlock` past that point, matching what Windows already did. A
caller that unwrapped the spawn will panic where it used to hang; retry instead.

### `io_uring` setup reports `QuotaExceeded`, not `OutOfMemory`

Ring setup failing on the locked-memory limit used to surface the raw `ENOMEM`
as `ErrorKind::OutOfMemory`, which sent the reader to look at free RAM. It is
`ErrorKind::QuotaExceeded` now, and the message reports the current
`RLIMIT_MEMLOCK`. Anything matching on `OutOfMemory` to detect this stops
matching.

### Socket deadlines no longer fail on kernels without the opcode

`recv_timeout`, `send_timeout`, `recv_from_timeout` and
`connect_stream_timeout` returned `ErrorKind::Unsupported` where their
deadline-free siblings quietly used the readiness path. They now fall back the
same way, applying what is left of the deadline through the runtime's timer.
Code with an `Unsupported` branch for this will find it unreachable.

### New `#[must_use]` warnings

`time::Sleep`, `YieldNow`, `RwLockReadFuture`, `RwLockWriteFuture`,
`MutexGuard`, `RwLockReadGuard`, `RwLockWriteGuard`, `SemaphorePermit` and
`watch::Ref` are now `#[must_use]`. These are warnings rather than errors, but
they fail a build that denies warnings — and each one they find is a real
no-op: `sleep(d);` and `let _ = semaphore.acquire().await;` both did nothing and
compiled silently. `JoinHandle` and `BlockingJoinHandle` are deliberately not
marked, because dropping a join handle detaches the task on purpose.

## New capabilities

Nothing here forces a source change; these exist so an application does not
have to reach outside runite for them. The
[changelog](../CHANGELOG.md) is the complete list; this section covers the ones
worth going out of your way for.

### A child can be started on a descriptor you own

`Stdio` gains `From<OwnedFd>` on Unix and `From<OwnedHandle>` on Windows, so a
standard stream can be wired to something runite does not model — a
pseudoterminal, a socket accepted elsewhere, a preopened log file:

```rust
let log: std::os::fd::OwnedFd = std::fs::File::create("child.log")?.into();
command.stdout(Stdio::from(log));
```

The descriptor is **duplicated at each spawn** rather than consumed, so one
`Command` can start several children and your original stays yours.

### `pre_exec`, on Unix

`runite::os::unix::process::CommandExt::pre_exec` runs a hook in the child
between fork and exec, mirroring `std::os::unix::process::CommandExt`. It is
the only place to `setsid`, acquire a controlling terminal with `TIOCSCTTY`,
change process group, or drop privileges.

It takes `Fn` rather than std's `FnMut`, because a runite `Command` may be
spawned more than once. As with std, the hook is `unsafe` to install: it runs
in a forked child where only async-signal-safe operations are sound, so it must
not allocate or take locks. Also as with std, hooks accumulate: registering a
second one does not replace the first, all of them run in registration order,
and the first to return `Err` aborts the spawn.

Together with the previous item, this is enough to start a shell on a
pseudoterminal without `std::process`.

### Startup failure can be reported instead of panicking

`try_block_on` is `block_on` with a reportable startup boundary:

```rust
fn main() -> std::process::ExitCode {
    match runite::try_block_on(run()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("could not start: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
```

Worth using if your program can run anywhere you do not control. `io_uring` is
disabled outright by some container and hardening policies
(`ErrorKind::Unsupported`), and the locked-memory budget can be exhausted by
something else in the process — a profiler charging its sample buffers to the
same limit is the usual case (`ErrorKind::QuotaExceeded`). Neither is the
application's fault, and neither is something a backtrace helps with.

Only startup is fallible. An error from your own future comes back inside
`Ok`.

### The runtime can be configured before it starts

`Builder` separates starting the thread's runtime from driving it, with the
same reportable boundary:

```rust
fn main() -> std::io::Result<()> {
    let runtime = runite::Builder::new().build()?;
    runtime.run();
    Ok(())
}
```

The `Runtime` it returns is a token for *this thread's* event loop, not an
object that owns one — `runite::spawn` and friends go on working next to it,
and dropping it shuts nothing down. Because a runtime is configured by the call
that creates it, `build()` has to be the thread's first runtime call:
afterwards it reports `ErrorKind::AlreadyExists` rather than silently ignoring
your settings.

The reason to want it is tuning. On Linux:

```rust
use runite::os::linux::BuilderExt;

fn main() -> std::io::Result<()> {
    let runtime = runite::Builder::new().ring_entries(32).build()?;
    runtime.run();
    Ok(())
}
```

`ring_entries` is the io_uring submission-queue size, 256 by default. Lowering
it is the fix for the `QuotaExceeded` startup failure above when you cannot
raise `RLIMIT_MEMLOCK`: it shrinks what the runtime pins so a profiler in the
same process still fits. Workers inherit it, and a size the kernel would round
up or clamp is rejected rather than quietly adjusted.

It lives on `os::linux::BuilderExt` rather than on `Builder` because kqueue and
IOCP have no ring to size, and a portable method that did nothing on two of the
three platforms would be worse than one you cannot call there at all.

#### Configuring a `#[runite::main]` program

A `Builder` *inside* an attribute body normally arrives too late and reports
`AlreadyExists`: an `async` body is already being driven by a runtime, and an
attribute carrying settings built one before the body ran. (A bare
`#[runite::main] fn main()` is the one shape where the body precedes the
runtime, so a `build()` there does succeed and the attribute's trailing `run()`
drives it — but there is nothing to gain by relying on that.) Give the settings
to the attribute instead and it builds the runtime it names:

```rust,ignore
#[runite::main(ring_entries = 32)]
async fn main() {
    // ...
}
```

`ring_entries` is Linux-only there too: on macOS or Windows the attribute is a
compile error that names the platform, rather than a setting that quietly does
nothing. A value the runtime rejects is a startup panic, since an entry point
has nowhere to return an error to — use `Builder` directly when you want to
handle it.

### A key that joins your diagnostics to the runtime's

`runite::current_turn()` returns a `TurnId` for the event-loop iteration
currently being driven, or `None` outside one. It exists so a layer with its
own instrumentation can line its records up with the runtime's without
guessing from timestamps:

```rust
// Stamp your own record with the turn that produced it.
let record = FlushRecord {
    turn: runite::current_turn(),
    effects_run,
};
```

A turn is one pass of the loop: drain driver events, drain remote tasks, flush
completed workers, run every microtask to quiescence, then run at most one
macrotask. So a reactive flush — which happens in the microtask checkpoint —
belongs to exactly one turn, and the join is structural rather than
approximate.

`TurnId` is opaque and comparable; identifiers increase and are never reused.

`runite::current_runtime_id()` is the coarser half of the same join:
a `RuntimeId` names one thread's event loop for the life of the process, which
is what separates records from several runtimes in one log. Both render exactly
as they appear in the `turn_id` and `runtime_id` fields of runite's own trace
events — that correspondence is the promise, and nothing else about the values
is specified.

`time::monotonic_now()` reads the clock the runtime schedules its own deadlines
on, so a measurement taken beside a `time::sleep` is on the same timebase rather
than on `Instant::now`'s.

### Watching several signal kinds on one stream

`signal::unix::signals` replaces the one-task-per-kind shape:

```rust
// 0.2 — one spawned task per kind, each repeating the teardown call
for kind in [SignalKind::Interrupt, SignalKind::Terminate, SignalKind::Hangup] {
    let mut stream = signal(kind)?;
    runite::spawn(async move { stream.recv().await; shut_down(); });
}

// 0.3 — one task, and it knows which signal arrived
let mut shutdown = signals(&[
    SignalKind::Interrupt,
    SignalKind::Terminate,
    SignalKind::Hangup,
])?;
runite::spawn(async move {
    if let Some(kind) = shutdown.recv().await {
        shut_down(kind);
    }
});
```

`Signals` also implements `io::Stream`. Duplicate kinds register once; an empty
slice is an error rather than a stream that never fires.

### The I/O traits work through pointers

`AsyncRead`, `AsyncBufRead`, `AsyncWrite` and `AsyncSeek` are now implemented
for `&mut T`, `Box<T>` and `Pin<P>`, so a borrowed reader can be wrapped:

```rust
// 0.2 — `BufReader::new` needs ownership, so this did not compile
let mut buffered = BufReader::new(&mut file);

// 0.2 workaround — give up the file, or restructure around it
let mut buffered = BufReader::new(file);

// 0.3 — the borrow is enough, and `file` is still yours afterwards
let mut buffered = BufReader::new(&mut file);
```

### Adopting a process runite did not spawn

`Child::from_pid` takes an already-running process and lets its exit be awaited
through the reactor rather than polled:

```rust
let started = std::process::Command::new("some-tool").spawn()?;
let mut child = runite::process::Child::from_pid(started.id())?;
let status = child.wait().await?;
```

Exit notification stays event-driven — a pidfd on Linux, a `kqueue` process
filter on macOS, a registered wait on Windows — so no thread is parked for the
process's lifetime, and an escalation ladder can be an ordinary task:

```rust
for signal in [SIGHUP, SIGTERM, SIGKILL] {
    send(signal)?;
    if time::timeout(settle, child.wait()).await.is_ok() {
        break;
    }
}
```

Three caveats. On Unix the process must be a **direct child**, because reading
an exit status requires being its parent; nothing at adoption time can tell
parentage, so adopting anything else succeeds and then `wait`, `try_wait` and
`kill` all fail straight away with `ECHILD`, without waiting and without
signalling. **Nothing else may reap it** — if a `std::process::Child` for the
same pid is still alive, whichever waits first takes the status. And a **pid is
not a stable identity**: it can be reused once the process is reaped, so a pid
obtained long ago may name something else.

Adopting a process that no longer exists fails at `from_pid` rather than
producing a handle whose `wait` never completes. Adoption is not an access
check, though: a Linux pidfd needs no rights over the target at all, and on
Windows adoption settles for synchronize and query-limited-information, leaving
`kill` to report a missing `PROCESS_TERMINATE`.

### TLS, behind the `rustls` feature

`tls::TlsConnector` and `tls::TlsAcceptor` handshake over anything implementing
runite's `AsyncRead + AsyncWrite` and hand back a `TlsStream` that is itself
such a transport, so a `TcpStream`, a Unix socket or a test duplex all work —
and with the `hyper` feature on too, hyper speaks HTTPS. Before this, reaching
an `https://` endpoint meant `hyper-rustls`, which depends on `tokio-rustls`
and drags in a second reactor nothing on this thread ever drives.

Two things to know before enabling it. **The crypto provider is yours to pick:**
runite depends on `rustls` with no provider feature, so an application that
enables neither `ring` nor `aws-lc-rs` gets rustls's panic about being unable to
determine the process-level `CryptoProvider`. And **rustls is re-exported as
`runite::tls::rustls`** — the public signatures are written in its types, so
name them through that path rather than a `rustls` dependency of your own that
cargo may or may not unify.

### Cooperative cancellation a task can observe

`sync::CancellationToken` is cloneable, hierarchical, and `!Send` like the rest
of `sync`. It complements `AbortHandle` rather than replacing it: an abort is
done *to* a task at its next suspension point, a token is something a task
chooses to check, so work that must flush a buffer or release a lock before
stopping can do so.

```rust
let token = runite::sync::CancellationToken::new();
let child = token.child_token();   // cancelled when its parent is, never upward

runite::spawn({
    let child = child.clone();
    async move {
        child.cancelled().await;
        flush_and_stop();
    }
});
```

### Running code on the way out

`on_shutdown` registers a closure to run when the thread's runtime is torn
down — before spawned tasks are cancelled and before the driver is destroyed,
so the hook is handed a runtime that can still do something. It is keyed to
teardown rather than to an entry point returning, because `run_until_stalled`
and `run_ready_tasks` return routinely and mean nothing by it.

`shutdown()` performs that teardown on the caller's own stack:

```rust
runite::run();
runite::shutdown();   // hooks run here, on this stack
```

**On Windows this is the only way a hook on an application-owned thread runs at
all.** Teardown at thread exit happens under the loader lock, where executing
arbitrary user code or closing a completion port can deadlock process shutdown,
so runite deliberately does neither. It is worth preferring on Unix too, where
TLS destructor order would otherwise decide when hooks run relative to the rest
of the thread's state. Calling it with no runtime installed does nothing, and
the thread may install a fresh one afterwards.

### Reading the runtime's own numbers

`metrics::snapshot()` returns three separate types, because conflating them is
the mistake the separation exists to prevent: `Gauges` are levels right now,
`Counters` are monotonic totals whose useful quantity is a difference, and
`Peaks` are high-water marks that answer "how bad did this get" after the level
has fallen back.

```rust
let before = runite::metrics::snapshot();
// ... run some work ...
let after = runite::metrics::snapshot();
let polls = after.counters.task_polls - before.counters.task_polls;
```

Reading a snapshot walks nothing and needs no subscriber; a thread with no
runtime installed reads zeroes rather than panicking. `Peaks` covers the
thread-local gauges only — there is no peak for
`remote_macrotask_queue_depth`, because sampling it every turn would take the
mutex `ThreadHandle::queue_macrotask` contends on; `counters.remote_tasks_rejected`
answers the same question.

### Closing a descriptor at a point you choose

`close_descriptor` on `File`, `TcpStream`, `TcpListener`, `UdpSocket`,
`UnixStream`, `UnixListener` and `UnixDatagram` returns
`io::Result<io::CloseOutcome>`. The point is **ordering**, not error reporting:
on Linux the close goes through the ring as `IORING_OP_CLOSE`, sequenced behind
operations already submitted against that descriptor. A `close(2)` from `Drop`
is not — the kernel keeps the underlying file alive until those finish, but
frees the descriptor *number* immediately, so a racing `open` elsewhere can be
handed it. macOS and Windows have no asynchronous close and gain only the
outcome.

`CloseOutcome::StillShared` is not an error: a split half, a listener's
`Incoming`, or an in-flight Windows operation can hold the descriptor, and
nothing leaks because the last holder still closes it. Do not reach for this to
catch close errors — `close(2)` error reporting is too unreliable to build on,
which is why `std` has no `File::close` either.

### Draining a raw descriptor, on Unix

`fd::read_chunks` encapsulates the readiness loop `wait_readable` otherwise asks
every caller to write. `on_chunk` runs for each read as it completes, so chunks
are delivered *before* the loop parks — a burst ending mid-frame is visible
immediately rather than at the next write — `Interrupted` retries instead of
waiting for readiness already reported, and returning `ControlFlow::Break` ends
the drain. The returned `fd::Drain` says which of the two ended it, so a
consumer draining a pseudoterminal can tell end of input (the child exited)
from its own byte budget running out.

### Timers that cancel themselves

`TimeoutHandle::cancel_on_drop` and `IntervalHandle::cancel_on_drop` wrap a
timer token in a guard that cancels when it leaves scope. The plain handles are
unchanged — dropping one leaves the timer running, matching
`setInterval`/`clearInterval` — so this is opt-in. It matters most for
intervals, where a leaked one keeps the runtime alive and stops `run()` from
ever returning. `into_inner` releases the timer to a longer-lived owner without
cancelling it.

### Telling a retryable `spawn_blocking` refusal from a terminal one

`task::is_retryable(&error)` is `true` only for a momentarily full queue and
`false` for a stopped or uncreatable pool. It takes `io::Error` rather than
introducing an error type, so `spawn_blocking` stays in `io::Result` and
composes with the rest of the crate.

### More of the diagnostic identity

Alongside `current_turn()`, `current_runtime_id()` names one thread's event loop
— process-unique, stable for the loop's life, never reused — and
`time::monotonic_now()` reads the clock runite arms its own deadlines against,
so your records and runite's can be correlated without proving two clocks are
the same one. Its epoch is documented: unspecified origin, so only differences
mean anything, but every thread in the process and every process on the same
running system reads the same clock.

`runite::runtime` at `TRACE` also emits one `event = "turn"` record per loop
iteration — why the loop woke, how long it parked, how long it spent runnable,
queue depths either side, and what it drained. It costs nothing when nothing is
collecting. README.md documents the fields.

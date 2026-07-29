# Migrating from runite 0.2 to 0.3

runite 0.3 keeps the event-loop-per-thread model and every architectural
property 0.2 established. What changes is the process API, which grows the
ability to attach a child to resources the caller already owns — and gives up
some derived traits to do it.

```toml
[dependencies]
runite = "0.3"
```

> **This guide is written as 0.3 is developed** and grows with it. Until 0.3 is
> released, treat it as the running record of what will need changing rather
> than a finished document.

## Required source changes

This section covers changes the compiler will force you to make.

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

Note that runite's public API report does not track derived trait impls, so
this change does not appear in `docs/public-api.md`.

## New capabilities

Nothing here forces a source change; these exist so an application does not
have to reach outside runite for them.

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
not allocate or take locks. Returning `Err` aborts the spawn.

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
an exit status requires being its parent. **Nothing else may reap it** — if a
`std::process::Child` for the same pid is still alive, whichever waits first
takes the status. And a **pid is not a stable identity**: it can be reused once
the process is reaped, so a pid obtained long ago may name something else.
Adopting a process that no longer exists fails at `from_pid` rather than
producing a handle whose `wait` never completes.

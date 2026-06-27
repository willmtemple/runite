# runite Roadmap

Tracking forward-looking work that is intentionally deferred. Items here are not
bugs; they record design intent so reserved scaffolding in the codebase has a
documented home.

## Explicit async close

Both backends currently close file descriptors synchronously via `OwnedFd`'s
`Drop` (a blocking `close(2)`). The operation enums carry reserved
`FsOp::Close` / `NetOp::Close` variants for a future *explicit* asynchronous
close API that would let callers `await` the completion of a close (e.g. to
surface flush/close errors, or to offload the syscall to io_uring on Linux).

This is deferred because:

- For most workloads a synchronous `close(2)` on `Drop` is correct and cheap.
- An awaitable close needs a coherent story for partially-closed resources and
  for the `Drop`-vs-`await` ordering, which is not yet designed.

When implemented, the reserved variants should be constructed by the high-level
`fs`/`net` types and dispatched through each backend's operation pipeline, and
the `#[allow(dead_code)]` annotations on those variants should be removed.

### Follow-up: symmetric removal of dead close scaffolding

The Linux backend still contains never-called `execution_path` / `close`
helpers and the `Close` match arms that pair with these reserved variants. The
matching macOS helpers have already been removed. The Linux-side removal is
deferred until it can be compiled and verified on Linux CI (it cannot be built
on the macOS development host). Either wire the variants up per the design above
or remove the dead Linux helpers symmetrically.

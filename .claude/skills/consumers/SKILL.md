---
name: consumers
description: Who depends on runite, where their requirements live, and the version constraint that couples them. Use when scoping runite work, deciding whether a feature has demand, judging a proposed API against real usage, or answering "does anyone actually need this?".
---

# runite's consumers

runite is not a general-purpose runtime looking for users. It has a small,
specific set of downstream projects whose needs drive it, and their
requirements are written down. Read them before designing anything; several
issues in the tracker are tokio-parity ideas that no consumer has ever asked
for, and the tracker does not distinguish those from the ones blocking someone.

## The stack

```
Kiln          terminal multiplexer — the end application
  └── RUIN    UI framework — hosts runite as an embedded loop
        └── adaptite   fine-grained reactive graph
              └── runite
```

All three are local checkouts beside this one:
`../kiln`, `../ruin`, `../adaptite`.

## Where the requirements are

- **`../kiln/docs/runite-notes.md`** — the designated file. Every entry states
  what Kiln needs, the workaround in place with `file:line`, and a proposed
  signature. Its "Filed —" footers are accurate; entries here are usually
  already issues.
- **`../kiln/docs/runite-ecosystem.md`** — capabilities Kiln would otherwise
  have to build. Less likely to be filed.
- **`../ruin/docs/runite-notes.md`** — RUIN's requirements, written as
  properties rather than signatures. Heavily observability-focused, and
  explicit about non-requirements, which is as useful as the requirements.
- **adaptite** files directly on this tracker rather than keeping a document.

**The designated files are usually complete.** The unfiled work is in the
documents nobody points at — `docs/roadmap.md`, `docs/perf-wishlist.md`,
`docs/architecture.md`, `docs/throughput.md`, `CLAUDE.md`, and `scripts/` that
enforce a constraint. Then read the source for workarounds: `std::process`,
`std::thread::sleep` on a loop thread, discarded errors, polling where an event
would do.

## The constraint that couples everything

**Exactly one runite version may resolve in an application's graph.** adaptite
and the application share runite's thread-local microtask queue, so two runite
minors means two queues, and one side's reactive flush never reaches the other.
Cargo permits this silently and nothing looks wrong until effects stop running.
Kiln enforces it in `../kiln/scripts/audit-runtime`.

Two consequences:

- **A runite minor bump is a breaking change for every adaptite consumer**, even
  when the API is untouched. Batch breaking changes into one release rather
  than spending that cost repeatedly.
- **A new runite release is unreachable from Kiln until adaptite republishes
  against it.** This stranded Kiln on 0.1 through the whole 0.2 cycle. It is not
  runite's issue to track, but it belongs in the release plan.

Related: nothing in the graph may pull in a second async runtime. That rules out
the usual off-the-shelf answers — `tokio-rustls`, `notify`, anything with a
built-in reactor — and is why runite ends up owning capabilities a runtime
would normally delegate.

## Standing constraints on runite's design

Collected from the requirement docs; violating any of these breaks a consumer:

- **`spawn` must stay non-`Send`.** Every Kiln future captures `Rc`/`Signal`.
- **A microtask queued during a turn runs before the next macrotask.** This is
  adaptite's entire batching model, and it is now documented in `src/lib.rs`
  and pinned by `tests/event_loop_order.rs`.
- **One-shot readiness must survive any `AsyncRead` for descriptors.** Kiln
  wants the byte budget and yield point that reading to completion inside a
  stream call would remove. Additive only.
- **Nothing on the loop thread may hold it for a frame.** 60 Hz budget; Kiln
  already spends 14.85 ms of it redrawing.
- **Diagnostics must be readable without a runtime mounted.** Kiln's frame
  benchmark has a plain `fn main()` and no reactor.
- **Diagnostic events must carry their own identity**, not rely on a subscriber
  reading ambient state — that only works if delivery is synchronous with
  production, which is a promise worth not making.

## Before scoping work

Grep the consumers for the API, and for workarounds around its absence. An
issue can be well-argued, correct, and wanted by nobody. That is invisible from
the issue and decisive for scheduling — `#34` was dropped from 0.3 on exactly
this basis after a grep showed neither Kiln nor RUIN uses it or works around
it.

Conversely, a capability a consumer *declines to use because it is too
expensive* is a gap, and never appears in a feature list.

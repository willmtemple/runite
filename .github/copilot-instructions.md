# GitHub Copilot Instructions for runite

The full agent & contributor guide lives in **[`AGENTS.md`](../AGENTS.md)** at the
repository root. Read it for the complete `cop` static-analysis reference, coding
conventions, and architecture notes. It is the single source of truth — this file
is intentionally a thin pointer so the guidance is not duplicated.

## Toolchain runs through `mise`

The Rust toolchain and the `cop` static analyzer are provisioned by
[`mise`](https://mise.jdx.dev/) from `mise.toml`. Run `mise install` once, then use
mise tasks rather than invoking tools directly:

| Task | Command |
|------|---------|
| Run cop checks | `mise run cop` |
| Verify cop rule files | `mise run cop-verify` |
| Full local gate (fmt, lint, test, cop) | `mise run check` |

**`cop` is not on your PATH — it is provided by `mise`.** Always invoke it through
mise: use `mise run cop` / `mise run cop-verify`, or prefix ad-hoc commands with
`mise exec --` (e.g. `mise exec -- cop cop-checks/main.cop -t .`).

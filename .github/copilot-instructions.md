# GitHub Copilot Instructions for runite

The full agent & contributor guide lives in **[`AGENTS.md`](../AGENTS.md)** at the
repository root. Read it for the toolchain, coding conventions, and architecture
notes. It is the single source of truth — this file is intentionally a thin
pointer so the guidance is not duplicated.

## Toolchain runs through `mise`

The Rust toolchain is provisioned by [`mise`](https://mise.jdx.dev/) from
`mise.toml`. Run `mise install` once, then use mise tasks rather than invoking
tools directly:

| Task | Command |
|------|---------|
| Full local gate (fmt, lint, test, workflow lint) | `mise run check` |
| Build / test / lint / bench / coverage | `mise run build` / `test` / `lint` / `bench` / `coverage` |
| Public API report / drift check | `mise run api-report` / `api-report-check` |
| Verify release artifacts | `mise run package-verify` |

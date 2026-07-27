# runite — Agent & Contributor Guide

> **Single source of truth.** This file (`AGENTS.md`) is the canonical agent guide.
> `.github/copilot-instructions.md` is a thin pointer to it — do not duplicate
> content there.

## Toolchain: everything runs through `mise`

The build/lint/test toolchain is provisioned by [`mise`](https://mise.jdx.dev/)
from `mise.toml`. Install it once with `mise install`, then use the tasks
instead of invoking tools directly:

| Task | Command |
|------|---------|
| Full local gate (fmt, lint, test, workflow lint) | `mise run check` |
| Build / test / lint / bench / coverage | `mise run build` / `test` / `lint` / `bench` / `coverage` |
| Public API report / drift check | `mise run api-report` / `api-report-check` |
| Verify release artifacts | `mise run package-verify` |



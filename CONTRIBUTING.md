# Contributing to runite

Thanks for your interest in improving `runite`! This document covers the basics for getting
a change merged.

## Getting started

`runite` pins its toolchain with [mise](https://mise.jdx.dev/):

```sh
mise install      # installs the pinned Rust toolchain and Agent Cop
mise run check    # fmt + clippy + tests + cop — the full local gate
```

If you do not use mise, a recent stable Rust toolchain (matching the `rust-version` /
`rust-toolchain.toml` pin) works too; install [Agent Cop](https://github.com/KrzysztofCwalina/cop)
separately to run the static-analysis checks.

## Before you open a pull request

Please make sure the full gate passes:

- `cargo fmt --all --check` — formatting.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — lints.
- `cargo test --workspace --all-features` — unit, integration, and doctests.
- `cop cop-checks/main.cop -t .` — Agent Cop checks.

CI additionally runs the macOS backend, `cargo-deny`, code coverage, and a benchmark
smoke-run, so consider running `mise run bench` and `mise run coverage` locally for changes
that touch hot paths or I/O.

## Code conventions

- **`unsafe`**: every `unsafe` block must carry a `// SAFETY:` comment stating the specific
  invariant that makes it sound. Soundness-critical invariants (the io_uring buffer-ownership
  and cancellation model in particular) are documented in
  [ARCHITECTURE.md](./ARCHITECTURE.md); update it when you change them.
- **Platform code** lives under `src/platform/` and `src/sys/`; keep the Linux and macOS
  backends behind the existing `cfg` gates and mirror behavior where practical.
- **Public API** changes should update doctests, the README, the CHANGELOG, and (for runtime
  semantics) ARCHITECTURE.md.

## Reporting bugs and security issues

Open a GitHub issue for ordinary bugs. For security-sensitive reports, follow
[SECURITY.md](./SECURITY.md) instead of filing a public issue.

## Licensing

By contributing you agree that your contributions are licensed under the project's dual
MIT OR Apache-2.0 license.

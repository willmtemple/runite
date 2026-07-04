# Contributing to runite

Thanks for your interest in improving `runite`! This document covers the basics for getting
a change merged.

## Building and running runite

`runite` pins its toolchain with [mise](https://mise.jdx.dev/):

```sh
mise install      # installs the pinned Rust toolchain and Agent Cop
mise run check    # fmt + clippy + tests + cop — the full local gate
```

If you do not use mise, a recent stable Rust toolchain (matching the `rust-version` /
`rust-toolchain.toml` pin) works too.

## Reporting an issue or making a change to runite

GitHub issues and pull requests are limited to collaborators. Please start by
[opening a discussion](https://github.com/willmtemple/runite/discussions).

## Code conventions

- **`unsafe`**: every `unsafe` block must carry a `// SAFETY:` comment stating the specific
  invariant that makes it sound. Soundness-critical invariants (the io_uring buffer-ownership
  and cancellation model in particular) are documented in
  [ARCHITECTURE.md](./ARCHITECTURE.md); update it when you change them.
- **Platform code** lives under `src/platform/` and `src/sys/`; keep the Linux, macOS, and
  Windows backends behind the existing `cfg` gates and mirror behavior where practical.
- **Public API** changes should update doctests, the README, the CHANGELOG, and (for runtime
  semantics) ARCHITECTURE.md, and regenerate the public API snapshot with
  `mise run api-report` (CI fails on a stale `docs/public-api.md`).

## Security issues

For security-sensitive reports, follow [SECURITY.md](./SECURITY.md) instead of posting
publicly.

## Licensing

By contributing you agree that your contributions are licensed under the project's dual
MIT OR Apache-2.0 license.

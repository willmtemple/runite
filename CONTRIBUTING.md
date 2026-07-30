# Contributing to runite

Thanks for your interest in improving `runite`! This document covers the basics for getting
a change merged.

## Building and running runite

`runite` pins its toolchain with [mise](https://mise.jdx.dev/):

```sh
mise install      # installs the pinned Rust toolchain and dev tools
mise run check    # fmt + clippy + tests + workflow lint + licences — the full local gate
```

If you do not use mise, a recent stable Rust toolchain (matching the `rust-version` /
`rust-toolchain.toml` pin) works too.

### Updating mise and the lockfile

CI and release jobs explicitly install mise `2026.6.11`, the version used to
generate the current `mise.lock`. Keep the lockfile generator and every
`jdx/mise-action` `version` input in sync:

1. Select the intended mise release locally with `mise self-update <version>`
   and confirm it with `mise --version`.
2. Update every `version` input in `.github/workflows/ci.yml` and
   `.github/workflows/release.yml`, plus `PINNED_MISE_VERSION` in
   `.github/scripts/test_mise_action_pin.py`.
3. Run `mise upgrade` and then `mise lock` with that exact mise release, and
   review the resulting tool versions, URLs, and checksums in `mise.lock`.
4. Run `mise run ci-script-test`, `mise run ci-lint`, and `mise run check`
   before committing the workflow and lockfile changes together.

Do not regenerate `mise.lock` with a newer local mise while the workflows still
install an older release; lockfile format or backend changes may not be
compatible.

## Reproducing CI

The required `Required CI` status aggregates every platform, safety, API,
package, docs/examples, coverage, license, and benchmark job. The focused
Linux jobs are available locally:

| Command | Coverage |
| --- | --- |
| `mise run ci-lint` | GitHub Actions YAML, expressions, and shell fragments via pinned `actionlint`. |
| `mise run miri` | Mock-driver scheduler, task, timer, channel, sync, waker, and pending-state logic. |
| `mise run asan` | Cancellation/drop, resource churn, and normal io_uring teardown on Linux. |
| `mise run tsan` | Mock-driver channel, watch, waker, worker, and concurrent completion logic. |
| `mise run capability-matrix` | Injected constrained io_uring opcode sets and old timer flags, then the io-facing tests and the doctests under two masked opcode profiles. |
| `mise run stress-issue-6` | Repeated merged doctests and `spawn_blocking` runtime-liveness regressions. |
| `mise run bench-io-uring-ab` | Immediate/deferred submission A/B data under `target/criterion/`. |

`miri`, `asan`, and `tsan` install the pinned `nightly-2026-07-01` plus
`miri`, `rust-src`, and `llvm-tools` through `mise run ci-nightly-install`.
The sanitizer, capability, stress, and io_uring A/B tasks require Linux and
GNU `timeout`. Every task and workflow job has a deadline; a stuck runtime is
terminated rather than leaving a runner behind. Override the local stress
count with `RUNITE_STRESS_ITERATIONS=50 mise run stress-issue-6`.

### Safety-tool boundaries

- **Miri** runs only `logic_safety_tests`, which enter `MockRuntimeHarness`.
  `cfg(miri)` removes the real Linux driver test modules. Miri never initializes
  io_uring and does not run filesystem, network, process, kqueue, or IOCP code.
- **TSan** uses the same driver-free tests. TSan cannot reliably intercept
  io_uring's kernel-owned accesses, so real-ring races belong to the native
  Linux and ASan jobs; pretending those syscalls are instrumented would give
  false confidence.
- **ASan** intentionally uses a compatible Ubuntu kernel and the real io_uring
  backend. Its focused suite covers cancellation, dropped buffers/descriptors,
  churn, and successful teardown without paying for every integration test.
- **Constrained kernels** are modeled by injecting the probe bitmap through the
  production dispatch seam. A container shares the GitHub runner's host kernel
  and cannot reliably provide an old io_uring implementation, so CI combines
  deterministic missing-opcode tests with one compatible Ubuntu kernel rather
  than claiming to boot a limited kernel. Injection proves the fallback
  branches run and stay consistent with the rest of the runtime; it cannot
  prove they are right about how a kernel that genuinely lacks the opcode
  behaves. That needs a VM on an old kernel and is tracked separately.

  Unit tests inject per test through a `#[cfg(test)]` thread-local. Integration
  tests and doctests link the non-test build and cannot reach it, so they use
  `RUNITE_IO_URING_DISABLE_OPCODES` (`above-5.6`, `all-optional`, or a
  comma-separated list of opcode numbers) which masks the probe result for the
  whole process. **That variable only exists in a build compiled with
  `--cfg runite_opcode_injection`**, which `mise run capability-matrix` sets
  and nothing else does. The gate is deliberately not `debug_assertions`: the
  constrained passes must be runnable in release, and masking is not harmless
  enough to ship — hiding an opcode with no fallback, such as
  `IORING_OP_SENDMSG`, would make `send_to` fail outright. A build carrying the
  cfg panics during ring setup if the variable is missing or unusable, and
  `opcode_injection_cfg_and_env_agree_and_reach_the_ring` fails if the variable
  reaches a build without the cfg, so a constrained pass cannot quietly
  degrade into an unmasked one.
- **Submission A/B** results are artifacts, not a pass/fail latency threshold.
  Hosted-runner hardware variance makes a wall-clock regression gate unsound.

If a local Linux host disables io_uring or blocks it with seccomp, the ASan,
capability, and A/B tasks will report that initialization failure; run those on
a kernel with io_uring enabled (5.6 minimum). If a sanitizer run is interrupted,
remove only its isolated cache (`target/asan`, `target/tsan`, or `target/miri`)
and rerun. A TSan “unexpected memory mapping” failure is a host/runtime
limitation, not a test skip; use the pinned Ubuntu CI image before diagnosing a
code race.

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
  `mise run api-report` (CI fails on a stale `docs/public-api.md` or
  `docs/public-api-traits.md`).

## Security issues

For security-sensitive reports, follow [SECURITY.md](./SECURITY.md) instead of posting
publicly.

## Licensing

By contributing you agree that your contributions are licensed under the project's dual
MIT OR Apache-2.0 license.

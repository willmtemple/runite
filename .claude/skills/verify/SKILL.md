---
name: verify
description: Run runite's verification gate before committing — the local checks, the Windows box, the macOS gap, and the platform traps that only one target catches. Use before any commit to runite, or when a change touches platform-specific code, docs, or the public API.
---

# Verifying a runite change

runite targets three platforms with three different I/O models, and each catches
things the others cannot. This is the order that finds problems soonest.

## Local gate

Everything here runs on Linux and is fast enough to run every time.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings; echo "EXIT=$?"
cargo test --workspace --all-features
mise run api-report-check          # regenerate with `mise run api-report` if it drifts
```

Read the clippy exit code rather than skimming its output. Piping through
`grep` and glancing is how a commit lands with errors in it.

Run these too when the change warrants:

```bash
mise run miri            # scheduler/task/timer/channel logic; ~20s, worth it for anything touching those
mise run ci-lint         # actionlint over the workflows
mise run ci-script-test   # the CI helper scripts
cargo bench --bench runtime -- --test   # smoke, if you added work to a hot path
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly-2026-07-01 doc --workspace --all-features --no-deps
```

That last one matters more than it looks. `--cfg docsrs` is only passed on
docs.rs, and **stable `cargo doc` does not report unresolved intra-doc links as
errors**. It has repeatedly caught broken links that every other check passed —
including links broken by removing an inherent method whose docs referenced it
with `Self::`.

## Windows

There is a box at `ssh wtemple@windev`, checkout at
`C:\Users\wtemple\Development\runite`.

```bash
# Sync changed files (it is not a git remote you can push to; scp is fine).
for f in $(git status --short | awk '{print $2}' | grep -E '\.rs$'); do
  scp -q -o BatchMode=yes "$f" "wtemple@windev:C:/Users/wtemple/Development/runite/$f"
done

ssh -o BatchMode=yes wtemple@windev \
  'cd C:\Users\wtemple\Development\runite && cargo clippy --workspace --all-targets --all-features -- -D warnings && cargo test --workspace --all-features'
```

Notes that cost time to rediscover:

- **`git fetch` there hangs** without `GIT_SSH_COMMAND='ssh -o BatchMode=yes'` —
  an interactive prompt with nowhere to go. Prefix it, or ship a `git bundle`
  over `scp`.
- **`;` is not a command separator in `cmd.exe`.** Use `&&` or `&`; a stray `;`
  ends up inside the previous argument and produces a baffling error.
- Long runs should go in the background; the full suite is several minutes.

Windows is the target most likely to catch a mistake the others miss, because
its cfg-gated code is invisible to a Linux build *and* to Linux clippy — tests
included. Two classes of bug came from exactly this: a Windows-only test file
that never compiled locally, and an ext-trait import inside a `#[cfg(unix)]`
block that portable tests needed on both platforms.

## macOS

**There is no macOS machine in this loop.** Ask the user to run the local gate
on a clone, and say plainly in the report that macOS was unverified until they
did.

The cross-target API probe (`mise run api-report`) typechecks the macOS cfg
path, which catches unresolved symbols — but it is *not* a lint pass, so
`dead_code` and friends go unreported there. Removing a `#[cfg]`-gated
`#[allow(dead_code)]` is not verified by it.

Predict BSD differences rather than discovering them; the tree already
documents several:

- **A pty's pending output is discarded when the last user-side descriptor
  closes.** Drain the controller *before* dropping it. `src/stdio.rs` documents
  this and works around it in its own tty tests.
- **A session leader's exit runs terminal teardown, which drains the output
  queue before tearing down the line, and that drain only advances while
  something reads the controller.** So `wait()`-then-read deadlocks on macOS
  where Linux tolerates it. Read the controller concurrently with the child's
  exit.
- Integer widths differ: `TIOCSCTTY` is `c_uint` on Apple and the request type
  elsewhere. `.into()` with `#[allow(clippy::useless_conversion)]` works on
  both; a hard-coded cast breaks on musl.

## What the checks do not cover

Worth knowing before claiming a change is verified:

- `--all-features` hides a break that only appears when one feature is absent.
  CI checks each alone; do the same locally if you touched feature-gated code.
- `api-report` typechecks every target but lints none of them.
- `cargo doc` on stable is quieter than the docs.rs configuration.
- Miri runs only the driver-free logic contract, not real I/O.

## Landing it

Regenerate `docs/public-api.md` and update `CHANGELOG.md` in the same commit as
the change — `api-report-check` is a CI gate, and a changelog written later is
written worse. Breaking changes also go in `docs/MIGRATING-0.3.md` with a
before/after, because that is what the release notes point at.

`Closes #N` only fires when the commit reaches `main`. On a branch, the issue
stays open; do not report it as closed.

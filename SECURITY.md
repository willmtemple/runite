# Security Policy

## Supported versions

`runite` is pre-1.0. Security fixes are applied to the latest published `0.x` release.

## Reporting a vulnerability

Please report suspected vulnerabilities privately rather than opening a public issue.

Use GitHub's [private vulnerability reporting](https://github.com/willmtemple/runite/security/advisories/new)
for this repository. Include:

- a description of the issue and its impact,
- the affected version(s) or commit,
- a minimal reproduction if possible, and
- any suggested remediation.

## Scope

`runite` is an async runtime that makes extensive use of `unsafe` to interact
with host-platform asynchronous I/O systems. The most safety-critical area is the
I/O buffer-ownership and cancellation model described in
[ARCHITECTURE.md](./ARCHITECTURE.md); soundness reports against that model are
especially valuable. Each `unsafe` block carries a `// SAFETY:` comment
documenting the invariant it relies on.

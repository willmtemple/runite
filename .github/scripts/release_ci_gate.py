"""Require a successful main-branch push CI run for a release commit."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from typing import Any


def successful_main_push_runs(
    payload: Mapping[str, Any], release_sha: str
) -> list[Mapping[str, Any]]:
    """Return only completed, successful main push runs for ``release_sha``."""
    runs = payload.get("workflow_runs", [])
    if not isinstance(runs, list):
        raise ValueError("GitHub Actions response has no workflow_runs list")

    return [
        run
        for run in runs
        if isinstance(run, Mapping)
        and run.get("head_sha") == release_sha
        and run.get("head_branch") == "main"
        and run.get("event") == "push"
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
    ]


def fetch_ci_runs(repository: str, release_sha: str) -> Mapping[str, Any]:
    """Fetch completed main push runs, retaining client-side validation."""
    command = [
        "gh",
        "api",
        "--method",
        "GET",
        f"/repos/{repository}/actions/workflows/ci.yml/runs",
        "-f",
        f"head_sha={release_sha}",
        "-f",
        "branch=main",
        "-f",
        "event=push",
        "-f",
        "status=completed",
        "-f",
        "per_page=100",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    payload = json.loads(result.stdout)
    if not isinstance(payload, Mapping):
        raise ValueError("GitHub Actions response is not an object")
    return payload


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <release-commit-sha>", file=sys.stderr)
        return 2

    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        print("GITHUB_REPOSITORY is required", file=sys.stderr)
        return 2

    release_sha = argv[1]
    try:
        runs = successful_main_push_runs(
            fetch_ci_runs(repository, release_sha), release_sha
        )
    except (json.JSONDecodeError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"Could not inspect CI runs: {error}", file=sys.stderr)
        return 1

    if not runs:
        print(
            "No successful completed CI push run on main exists for "
            f"release commit {release_sha}",
            file=sys.stderr,
        )
        return 1

    print(f"CI gate satisfied by run {runs[0].get('html_url', runs[0].get('id'))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

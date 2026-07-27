"""Validate Criterion A/B baseline identities and sample data."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


class ArtifactError(ValueError):
    """A Criterion baseline artifact is absent or invalid."""


def _read_object(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ArtifactError(f"cannot read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise ArtifactError(f"{path} is not valid JSON: {error}") from error
    if not isinstance(value, Mapping):
        raise ArtifactError(f"{path} must contain a JSON object")
    return value


def _positive_measurements(
    sample: Mapping[str, Any], key: str, path: Path
) -> list[int | float]:
    values = sample.get(key)
    if not isinstance(values, list):
        raise ArtifactError(f"{path} has no {key!r} sample array")
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        for value in values
    ):
        raise ArtifactError(f"{path} has invalid {key!r} sample data")
    return values


def validate_baseline(
    criterion_root: Path,
    benchmark: str,
    baseline: str,
    minimum_samples: int,
) -> int:
    """Validate one saved Criterion baseline and return its sample count."""
    parts = benchmark.split("/")
    if (
        not parts
        or any(part in {"", ".", ".."} for part in parts)
        or Path(benchmark).is_absolute()
    ):
        raise ArtifactError(f"invalid benchmark identifier: {benchmark!r}")
    if baseline in {"", ".", ".."} or "/" in baseline or "\\" in baseline:
        raise ArtifactError(f"invalid baseline name: {baseline!r}")
    if minimum_samples < 1:
        raise ArtifactError("minimum sample count must be positive")

    directory = criterion_root.joinpath(*parts, baseline)
    descriptor_path = directory / "benchmark.json"
    descriptor = _read_object(descriptor_path)
    if descriptor.get("full_id") != benchmark:
        raise ArtifactError(
            f"{descriptor_path} identifies {descriptor.get('full_id')!r}, "
            f"expected {benchmark!r}"
        )

    sample_path = directory / "sample.json"
    sample = _read_object(sample_path)
    iterations = _positive_measurements(sample, "iters", sample_path)
    times = _positive_measurements(sample, "times", sample_path)
    if len(iterations) != len(times):
        raise ArtifactError(
            f"{sample_path} has {len(iterations)} iteration counts but "
            f"{len(times)} timings"
        )
    if len(times) < minimum_samples:
        raise ArtifactError(
            f"{sample_path} has {len(times)} samples; expected at least "
            f"{minimum_samples}"
        )
    return len(times)


def validate_baselines(
    criterion_root: Path,
    benchmark: str,
    baselines: list[str],
    minimum_samples: int,
) -> dict[str, int]:
    """Validate every requested baseline."""
    if not baselines:
        raise ArtifactError("at least one baseline is required")
    if len(set(baselines)) != len(baselines):
        raise ArtifactError("baseline names must be unique")
    return {
        baseline: validate_baseline(
            criterion_root, benchmark, baseline, minimum_samples
        )
        for baseline in baselines
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--criterion-root", type=Path, required=True)
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--baseline", action="append", required=True)
    parser.add_argument("--minimum-samples", type=int, default=1)
    args = parser.parse_args(argv[1:])
    try:
        counts = validate_baselines(
            args.criterion_root,
            args.benchmark,
            args.baseline,
            args.minimum_samples,
        )
    except ArtifactError as error:
        print(f"Criterion A/B artifact validation failed: {error}", file=sys.stderr)
        return 1

    details = ", ".join(f"{name}={count}" for name, count in counts.items())
    print(f"validated Criterion samples for {args.benchmark}: {details}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

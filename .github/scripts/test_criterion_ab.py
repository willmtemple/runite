import contextlib
import io
import json
import shutil
import unittest
from pathlib import Path

from criterion_ab import ArtifactError, main, validate_baselines


REPOSITORY = Path(__file__).resolve().parents[2]


class CriterionAbTests(unittest.TestCase):
    def setUp(self):
        self.root = (
            REPOSITORY / "target" / "ci-script-tests" / self._testMethodName
        )
        shutil.rmtree(self.root, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_accepts_two_nonempty_matching_baselines(self):
        self._write_baseline("immediate", [1.0, 2.0])
        self._write_baseline("deferred", [3.0, 4.0])

        self.assertEqual(
            validate_baselines(
                self.root,
                "tcp_echo/loopback_16k",
                ["immediate", "deferred"],
                2,
            ),
            {"immediate": 2, "deferred": 2},
        )

    def test_rejects_missing_baseline_from_nonexistent_filter(self):
        self._write_baseline("immediate", [1.0, 2.0])

        with self.assertRaisesRegex(ArtifactError, "deferred"):
            validate_baselines(
                self.root,
                "tcp_echo/loopback_16k",
                ["immediate", "deferred"],
                2,
            )

    def test_rejects_empty_sample_data(self):
        self._write_baseline("immediate", [])

        with self.assertRaisesRegex(ArtifactError, "expected at least"):
            validate_baselines(
                self.root, "tcp_echo/loopback_16k", ["immediate"], 1
            )

    def test_rejects_mismatched_benchmark_identity(self):
        self._write_baseline(
            "immediate", [1.0], full_id="tcp_echo/different_benchmark"
        )

        with self.assertRaisesRegex(ArtifactError, "different_benchmark"):
            validate_baselines(
                self.root, "tcp_echo/loopback_16k", ["immediate"], 1
            )

    def test_cli_returns_failure_when_filter_created_no_artifacts(self):
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            result = main(
                [
                    "criterion_ab.py",
                    "--criterion-root",
                    str(self.root),
                    "--benchmark",
                    "tcp_echo/loopback_16k",
                    "--baseline",
                    "immediate",
                    "--baseline",
                    "deferred",
                    "--minimum-samples",
                    "10",
                ]
            )

        self.assertEqual(result, 1)

    def _write_baseline(
        self, baseline: str, times: list[float], *, full_id: str | None = None
    ):
        directory = (
            self.root / "tcp_echo" / "loopback_16k" / baseline
        )
        directory.mkdir(parents=True)
        (directory / "benchmark.json").write_text(
            json.dumps({"full_id": full_id or "tcp_echo/loopback_16k"}),
            encoding="utf-8",
        )
        (directory / "sample.json").write_text(
            json.dumps(
                {
                    "sampling_mode": "Linear",
                    "iters": list(range(1, len(times) + 1)),
                    "times": times,
                }
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()

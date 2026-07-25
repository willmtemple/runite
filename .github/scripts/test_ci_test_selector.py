import contextlib
import io
import sys
import unittest
from unittest.mock import patch

from ci_test_selector import (
    CONTRACTS,
    SelectionContract,
    SelectionError,
    listed_test_names,
    listing_command,
    run_selection,
    validate_selection,
)


class CiTestSelectorTests(unittest.TestCase):
    def test_listing_command_preserves_harness_selector_options(self):
        command = [
            "cargo",
            "test",
            "suite::test",
            "--",
            "--exact",
            "--test-threads=1",
        ]

        self.assertEqual(listing_command(command), [*command, "--list"])

    def test_listing_command_adds_libtest_separator(self):
        self.assertEqual(
            listing_command(["cargo", "test", "suite::"]),
            ["cargo", "test", "suite::", "--", "--list"],
        )

    def test_extracts_only_tests(self):
        output = "suite::one: test\nsuite::bench: benchmark\n\n1 test, 1 benchmark\n"

        self.assertEqual(listed_test_names(output), ["suite::one"])

    def test_rejects_missing_expected_name_even_when_minimum_is_met(self):
        contract = SelectionContract(minimum=1, expected=("suite::required",))

        with self.assertRaisesRegex(SelectionError, "suite::required"):
            validate_selection("suite::other: test\n", contract)

    def test_runs_command_after_successful_preflight(self):
        contract = SelectionContract(minimum=1, expected=("suite::exists",))
        command = self._fake_test_command("exists")

        with (
            patch.dict(CONTRACTS, {"fixture": contract}),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(run_selection("fixture", command), 0)

    def test_nonexistent_filter_fails_without_running_tests(self):
        contract = SelectionContract(minimum=1, expected=("suite::exists",))
        command = self._fake_test_command("nonexistent", actual_exit=91)

        with (
            patch.dict(CONTRACTS, {"fixture": contract}),
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(run_selection("fixture", command), 1)

    @staticmethod
    def _fake_test_command(selector: str, actual_exit: int = 0) -> list[str]:
        program = """
import sys
selector = sys.argv[1]
if "--list" in sys.argv:
    if selector == "exists":
        print("suite::exists: test")
    print("1 test, 0 benchmarks" if selector == "exists" else "0 tests, 0 benchmarks")
    raise SystemExit(0)
raise SystemExit(int(sys.argv[2]))
"""
        return [
            sys.executable,
            "-c",
            program,
            selector,
            str(actual_exit),
            "--",
            "--test-threads=1",
        ]


if __name__ == "__main__":
    unittest.main()

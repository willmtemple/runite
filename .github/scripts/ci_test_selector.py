"""Preflight focused Rust test selectors before running them."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class SelectionContract:
    minimum: int
    expected: tuple[str, ...]


CONTRACTS: Final[dict[str, SelectionContract]] = {
    "logic-safety": SelectionContract(
        minimum=4,
        expected=(
            "logic_safety_tests::logic_safety_channel_watch_and_concurrent_completions",
            "logic_safety_tests::logic_safety_pending_state_cancels_owned_operation_before_shutdown",
            "logic_safety_tests::logic_safety_scheduler_task_timer_and_sync",
            "logic_safety_tests::logic_safety_waker_clone_drop_and_worker",
        ),
    ),
    "asan-cancellation": SelectionContract(
        minimum=5,
        expected=(
            "abort_before_first_poll_yields_aborted",
            "abort_handle_cancels_from_elsewhere",
            "abort_while_parked_on_timer_cancels_without_completing",
            "completed_task_reports_finished_and_abort_is_noop",
            "join_handle_resolves_to_output_when_not_aborted",
        ),
    ),
    "asan-io-compat": SelectionContract(
        minimum=2,
        expected=(
            "compat_file_scalar_write_cancellation_cannot_credit_the_next_buffer",
            "compat_file_vectored_write_cancellation_cannot_credit_the_next_buffers",
        ),
    ),
    "asan-io-traits": SelectionContract(
        minimum=3,
        expected=(
            "cancelled_buf_reader_read_line_keeps_its_consumed_prefix_recoverable",
            "cancelled_cloned_file_write_leaves_no_stale_queue_entry",
            "file_vectored_write_cancellation_keeps_operation_identity",
        ),
    ),
    "asan-driver-shutdown": SelectionContract(
        minimum=1,
        expected=(
            "platform::linux::driver::tests::shutdown_flushes_cancel_and_drains_original_terminal_cqe",
        ),
    ),
    "asan-runtime-read-teardown": SelectionContract(
        minimum=1,
        expected=(
            "platform::linux::runtime::tests::pending_read_teardown_quiesces_before_retained_handle_drops",
        ),
    ),
    "asan-fs-read-drop": SelectionContract(
        minimum=1,
        expected=("fs::tests::read_drop_during_inflight_does_not_uaf",),
    ),
    "asan-net-accept": SelectionContract(
        minimum=1,
        expected=(
            "sys::linux::net::tests::uninterested_accept_completion_closes_owned_descriptor",
        ),
    ),
    "capability-matrix": SelectionContract(
        minimum=10,
        expected=(
            "platform::linux::driver::tests::capability_matrix_eventfd_fallback_survives_worker_ring_rehome",
            "platform::linux::driver::tests::capability_matrix_eventfd_fallback_wakes_parked_runtime",
            "platform::linux::driver::tests::capability_matrix_eventfd_fallback_wakes_target_ring",
            "platform::linux::uring::tests::capability_matrix_old_timer_remove_avoids_cqe_skip",
            "sys::linux::fs::tests::capability_matrix_directory_ops_fall_back_without_newer_opcodes",
            "sys::linux::fs::tests::capability_matrix_set_len_falls_back_without_ftruncate",
            "sys::linux::net::tests::capability_matrix_bind_falls_back_without_bind_opcode",
            "sys::linux::net::tests::capability_matrix_listen_falls_back_without_listen_opcode",
            "sys::linux::net::tests::capability_matrix_missing_socket_opcodes_use_production_fallbacks",
            "sys::linux::net::tests::capability_matrix_socket_deadlines_survive_missing_send_and_recv",
        ),
    ),
    "stress-task": SelectionContract(
        minimum=4,
        expected=(
            "task::tests::accepted_job_keeps_runtime_alive_until_terminalization",
            "task::tests::spawn_blocking_returns_complex_value",
            "task::tests::spawn_blocking_returns_value",
            "task::tests::successful_closure_cannot_race_to_cancelled",
        ),
    ),
    "stress-completion": SelectionContract(
        minimum=1,
        expected=(
            "platform::runtime_shared::test_support::tests::completion_between_ready_check_and_idle_commit_is_not_cancelled",
        ),
    ),
}


class SelectionError(ValueError):
    """A focused selector did not satisfy its declared contract."""


def listing_command(command: list[str]) -> list[str]:
    """Return the equivalent Rust test command in list-only mode."""
    if not command:
        raise SelectionError("test command must not be empty")
    if "--" in command:
        return [*command, "--list"]
    return [*command, "--", "--list"]


def listed_test_names(output: str) -> list[str]:
    """Extract test names from libtest's ``--list`` output."""
    suffix = ": test"
    return [
        line.removesuffix(suffix)
        for raw_line in output.splitlines()
        if (line := raw_line.strip()).endswith(suffix)
    ]


def validate_selection(output: str, contract: SelectionContract) -> list[str]:
    """Validate and return the names selected by a libtest listing."""
    names = listed_test_names(output)
    missing = sorted(set(contract.expected).difference(names))
    problems = []
    if len(names) < contract.minimum:
        problems.append(
            f"matched {len(names)} tests, but at least {contract.minimum} are required"
        )
    if missing:
        problems.append(f"missing expected tests: {', '.join(missing)}")
    if problems:
        raise SelectionError("; ".join(problems))
    return names


def run_selection(
    contract_name: str, command: list[str], *, preflight_only: bool = False
) -> int:
    """List, validate, and optionally execute a focused test command."""
    contract = CONTRACTS[contract_name]
    preflight = listing_command(command)
    print(
        f"== selector preflight ({contract_name}): {shlex.join(preflight)}",
        flush=True,
    )
    try:
        result = subprocess.run(preflight, capture_output=True, text=True, check=False)
    except OSError as error:
        print(f"could not run selector preflight: {error}", file=sys.stderr)
        return 1

    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    sys.stdout.flush()
    sys.stderr.flush()
    if result.returncode != 0:
        print(
            f"selector preflight command failed with exit code {result.returncode}",
            file=sys.stderr,
        )
        return result.returncode

    try:
        names = validate_selection(result.stdout, contract)
    except SelectionError as error:
        print(f"selector contract {contract_name!r} failed: {error}", file=sys.stderr)
        return 1

    print(
        f"selector contract {contract_name!r} matched {len(names)} tests",
        flush=True,
    )
    if preflight_only:
        return 0

    try:
        return subprocess.run(command, check=False).returncode
    except OSError as error:
        print(f"could not run selected tests: {error}", file=sys.stderr)
        return 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", required=True, choices=sorted(CONTRACTS))
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="validate the selector without running the selected tests",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv[1:])
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a test command is required after --")
    return run_selection(
        args.contract, command, preflight_only=args.preflight_only
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

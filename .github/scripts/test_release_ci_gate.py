import unittest

from release_ci_gate import successful_main_push_runs


def run(**overrides):
    candidate = {
        "id": 1,
        "head_sha": "a" * 40,
        "head_branch": "main",
        "event": "push",
        "status": "completed",
        "conclusion": "success",
    }
    candidate.update(overrides)
    return candidate


class SuccessfulMainPushRunsTests(unittest.TestCase):
    def test_accepts_only_exact_successful_main_push(self):
        expected = run()
        payload = {
            "workflow_runs": [
                run(id=2, head_sha="b" * 40),
                run(id=3, head_branch="release"),
                run(id=4, conclusion="failure"),
                expected,
            ]
        }

        self.assertEqual(
            successful_main_push_runs(payload, "a" * 40),
            [expected],
        )

    def test_rejects_pull_request_synthetic_merge_for_exact_sha(self):
        payload = {
            "workflow_runs": [
                run(event="pull_request", head_branch="feature/release"),
            ]
        }

        self.assertEqual(successful_main_push_runs(payload, "a" * 40), [])

    def test_rejects_incomplete_or_cancelled_runs(self):
        payload = {
            "workflow_runs": [
                run(id=2, status="in_progress", conclusion=None),
                run(id=3, conclusion="cancelled"),
            ]
        }

        self.assertEqual(successful_main_push_runs(payload, "a" * 40), [])

    def test_requires_a_workflow_runs_list(self):
        with self.assertRaises(ValueError):
            successful_main_push_runs({"workflow_runs": None}, "a" * 40)


if __name__ == "__main__":
    unittest.main()

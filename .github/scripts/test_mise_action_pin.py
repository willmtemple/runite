import re
import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[2]
PINNED_MISE_VERSION = "2026.6.11"
MISE_ACTION = "uses: jdx/mise-action@"
MISE_PIN = re.compile(
    rf"(?m)^\s*{re.escape(MISE_ACTION)}[^\n]+\n"
    r"\s+with:\n"
    r"\s+version:\s+([^\s#]+)"
)


class MiseActionPinTests(unittest.TestCase):
    def test_every_workflow_invocation_pins_the_lockfile_mise_version(self):
        invocation_count = 0
        versions = []
        for workflow in sorted((REPOSITORY / ".github" / "workflows").glob("*.yml")):
            text = workflow.read_text(encoding="utf-8")
            invocation_count += text.count(MISE_ACTION)
            versions.extend(MISE_PIN.findall(text))

        self.assertGreater(invocation_count, 0)
        self.assertEqual(len(versions), invocation_count)
        self.assertEqual(set(versions), {PINNED_MISE_VERSION})


if __name__ == "__main__":
    unittest.main()

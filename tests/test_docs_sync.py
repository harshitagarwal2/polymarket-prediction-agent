from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_WITH_CANONICAL_PATH = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "ARCHITECTURE.md",
    REPO_ROOT / "docs" / "FORECASTING_PLATFORM.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
)
DOCS_WITH_OPERATOR_COMMANDS = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
)
DOCS_WITH_STATUS_ADVISORY = DOCS_WITH_OPERATOR_COMMANDS


class DocsSyncTests(unittest.TestCase):
    def test_canonical_llm_advisory_path_is_consistent(self):
        for path in DOCS_WITH_CANONICAL_PATH:
            with self.subTest(path=path):
                self.assertIn(
                    "runtime/data/current/llm_advisory.json",
                    path.read_text(),
                )

    def test_operator_docs_reference_the_same_advisory_commands(self):
        for path in DOCS_WITH_OPERATOR_COMMANDS:
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("build-llm-advisory", text)
                self.assertIn("show-llm-advisory", text)

    def test_operator_docs_reference_status_advisory_summary(self):
        for path in DOCS_WITH_STATUS_ADVISORY:
            with self.subTest(path=path):
                self.assertIn(
                    "operator-cli status --state-file runtime/safety-state.json --llm-advisory-file runtime/data/current/llm_advisory.json",
                    path.read_text(),
                )


if __name__ == "__main__":
    unittest.main()

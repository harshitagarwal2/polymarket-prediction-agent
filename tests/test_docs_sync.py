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
    REPO_ROOT / "docs" / "ARCHITECTURE.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
)
DOCS_WITH_STATUS_ADVISORY = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
)
DOCS_WITH_POLICY_GUIDANCE = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
    REPO_ROOT / "docs" / "FORECASTING_PLATFORM.md",
)
DOCS_WITH_DATASET_COMMANDS = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "VERIFICATION_SPORTS_POLYMARKET.md",
)
DOCS_WITH_REPLAY_ATTRIBUTION_CLI = (REPO_ROOT / "docs" / "BENCHMARK_CASE_SCHEMA.md",)
DOCS_WITH_SPORTSBOOK_CAPTURE_WORKER = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "VERIFICATION_SPORTS_POLYMARKET.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
    REPO_ROOT / "docs" / "ARCHITECTURE.md",
)
DOCS_WITH_POLYMARKET_CAPTURE_AND_PROJECTION = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "VERIFICATION_SPORTS_POLYMARKET.md",
    REPO_ROOT / "docs" / "ARCHITECTURE.md",
)
VERIFICATION_JSON_PATH = REPO_ROOT / "docs" / "verification_sports_polymarket.json"
DOCS_WITH_CURRENT_CI_SUMMARY = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "ARCHITECTURE.md",
)
DOCS_WITH_PHASE1_ENTRYPOINTS = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
)


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

    def test_docs_reference_policy_guidance_for_advisory_preview_alignment(self):
        for path in DOCS_WITH_POLICY_GUIDANCE:
            with self.subTest(path=path):
                self.assertIn("--policy-file runtime/policy.json", path.read_text())

    def test_readme_ci_section_matches_current_workflow(self):
        text = (REPO_ROOT / "README.md").read_text()
        lowered = text.lower()
        self.assertIn("Run advisory and docs contract regressions", text)
        self.assertIn("compileall", text)
        self.assertIn("ruff", lowered)
        self.assertIn("mypy", lowered)
        self.assertIn("coverage", lowered)
        self.assertIn("pip-audit", lowered)
        self.assertIn("gitleaks", lowered)

    def test_ci_docs_reference_current_workflow_shape(self):
        for path in DOCS_WITH_CURRENT_CI_SUMMARY:
            with self.subTest(path=path):
                text = path.read_text()
                lowered = text.lower()
                self.assertIn("compileall", text)
                self.assertIn("Run advisory and docs contract regressions", text)
                self.assertIn("ruff", lowered)
                self.assertIn("mypy", lowered)
                self.assertIn("coverage", lowered)

    def test_dataset_materialization_commands_are_documented(self):
        for path in DOCS_WITH_DATASET_COMMANDS:
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("build-inference-dataset", text)
                self.assertIn("build-training-dataset", text)
                self.assertIn("historical-training-dataset", text)

    def test_verification_json_mentions_dataset_materialization(self):
        text = VERIFICATION_JSON_PATH.read_text()
        self.assertIn("build-inference-dataset", text)
        self.assertIn("build-training-dataset", text)
        self.assertIn("historical-training-dataset", text)

    def test_docs_reference_replay_attribution_cli_contract(self):
        for path in DOCS_WITH_REPLAY_ATTRIBUTION_CLI:
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("run_replay_attribution.py", text)
                self.assertIn("trade_attributions", text)
                self.assertIn("attribution_summary", text)

    def test_worker_docs_reference_the_postgres_backed_sportsbook_capture_split(self):
        for path in DOCS_WITH_SPORTSBOOK_CAPTURE_WORKER:
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("run-sportsbook-capture", text)
                if path == REPO_ROOT / "docs" / "VERIFICATION_SPORTS_POLYMARKET.md":
                    self.assertIn("source_health_events", text)
                if path == REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md":
                    self.assertIn("bootstrap-postgres", text)

    def test_verification_json_mentions_dedicated_sportsbook_worker(self):
        text = VERIFICATION_JSON_PATH.read_text()
        self.assertIn("dedicated_sportsbook_capture_worker", text)
        self.assertIn("run_sportsbook_capture", text.replace("-", "_"))

    def test_verification_json_mentions_projection_polymarket_and_attribution_surfaces(
        self,
    ):
        text = VERIFICATION_JSON_PATH.read_text().replace("-", "_")
        self.assertIn("dedicated_polymarket_capture_worker", text)
        self.assertIn("run_polymarket_capture", text)
        self.assertIn("current_projection_worker", text)
        self.assertIn("run_current_projection", text)
        self.assertIn("replay_attribution_cli", text)
        self.assertIn("run_replay_attribution", text)
        self.assertIn("walk_forward_benchmark_attribution_ledger", text)

    def test_docs_reference_polymarket_capture_and_projection_workers(self):
        for path in DOCS_WITH_POLYMARKET_CAPTURE_AND_PROJECTION:
            with self.subTest(path=path):
                text = path.read_text().replace("-", "_")
                self.assertIn("run_polymarket_capture", text)
                self.assertIn("run_current_projection", text)

    def test_readme_lists_all_console_entrypoints(self):
        text = (REPO_ROOT / "README.md").read_text()
        self.assertIn("render-model-vs-market-dashboard", text)
        self.assertIn("scaffold-forecasting-pipeline", text)

    def test_docs_list_phase1_console_entrypoints(self):
        for path in DOCS_WITH_PHASE1_ENTRYPOINTS:
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("run-current-projection", text)
                self.assertIn("run-replay-attribution", text)


if __name__ == "__main__":
    unittest.main()

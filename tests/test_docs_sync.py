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
DOCS_WITH_AUTHORITY_ADR = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs" / "GETTING_STARTED.md",
    REPO_ROOT / "docs" / "ARCHITECTURE.md",
    REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
    REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
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

    def test_operator_docs_reference_status_output_sidecar(self):
        for path in (
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
            REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
        ):
            with self.subTest(path=path):
                self.assertIn("runtime_status.json", path.read_text())

    def test_operator_docs_reference_alerting_baseline_commands(self):
        for path in (
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("build-alerts", text)
                self.assertIn("send-alerts", text)
                self.assertIn("smoke-alerting", text)

    def test_operator_docs_reference_heartbeat_baseline_commands(self):
        for path in (
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
            REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("build-heartbeat", text)
                self.assertIn("send-heartbeat", text)
                self.assertIn("smoke-heartbeat", text)

    def test_operator_docs_reference_tax_audit_baseline_commands(self):
        for path in (
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
            REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("export-tax-audit", text)
                self.assertIn("smoke-tax-audit", text)

    def test_operator_docs_reference_model_drift_baseline_commands(self):
        for path in (
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
            REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("build-model-drift", text)
                self.assertIn("smoke-model-drift", text)
                self.assertIn("smoke-unattended-guardrails", text)

    def test_operator_docs_reference_autonomous_mode_contract(self):
        for path in (
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
            REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("--autonomous-mode", text)
                self.assertIn("autonomous_mode", text)
                self.assertIn("--execution-lock-name", text)
                self.assertIn("--drift-report-file", text)

    def test_operator_docs_reference_private_key_file_and_route_attestation(self):
        for path in (
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
            REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md",
            REPO_ROOT / "docs" / "PRODUCTION_READINESS.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("POLYMARKET_PRIVATE_KEY_FILE", text)
                self.assertIn("POLYMARKET_PRIVATE_KEY_COMMAND", text)
                self.assertIn("POLYMARKET_ROUTE_LABEL", text)
                self.assertIn("POLYMARKET_GEO_COMPLIANCE_ACK", text)
                self.assertIn("POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED", text)
                self.assertIn("PREDICTION_MARKET_HTTP_MIN_INTERVAL_SECONDS", text)

    def test_operator_runbook_references_longer_window_drawdown_guards(self):
        text = (REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md").read_text()
        self.assertIn("max_weekly_loss", text)
        self.assertIn("max_cumulative_loss", text)
        self.assertIn("max_active_wallet_balance", text)

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

    def test_docs_reference_second_sportsbook_source_and_schedule_feed_helper(self):
        for path in (
            REPO_ROOT / "README.md",
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertIn("sportsgameodds", text)
                self.assertIn("build_event_map_from_schedule_feed", text)

    def test_verification_json_mentions_dedicated_sportsbook_worker(self):
        text = VERIFICATION_JSON_PATH.read_text()
        self.assertIn("dedicated_sportsbook_capture_worker", text)
        self.assertIn("run_sportsbook_capture", text.replace("-", "_"))

    def test_verification_artifacts_use_sanctioned_live_builder_chain(self):
        verification_markdown = (
            REPO_ROOT / "docs" / "VERIFICATION_SPORTS_POLYMARKET.md"
        ).read_text()
        verification_json = VERIFICATION_JSON_PATH.read_text().replace("-", "_")

        self.assertIn("run-sportsbook-capture", verification_markdown)
        self.assertIn("run-current-projection", verification_markdown)
        self.assertNotIn(
            "python -m scripts.ingest_live_data sportsbook-odds", verification_markdown
        )

        self.assertIn("run_sportsbook_capture", verification_json)
        self.assertIn("run_current_projection", verification_json)

    def test_readme_and_getting_started_no_longer_publish_retired_sportsbook_ingest(
        self,
    ):
        for path in (
            REPO_ROOT / "README.md",
            REPO_ROOT / "docs" / "GETTING_STARTED.md",
        ):
            with self.subTest(path=path):
                text = path.read_text()
                self.assertNotIn(
                    "python -m scripts.ingest_live_data sportsbook-odds",
                    text,
                )
                self.assertIn("python -m scripts.run_sportsbook_capture", text)

    def test_getting_started_projects_user_channel_account_truth(self):
        text = (REPO_ROOT / "docs" / "GETTING_STARTED.md").read_text()
        self.assertIn("account-truth lanes", text)
        self.assertIn("run-polymarket-capture user", text)
        self.assertIn("smoke-supervised-live-account-truth", text)

    def test_operator_runbook_documents_user_capture_market_contract(self):
        text = (REPO_ROOT / "docs" / "OPERATOR_RUNBOOK.md").read_text()
        self.assertIn("run-polymarket-capture user", text)
        self.assertIn("POLYMARKET_LIVE_USER_MARKETS", text)
        self.assertIn("smoke-supervised-live-account-truth", text)

    def test_production_readiness_mentions_projected_account_truth(self):
        text = (REPO_ROOT / "docs" / "PRODUCTION_READINESS.md").read_text()
        self.assertIn("projection_polymarket_user_channel", text)
        self.assertIn("polymarket_orders.json", text)

    def test_production_readiness_links_replay_freeze_lift_contract(self):
        text = (REPO_ROOT / "docs" / "PRODUCTION_READINESS.md").read_text()
        self.assertIn("docs/REPLAY_FREEZE_LIFT.md", text)

    def test_replay_freeze_lift_doc_lists_verification_commands(self):
        text = (REPO_ROOT / "docs" / "REPLAY_FREEZE_LIFT.md").read_text()
        self.assertIn("test_strategy_and_replay.py", text)
        self.assertIn("test_replay_attribution_cli.py", text)
        self.assertIn("test_llm_advisory.py", text)

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

    def test_docs_link_the_authority_adr(self):
        for path in DOCS_WITH_AUTHORITY_ADR:
            with self.subTest(path=path):
                self.assertIn(
                    "authority-and-reconciliation.md",
                    path.read_text(),
                )


if __name__ == "__main__":
    unittest.main()

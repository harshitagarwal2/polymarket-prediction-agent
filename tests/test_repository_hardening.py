from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class RepositoryHardeningTests(unittest.TestCase):
    def test_repo_policy_files_exist(self):
        for relative_path in (
            ".editorconfig",
            ".pre-commit-config.yaml",
            ".github/CODEOWNERS",
            ".github/dependabot.yml",
            ".github/ISSUE_TEMPLATE/bug_report.yml",
            ".github/ISSUE_TEMPLATE/feature_request.yml",
            ".github/ISSUE_TEMPLATE/config.yml",
            ".github/PULL_REQUEST_TEMPLATE.md",
            ".github/workflows/security.yml",
            ".github/workflows/release-artifacts.yml",
            "CODE_OF_CONDUCT.md",
            "CONTRIBUTING.md",
            "SECURITY.md",
        ):
            with self.subTest(relative_path=relative_path):
                self.assertTrue((REPO_ROOT / relative_path).exists())

    def test_dependabot_tracks_uv_and_actions(self):
        text = (REPO_ROOT / ".github" / "dependabot.yml").read_text()
        self.assertIn("package-ecosystem: github-actions", text)
        self.assertIn("package-ecosystem: uv", text)
        self.assertNotIn("labels:", text)

    def test_release_workflow_uses_locked_non_isolated_build(self):
        text = (
            REPO_ROOT / ".github" / "workflows" / "release-artifacts.yml"
        ).read_text()
        self.assertIn("uv sync --locked --extra dev", text)
        self.assertIn("uv run --no-sync", text)
        self.assertIn("python -m build --no-isolation", text)

    def test_smoke_compose_runs_compose_config_validation(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("docker compose config >/dev/null", text)

    def test_gitignore_covers_generated_outputs_and_secret_markers(self):
        text = (REPO_ROOT / ".gitignore").read_text()
        for entry in (
            "*.log",
            "prediction-market-agent*.tar.gz",
            ".env.*",
            "!.env.example",
            ".sisyphus/",
            "runtime/benchmark-suite/",
            "runtime/track5_single_*.json",
            "**/postgres.dsn",
            "**/.postgres.dsn",
            "**/database_url.txt",
        ):
            with self.subTest(entry=entry):
                self.assertIn(entry, text)

    def test_dockerignore_excludes_local_runtime_and_secret_state(self):
        text = (REPO_ROOT / ".dockerignore").read_text()
        for entry in (
            ".env",
            ".env.*",
            "runtime",
            "*.log",
            ".omx",
            "prediction-market-agent*.tar.gz",
            "**/postgres.dsn",
            "**/.postgres.dsn",
            "**/database_url.txt",
        ):
            with self.subTest(entry=entry):
                self.assertIn(entry, text)

    def test_env_example_keeps_secret_values_as_placeholders(self):
        text = (REPO_ROOT / ".env.example").read_text()
        for line in (
            "THE_ODDS_API_KEY=replace-me",
            "SPORTSGAMEODDS_API_KEY=replace-me",
            "SPORTSBOOK_CAPTURE_PROVIDER=theoddsapi",
            "PREDICTION_MARKET_HTTP_MIN_INTERVAL_SECONDS=",
            "POLYMARKET_PRIVATE_KEY=replace-me",
            "POLYMARKET_PRIVATE_KEY_COMMAND=",
            "POLYMARKET_PRIVATE_KEY_FILE=",
            "POLYMARKET_FUNDER=replace-me",
            "POLYMARKET_ACCOUNT_ADDRESS=replace-me",
            "POLYMARKET_CLOB_HOST=https://clob.polymarket.com",
            "POLYMARKET_DATA_API_HOST=https://data-api.polymarket.com",
            "POLYMARKET_ROUTE_LABEL=",
            "POLYMARKET_GEO_COMPLIANCE_ACK=false",
            "POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED=false",
            "POLYMARKET_ASSET_ID=replace-me",
            "POLYMARKET_LIVE_USER_MARKETS=replace-me",
        ):
            with self.subTest(line=line):
                self.assertIn(line, text)

    def test_security_policy_preserves_supervised_boundary(self):
        text = (REPO_ROOT / "SECURITY.md").read_text()
        self.assertIn("supervised, fail-closed", text)
        self.assertIn("positioned as an unattended live trading system", text)
        self.assertIn(".env.example", text)

    def test_compose_postgres_is_bound_to_localhost(self):
        text = (REPO_ROOT / "docker-compose.yml").read_text()
        self.assertIn('"127.0.0.1:5432:5432"', text)

    def test_compose_exposes_polymarket_user_capture_service(self):
        text = (REPO_ROOT / "docker-compose.yml").read_text()
        self.assertIn("run-polymarket-capture-user:", text)
        self.assertIn('"run-polymarket-capture"', text)
        self.assertIn('"user"', text)
        self.assertIn("POLYMARKET_LIVE_USER_MARKETS", text)

    def test_compose_sportsbook_capture_supports_provider_selection(self):
        text = (REPO_ROOT / "docker-compose.yml").read_text()
        self.assertIn("SPORTSBOOK_CAPTURE_PROVIDER", text)
        self.assertIn("SPORTSBOOK_PROVIDER_URL", text)
        self.assertIn("SPORTSGAMEODDS_API_KEY", text)

    def test_makefile_exposes_supervised_live_account_truth_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-supervised-live-account-truth", text)

    def test_makefile_exposes_alerting_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-alerting", text)

    def test_makefile_exposes_heartbeat_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-heartbeat", text)

    def test_makefile_exposes_tax_audit_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-tax-audit", text)

    def test_makefile_exposes_model_drift_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-model-drift", text)

    def test_makefile_exposes_continuous_builder_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-continuous-builders", text)

    def test_makefile_exposes_multi_provider_sportsbook_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-multi-provider-sportsbook", text)

    def test_makefile_exposes_polymarket_depth_trades_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-polymarket-depth-trades", text)

    def test_makefile_exposes_unattended_guardrails_smoke(self):
        text = (REPO_ROOT / "Makefile").read_text()
        self.assertIn("smoke-unattended-guardrails", text)

    def test_readme_links_community_docs(self):
        text = (REPO_ROOT / "README.md").read_text()
        self.assertIn("CONTRIBUTING.md", text)
        self.assertIn("CODE_OF_CONDUCT.md", text)

    def test_issue_template_and_pr_template_contracts_exist(self):
        issue_config = (
            REPO_ROOT / ".github" / "ISSUE_TEMPLATE" / "config.yml"
        ).read_text()
        pr_template = (REPO_ROOT / ".github" / "PULL_REQUEST_TEMPLATE.md").read_text()
        self.assertIn("Security policy", issue_config)
        self.assertIn("Contribution guide", issue_config)
        self.assertIn("## Summary", pr_template)


if __name__ == "__main__":
    unittest.main()

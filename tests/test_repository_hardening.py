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
            ".github/workflows/security.yml",
            ".github/workflows/release-artifacts.yml",
            "SECURITY.md",
        ):
            with self.subTest(relative_path=relative_path):
                self.assertTrue((REPO_ROOT / relative_path).exists())

    def test_dependabot_tracks_uv_and_actions(self):
        text = (REPO_ROOT / ".github" / "dependabot.yml").read_text()
        self.assertIn("package-ecosystem: github-actions", text)
        self.assertIn("package-ecosystem: uv", text)

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
            "POLYMARKET_PRIVATE_KEY=replace-me",
            "POLYMARKET_FUNDER=replace-me",
            "POLYMARKET_ACCOUNT_ADDRESS=replace-me",
            "POLYMARKET_ASSET_ID=replace-me",
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


if __name__ == "__main__":
    unittest.main()

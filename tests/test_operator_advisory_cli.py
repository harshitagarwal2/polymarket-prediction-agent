from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from llm import (
    build_llm_advisory_artifact,
    load_llm_advisory_contract_rows,
    write_llm_advisory_artifacts,
)
from llm.advisory_context import build_preview_runtime_context
from scripts import operator_cli


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "llm_advisory"
LLM_INPUT_PATH = FIXTURE_DIR / "llm_input.json"
RUNTIME_ROOT = FIXTURE_DIR / "runtime"


def _write_fixture_advisory(output_path: Path) -> None:
    artifact = build_llm_advisory_artifact(
        load_llm_advisory_contract_rows(LLM_INPUT_PATH),
        preview_order_proposals=build_preview_runtime_context(
            RUNTIME_ROOT
        ).preview_order_proposals,
        blocked_preview_orders=build_preview_runtime_context(
            RUNTIME_ROOT
        ).blocked_preview_orders,
        runtime_health={"state": "recovering", "open_recovery_count": 1},
        provider_name="offline-review",
        provider_model="fixture-v1",
        prompt_version="prompt-1",
        generated_at=datetime(2026, 4, 22, tzinfo=timezone.utc),
    )
    write_llm_advisory_artifacts(artifact, output_path)


class OperatorAdvisoryCliTests(unittest.TestCase):
    def test_build_llm_advisory_command_writes_json_and_markdown(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "llm_advisory.json"
            state_path = Path(temp_dir) / "safety-state.json"
            stdout = io.StringIO()

            with (
                patch(
                    "sys.argv",
                    [
                        "operator_cli.py",
                        "build-llm-advisory",
                        "--llm-input",
                        str(LLM_INPUT_PATH),
                        "--opportunity-root",
                        str(RUNTIME_ROOT),
                        "--output",
                        str(output_path),
                        "--state-file",
                        str(state_path),
                        "--provider-name",
                        "offline-review",
                        "--provider-model",
                        "fixture-v1",
                        "--prompt-version",
                        "prompt-1",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.main()

            payload = json.loads(output_path.read_text())
            rendered = json.loads(stdout.getvalue())
            markdown_exists = output_path.with_suffix(".md").exists()

        self.assertEqual(result, 0)
        self.assertEqual(payload["contract_count"], 2)
        self.assertEqual(payload["preview_order_proposal_count"], 1)
        self.assertEqual(payload["blocked_preview_order_count"], 1)
        self.assertTrue(markdown_exists)
        self.assertEqual(rendered["contract_count"], 2)

    def test_show_llm_advisory_markdown_can_filter_single_contract(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "llm_advisory.json"
            _write_fixture_advisory(output_path)
            stdout = io.StringIO()

            with (
                patch(
                    "sys.argv",
                    [
                        "operator_cli.py",
                        "show-llm-advisory",
                        "--llm-advisory-file",
                        str(output_path),
                        "--contract-id",
                        "contract-pm-2",
                        "--format",
                        "markdown",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.main()

        rendered = stdout.getvalue()
        self.assertEqual(result, 0)
        self.assertIn("contract-pm-2", rendered)
        self.assertNotIn("contract-pm-1", rendered)
        self.assertIn("Preview blocked reason: missing mapping", rendered)

    def test_show_llm_advisory_json_preserves_enriched_preview_payloads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "llm_advisory.json"
            _write_fixture_advisory(output_path)
            expected_preview_context = build_preview_runtime_context(RUNTIME_ROOT)
            stdout = io.StringIO()

            with (
                patch(
                    "sys.argv",
                    [
                        "operator_cli.py",
                        "show-llm-advisory",
                        "--llm-advisory-file",
                        str(output_path),
                        "--contract-id",
                        "contract-pm-1",
                        "--format",
                        "json",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        self.assertEqual(payload["preview_order_proposal_count"], 1)
        self.assertEqual(
            payload["preview_order_proposals"][0]["edge_buy_after_costs_bps"],
            expected_preview_context.preview_order_proposals[0][
                "edge_buy_after_costs_bps"
            ],
        )
        self.assertEqual(
            payload["contracts"][0]["preview_context"]["edge_buy_after_costs_bps"],
            expected_preview_context.preview_order_proposals[0][
                "edge_buy_after_costs_bps"
            ],
        )

    def test_status_includes_llm_advisory_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "llm_advisory.json"
            state_path = Path(temp_dir) / "safety-state.json"
            _write_fixture_advisory(output_path)
            stdout = io.StringIO()
            args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue=None,
                symbol=None,
                outcome="unknown",
                llm_advisory_file=str(output_path),
            )

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_status(args)

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        self.assertEqual(payload["llm_advisory_summary"]["contract_count"], 2)
        self.assertEqual(
            payload["llm_advisory_summary"]["ambiguous_contract_count"],
            1,
        )


if __name__ == "__main__":
    unittest.main()

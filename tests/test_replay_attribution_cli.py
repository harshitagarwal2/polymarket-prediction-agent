from __future__ import annotations

import importlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "research"
    / "fixtures"
    / "sports_benchmark_tiny.json"
)


class ReplayAttributionCliTests(unittest.TestCase):
    @staticmethod
    def _module():
        return importlib.import_module("scripts.run_replay_attribution")

    def test_cli_quiet_suppresses_stdout(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle:
            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "run_replay_attribution.py",
                        "--fixture",
                        "sports_benchmark_tiny.json",
                        "--output",
                        output_handle.name,
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                self._module().main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(payload["case_name"], "sports-benchmark-tiny")

    def test_cli_runs_packaged_fixture_and_writes_attribution_report(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle:
            with patch(
                "sys.argv",
                [
                    "run_replay_attribution.py",
                    "--fixture",
                    "sports_benchmark_tiny.json",
                    "--output",
                    output_handle.name,
                ],
            ):
                self._module().main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(
            set(payload),
            {"case_name", "description", "trade_attributions", "attribution_summary"},
        )
        self.assertEqual(payload["case_name"], "sports-benchmark-tiny")
        self.assertEqual(payload["attribution_summary"]["trade_count"], 1)
        self.assertEqual(len(payload["trade_attributions"]), 1)
        self.assertIn("slippage_bps", payload["trade_attributions"][0])

    def test_cli_rejects_case_without_replay_report(self):
        fair_value_only_payload = {
            "name": "fair-only",
            "fair_value_case": json.loads(FIXTURE_PATH.read_text())["fair_value_case"],
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as case_handle:
            json.dump(fair_value_only_payload, case_handle)
            case_handle.flush()

            with patch(
                "sys.argv",
                [
                    "run_replay_attribution.py",
                    "--case",
                    case_handle.name,
                ],
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "benchmark case did not produce replay attribution",
                ):
                    self._module().main()

    def test_cli_can_materialize_replay_execution_label_dataset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "runtime-data"
            stdout = io.StringIO()

            with (
                patch(
                    "sys.argv",
                    [
                        "run_replay_attribution.py",
                        "--fixture",
                        "sports_benchmark_tiny.json",
                        "--dataset-root",
                        str(runtime_root),
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                self._module().main()

            payload = json.loads(stdout.getvalue())
            label_output = (
                runtime_root
                / "processed"
                / "replay"
                / "replay_execution_label_dataset.jsonl"
            )
            rows = [
                json.loads(line)
                for line in label_output.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

            self.assertIn("replay_execution_label_dataset", payload)
            self.assertEqual(payload["replay_execution_label_dataset"]["row_count"], 1)
            self.assertTrue(label_output.exists())
            self.assertEqual(rows[0]["market_id"], "token-home:yes")
            self.assertIn("slippage_bps", rows[0])
            self.assertIn("decision_fair_value", rows[0])
            self.assertIn("decision_reference_price", rows[0])
            self.assertIn("decision_best_bid", rows[0])
            self.assertIn("decision_best_ask", rows[0])
            self.assertIn("decision_midpoint", rows[0])
            self.assertEqual(rows[0]["replay_step_index"], 0)
            self.assertIn("cancel_requested_step", rows[0])
            self.assertIn("cancel_effective_step", rows[0])
            self.assertIn("cancel_race_fill", rows[0])


if __name__ == "__main__":
    unittest.main()

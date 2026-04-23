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


if __name__ == "__main__":
    unittest.main()

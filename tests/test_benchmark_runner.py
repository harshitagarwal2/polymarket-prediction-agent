from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research.benchmark_runner import (
    load_and_run_benchmark_case,
    write_benchmark_report,
)


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "research"
    / "fixtures"
    / "sports_benchmark_tiny.json"
)


class BenchmarkRunnerTests(unittest.TestCase):
    def test_load_and_run_benchmark_case(self):
        report = load_and_run_benchmark_case(FIXTURE_PATH)

        self.assertEqual(report.case_name, "sports-benchmark-tiny")
        self.assertIsNotNone(report.fair_value_report)
        self.assertIsNotNone(report.replay_report)
        if report.fair_value_report is None or report.replay_report is None:
            self.fail("expected both fair-value and replay reports")
        self.assertEqual(report.fair_value_report.resolved_row_count, 2)
        self.assertEqual(
            report.fair_value_report.resolved_market_keys,
            ("token-home:no", "token-home:yes"),
        )
        self.assertIsNotNone(report.fair_value_report.forecast_score)
        if report.fair_value_report.forecast_score is None:
            self.fail("expected forecast score for labeled fixture")
        self.assertAlmostEqual(
            report.fair_value_report.forecast_score.brier_score,
            0.180625,
        )
        self.assertEqual(report.replay_report.score.trade_count, 1)
        self.assertAlmostEqual(report.replay_report.score.net_pnl, 0.5)

    def test_write_benchmark_report_emits_json(self):
        report = load_and_run_benchmark_case(FIXTURE_PATH)

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            write_benchmark_report(report, handle.name)
            handle.seek(0)
            payload = handle.read()

        self.assertIn('"case_name": "sports-benchmark-tiny"', payload)
        self.assertIn('"resolved_row_count": 2', payload)

    def test_replay_report_payload_includes_baselines(self):
        replay_only_fixture = (
            Path(__file__).resolve().parents[1]
            / "research"
            / "fixtures"
            / "sports_benchmark_round_trip.json"
        )
        report = load_and_run_benchmark_case(replay_only_fixture)

        self.assertIsNotNone(report.replay_report)
        if report.replay_report is None:
            self.fail("expected replay report")
        payload = report.replay_report.to_payload()
        self.assertIn("baselines", payload)
        baselines = payload["baselines"]
        self.assertIsInstance(baselines, list)
        if not isinstance(baselines, list) or not baselines:
            self.fail("expected serialized replay baselines")
        self.assertEqual(baselines[0]["name"], "noop_strategy")

    def test_missing_expected_market_keys_fail_closed(self):
        payload = json.loads(FIXTURE_PATH.read_text())
        payload["fair_value_case"]["expected_market_keys"].append("token-missing:yes")

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                r"benchmark fair-value case missing expected market keys: token-missing:yes",
            ):
                load_and_run_benchmark_case(handle.name)

    def test_missing_labeled_market_keys_fail_closed(self):
        payload = json.loads(FIXTURE_PATH.read_text())
        payload["fair_value_case"]["outcome_labels"]["token-missing:yes"] = 1

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                r"benchmark fair-value case missing labeled market keys: token-missing:yes",
            ):
                load_and_run_benchmark_case(handle.name)


if __name__ == "__main__":
    unittest.main()

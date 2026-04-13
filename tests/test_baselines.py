from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research.benchmark_runner import (
    load_and_run_benchmark_case,
    run_fair_value_benchmark,
)
from research.schemas import FairValueBenchmarkCase


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"


class BaselineTests(unittest.TestCase):
    def test_fair_value_case_exposes_baseline_scores(self):
        report = load_and_run_benchmark_case(
            FIXTURES_DIR / "sports_benchmark_best_line.json"
        )

        self.assertIsNotNone(report.fair_value_report)
        if report.fair_value_report is None:
            self.fail("expected fair-value report")
        baselines = {
            baseline.name: baseline for baseline in report.fair_value_report.baselines
        }
        self.assertIn("market_midpoint", baselines)
        self.assertIsNotNone(baselines["market_midpoint"].forecast_score)
        self.assertIn("bookmaker_multiplicative_best_line", baselines)

    def test_fair_value_case_exposes_model_and_blended_baselines(self):
        case = FairValueBenchmarkCase.from_payload(
            {
                "rows": [
                    {
                        "market_key": "token-home:yes",
                        "bookmaker": "book-a",
                        "outcome": "yes",
                        "captured_at": "2026-04-07T12:00:00Z",
                        "decimal_odds": 1.7,
                        "condition_id": "condition-1",
                        "event_key": "event-1",
                    },
                    {
                        "market_key": "token-home:no",
                        "bookmaker": "book-a",
                        "outcome": "no",
                        "captured_at": "2026-04-07T12:00:00Z",
                        "decimal_odds": 2.3,
                        "condition_id": "condition-1",
                        "event_key": "event-1",
                    },
                ],
                "outcome_labels": {"token-home:yes": 1, "token-home:no": 0},
                "model_fair_values": {"token-home:yes": 0.65, "token-home:no": 0.35},
                "model_blend_weight": 0.5,
            }
        )

        report = run_fair_value_benchmark(case)

        baselines = {baseline.name: baseline for baseline in report.baselines}
        self.assertIn("model_fair_value", baselines)
        self.assertIn("blended_fair_value", baselines)
        self.assertIsNotNone(baselines["model_fair_value"].forecast_score)
        self.assertIsNone(baselines["model_fair_value"].skipped_reason)
        self.assertIsNotNone(baselines["model_fair_value"].prediction_map)
        self.assertIsNotNone(baselines["blended_fair_value"].forecast_score)
        self.assertIsNone(baselines["blended_fair_value"].skipped_reason)
        self.assertIsNotNone(baselines["blended_fair_value"].prediction_map)

    def test_fair_value_baseline_payloads_include_prediction_maps_when_available(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())[
            "fair_value_case"
        ]
        payload["model_fair_values"] = {
            "token-home:yes": 0.65,
            "token-home:no": 0.35,
        }
        payload["model_blend_weight"] = 0.25
        for market in payload["markets"]:
            market["midpoint"] = 0.6 if market["contract"]["outcome"] == "yes" else 0.4

        report = run_fair_value_benchmark(FairValueBenchmarkCase.from_payload(payload))

        baselines_payload = report.to_payload().get("baselines")
        self.assertIsInstance(baselines_payload, list)
        if not isinstance(baselines_payload, list):
            self.fail("expected serialized fair-value baselines")
        baseline_payloads = {
            str(baseline["name"]): baseline
            for baseline in baselines_payload
            if isinstance(baseline, dict)
        }
        expected_market_keys = {"token-home:yes", "token-home:no"}
        for name in (
            "bookmaker_multiplicative_independent",
            "bookmaker_power_independent",
            "bookmaker_multiplicative_best_line",
            "market_midpoint",
            "model_fair_value",
            "blended_fair_value",
        ):
            self.assertIn("prediction_map", baseline_payloads[name])
            self.assertEqual(
                set(baseline_payloads[name]["prediction_map"]),
                expected_market_keys,
            )

        self.assertEqual(
            baseline_payloads["market_midpoint"]["prediction_map"],
            {"token-home:yes": 0.6, "token-home:no": 0.4},
        )

    def test_model_and_blended_baselines_skip_when_model_coverage_is_incomplete(self):
        case = FairValueBenchmarkCase.from_payload(
            {
                "rows": [
                    {
                        "market_key": "token-home:yes",
                        "bookmaker": "book-a",
                        "outcome": "yes",
                        "captured_at": "2026-04-07T12:00:00Z",
                        "decimal_odds": 1.7,
                        "condition_id": "condition-1",
                        "event_key": "event-1",
                    },
                    {
                        "market_key": "token-home:no",
                        "bookmaker": "book-a",
                        "outcome": "no",
                        "captured_at": "2026-04-07T12:00:00Z",
                        "decimal_odds": 2.3,
                        "condition_id": "condition-1",
                        "event_key": "event-1",
                    },
                ],
                "outcome_labels": {"token-home:yes": 1, "token-home:no": 0},
                "model_fair_values": {"token-home:yes": 0.65},
                "model_blend_weight": 0.5,
            }
        )

        report = run_fair_value_benchmark(case)

        baselines = {baseline.name: baseline for baseline in report.baselines}
        self.assertIsNone(baselines["model_fair_value"].forecast_score)
        self.assertEqual(
            baselines["model_fair_value"].skipped_reason,
            "missing labeled model fair values: token-home:no",
        )
        self.assertIsNone(baselines["model_fair_value"].prediction_map)
        self.assertIsNone(baselines["blended_fair_value"].forecast_score)
        self.assertEqual(
            baselines["blended_fair_value"].skipped_reason,
            "missing labeled model fair values: token-home:no",
        )
        self.assertIsNone(baselines["blended_fair_value"].prediction_map)

        baselines_payload = report.to_payload().get("baselines")
        self.assertIsInstance(baselines_payload, list)
        if not isinstance(baselines_payload, list):
            self.fail("expected serialized fair-value baselines")
        baseline_payloads = {
            str(baseline["name"]): baseline
            for baseline in baselines_payload
            if isinstance(baseline, dict)
        }
        self.assertNotIn("prediction_map", baseline_payloads["model_fair_value"])
        self.assertNotIn("prediction_map", baseline_payloads["blended_fair_value"])

    def test_blended_baseline_skips_without_blend_weight(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        payload["fair_value_case"]["model_fair_values"] = {
            "token-home:yes": 0.65,
            "token-home:no": 0.35,
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()
            report = load_and_run_benchmark_case(handle.name)

        self.assertIsNotNone(report.fair_value_report)
        if report.fair_value_report is None:
            self.fail("expected fair-value report")
        baselines = {
            baseline.name: baseline for baseline in report.fair_value_report.baselines
        }
        self.assertIsNotNone(baselines["model_fair_value"].forecast_score)
        self.assertIsNone(baselines["blended_fair_value"].forecast_score)
        self.assertEqual(
            baselines["blended_fair_value"].skipped_reason,
            "case does not define model blend weight",
        )

    def test_replay_case_exposes_noop_baseline(self):
        report = load_and_run_benchmark_case(
            FIXTURES_DIR / "sports_benchmark_round_trip.json"
        )

        self.assertIsNotNone(report.replay_report)
        if report.replay_report is None:
            self.fail("expected replay report")
        baselines = {
            baseline.name: baseline for baseline in report.replay_report.baselines
        }
        self.assertIn("noop_strategy", baselines)
        self.assertIsNotNone(baselines["noop_strategy"].score)
        if baselines["noop_strategy"].score is None:
            self.fail("expected noop baseline score")
        self.assertAlmostEqual(baselines["noop_strategy"].score.net_pnl, 0.0)
        self.assertGreater(report.replay_report.score.net_pnl, 0.0)


if __name__ == "__main__":
    unittest.main()

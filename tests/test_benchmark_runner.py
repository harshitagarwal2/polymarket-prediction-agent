from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research.benchmark_runner import run_replay_benchmark
from research.benchmark_runner import (
    load_and_run_benchmark_case,
    write_benchmark_report,
)
from research.schemas import (
    FairValueBenchmarkCase,
    ReplayBenchmarkCase,
    ReplayRiskConfig,
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
        metadata = report.fair_value_report.manifest.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata in benchmark report")
        self.assertEqual(metadata["provenance"]["devig_method"], "multiplicative")
        self.assertEqual(metadata["provenance"]["book_aggregation"], "independent")
        self.assertEqual(metadata["coverage"]["input_row_count"], 2)
        self.assertEqual(metadata["coverage"]["value_count"], 2)
        self.assertEqual(metadata["coverage"]["skipped_group_count"], 0)
        self.assertEqual(
            metadata["match_quality"]["match_strategy_counts"],
            {"market_snapshot": 2},
        )
        self.assertIsNotNone(report.fair_value_report.forecast_score)
        if report.fair_value_report.forecast_score is None:
            self.fail("expected forecast score for labeled fixture")
        self.assertAlmostEqual(
            report.fair_value_report.forecast_score.brier_score,
            0.180625,
        )
        self.assertEqual(len(report.fair_value_report.evaluation_rows), 2)
        first_row = report.fair_value_report.evaluation_rows[0]
        self.assertEqual(first_row.market_key, "token-home:no")
        self.assertEqual(first_row.outcome_label, 0)
        self.assertAlmostEqual(first_row.fair_value, 0.425)
        self.assertAlmostEqual(first_row.brier_error, 0.180625)
        self.assertTrue(first_row.correct)
        self.assertIsNone(report.fair_value_report.calibrated_forecast_score)
        self.assertIsNone(report.fair_value_report.calibration)
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
        self.assertIn('"evaluation_rows": [', payload)

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

    def test_fair_value_benchmark_reports_calibrated_scores_when_samples_provided(self):
        payload = json.loads(FIXTURE_PATH.read_text())
        payload["fair_value_case"]["calibration_samples"] = [
            {"prediction": 0.42, "outcome": 0},
            {"prediction": 0.45, "outcome": 0},
            {"prediction": 0.55, "outcome": 1},
            {"prediction": 0.58, "outcome": 1},
        ]
        payload["fair_value_case"]["calibration_bin_count"] = 2

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            report = load_and_run_benchmark_case(handle.name)

        fair_value_report = report.fair_value_report
        self.assertIsNotNone(fair_value_report)
        if fair_value_report is None:
            self.fail("expected fair-value report")
        self.assertIsNotNone(fair_value_report.forecast_score)
        self.assertIsNotNone(fair_value_report.calibrated_forecast_score)
        self.assertIsNotNone(fair_value_report.calibration)
        if (
            fair_value_report.forecast_score is None
            or fair_value_report.calibrated_forecast_score is None
            or fair_value_report.calibration is None
        ):
            self.fail("expected raw and calibrated forecast reporting")
        metric_delta = fair_value_report.calibration.get("metric_delta")
        if not isinstance(metric_delta, dict):
            self.fail("expected calibration metric deltas")

        self.assertLess(
            fair_value_report.calibrated_forecast_score.brier_score,
            fair_value_report.forecast_score.brier_score,
        )
        self.assertLess(
            fair_value_report.calibrated_forecast_score.log_loss,
            fair_value_report.forecast_score.log_loss,
        )
        self.assertAlmostEqual(
            fair_value_report.calibrated_forecast_score.expected_calibration_error,
            fair_value_report.forecast_score.expected_calibration_error,
        )
        self.assertEqual(fair_value_report.calibration["sample_count"], 4)
        self.assertEqual(
            fair_value_report.calibration["calibrated_market_probabilities"],
            {"token-home:no": 0.0, "token-home:yes": 1.0},
        )
        self.assertEqual(len(fair_value_report.evaluation_rows), 2)
        evaluation_rows = {
            row.market_key: row for row in fair_value_report.evaluation_rows
        }
        self.assertEqual(
            evaluation_rows["token-home:yes"].calibrated_fair_value,
            1.0,
        )
        self.assertEqual(
            evaluation_rows["token-home:no"].calibrated_fair_value,
            0.0,
        )
        self.assertAlmostEqual(
            float(metric_delta["brier_improvement"]),
            0.180625,
        )
        self.assertAlmostEqual(
            float(metric_delta["expected_calibration_error_improvement"]),
            0.0,
        )

    def test_run_replay_benchmark_applies_case_risk_limits(self):
        payload = json.loads(FIXTURE_PATH.read_text())
        replay_case = ReplayBenchmarkCase.from_payload(payload["replay_case"])
        constrained_case = ReplayBenchmarkCase(
            steps=replay_case.steps,
            strategy=replay_case.strategy,
            broker=replay_case.broker,
            risk_limits=ReplayRiskConfig(
                max_global_contracts=10,
                max_contracts_per_market=10,
                max_order_notional=2.0,
            ),
        )

        report = run_replay_benchmark(constrained_case)

        self.assertEqual(report.score.trade_count, 0)
        self.assertEqual(
            report.replay_result.events[0].rejected, ["order notional exceeds cap"]
        )
        self.assertEqual(report.ending_positions, {})

    def test_run_replay_benchmark_uses_global_exposure_across_contract_steps(self):
        case = ReplayBenchmarkCase.from_payload(
            {
                "strategy": {
                    "quantity": 1,
                    "edge_threshold": 0.03,
                    "aggressive": True,
                },
                "risk_limits": {
                    "max_global_contracts": 1,
                    "max_contracts_per_market": 10,
                },
                "broker": {"cash": 100.0},
                "steps": [
                    {
                        "book": {
                            "contract": {
                                "venue": "polymarket",
                                "symbol": "token-a",
                                "outcome": "yes",
                                "title": None,
                            },
                            "bids": [{"price": 0.45, "quantity": 10}],
                            "asks": [{"price": 0.50, "quantity": 10}],
                            "midpoint": 0.475,
                            "last_price": None,
                            "observed_at": "2026-04-07T12:00:00+00:00",
                            "raw": None,
                        },
                        "fair_value": 0.60,
                        "metadata": {},
                    },
                    {
                        "book": {
                            "contract": {
                                "venue": "polymarket",
                                "symbol": "token-b",
                                "outcome": "yes",
                                "title": None,
                            },
                            "bids": [{"price": 0.45, "quantity": 10}],
                            "asks": [{"price": 0.50, "quantity": 10}],
                            "midpoint": 0.475,
                            "last_price": None,
                            "observed_at": "2026-04-07T12:05:00+00:00",
                            "raw": None,
                        },
                        "fair_value": 0.60,
                        "metadata": {},
                    },
                ],
            }
        )

        report = run_replay_benchmark(case)

        self.assertEqual(report.score.trade_count, 1)
        self.assertEqual(report.replay_result.events[1].approved, [])
        self.assertEqual(
            report.replay_result.events[1].rejected,
            ["global exposure cap exceeded"],
        )
        self.assertEqual(report.ending_positions, {"token-a:yes": 1.0})

    def test_run_replay_benchmark_applies_daily_loss_state_from_case_risk_limits(self):
        payload = json.loads(FIXTURE_PATH.read_text())
        replay_case = ReplayBenchmarkCase.from_payload(payload["replay_case"])
        constrained_case = ReplayBenchmarkCase(
            steps=replay_case.steps,
            strategy=replay_case.strategy,
            broker=replay_case.broker,
            risk_limits=ReplayRiskConfig(
                max_global_contracts=10,
                max_contracts_per_market=10,
                max_daily_loss=5.0,
                daily_realized_pnl=-5.0,
            ),
        )

        report = run_replay_benchmark(constrained_case)

        self.assertEqual(report.score.trade_count, 0)
        self.assertEqual(
            report.replay_result.events[0].rejected, ["daily loss limit reached"]
        )
        self.assertEqual(report.ending_positions, {})

    def test_load_and_run_benchmark_case_reports_optional_calibration(self):
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
                "calibration_samples": [
                    {"prediction": 0.55, "outcome": 1},
                    {"prediction": 0.58, "outcome": 1},
                    {"prediction": 0.42, "outcome": 0},
                    {"prediction": 0.45, "outcome": 0},
                ],
                "calibration_bin_count": 2,
            }
        )

        report = load_and_run_benchmark_case(
            _write_case_to_temp(
                {"name": "calibrated-case", "fair_value_case": case.to_payload()}
            )
        )

        self.assertIsNotNone(report.fair_value_report)
        if report.fair_value_report is None:
            self.fail("expected fair-value report")
        self.assertIsNotNone(report.fair_value_report.calibrated_forecast_score)
        self.assertIsNotNone(report.fair_value_report.calibration)
        calibration = report.fair_value_report.calibration
        if calibration is None:
            self.fail("expected calibration payload")
        self.assertEqual(calibration["sample_count"], 4)


def _write_case_to_temp(payload: dict[str, object]) -> str:
    handle = tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False)
    try:
        json.dump(payload, handle)
        handle.flush()
        return handle.name
    finally:
        handle.close()


if __name__ == "__main__":
    unittest.main()

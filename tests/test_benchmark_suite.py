from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path

from research.calibration import load_calibration_artifact
from research.benchmark_suite import (
    run_benchmark_suite,
    run_walk_forward_benchmark_suite,
    write_suite_report,
    write_walk_forward_suite_report,
)
from research.datasets import DatasetRegistry
from research.scoring import score_binary_forecasts
from research.schemas import SportsBenchmarkCase


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"
CASE_PATHS = [
    FIXTURES_DIR / "sports_benchmark_tiny.json",
    FIXTURES_DIR / "sports_benchmark_best_line.json",
    FIXTURES_DIR / "sports_benchmark_round_trip.json",
]


class BenchmarkSuiteTests(unittest.TestCase):
    def test_run_benchmark_suite_aggregates_cases(self):
        report = run_benchmark_suite(CASE_PATHS)

        self.assertEqual(report.aggregate.total_cases, 3)
        self.assertEqual(report.aggregate.successful_cases, 3)
        self.assertEqual(report.aggregate.failed_cases, 0)
        self.assertEqual(report.aggregate.fair_value_case_count, 2)
        self.assertEqual(report.aggregate.replay_case_count, 2)
        self.assertIsNotNone(report.aggregate.average_brier_score)
        self.assertIsNotNone(report.aggregate.average_replay_net_pnl)
        self.assertEqual(report.aggregate.edge_ledger_row_count, 4)
        self.assertEqual(
            report.aggregate.attribution_ledger_row_count,
            len(report.attribution_ledger.rows),
        )
        self.assertIsNotNone(report.aggregate.average_realized_edge_bps)
        self.assertIsNotNone(report.aggregate.average_value_capture_bps)
        self.assertGreaterEqual(report.aggregate.replay_resting_trade_count, 0)
        self.assertEqual(len(report.edge_ledger.rows), 4)
        self.assertIn(
            "bookmaker_multiplicative_independent",
            report.aggregate.fair_value_comparison_stats,
        )
        self.assertIn("noop_strategy", report.aggregate.replay_baseline_deltas)

    def test_run_benchmark_suite_aggregates_calibrated_metrics(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
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

            report = run_benchmark_suite([handle.name])

        self.assertEqual(report.aggregate.calibrated_case_count, 1)
        calibrated_brier = report.aggregate.average_calibrated_brier_score
        brier_improvement = report.aggregate.average_calibrated_brier_improvement
        ece_improvement = (
            report.aggregate.average_calibrated_expected_calibration_error_improvement
        )
        if (
            calibrated_brier is None
            or brier_improvement is None
            or ece_improvement is None
        ):
            self.fail("expected calibrated aggregate metrics")
        self.assertAlmostEqual(calibrated_brier, 0.0)
        self.assertAlmostEqual(brier_improvement, 0.180625)
        self.assertAlmostEqual(ece_improvement, 0.0)

    def test_run_benchmark_suite_aggregates_model_and_blended_baseline_deltas(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        payload["fair_value_case"]["model_fair_values"] = {
            "token-home:yes": 0.65,
            "token-home:no": 0.35,
        }
        payload["fair_value_case"]["model_blend_weight"] = 0.25

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            report = run_benchmark_suite([handle.name])

        fair_value_baseline_deltas = report.aggregate.fair_value_baseline_deltas
        self.assertIn("model_fair_value", fair_value_baseline_deltas)
        self.assertIn("blended_fair_value", fair_value_baseline_deltas)
        self.assertEqual(
            fair_value_baseline_deltas["model_fair_value"]["case_count"], 1
        )
        self.assertEqual(
            fair_value_baseline_deltas["blended_fair_value"]["case_count"],
            1,
        )

    def test_run_benchmark_suite_serializes_fair_value_comparison_stats(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        payload["fair_value_case"]["calibration_samples"] = [
            {"prediction": 0.42, "outcome": 0},
            {"prediction": 0.45, "outcome": 0},
            {"prediction": 0.55, "outcome": 1},
            {"prediction": 0.58, "outcome": 1},
        ]
        payload["fair_value_case"]["calibration_bin_count"] = 2
        payload["fair_value_case"]["model_fair_values"] = {
            "token-home:yes": 0.65,
            "token-home:no": 0.35,
        }
        payload["fair_value_case"]["model_blend_weight"] = 0.25
        for market in payload["fair_value_case"]["markets"]:
            market["midpoint"] = 0.6 if market["contract"]["outcome"] == "yes" else 0.4

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            report = run_benchmark_suite([handle.name])

        comparison_stats = report.aggregate.fair_value_comparison_stats
        self.assertEqual(
            set(comparison_stats),
            {
                "bookmaker_multiplicative_best_line",
                "bookmaker_multiplicative_independent",
                "bookmaker_power_independent",
                "calibrated_fair_value",
                "market_midpoint",
                "model_fair_value",
                "blended_fair_value",
            },
        )
        model_stats = comparison_stats["model_fair_value"]
        calibrated_stats = comparison_stats["calibrated_fair_value"]
        bookmaker_stats = comparison_stats["bookmaker_multiplicative_independent"]
        midpoint_stats = comparison_stats["market_midpoint"]
        self.assertEqual(model_stats["case_count"], 1)
        self.assertEqual(model_stats["row_count"], 2)
        self.assertEqual(model_stats["source"], "evaluation_rows")
        self.assertEqual(bookmaker_stats["source"], "baseline_prediction_map")
        self.assertEqual(bookmaker_stats["case_count"], 1)
        self.assertEqual(bookmaker_stats["row_count"], 2)
        self.assertEqual(midpoint_stats["source"], "baseline_prediction_map")
        self.assertEqual(midpoint_stats["case_count"], 1)
        self.assertEqual(midpoint_stats["row_count"], 2)
        self.assertEqual(
            model_stats["loss_differential_direction"],
            "primary_metric_minus_comparison_metric",
        )
        model_metrics = model_stats["metrics"]
        calibrated_metrics = calibrated_stats["metrics"]
        bookmaker_metrics = bookmaker_stats["metrics"]
        midpoint_metrics = midpoint_stats["metrics"]
        self.assertIsInstance(model_metrics, dict)
        self.assertIsInstance(calibrated_metrics, dict)
        self.assertIsInstance(bookmaker_metrics, dict)
        self.assertIsInstance(midpoint_metrics, dict)
        if (
            not isinstance(model_metrics, dict)
            or not isinstance(calibrated_metrics, dict)
            or not isinstance(bookmaker_metrics, dict)
            or not isinstance(midpoint_metrics, dict)
        ):
            self.fail("expected comparison metric payloads")
        model_brier_stats = model_metrics.get("brier_error")
        model_log_loss_stats = model_metrics.get("log_loss")
        calibrated_brier_stats = calibrated_metrics.get("brier_error")
        bookmaker_brier_stats = bookmaker_metrics.get("brier_error")
        midpoint_brier_stats = midpoint_metrics.get("brier_error")
        self.assertIsInstance(model_brier_stats, dict)
        self.assertIsInstance(model_log_loss_stats, dict)
        self.assertIsInstance(calibrated_brier_stats, dict)
        self.assertIsInstance(bookmaker_brier_stats, dict)
        self.assertIsInstance(midpoint_brier_stats, dict)
        if (
            not isinstance(model_brier_stats, dict)
            or not isinstance(model_log_loss_stats, dict)
            or not isinstance(calibrated_brier_stats, dict)
            or not isinstance(bookmaker_brier_stats, dict)
            or not isinstance(midpoint_brier_stats, dict)
        ):
            self.fail("expected paired comparison metric stats")
        self.assertAlmostEqual(
            model_brier_stats["mean_loss_differential"],
            0.058125,
        )
        self.assertAlmostEqual(
            model_log_loss_stats["mean_loss_differential"],
            math.log(0.65 / 0.575),
        )
        self.assertAlmostEqual(
            calibrated_brier_stats["mean_loss_differential"],
            0.180625,
        )
        self.assertAlmostEqual(
            bookmaker_brier_stats["mean_loss_differential"],
            0.0,
        )
        self.assertAlmostEqual(
            midpoint_brier_stats["mean_loss_differential"],
            0.020625,
        )
        self.assertEqual(
            model_brier_stats["comparison_method"],
            "diebold_mariano_style_two_sided_normal_approximation",
        )
        model_brier_interval = model_brier_stats.get(
            "bootstrap_mean_confidence_interval"
        )
        self.assertIsInstance(model_brier_interval, dict)
        if not isinstance(model_brier_interval, dict):
            self.fail("expected bootstrap interval payload")
        self.assertEqual(
            model_brier_interval["interval_method"],
            "percentile_bootstrap",
        )

        aggregate_payload = report.to_payload()["aggregate"]
        self.assertIsInstance(aggregate_payload, dict)
        if not isinstance(aggregate_payload, dict):
            self.fail("expected aggregate payload")
        self.assertIn("fair_value_comparison_stats", aggregate_payload)
        aggregate_comparison_stats = aggregate_payload.get(
            "fair_value_comparison_stats"
        )
        self.assertIsInstance(aggregate_comparison_stats, dict)
        if not isinstance(aggregate_comparison_stats, dict):
            self.fail("expected aggregate comparison stats payload")
        self.assertEqual(
            aggregate_comparison_stats["model_fair_value"],
            model_stats,
        )

    def test_write_suite_report_outputs_summary_and_case_reports(self):
        report = run_benchmark_suite(CASE_PATHS)

        with tempfile.TemporaryDirectory() as output_dir:
            summary_path, markdown_path = write_suite_report(report, output_dir)

            self.assertTrue(summary_path.exists())
            self.assertTrue(markdown_path.exists())
            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_edge_ledger.json").exists()
            )
            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_execution_ledger.json").exists()
            )
            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_attribution_ledger.json").exists()
            )
            self.assertTrue(
                (Path(output_dir) / "cases" / "sports-benchmark-tiny.json").exists()
            )

    def test_suite_edge_ledger_includes_case_context_and_row_metrics(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])

        payload = report.to_payload()["edge_ledger"]
        self.assertIsInstance(payload, dict)
        if not isinstance(payload, dict):
            self.fail("expected suite edge ledger payload")
        self.assertEqual(payload["row_count"], 2)
        rows = payload["rows"]
        self.assertIsInstance(rows, list)
        if not isinstance(rows, list) or not rows:
            self.fail("expected suite edge ledger rows")
        first_row = rows[0]
        self.assertEqual(first_row["case_name"], "sports-benchmark-tiny")
        self.assertEqual(first_row["market_key"], "token-home:no")
        self.assertIn("case_path", first_row)
        self.assertIn("brier_error", first_row)
        self.assertIn("fair_value", first_row)

    def test_write_suite_report_writes_edge_ledger_payload(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])

        with tempfile.TemporaryDirectory() as output_dir:
            write_suite_report(report, output_dir)
            edge_ledger_payload = json.loads(
                (Path(output_dir) / "benchmark_suite_edge_ledger.json").read_text()
            )

        self.assertEqual(edge_ledger_payload["row_count"], 2)
        self.assertEqual(len(edge_ledger_payload["rows"]), 2)
        self.assertEqual(
            edge_ledger_payload["rows"][0]["case_name"], "sports-benchmark-tiny"
        )

    def test_write_suite_report_writes_execution_ledger_payload(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])

        with tempfile.TemporaryDirectory() as output_dir:
            write_suite_report(report, output_dir)
            execution_ledger_payload = json.loads(
                (Path(output_dir) / "benchmark_suite_execution_ledger.json").read_text()
            )

        self.assertEqual(execution_ledger_payload["row_count"], 1)
        self.assertEqual(len(execution_ledger_payload["rows"]), 1)
        self.assertEqual(
            execution_ledger_payload["rows"][0]["case_name"],
            "sports-benchmark-tiny",
        )
        self.assertIn("fill_ratio", execution_ledger_payload["rows"][0])

    def test_write_suite_report_writes_attribution_ledger_payload(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])

        with tempfile.TemporaryDirectory() as output_dir:
            write_suite_report(report, output_dir)
            attribution_ledger_payload = json.loads(
                (
                    Path(output_dir) / "benchmark_suite_attribution_ledger.json"
                ).read_text()
            )

        self.assertEqual(attribution_ledger_payload["row_count"], 1)
        self.assertEqual(len(attribution_ledger_payload["rows"]), 1)
        self.assertEqual(
            attribution_ledger_payload["rows"][0]["case_name"],
            "sports-benchmark-tiny",
        )
        self.assertIn("slippage_bps", attribution_ledger_payload["rows"][0])
        self.assertIn("expected_edge_bps", attribution_ledger_payload["rows"][0])

    def test_suite_attribution_ledger_includes_case_context_and_trade_metrics(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])

        payload = report.to_payload()["attribution_ledger"]
        self.assertIsInstance(payload, dict)
        if not isinstance(payload, dict):
            self.fail("expected suite attribution ledger payload")
        self.assertEqual(payload["row_count"], 1)
        rows = payload["rows"]
        self.assertIsInstance(rows, list)
        if not isinstance(rows, list) or not rows:
            self.fail("expected suite attribution ledger rows")
        first_row = rows[0]
        self.assertEqual(first_row["case_name"], "sports-benchmark-tiny")
        self.assertEqual(first_row["market_id"], "token-home:yes")
        self.assertIn("case_path", first_row)
        self.assertIn("realized_edge_bps", first_row)
        self.assertIn("execution_drag_bps", first_row)

    def test_suite_edge_ledger_surfaces_model_and_blended_values_when_present(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        payload["fair_value_case"]["model_fair_values"] = {
            "token-home:yes": 0.65,
            "token-home:no": 0.35,
        }
        payload["fair_value_case"]["model_blend_weight"] = 0.25

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()
            report = run_benchmark_suite([handle.name])

        edge_ledger_payload = report.edge_ledger.to_payload()
        rows = edge_ledger_payload["rows"]
        self.assertIsInstance(rows, list)
        if not isinstance(rows, list) or not rows:
            self.fail("expected suite edge ledger rows")
        self.assertIn("model_fair_value", rows[0])
        self.assertIn("blended_fair_value", rows[0])

    def test_suite_edge_ledger_payload_can_seed_calibration_artifact(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])
        payload = report.to_payload()["edge_ledger"]
        self.assertIsInstance(payload, dict)
        if not isinstance(payload, dict):
            self.fail("expected edge ledger payload")

        calibrator = load_calibration_artifact(payload, bin_count=2)

        self.assertEqual(calibrator.sample_count, 2)
        self.assertEqual(calibrator.bin_count, 2)

    def test_write_suite_report_includes_calibration_delta_section(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
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
            report = run_benchmark_suite([handle.name])

        with tempfile.TemporaryDirectory() as output_dir:
            _, markdown_path = write_suite_report(report, output_dir)
            markdown = markdown_path.read_text()

        self.assertIn("## Calibration deltas", markdown)
        self.assertIn("Average calibrated ECE improvement", markdown)

    def test_write_suite_report_includes_replay_execution_sections(self):
        report = run_benchmark_suite([FIXTURES_DIR / "sports_benchmark_tiny.json"])

        with tempfile.TemporaryDirectory() as output_dir:
            _, markdown_path = write_suite_report(report, output_dir)
            markdown = markdown_path.read_text()

        self.assertIn("## Replay execution realism", markdown)
        self.assertIn("## Replay attribution summary", markdown)
        self.assertIn("Average replay fill rate", markdown)
        self.assertIn("Average realized edge (bps)", markdown)
        self.assertIn("Average value capture (bps)", markdown)
        self.assertIn("Resting replay trades", markdown)

    def test_write_suite_report_renders_fair_value_comparison_stats_section(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        payload["fair_value_case"]["calibration_samples"] = [
            {"prediction": 0.42, "outcome": 0},
            {"prediction": 0.45, "outcome": 0},
            {"prediction": 0.55, "outcome": 1},
            {"prediction": 0.58, "outcome": 1},
        ]
        payload["fair_value_case"]["calibration_bin_count"] = 2
        payload["fair_value_case"]["model_fair_values"] = {
            "token-home:yes": 0.65,
            "token-home:no": 0.35,
        }
        payload["fair_value_case"]["model_blend_weight"] = 0.25
        for market in payload["fair_value_case"]["markets"]:
            market["midpoint"] = 0.6 if market["contract"]["outcome"] == "yes" else 0.4

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()
            report = run_benchmark_suite([handle.name])

        with tempfile.TemporaryDirectory() as output_dir:
            _, markdown_path = write_suite_report(report, output_dir)
            markdown = markdown_path.read_text()

        self.assertIn("## Fair-value paired comparison stats", markdown)
        self.assertIn(
            "Percentile bootstrap confidence intervals are computed",
            markdown,
        )
        self.assertIn("calibrated_fair_value", markdown)
        self.assertIn("model_fair_value", markdown)
        self.assertIn("blended_fair_value", markdown)
        self.assertIn("bookmaker_multiplicative_independent", markdown)
        self.assertIn("bookmaker_power_independent", markdown)
        self.assertIn("bookmaker_multiplicative_best_line", markdown)
        self.assertIn("market_midpoint", markdown)

    def test_write_suite_report_omits_fair_value_comparison_stats_without_data(self):
        report = run_benchmark_suite(
            [FIXTURES_DIR / "sports_benchmark_round_trip.json"]
        )

        with tempfile.TemporaryDirectory() as output_dir:
            _, markdown_path = write_suite_report(report, output_dir)
            markdown = markdown_path.read_text()

        self.assertNotIn("## Fair-value paired comparison stats", markdown)

    def test_write_suite_report_sanitizes_case_name_for_artifacts(self):
        malicious_case = FIXTURES_DIR / "malicious_case.json"
        malicious_case.write_text(
            '{"name":"../../escape","replay_case":{"steps":[],"broker":{"cash":100}}}'
        )
        self.addCleanup(lambda: malicious_case.unlink(missing_ok=True))

        report = run_benchmark_suite([malicious_case])

        with tempfile.TemporaryDirectory() as output_dir:
            write_suite_report(report, output_dir)

            self.assertTrue((Path(output_dir) / "cases" / "escape.json").exists())
            self.assertFalse((Path(output_dir).parent / "escape.json").exists())

    def test_write_suite_report_escapes_case_names_in_markdown(self):
        payload = json.loads(
            (FIXTURES_DIR / "sports_benchmark_round_trip.json").read_text()
        )
        payload["name"] = "evil|<script>alert(1)</script>"

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()
            report = run_benchmark_suite([handle.name])

        with tempfile.TemporaryDirectory() as output_dir:
            _, markdown_path = write_suite_report(report, output_dir)
            markdown = markdown_path.read_text()

        self.assertIn("evil\\|&lt;script&gt;alert(1)&lt;/script&gt;", markdown)

    def test_suite_aggregate_tracks_calibrated_metrics_when_present(self):
        case = SportsBenchmarkCase.from_payload(
            {
                "name": "calibrated-suite-case",
                "fair_value_case": {
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
                },
            }
        )

        with tempfile.TemporaryDirectory() as output_dir:
            case_path = Path(output_dir) / "calibrated_case.json"
            case_path.write_text(json.dumps(case.to_payload()))
            report = run_benchmark_suite([case_path])

        self.assertEqual(report.aggregate.calibrated_case_count, 1)
        self.assertIsNotNone(report.aggregate.average_calibrated_brier_score)
        self.assertEqual(report.aggregate.edge_ledger_row_count, 2)

    def test_run_benchmark_suite_prefit_calibration_ignores_case_level_samples(self):
        payload = json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        payload["fair_value_case"]["calibration_samples"] = [
            {"prediction": 0.42, "outcome": 1},
            {"prediction": 0.45, "outcome": 1},
            {"prediction": 0.55, "outcome": 0},
            {"prediction": 0.58, "outcome": 0},
        ]
        payload["fair_value_case"]["calibration_bin_count"] = 2
        calibration_rows = [
            {"fair_value": 0.42, "outcome_label": 0},
            {"fair_value": 0.45, "outcome_label": 0},
            {"fair_value": 0.55, "outcome_label": 1},
            {"fair_value": 0.58, "outcome_label": 1},
        ]
        calibrator = load_calibration_artifact(calibration_rows, bin_count=2)

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()
            report = run_benchmark_suite(
                [handle.name],
                prefit_calibration=calibrator,
                strip_case_calibration_samples=True,
            )

        fair_value_report = report.case_results[0].report.fair_value_report
        self.assertIsNotNone(fair_value_report)
        if fair_value_report is None or fair_value_report.calibration is None:
            self.fail("expected fair-value calibration payload")
        self.assertEqual(fair_value_report.calibration["source"], "prefit")
        self.assertEqual(fair_value_report.calibration["sample_count"], 4)
        self.assertEqual(
            fair_value_report.calibration["calibrated_market_probabilities"],
            {"token-home:no": 0.0, "token-home:yes": 1.0},
        )

    def test_run_benchmark_suite_pools_fair_value_metrics_across_rows(self):
        primary_case = SportsBenchmarkCase.from_payload(
            {
                "name": "pooled-primary-case",
                "fair_value_case": {
                    "rows": [
                        {
                            "market_key": "token-a:yes",
                            "bookmaker": "book-a",
                            "outcome": "yes",
                            "captured_at": "2026-04-07T12:00:00Z",
                            "decimal_odds": 1.0582010582010581,
                            "condition_id": "condition-a",
                            "event_key": "event-a",
                        },
                        {
                            "market_key": "token-a:no",
                            "bookmaker": "book-a",
                            "outcome": "no",
                            "captured_at": "2026-04-07T12:00:00Z",
                            "decimal_odds": 9.523809523809524,
                            "condition_id": "condition-a",
                            "event_key": "event-a",
                        },
                    ],
                    "outcome_labels": {"token-a:yes": 1, "token-a:no": 0},
                },
            }
        )
        secondary_case = SportsBenchmarkCase.from_payload(
            {
                "name": "pooled-secondary-case",
                "fair_value_case": {
                    "rows": [
                        {
                            "market_key": "token-b:yes",
                            "bookmaker": "book-a",
                            "outcome": "yes",
                            "captured_at": "2026-04-07T12:05:00Z",
                            "decimal_odds": 1.5873015873015872,
                            "condition_id": "condition-b",
                            "event_key": "event-b",
                        },
                        {
                            "market_key": "token-b:no",
                            "bookmaker": "book-a",
                            "outcome": "no",
                            "captured_at": "2026-04-07T12:05:00Z",
                            "decimal_odds": 2.380952380952381,
                            "condition_id": "condition-b",
                            "event_key": "event-b",
                        },
                        {
                            "market_key": "token-c:yes",
                            "bookmaker": "book-a",
                            "outcome": "yes",
                            "captured_at": "2026-04-07T12:10:00Z",
                            "decimal_odds": 1.5873015873015872,
                            "condition_id": "condition-c",
                            "event_key": "event-c",
                        },
                        {
                            "market_key": "token-c:no",
                            "bookmaker": "book-a",
                            "outcome": "no",
                            "captured_at": "2026-04-07T12:10:00Z",
                            "decimal_odds": 2.380952380952381,
                            "condition_id": "condition-c",
                            "event_key": "event-c",
                        },
                    ],
                    "outcome_labels": {
                        "token-b:yes": 1,
                        "token-b:no": 0,
                        "token-c:yes": 1,
                        "token-c:no": 0,
                    },
                },
            }
        )

        with tempfile.TemporaryDirectory() as output_dir:
            primary_case_path = Path(output_dir) / "pooled_primary_case.json"
            secondary_case_path = Path(output_dir) / "pooled_secondary_case.json"
            primary_case_path.write_text(json.dumps(primary_case.to_payload()))
            secondary_case_path.write_text(json.dumps(secondary_case.to_payload()))
            report = run_benchmark_suite([primary_case_path, secondary_case_path])

        expected_pooled_score = score_binary_forecasts(
            {
                "token-a:yes": 0.9,
                "token-a:no": 0.1,
                "token-b:yes": 0.6,
                "token-b:no": 0.4,
                "token-c:yes": 0.6,
                "token-c:no": 0.4,
            },
            {
                "token-a:yes": 1,
                "token-a:no": 0,
                "token-b:yes": 1,
                "token-b:no": 0,
                "token-c:yes": 1,
                "token-c:no": 0,
            },
        )
        simple_case_average_brier = (0.01 + 0.16) / 2
        average_brier_score = report.aggregate.average_brier_score
        average_log_loss = report.aggregate.average_log_loss
        average_ece = report.aggregate.average_expected_calibration_error
        self.assertIsNotNone(average_brier_score)
        self.assertIsNotNone(average_log_loss)
        self.assertIsNotNone(average_ece)
        if (
            average_brier_score is None
            or average_log_loss is None
            or average_ece is None
        ):
            self.fail("expected pooled fair-value aggregate metrics")

        self.assertAlmostEqual(
            average_brier_score,
            expected_pooled_score.brier_score,
        )
        self.assertAlmostEqual(
            average_log_loss,
            expected_pooled_score.log_loss,
        )
        self.assertAlmostEqual(
            average_ece,
            expected_pooled_score.expected_calibration_error,
        )
        self.assertNotAlmostEqual(
            average_brier_score,
            simple_case_average_brier,
        )

    def test_walk_forward_suite_writes_split_artifacts_and_root_provenance(self):
        training_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        training_case["recorded_at"] = "2026-04-01T12:00:00Z"
        eval_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case["name"] = "sports-benchmark-walk-forward-eval"
        eval_case["recorded_at"] = "2026-04-02T12:00:00Z"
        eval_case["fair_value_case"]["calibration_samples"] = [
            {"prediction": 0.42, "outcome": 1},
            {"prediction": 0.45, "outcome": 1},
            {"prediction": 0.55, "outcome": 0},
            {"prediction": 0.58, "outcome": 0},
        ]
        eval_case["fair_value_case"]["calibration_bin_count"] = 2

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            output_dir = Path(temp_dir) / "output"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "walk-forward-benchmark-cases",
                [training_case, eval_case],
                version="v1",
                timestamp_field="recorded_at",
            )

            report = run_walk_forward_benchmark_suite(
                dataset_name="walk-forward-benchmark-cases",
                dataset_root=dataset_root,
                version="v1",
                min_train_size=1,
                test_size=1,
                calibration_bin_count=2,
            )
            summary_path = write_walk_forward_suite_report(report, output_dir)
            payload = json.loads(summary_path.read_text())
            split_summary_path = (
                output_dir / payload["splits"][0]["report_artifacts"]["summary_json"]
            )
            split_attribution_ledger_path = (
                output_dir
                / payload["splits"][0]["report_artifacts"]["attribution_ledger_json"]
            )
            pooled_attribution_ledger_path = (
                output_dir / payload["report_artifacts"]["attribution_ledger_json"]
            )
            self.assertTrue(split_summary_path.exists())
            self.assertTrue(split_attribution_ledger_path.exists())
            self.assertTrue(pooled_attribution_ledger_path.exists())
            split_summary = json.loads(split_summary_path.read_text())
            pooled_attribution_ledger = json.loads(
                pooled_attribution_ledger_path.read_text()
            )

        self.assertEqual(
            payload["dataset"]["dataset_name"], "walk-forward-benchmark-cases"
        )
        self.assertEqual(payload["walk_forward"]["split_count"], 1)
        self.assertEqual(payload["aggregate"], split_summary["aggregate"])
        self.assertEqual(
            payload["aggregate"]["attribution_ledger_row_count"],
            pooled_attribution_ledger["row_count"],
        )
        self.assertEqual(
            payload["splits"][0]["split"]["train_record_ids"],
            ["case-000000-sports-benchmark-tiny"],
        )
        self.assertEqual(
            payload["splits"][0]["split"]["test_record_ids"],
            ["case-000001-sports-benchmark-walk-forward-eval"],
        )
        split_case_payload = split_summary["case_results"][0]["report"]["fair_value"]
        self.assertEqual(split_case_payload["calibration"]["source"], "prefit")
        self.assertEqual(split_case_payload["calibration"]["sample_count"], 2)
        self.assertEqual(
            split_case_payload["calibration"]["calibrated_market_probabilities"],
            {"token-home:no": 0.0, "token-home:yes": 1.0},
        )

    def test_walk_forward_suite_generates_elo_model_fair_values(self):
        training_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        training_case["recorded_at"] = "2026-04-01T12:00:00Z"
        eval_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case["name"] = "sports-benchmark-elo-eval"
        eval_case["recorded_at"] = "2026-04-02T12:00:00Z"

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "walk-forward-elo-cases",
                [training_case, eval_case],
                version="v1",
                timestamp_field="recorded_at",
            )

            report = run_walk_forward_benchmark_suite(
                dataset_name="walk-forward-elo-cases",
                dataset_root=dataset_root,
                version="v1",
                min_train_size=1,
                test_size=1,
                step_size=1,
                model_generator="elo",
            )
            payload = report.to_payload()

        self.assertIsInstance(payload, dict)
        if not isinstance(payload, dict):
            self.fail("expected walk-forward payload")
        walk_forward = payload.get("walk_forward")
        splits = payload.get("splits")
        self.assertIsInstance(walk_forward, dict)
        self.assertIsInstance(splits, list)
        if not isinstance(walk_forward, dict) or not isinstance(splits, list):
            self.fail("expected walk-forward payload sections")
        self.assertEqual(walk_forward["model_generator"], "elo")
        split_payload = splits[0]
        self.assertIsInstance(split_payload["model"], dict)
        if not isinstance(split_payload["model"], dict):
            self.fail("expected split-level model payload")
        self.assertEqual(split_payload["model"]["model_generator"], "elo")
        self.assertEqual(split_payload["model"]["training_match_count"], 1)

        split_case_report = report.splits[0].report.case_results[0].report
        self.assertIsNotNone(split_case_report.fair_value_report)
        if split_case_report.fair_value_report is None:
            self.fail("expected fair-value report")
        baselines = {
            baseline.name: baseline
            for baseline in split_case_report.fair_value_report.baselines
        }
        self.assertIsNotNone(baselines["model_fair_value"].forecast_score)
        evaluation_rows = split_case_report.fair_value_report.evaluation_rows
        self.assertGreater(evaluation_rows[1].model_fair_value or 0.0, 0.5)
        self.assertLess(evaluation_rows[0].model_fair_value or 1.0, 0.5)

    def test_walk_forward_suite_pools_out_of_fold_aggregate_across_splits(self):
        training_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        training_case["name"] = "sports-benchmark-train"
        training_case["recorded_at"] = "2026-04-01T12:00:00Z"

        eval_case_one = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case_one["name"] = "sports-benchmark-eval-one"
        eval_case_one["recorded_at"] = "2026-04-02T12:00:00Z"

        eval_case_two = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case_two["name"] = "sports-benchmark-eval-two"
        eval_case_two["recorded_at"] = "2026-04-03T12:00:00Z"

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "walk-forward-aggregate-cases",
                [training_case, eval_case_one, eval_case_two],
                version="v1",
                timestamp_field="recorded_at",
            )

            report = run_walk_forward_benchmark_suite(
                dataset_name="walk-forward-aggregate-cases",
                dataset_root=dataset_root,
                version="v1",
                min_train_size=1,
                test_size=1,
                step_size=1,
            )
            payload = report.to_payload()

        self.assertIsInstance(payload, dict)
        if not isinstance(payload, dict):
            self.fail("expected walk-forward payload")
        walk_forward = payload.get("walk_forward")
        aggregate = payload.get("aggregate")
        self.assertIsInstance(walk_forward, dict)
        self.assertIsInstance(aggregate, dict)
        if not isinstance(walk_forward, dict) or not isinstance(aggregate, dict):
            self.fail("expected walk-forward aggregate payloads")

        self.assertEqual(walk_forward["split_count"], 2)
        self.assertEqual(walk_forward["total_test_cases"], 2)
        self.assertEqual(aggregate["total_cases"], 2)
        self.assertEqual(aggregate["successful_cases"], 2)
        self.assertEqual(aggregate["fair_value_case_count"], 2)
        self.assertEqual(aggregate["edge_ledger_row_count"], 4)
        self.assertAlmostEqual(float(aggregate["average_brier_score"]), 0.180625)


if __name__ == "__main__":
    unittest.main()

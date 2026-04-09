from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research.calibration import load_calibration_artifact
from research.benchmark_suite import run_benchmark_suite, write_suite_report
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
        self.assertEqual(len(report.edge_ledger.rows), 4)
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


if __name__ == "__main__":
    unittest.main()

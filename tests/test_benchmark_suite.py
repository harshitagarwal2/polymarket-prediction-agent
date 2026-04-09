from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research.benchmark_suite import run_benchmark_suite, write_suite_report


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
        self.assertIn("noop_strategy", report.aggregate.replay_baseline_deltas)

    def test_write_suite_report_outputs_summary_and_case_reports(self):
        report = run_benchmark_suite(CASE_PATHS)

        with tempfile.TemporaryDirectory() as output_dir:
            summary_path, markdown_path = write_suite_report(report, output_dir)

            self.assertTrue(summary_path.exists())
            self.assertTrue(markdown_path.exists())
            self.assertTrue(
                (Path(output_dir) / "cases" / "sports-benchmark-tiny.json").exists()
            )

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


if __name__ == "__main__":
    unittest.main()

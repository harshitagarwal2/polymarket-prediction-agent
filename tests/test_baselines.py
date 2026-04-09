from __future__ import annotations

import unittest
from pathlib import Path

from research.benchmark_runner import load_and_run_benchmark_case


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

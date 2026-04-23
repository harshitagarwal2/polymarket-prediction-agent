from __future__ import annotations

import unittest

from research.eval.calibration_eval import evaluate_probability_calibration
from research.eval.closing_value import evaluate_closing_value
from research.eval.execution_metrics import summarize_execution_metrics


class ResearchEvalHelperTests(unittest.TestCase):
    def test_closing_value_for_buy_yes_is_positive_when_market_moves_up(self):
        report = evaluate_closing_value(
            signal_price=0.52,
            closing_price=0.58,
            side="buy_yes",
            fair_value=0.60,
        )

        self.assertGreater(report.closing_edge_bps, 0.0)
        self.assertGreater(report.value_capture_bps, 0.0)

    def test_calibration_eval_computes_expected_calibration_error(self):
        summary = evaluate_probability_calibration(
            [
                {"fair_value": 0.20, "outcome_label": 0},
                {"fair_value": 0.30, "outcome_label": 0},
                {"fair_value": 0.70, "outcome_label": 1},
                {"fair_value": 0.80, "outcome_label": 1},
            ],
            bin_count=2,
        )

        self.assertEqual(summary.bin_count, 2)
        self.assertGreaterEqual(summary.expected_calibration_error, 0.0)
        self.assertLessEqual(summary.expected_calibration_error, 1.0)

    def test_execution_metrics_summarize_fill_rate_and_slippage(self):
        metrics = summarize_execution_metrics(
            [
                {
                    "expected_edge_bps": 120.0,
                    "realized_edge_bps": 80.0,
                    "slippage_bps": 15.0,
                    "filled": True,
                    "stale_data_flag": False,
                },
                {
                    "expected_edge_bps": 90.0,
                    "realized_edge_bps": 0.0,
                    "slippage_bps": 0.0,
                    "filled": False,
                    "stale_data_flag": True,
                },
            ]
        )

        self.assertEqual(metrics.trade_count, 2)
        self.assertEqual(metrics.filled_trade_count, 1)
        self.assertAlmostEqual(metrics.fill_rate, 0.5)
        self.assertAlmostEqual(metrics.average_realized_slippage_bps, 15.0)
        self.assertEqual(metrics.stale_data_count, 1)


if __name__ == "__main__":
    unittest.main()

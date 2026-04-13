from __future__ import annotations

import math
import unittest

from adapters.types import Contract, OrderBookSnapshot, OutcomeSide, PriceLevel, Venue
from engine.strategies import FairValueBandStrategy
from research.paper import PaperBroker
from research.replay import ReplayRunner, ReplayStep
from research.scoring import (
    bootstrap_mean_confidence_interval,
    compare_paired_loss_differentials,
    score_binary_forecasts,
    score_replay_result,
)
from risk.limits import RiskEngine, RiskLimits


class ScoringTests(unittest.TestCase):
    def test_score_binary_forecasts_reports_expected_metrics(self):
        score = score_binary_forecasts(
            {"home": 0.575, "away": 0.425},
            {"home": 1, "away": 0},
            bin_count=2,
        )

        self.assertEqual(score.count, 2)
        self.assertAlmostEqual(score.brier_score, 0.180625)
        self.assertAlmostEqual(score.log_loss, -math.log(0.575))
        self.assertAlmostEqual(score.accuracy, 1.0)
        self.assertEqual(len(score.calibration_bins), 2)

    def test_score_replay_result_reports_summary_metrics(self):
        contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-home",
            outcome=OutcomeSide.YES,
        )
        runner = ReplayRunner(
            strategy=FairValueBandStrategy(quantity=5, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
            broker=PaperBroker(cash=100),
        )
        result = runner.run(
            [
                ReplayStep(
                    book=OrderBookSnapshot(
                        contract=contract,
                        bids=[PriceLevel(price=0.45, quantity=10)],
                        asks=[PriceLevel(price=0.50, quantity=5)],
                        midpoint=0.475,
                    ),
                    fair_value=0.60,
                ),
                ReplayStep(
                    book=OrderBookSnapshot(
                        contract=contract,
                        bids=[PriceLevel(price=0.59, quantity=10)],
                        asks=[PriceLevel(price=0.61, quantity=10)],
                        midpoint=0.60,
                    ),
                    fair_value=0.60,
                ),
            ]
        )

        score = score_replay_result(result)

        self.assertEqual(score.trade_count, 1)
        self.assertEqual(score.filled_trade_count, 1)
        self.assertEqual(score.rejection_count, 0)
        self.assertAlmostEqual(score.net_pnl, 0.5)
        self.assertAlmostEqual(score.return_pct, 0.5)

    def test_score_binary_forecasts_rejects_non_finite_predictions(self):
        with self.assertRaisesRegex(ValueError, "predictions must be finite"):
            score_binary_forecasts({"home": float("nan")}, {"home": 1})

    def test_bootstrap_mean_confidence_interval_is_deterministic(self):
        first = bootstrap_mean_confidence_interval(
            [0.1, 0.2, 0.3, 0.4],
            confidence_level=0.9,
            resample_count=250,
            seed=7,
        )
        second = bootstrap_mean_confidence_interval(
            [0.1, 0.2, 0.3, 0.4],
            confidence_level=0.9,
            resample_count=250,
            seed=7,
        )

        self.assertEqual(first, second)
        self.assertEqual(first.sample_count, 4)
        self.assertAlmostEqual(first.sample_mean, 0.25)
        self.assertEqual(first.interval_method, "percentile_bootstrap")
        self.assertEqual(first.statistic, "mean")
        self.assertEqual(first.resample_count, 250)
        self.assertEqual(first.seed, 7)
        self.assertLessEqual(first.lower_bound, first.sample_mean)
        self.assertGreaterEqual(first.upper_bound, first.sample_mean)

    def test_compare_paired_loss_differentials_reports_deterministic_stats(self):
        loss_differentials = [0.2, -0.1, 0.05, 0.15, -0.05]

        first = compare_paired_loss_differentials(
            loss_differentials,
            confidence_level=0.9,
            bootstrap_resample_count=250,
            seed=11,
        )
        second = compare_paired_loss_differentials(
            loss_differentials,
            confidence_level=0.9,
            bootstrap_resample_count=250,
            seed=11,
        )

        mean_loss_differential = sum(loss_differentials) / len(loss_differentials)
        centered_sum = sum(
            (value - mean_loss_differential) ** 2 for value in loss_differentials
        )
        expected_standard_error = math.sqrt(
            (centered_sum / (len(loss_differentials) - 1)) / len(loss_differentials)
        )
        expected_test_statistic = mean_loss_differential / expected_standard_error
        expected_p_value = math.erfc(abs(expected_test_statistic) / math.sqrt(2.0))

        self.assertEqual(first, second)
        self.assertEqual(first.sample_count, 5)
        self.assertAlmostEqual(first.mean_loss_differential, mean_loss_differential)
        self.assertAlmostEqual(first.standard_error, expected_standard_error)
        self.assertIsNotNone(first.test_statistic)
        self.assertIsNotNone(first.p_value_two_sided)
        if first.test_statistic is None or first.p_value_two_sided is None:
            self.fail("expected finite DM-style comparison stats")
        self.assertAlmostEqual(first.test_statistic, expected_test_statistic)
        self.assertAlmostEqual(first.p_value_two_sided, expected_p_value)
        self.assertEqual(
            first.comparison_method,
            "diebold_mariano_style_two_sided_normal_approximation",
        )
        self.assertEqual(
            first.variance_estimator,
            "sample_variance_of_paired_loss_differentials",
        )
        self.assertEqual(
            first.bootstrap_mean_confidence_interval.interval_method,
            "percentile_bootstrap",
        )
        self.assertEqual(first.bootstrap_mean_confidence_interval.seed, 11)


if __name__ == "__main__":
    unittest.main()

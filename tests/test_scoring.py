from __future__ import annotations

import math
import unittest

from adapters.types import Contract, OrderBookSnapshot, OutcomeSide, PriceLevel, Venue
from engine.strategies import FairValueBandStrategy
from research.paper import PaperBroker
from research.replay import ReplayRunner, ReplayStep
from research.scoring import score_binary_forecasts, score_replay_result
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


if __name__ == "__main__":
    unittest.main()

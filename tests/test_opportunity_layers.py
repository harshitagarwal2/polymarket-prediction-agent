from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from adapters import MarketSummary
from adapters.types import Contract, OrderAction, OutcomeSide, Venue
from forecasting.fair_value_engine import StaticFairValueProvider
from opportunity import (
    OpportunityRanker,
    assess_executable_edge,
    estimate_fillability_from_market,
)


class OpportunityLayerTests(unittest.TestCase):
    def _market(self) -> MarketSummary:
        return MarketSummary(
            contract=Contract(venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES),
            title="Will something happen?",
            best_bid=0.40,
            best_ask=0.45,
            volume=10.0,
            category="politics",
            active=True,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=5),
            raw={"market": {"condition_id": "condition-1"}},
        )

    def test_assess_executable_edge_applies_fee_drag(self):
        result = assess_executable_edge(
            fair_value=0.60,
            quoted_price=0.45,
            action=OrderAction.BUY,
            fee_rate=0.02,
        )
        self.assertGreater(result.edge, 0.0)
        self.assertGreater(result.fee_drag, 0.0)

    def test_estimate_fillability_from_market_uses_visible_volume(self):
        estimate = estimate_fillability_from_market(
            self._market(),
            action=OrderAction.BUY,
            quantity=3.0,
        )
        self.assertEqual(estimate.fillable_quantity, 3.0)
        self.assertEqual(estimate.completion_ratio, 1.0)

    def test_ranker_emits_candidate_for_positive_edge(self):
        market = self._market()
        candidates = OpportunityRanker(edge_threshold=0.03).rank(
            [market],
            StaticFairValueProvider({market.contract.market_key: 0.60}),
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].action, OrderAction.BUY)


if __name__ == "__main__":
    unittest.main()

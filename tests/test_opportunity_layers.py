from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from adapters import MarketSummary
from adapters.types import Contract, OrderAction, OutcomeSide, Venue
from forecasting.fair_value_engine import StaticFairValueProvider
from opportunity import (
    OpportunityRanker,
    assess_executable_edge,
    compute_edge,
    estimate_fillability_from_market,
    opportunity_from_prices,
)
from risk.freeze_windows import FreezeWindowPolicy


class OpportunityLayerTests(unittest.TestCase):
    def _market(self) -> MarketSummary:
        return MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
            ),
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

    def test_compute_edge_uses_executable_bid_ask_not_midpoint(self):
        edge = compute_edge(
            fair_yes_prob=0.60,
            best_bid_yes=0.40,
            best_ask_yes=0.58,
            fee_bps=10.0,
            slippage_bps=10.0,
        )
        self.assertAlmostEqual(edge["edge_buy_raw_bps"], 200.0)
        self.assertAlmostEqual(edge["edge_sell_raw_bps"], -2000.0)
        self.assertAlmostEqual(edge["edge_buy_after_costs_bps"], 184.2)
        self.assertAlmostEqual(edge["edge_sell_after_costs_bps"], -2014.0)
        self.assertAlmostEqual(
            edge["edge_after_costs_bps"], edge["edge_buy_after_costs_bps"]
        )

    def test_opportunity_from_prices_chooses_side_from_after_cost_edge(self):
        opportunity = opportunity_from_prices(
            market_id="pm-1",
            fair_yes_prob=0.5001,
            best_bid_yes=0.49,
            best_ask_yes=0.51,
            fillable_size=5.0,
            confidence=0.98,
            slippage_bps=200.0,
        )

        self.assertEqual(opportunity.side, "sell_yes")
        self.assertGreater(
            opportunity.edge_sell_after_costs_bps,
            opportunity.edge_buy_after_costs_bps,
        )

    def test_opportunity_from_prices_preserves_structured_blocked_reasons(self):
        opportunity = opportunity_from_prices(
            market_id="pm-1",
            fair_yes_prob=0.60,
            best_bid_yes=0.50,
            best_ask_yes=0.52,
            fillable_size=8.0,
            confidence=0.98,
            blocked_reasons=("missing fair value", "missing executable bbo"),
        )

        self.assertEqual(opportunity.blocked_reason, "missing fair value")
        self.assertEqual(
            opportunity.blocked_reasons,
            ("missing fair value", "missing executable bbo"),
        )

    def test_opportunity_from_prices_uses_selected_side_visible_depth(self):
        opportunity = opportunity_from_prices(
            market_id="pm-1",
            fair_yes_prob=0.50,
            best_bid_yes=0.54,
            best_ask_yes=0.60,
            fillable_size=2.0,
            buy_yes_fillable_size=2.0,
            sell_yes_fillable_size=9.0,
            confidence=0.98,
        )

        self.assertEqual(opportunity.side, "sell_yes")
        self.assertEqual(opportunity.fillable_size, 9.0)

    def test_ranker_blocks_market_inside_freeze_window(self):
        market = self._market()
        market.start_time = datetime.now(timezone.utc) + timedelta(minutes=4)
        candidates = OpportunityRanker(
            edge_threshold=0.03,
            freeze_window_policy=FreezeWindowPolicy(freeze_minutes_before_start=5),
        ).rank(
            [market],
            StaticFairValueProvider({market.contract.market_key: 0.60}),
        )
        self.assertEqual(candidates, [])

    def test_ranker_applies_time_lock_penalty_to_long_dated_markets(self):
        near_market = self._market()
        near_market.contract = Contract(
            venue=Venue.POLYMARKET, symbol="near-token", outcome=OutcomeSide.YES
        )
        near_market.expires_at = datetime.now(timezone.utc) + timedelta(hours=4)

        far_market = self._market()
        far_market.contract = Contract(
            venue=Venue.POLYMARKET, symbol="far-token", outcome=OutcomeSide.YES
        )
        far_market.expires_at = datetime.now(timezone.utc) + timedelta(hours=168)

        provider = StaticFairValueProvider(
            {
                near_market.contract.market_key: 0.60,
                far_market.contract.market_key: 0.60,
            }
        )

        candidates = OpportunityRanker(
            edge_threshold=0.03,
            time_lock_penalty_weight=0.05,
            time_lock_penalty_saturation_hours=168.0,
        ).rank([far_market, near_market], provider)

        self.assertEqual(
            [candidate.contract.symbol for candidate in candidates],
            ["near-token", "far-token"],
        )
        self.assertGreater(candidates[0].score, candidates[1].score)
        self.assertIn("time_lock_penalty", candidates[1].rationale)


if __name__ == "__main__":
    unittest.main()

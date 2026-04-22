from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from execution import ExecutionPlanner
from opportunity.models import Opportunity


class ExecutionPlannerTests(unittest.TestCase):
    def _opportunity(self, **overrides) -> Opportunity:
        payload = {
            "market_id": "pm-1",
            "side": "buy_yes",
            "fair_yes_prob": 0.61,
            "best_bid_yes": 0.45,
            "best_ask_yes": 0.47,
            "edge_buy_bps": 1400.0,
            "edge_sell_bps": -1600.0,
            "edge_after_costs_bps": 175.0,
            "fillable_size": 12.0,
            "confidence": 0.98,
            "blocked_reason": None,
        }
        payload.update(overrides)
        return Opportunity(**payload)

    def test_planner_blocks_low_confidence(self):
        proposal = ExecutionPlanner().proposal_for(
            self._opportunity(confidence=0.8),
            source_age_ms=1000,
            book_dispersion=0.01,
        )
        self.assertIsNone(proposal)

    def test_planner_blocks_stale_source(self):
        proposal = ExecutionPlanner().proposal_for(
            self._opportunity(),
            source_age_ms=9000,
            book_dispersion=0.01,
        )
        self.assertIsNone(proposal)

    def test_planner_emits_place_proposal_for_clean_opportunity(self):
        proposal = ExecutionPlanner().proposal_for(
            self._opportunity(),
            source_age_ms=1000,
            book_dispersion=0.01,
        )
        self.assertIsNotNone(proposal)
        assert proposal is not None
        self.assertEqual(proposal.action, "place")
        self.assertEqual(proposal.price, 0.47)

    def test_planner_blocks_within_freeze_window(self):
        proposal = ExecutionPlanner().proposal_for(
            self._opportunity(),
            source_age_ms=1000,
            book_dispersion=0.01,
            event_start_time=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        self.assertIsNone(proposal)

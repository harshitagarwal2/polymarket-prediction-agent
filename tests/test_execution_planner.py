from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from execution import ExecutionPlanner
from opportunity.models import Opportunity
from risk.correlated_exposure import CorrelatedExposureDecision


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

    def test_planner_evaluate_surfaces_source_health_reason(self):
        decision = ExecutionPlanner().evaluate(
            self._opportunity(),
            source_age_ms=1000,
            book_dispersion=0.01,
            source_health={
                "polymarket_market_channel": {
                    "status": "red",
                    "last_success_at": "2026-04-22T00:00:00+00:00",
                    "stale_after_ms": 4000,
                }
            },
            required_sources=("polymarket_market_channel",),
            now=datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc),
        )
        self.assertIsNone(decision.proposal)
        self.assertEqual(decision.blocked_reason, "source polymarket_market_channel unhealthy")

    def test_planner_blocks_pre_expiry_window(self):
        planner = ExecutionPlanner()
        planner.thresholds = planner.thresholds.__class__(
            freeze_minutes_before_expiry=30,
        )
        decision = planner.evaluate(
            self._opportunity(),
            source_age_ms=1000,
            book_dispersion=0.01,
            market_end_time=datetime.now(timezone.utc) + timedelta(minutes=20),
        )
        self.assertIsNone(decision.proposal)
        self.assertEqual(decision.blocked_reason, "market within pre-expiry freeze window")

    def test_planner_blocks_cluster_exposure_decision(self):
        decision = ExecutionPlanner().evaluate(
            self._opportunity(),
            source_age_ms=1000,
            book_dispersion=0.01,
            correlated_exposure=CorrelatedExposureDecision(
                allowed=False,
                cluster_key="event:event-1",
                current_cluster_exposure=2.0,
                projected_cluster_exposure=3.0,
                max_cluster_exposure=2.0,
                reason="cluster exposure cap exceeded",
            ),
        )
        self.assertIsNone(decision.proposal)
        self.assertEqual(decision.blocked_reason, "cluster exposure cap exceeded")

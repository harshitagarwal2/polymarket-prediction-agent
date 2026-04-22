from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from adapters import MarketSummary
from adapters.types import Contract, OutcomeSide, Venue
from contracts import (
    contract_freeze_reasons,
    map_market,
    evaluate_contract_match_confidence,
    map_market_to_contract,
    market_group_key,
    market_identity_from_market,
    ResolutionRules,
)
from contracts.resolution_rules import ContractRuleFreezePolicy


class ContractsLayerTests(unittest.TestCase):
    def _market(
        self,
        *,
        symbol: str = "token-1",
        outcome: OutcomeSide = OutcomeSide.YES,
        title: str = "Will team A win?",
        raw: dict | None = None,
    ) -> MarketSummary:
        return MarketSummary(
            contract=Contract(venue=Venue.POLYMARKET, symbol=symbol, outcome=outcome),
            title=title,
            category="sports",
            sport="nba",
            series="playoffs",
            event_key="event-1",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=2),
            raw=raw or {"market": {"condition_id": "winner-1"}},
        )

    def test_market_identity_uses_group_key_and_labels(self):
        market = self._market()
        identity = market_identity_from_market(market)
        self.assertEqual(identity.group_key, "winner-1")
        self.assertIn("sports", identity.labels)
        self.assertEqual(market_group_key(market), "winner-1")

    def test_confidence_scores_related_contracts_higher(self):
        left = market_identity_from_market(self._market(symbol="yes"))
        right = market_identity_from_market(
            self._market(symbol="no", outcome=OutcomeSide.NO)
        )
        confidence = evaluate_contract_match_confidence(left, right)
        self.assertGreaterEqual(confidence.score, 0.85)
        self.assertEqual(confidence.level, "high")

    def test_map_market_to_contract_attaches_rules_and_confidence(self):
        market = self._market(raw={"market": {"condition_id": "winner-1", "closed": False}})
        mapped = map_market_to_contract(market)
        self.assertEqual(mapped.identity.group_key, "winner-1")
        self.assertEqual(mapped.confidence.level, "high")
        self.assertFalse(mapped.rules.closed)

    def test_freeze_reasons_read_resolution_rules_from_contracts_package(self):
        market = self._market(
            raw={"market": {"condition_id": "winner-1", "acceptingOrders": False}}
        )
        reasons = contract_freeze_reasons(
            market,
            policy=ContractRuleFreezePolicy(freeze_before_expiry_seconds=7200),
            now=datetime.now(timezone.utc),
        )
        self.assertTrue(any("not accepting orders" in reason for reason in reasons))

    def test_map_market_blocks_overtime_rule_mismatch(self):
        match = map_market(
            {
                "market_id": "pm-1",
                "question": "Will Home Team beat Away Team?",
                "sports_market_type": "moneyline",
                "gameStartTime": "2026-04-21T19:00:00Z",
            },
            {
                "sportsbook_event_id": "sb-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            "moneyline",
            ResolutionRules(
                includes_overtime=True,
                void_on_postponement=True,
                requires_player_to_start=None,
                resolution_source="league",
            ),
            ResolutionRules(
                includes_overtime=False,
                void_on_postponement=True,
                requires_player_to_start=None,
                resolution_source="book",
            ),
        )
        self.assertEqual(match.match_confidence, 0.0)
        self.assertEqual(match.mismatch_reason, "overtime/regulation mismatch")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from adapters import MarketSummary
from adapters.types import Contract, OutcomeSide, Venue
from contracts import (
    MappingStatus,
    compare_rule_semantics,
    contract_freeze_reasons,
    evaluate_contract_match_confidence,
    map_contract_candidate,
    map_market,
    map_market_to_contract,
    market_group_key,
    market_identity_from_market,
    mapping_blocked_reason,
    semantics_from_market_type,
    ResolutionRules,
)
from contracts.resolution_rules import ContractRuleFreezePolicy
from contracts.mapping_identity import (
    polymarket_contract_identity,
    sportsbook_contract_identity,
)
from contracts.mapping_semantics import RuleSemantics, GradingScope


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
        market = self._market(
            raw={"market": {"condition_id": "winner-1", "closed": False}}
        )
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

    def test_rule_semantics_detect_postponement_mismatch(self):
        compatible, reason = compare_rule_semantics(
            RuleSemantics(
                grading_scope=GradingScope.INCLUDE_OVERTIME,
                includes_overtime=True,
                void_on_postponement=True,
                requires_player_to_start=None,
                resolution_source="league",
            ),
            RuleSemantics(
                grading_scope=GradingScope.INCLUDE_OVERTIME,
                includes_overtime=True,
                void_on_postponement=False,
                requires_player_to_start=None,
                resolution_source="book",
            ),
        )

        self.assertFalse(compatible)
        self.assertEqual(reason, "postponement/void mismatch")

    def test_research_mapping_uses_explicit_identity_and_semantics(self):
        decision = map_contract_candidate(
            {
                "market_id": "pm-1",
                "condition_id": "condition-1",
                "eventKey": "event-1",
                "gameId": "game-1",
                "sport": "nba",
                "series": "playoffs",
                "sportsMarketType": "moneyline",
                "question": "Will Home Team beat Away Team?",
                "gameStartTime": "2026-04-21T19:00:00Z",
            },
            {
                "sportsbook_event_id": "sb-1",
                "event_key": "event-1",
                "game_id": "game-1",
                "sport": "nba",
                "series": "playoffs",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            sportsbook_market_type="moneyline",
            pm_semantics=semantics_from_market_type("moneyline", source="league"),
            sb_semantics=semantics_from_market_type("moneyline", source="book"),
        )

        self.assertIsNone(decision.blocked_reason)
        self.assertEqual(decision.mapping_status, MappingStatus.EXACT_MATCH)
        self.assertGreaterEqual(decision.match_confidence, 0.9)
        self.assertEqual(decision.event_key, "event-1")
        self.assertEqual(decision.game_id, "game-1")

    def test_research_mapping_blocks_explicit_event_identity_mismatch(self):
        pm_identity = polymarket_contract_identity(
            {
                "market_id": "pm-1",
                "eventKey": "event-1",
                "gameId": "game-1",
                "sportsMarketType": "moneyline",
                "question": "Will Home Team beat Away Team?",
                "gameStartTime": "2026-04-21T19:00:00Z",
            }
        )
        sb_identity = sportsbook_contract_identity(
            {
                "sportsbook_event_id": "sb-1",
                "event_key": "event-2",
                "game_id": "game-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            sportsbook_market_type="moneyline",
        )

        self.assertEqual(pm_identity.event_key, "event-1")
        self.assertEqual(sb_identity.event_key, "event-2")

        decision = map_contract_candidate(
            {
                "market_id": "pm-1",
                "eventKey": "event-1",
                "gameId": "game-1",
                "sportsMarketType": "moneyline",
                "question": "Will Home Team beat Away Team?",
                "gameStartTime": "2026-04-21T19:00:00Z",
            },
            {
                "sportsbook_event_id": "sb-1",
                "event_key": "event-2",
                "game_id": "game-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            sportsbook_market_type="moneyline",
            pm_semantics=semantics_from_market_type("moneyline", source="league"),
            sb_semantics=semantics_from_market_type("moneyline", source="book"),
        )

        self.assertEqual(decision.match_confidence, 0.0)
        self.assertEqual(decision.mapping_status, MappingStatus.BLOCKED)
        blocked_reason = decision.blocked_reason
        self.assertIsNotNone(blocked_reason)
        if blocked_reason is None:
            self.fail("expected blocked reason for explicit event mismatch")
        self.assertEqual(blocked_reason.code, "event_key_mismatch")
        self.assertEqual(blocked_reason.message, "event key mismatch")

    def test_research_mapping_zeroes_confidence_evidence_for_market_type_mismatch(self):
        decision = map_contract_candidate(
            {
                "market_id": "pm-1",
                "sportsMarketType": "spread",
                "question": "Will Home Team beat Away Team?",
                "gameStartTime": "2026-04-21T19:00:00Z",
            },
            {
                "sportsbook_event_id": "sb-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            sportsbook_market_type="moneyline",
        )

        self.assertEqual(decision.mapping_status, MappingStatus.BLOCKED)
        self.assertEqual(decision.match_confidence, 0.0)
        self.assertEqual(decision.mapping_confidence.components, {})
        self.assertEqual(decision.mapping_confidence.reasons, ())
        blocked_reason = decision.blocked_reason
        self.assertIsNotNone(blocked_reason)
        if blocked_reason is None:
            self.fail("expected blocked reason for market type mismatch")
        self.assertEqual(blocked_reason.code, "market_type_mismatch")

    def test_research_mapping_allows_strong_team_and_time_alignment_without_ids(self):
        decision = map_contract_candidate(
            {
                "market_id": "pm-1",
                "sportsMarketType": "moneyline",
                "sport": "nba",
                "question": "Will Home Team beat Away Team?",
                "gameStartTime": "2026-04-21T19:00:00Z",
            },
            {
                "sportsbook_event_id": "sb-1",
                "sport": "basketball_nba",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            sportsbook_market_type="h2h",
            pm_semantics=semantics_from_market_type("moneyline", source="league"),
            sb_semantics=semantics_from_market_type("h2h", source="book"),
        )

        self.assertIsNone(decision.blocked_reason)
        self.assertEqual(decision.mapping_status, MappingStatus.NORMALIZED_MATCH)
        self.assertGreaterEqual(decision.match_confidence, 0.75)

    def test_mapping_payload_serializes_structured_confidence_and_blocked_reason(self):
        decision = map_contract_candidate(
            {
                "market_id": "pm-1",
                "eventKey": "event-1",
                "sportsMarketType": "moneyline",
                "question": "Will Home Team beat Away Team?",
                "gameStartTime": "2026-04-21T19:00:00Z",
            },
            {
                "sportsbook_event_id": "sb-1",
                "event_key": "event-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
            },
            sportsbook_market_type="moneyline",
        )

        payload = decision.to_payload(
            blocked_reason_override=mapping_blocked_reason(
                "missing upstream event identity"
            ),
            confidence_score_override=0.59,
            is_active=False,
        )

        self.assertEqual(payload["mapping_status"], MappingStatus.BLOCKED.value)
        confidence_payload = payload["mapping_confidence"]
        blocked_reason_payload = payload["blocked_reason"]
        self.assertIsInstance(confidence_payload, dict)
        self.assertIsInstance(blocked_reason_payload, dict)
        if not isinstance(confidence_payload, dict) or not isinstance(
            blocked_reason_payload, dict
        ):
            self.fail("expected structured mapping payload dictionaries")
        self.assertEqual(confidence_payload["score"], 0.59)
        self.assertEqual(
            blocked_reason_payload["code"],
            "missing_upstream_event_identity",
        )
        self.assertFalse(payload["is_active"])

    def test_map_market_tolerates_malformed_times_without_raising(self):
        match = map_market(
            {
                "market_id": "pm-1",
                "question": "Will Home Team beat Away Team?",
                "sports_market_type": "moneyline",
                "gameStartTime": "not-a-time",
            },
            {
                "sportsbook_event_id": "sb-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "still-not-a-time",
            },
            "moneyline",
            ResolutionRules(
                includes_overtime=True,
                void_on_postponement=True,
                requires_player_to_start=None,
                resolution_source="league",
            ),
            ResolutionRules(
                includes_overtime=True,
                void_on_postponement=True,
                requires_player_to_start=None,
                resolution_source="book",
            ),
        )

        self.assertGreater(match.match_confidence, 0.0)
        self.assertIsNone(match.mismatch_reason)


if __name__ == "__main__":
    unittest.main()

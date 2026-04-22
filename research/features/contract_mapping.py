from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Mapping

from contracts.ontology import normalize_market_type
from research.features.contract_identity import (
    polymarket_contract_identity,
    sportsbook_contract_identity,
    start_time_alignment_score,
    team_overlap_score,
)
from research.features.rules_semantics import (
    RuleSemantics,
    compare_rule_semantics,
    semantics_from_market_type,
)


@dataclass(frozen=True)
class MappingDecision:
    polymarket_market_id: str
    sportsbook_event_id: str
    sportsbook_market_type: str
    normalized_market_type: str
    match_confidence: float
    resolution_risk: float
    blocked_reason: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None


def _token_match(left: str | None, right: str | None) -> bool:
    if left in (None, "") or right in (None, ""):
        return False
    left_tokens = {
        token for token in re.split(r"[^a-z0-9]+", str(left).strip().lower()) if token
    }
    right_tokens = {
        token for token in re.split(r"[^a-z0-9]+", str(right).strip().lower()) if token
    }
    if not left_tokens or not right_tokens:
        return False
    return bool(left_tokens.intersection(right_tokens))


def map_contract_candidate(
    pm_market: Mapping[str, object],
    sb_event: Mapping[str, object],
    *,
    sportsbook_market_type: str,
    pm_semantics: RuleSemantics | None = None,
    sb_semantics: RuleSemantics | None = None,
) -> MappingDecision:
    pm_identity = polymarket_contract_identity(pm_market)
    sb_identity = sportsbook_contract_identity(
        sb_event,
        sportsbook_market_type=sportsbook_market_type,
    )

    normalized_market_type = normalize_market_type(
        pm_identity.sports_market_type or sportsbook_market_type
    ).value
    sportsbook_normalized_market_type = normalize_market_type(
        sportsbook_market_type
    ).value
    if normalized_market_type != sportsbook_normalized_market_type:
        return MappingDecision(
            polymarket_market_id=pm_identity.market_id or pm_identity.condition_id or "",
            sportsbook_event_id=sb_identity.sportsbook_event_id or "",
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            match_confidence=0.0,
            resolution_risk=1.0,
            blocked_reason="market type mismatch",
            event_key=pm_identity.event_key or sb_identity.event_key,
            sport=pm_identity.sport or sb_identity.sport,
            series=pm_identity.series or sb_identity.series,
            game_id=pm_identity.game_id or sb_identity.game_id,
        )

    if (
        pm_identity.event_key not in (None, "")
        and sb_identity.event_key not in (None, "")
        and pm_identity.event_key != sb_identity.event_key
    ):
        return MappingDecision(
            polymarket_market_id=pm_identity.market_id or pm_identity.condition_id or "",
            sportsbook_event_id=sb_identity.sportsbook_event_id or "",
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            match_confidence=0.0,
            resolution_risk=1.0,
            blocked_reason="event key mismatch",
            event_key=pm_identity.event_key,
            sport=pm_identity.sport or sb_identity.sport,
            series=pm_identity.series or sb_identity.series,
            game_id=pm_identity.game_id or sb_identity.game_id,
        )

    if (
        pm_identity.game_id not in (None, "")
        and sb_identity.game_id not in (None, "")
        and pm_identity.game_id != sb_identity.game_id
    ):
        return MappingDecision(
            polymarket_market_id=pm_identity.market_id or pm_identity.condition_id or "",
            sportsbook_event_id=sb_identity.sportsbook_event_id or "",
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            match_confidence=0.0,
            resolution_risk=1.0,
            blocked_reason="game id mismatch",
            event_key=pm_identity.event_key or sb_identity.event_key,
            sport=pm_identity.sport or sb_identity.sport,
            series=pm_identity.series or sb_identity.series,
            game_id=pm_identity.game_id or sb_identity.game_id,
        )

    resolved_pm_semantics = pm_semantics or semantics_from_market_type(
        pm_identity.sports_market_type or sportsbook_market_type,
        source="polymarket",
    )
    resolved_sb_semantics = sb_semantics or semantics_from_market_type(
        sportsbook_market_type,
        source="sportsbook",
    )
    compatible, blocked_reason = compare_rule_semantics(
        resolved_pm_semantics,
        resolved_sb_semantics,
    )
    if not compatible:
        return MappingDecision(
            polymarket_market_id=pm_identity.market_id or pm_identity.condition_id or "",
            sportsbook_event_id=sb_identity.sportsbook_event_id or "",
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            match_confidence=0.0,
            resolution_risk=1.0,
            blocked_reason=blocked_reason,
            event_key=pm_identity.event_key or sb_identity.event_key,
            sport=pm_identity.sport or sb_identity.sport,
            series=pm_identity.series or sb_identity.series,
            game_id=pm_identity.game_id or sb_identity.game_id,
        )

    team_score = team_overlap_score(pm_identity, sb_identity)
    time_score = start_time_alignment_score(pm_identity, sb_identity)
    explicit_event_match = (
        pm_identity.event_key not in (None, "")
        and sb_identity.event_key not in (None, "")
        and pm_identity.event_key == sb_identity.event_key
    )
    explicit_game_match = (
        pm_identity.game_id not in (None, "")
        and sb_identity.game_id not in (None, "")
        and pm_identity.game_id == sb_identity.game_id
    )

    if not explicit_event_match and not explicit_game_match:
        if team_score < 0.25:
            blocked_reason = "team names do not match with enough confidence"
        elif time_score <= 0.0:
            blocked_reason = "event start times do not match"

    score = 0.0
    score += 0.25
    score += 0.20
    if explicit_event_match:
        score += 0.15
    if explicit_game_match:
        score += 0.10
    if _token_match(pm_identity.sport, sb_identity.sport):
        score += 0.05
    if _token_match(pm_identity.series, sb_identity.series):
        score += 0.05
    score += 0.20 * team_score
    score += 0.15 * time_score
    score = min(score, 1.0)

    if blocked_reason is None and score < 0.6:
        blocked_reason = "ambiguous market identity"

    if blocked_reason is not None and score > 0.0:
        score = min(score, 0.59)

    return MappingDecision(
        polymarket_market_id=pm_identity.market_id or pm_identity.condition_id or "",
        sportsbook_event_id=sb_identity.sportsbook_event_id or "",
        sportsbook_market_type=sportsbook_market_type,
        normalized_market_type=normalized_market_type,
        match_confidence=round(score, 4),
        resolution_risk=round(max(0.0, 1.0 - score), 4),
        blocked_reason=blocked_reason,
        event_key=pm_identity.event_key or sb_identity.event_key,
        sport=pm_identity.sport or sb_identity.sport,
        series=pm_identity.series or sb_identity.series,
        game_id=pm_identity.game_id or sb_identity.game_id,
    )

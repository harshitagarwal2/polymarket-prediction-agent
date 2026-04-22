from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from contracts.models import ContractMatch
from contracts.ontology import NormalizedContractIdentity
from contracts.rules import ResolutionRules, rules_compatible


@dataclass(frozen=True)
class ContractMatchConfidence:
    score: float
    level: str
    reasons: tuple[str, ...]


def _level_for_score(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def evaluate_contract_match_confidence(
    left: NormalizedContractIdentity,
    right: NormalizedContractIdentity,
) -> ContractMatchConfidence:
    score = 0.0
    reasons: list[str] = []
    if left.group_key == right.group_key:
        score += 0.4
        reasons.append("group key match")
    if left.event_key not in (None, "") and left.event_key == right.event_key:
        score += 0.2
        reasons.append("event key match")
    if left.sport not in (None, "") and left.sport == right.sport:
        score += 0.1
        reasons.append("sport match")
    if left.series not in (None, "") and left.series == right.series:
        score += 0.1
        reasons.append("series match")
    if left.contract_type == right.contract_type:
        score += 0.1
        reasons.append("contract type match")
    shared_labels = set(left.labels).intersection(right.labels)
    if shared_labels:
        score += min(0.1, 0.02 * len(shared_labels))
        reasons.append(f"shared labels: {', '.join(sorted(shared_labels)[:3])}")
    if left.outcome == right.outcome:
        score += 0.05
        reasons.append("outcome alignment")
    score = min(score, 1.0)
    return ContractMatchConfidence(
        score=round(score, 4),
        level=_level_for_score(score),
        reasons=tuple(reasons),
    )


def score_contract_match(
    *,
    team_match: float,
    time_match: float,
    market_type_match: bool,
    pm_rules: ResolutionRules,
    sb_rules: ResolutionRules,
) -> tuple[float, float, str | None]:
    compatible, mismatch_reason = rules_compatible(pm_rules, sb_rules)
    if not market_type_match:
        return 0.0, 1.0, "market type mismatch"
    if not compatible:
        return 0.0, 1.0, mismatch_reason
    score = 0.35 * max(0.0, min(team_match, 1.0))
    score += 0.20 * max(0.0, min(time_match, 1.0))
    score += 0.25
    score += 0.20
    resolution_risk = round(max(0.0, 1.0 - score), 4)
    return round(min(score, 1.0), 4), resolution_risk, None


def contract_match_from_score(
    *,
    polymarket_market_id: str,
    sportsbook_event_id: str,
    sportsbook_market_type: str,
    normalized_market_type: str,
    match_confidence: float,
    resolution_risk: float,
    mismatch_reason: str | None,
) -> ContractMatch:
    return ContractMatch(
        polymarket_market_id=polymarket_market_id,
        sportsbook_event_id=sportsbook_event_id,
        sportsbook_market_type=sportsbook_market_type,
        normalized_market_type=normalized_market_type,
        match_confidence=match_confidence,
        resolution_risk=resolution_risk,
        mismatch_reason=mismatch_reason,
    )

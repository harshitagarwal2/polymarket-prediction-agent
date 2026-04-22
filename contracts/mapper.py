from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any

from adapters import MarketSummary
from contracts.confidence import (
    ContractMatchConfidence,
    contract_match_from_score,
    evaluate_contract_match_confidence,
    score_contract_match,
)
from contracts.models import ContractMatch
from contracts.ontology import (
    NormalizedContractIdentity,
    market_identity_from_market,
    normalize_market_type,
)
from contracts.resolution_rules import ParsedContractRules, parse_contract_rules
from contracts.rules import ResolutionRules


@dataclass(frozen=True)
class MappedContract:
    identity: NormalizedContractIdentity
    rules: ParsedContractRules
    confidence: ContractMatchConfidence


def map_market_to_contract(
    market: MarketSummary,
    *,
    reference: MarketSummary | None = None,
) -> MappedContract:
    identity = market_identity_from_market(market)
    confidence = ContractMatchConfidence(
        score=1.0,
        level="high",
        reasons=("self-derived market identity",),
    )
    if reference is not None:
        confidence = evaluate_contract_match_confidence(
            identity,
            market_identity_from_market(reference),
        )
    return MappedContract(
        identity=identity,
        rules=parse_contract_rules(market),
        confidence=confidence,
    )


def _normalized_tokens(*values: str | None) -> set[str]:
    text = " ".join(value for value in values if value)
    return {token for token in re.split(r"[^a-z0-9]+", text.lower()) if token}


def _team_match(pm_market: dict[str, Any], sb_event: dict[str, Any]) -> float:
    pm_tokens = _normalized_tokens(
        str(pm_market.get("question") or pm_market.get("title") or ""),
    )
    sb_tokens = _normalized_tokens(
        str(sb_event.get("home_team") or ""),
        str(sb_event.get("away_team") or ""),
    )
    if not pm_tokens or not sb_tokens:
        return 0.0
    overlap = pm_tokens.intersection(sb_tokens)
    return len(overlap) / max(1, len(sb_tokens))


def _time_match(pm_market: dict[str, Any], sb_event: dict[str, Any]) -> float:
    pm_time = pm_market.get("start_time") or pm_market.get("gameStartTime") or pm_market.get("endDate")
    sb_time = sb_event.get("start_time") or sb_event.get("commence_time")
    if not pm_time or not sb_time:
        return 0.0
    try:
        left = datetime.fromisoformat(str(pm_time).replace("Z", "+00:00"))
        right = datetime.fromisoformat(str(sb_time).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if left.tzinfo is None:
        left = left.replace(tzinfo=timezone.utc)
    if right.tzinfo is None:
        right = right.replace(tzinfo=timezone.utc)
    diff_minutes = abs((left - right).total_seconds()) / 60.0
    if diff_minutes <= 10:
        return 1.0
    if diff_minutes <= 60:
        return 0.5
    return 0.0


def map_market(
    pm_market: dict[str, Any],
    sb_event: dict[str, Any],
    sb_market_type: str,
    pm_rules: ResolutionRules,
    sb_rules: ResolutionRules,
) -> ContractMatch:
    normalized_market_type = normalize_market_type(
        str(pm_market.get("sports_market_type") or sb_market_type)
    ).value
    sportsbook_market_type = normalize_market_type(sb_market_type).value
    team_match = _team_match(pm_market, sb_event)
    time_match = _time_match(pm_market, sb_event)
    match_confidence, resolution_risk, mismatch_reason = score_contract_match(
        team_match=team_match,
        time_match=time_match,
        market_type_match=normalized_market_type == sportsbook_market_type,
        pm_rules=pm_rules,
        sb_rules=sb_rules,
    )
    if mismatch_reason is None and team_match < 0.25:
        mismatch_reason = "team names do not match with enough confidence"
    return contract_match_from_score(
        polymarket_market_id=str(
            pm_market.get("market_id")
            or pm_market.get("conditionId")
            or pm_market.get("condition_id")
            or pm_market.get("id")
            or ""
        ),
        sportsbook_event_id=str(sb_event.get("sportsbook_event_id") or sb_event.get("id") or ""),
        sportsbook_market_type=sb_market_type,
        normalized_market_type=normalized_market_type,
        match_confidence=match_confidence,
        resolution_risk=resolution_risk,
        mismatch_reason=mismatch_reason,
    )

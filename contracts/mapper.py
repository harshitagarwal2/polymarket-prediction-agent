from __future__ import annotations

from dataclasses import dataclass

from adapters import MarketSummary
from contracts.confidence import (
    ContractMatchConfidence,
    score_contract_match,
    evaluate_contract_match_confidence,
    contract_match_from_score,
)
from contracts.mapping import map_contract_candidate
from contracts.mapping_identity import (
    polymarket_contract_identity,
    sportsbook_contract_identity,
    start_time_alignment_score,
    team_overlap_score,
)
from contracts.mapping_semantics import GradingScope, RuleSemantics
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


def _semantics_from_resolution_rules(rules: ResolutionRules) -> RuleSemantics:
    return RuleSemantics(
        grading_scope=(
            GradingScope.INCLUDE_OVERTIME
            if rules.includes_overtime
            else GradingScope.REGULATION_ONLY
        ),
        includes_overtime=rules.includes_overtime,
        void_on_postponement=rules.void_on_postponement,
        requires_player_to_start=rules.requires_player_to_start,
        resolution_source=rules.resolution_source,
    )


def map_market(
    pm_market: dict[str, object],
    sb_event: dict[str, object],
    sb_market_type: str,
    pm_rules: ResolutionRules,
    sb_rules: ResolutionRules,
) -> ContractMatch:
    decision = map_contract_candidate(
        pm_market,
        sb_event,
        sportsbook_market_type=sb_market_type,
        pm_semantics=_semantics_from_resolution_rules(pm_rules),
        sb_semantics=_semantics_from_resolution_rules(sb_rules),
    )
    pm_identity = polymarket_contract_identity(pm_market)
    sb_identity = sportsbook_contract_identity(
        sb_event,
        sportsbook_market_type=sb_market_type,
    )
    if (
        decision.blocked_reason is not None
        and decision.blocked_reason.code == "event_start_time_mismatch"
        and all(
            value in (None, "")
            for value in (
                pm_identity.event_key,
                pm_identity.game_id,
                sb_identity.event_key,
                sb_identity.game_id,
            )
        )
    ):
        normalized_market_type = normalize_market_type(
            str(pm_market.get("sports_market_type") or sb_market_type)
        ).value
        sportsbook_market_type = normalize_market_type(sb_market_type).value
        team_match = team_overlap_score(pm_identity, sb_identity)
        time_match = start_time_alignment_score(pm_identity, sb_identity)
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
            sportsbook_event_id=str(
                sb_event.get("sportsbook_event_id") or sb_event.get("id") or ""
            ),
            sportsbook_market_type=sb_market_type,
            normalized_market_type=normalized_market_type,
            match_confidence=match_confidence,
            resolution_risk=resolution_risk,
            mismatch_reason=mismatch_reason,
        )
    return contract_match_from_score(
        polymarket_market_id=str(
            pm_market.get("market_id")
            or pm_market.get("conditionId")
            or pm_market.get("condition_id")
            or pm_market.get("id")
            or ""
        ),
        sportsbook_event_id=str(
            sb_event.get("sportsbook_event_id") or sb_event.get("id") or ""
        ),
        sportsbook_market_type=sb_market_type,
        normalized_market_type=decision.normalized_market_type,
        match_confidence=decision.match_confidence,
        resolution_risk=decision.resolution_risk,
        mismatch_reason=(
            decision.blocked_reason.message if decision.blocked_reason else None
        ),
    )

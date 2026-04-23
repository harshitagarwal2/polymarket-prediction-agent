from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from execution.models import OrderProposal
from opportunity.models import Opportunity, normalize_blocked_reasons
from risk.correlated_exposure import CorrelatedExposureDecision
from risk.freeze_windows import FreezeWindowPolicy, freeze_reasons_for_state


@dataclass(frozen=True)
class PlannerThresholds:
    min_match_confidence: float = 0.95
    max_source_age_ms: int = 4000
    max_book_dispersion: float = 0.03
    entry_edge_bps: float = 150.0
    exit_edge_bps: float = 50.0
    freeze_minutes_before_start: int = 10
    freeze_minutes_before_expiry: int = 0
    cooldown_seconds_after_score_change: int = 15
    block_on_unhealthy_source: bool = True


@dataclass(frozen=True)
class ProposalDecision:
    proposal: OrderProposal | None
    blocked_reasons: tuple[str, ...] = ()
    blocked_reason: str | None = None


class ExecutionPlanner:
    def __init__(self, thresholds: PlannerThresholds | None = None) -> None:
        self.thresholds = thresholds or PlannerThresholds()

    def evaluate(
        self,
        opportunity: Opportunity,
        *,
        source_age_ms: int,
        book_dispersion: float,
        event_start_time: datetime | None = None,
        market_end_time: datetime | None = None,
        market_active: bool | None = True,
        market_resolved: bool | None = None,
        source_health: Mapping[str, Any] | None = None,
        required_sources: tuple[str, ...] = (),
        correlated_exposure: CorrelatedExposureDecision | None = None,
        now: datetime | None = None,
    ) -> ProposalDecision:
        blocked_reasons: list[str] = list(
            normalize_blocked_reasons(
                opportunity.blocked_reasons,
                opportunity.blocked_reason,
            )
        )
        if opportunity.confidence < self.thresholds.min_match_confidence:
            blocked_reasons.append("low match confidence")
        if source_age_ms > self.thresholds.max_source_age_ms:
            blocked_reasons.append("source data stale")
        if book_dispersion > self.thresholds.max_book_dispersion:
            blocked_reasons.append("book dispersion exceeds threshold")
        if opportunity.edge_after_costs_bps < self.thresholds.entry_edge_bps:
            blocked_reasons.append("edge below entry threshold")
        if correlated_exposure is not None and not correlated_exposure.allowed:
            blocked_reasons.append(
                correlated_exposure.reason or "cluster exposure cap exceeded"
            )
        current = now or datetime.now(timezone.utc)
        freeze_policy = FreezeWindowPolicy(
            freeze_minutes_before_start=self.thresholds.freeze_minutes_before_start,
            freeze_minutes_before_expiry=self.thresholds.freeze_minutes_before_expiry,
            freeze_when_source_unhealthy=self.thresholds.block_on_unhealthy_source,
        )
        freeze_reasons = freeze_reasons_for_state(
            policy=freeze_policy,
            now=current,
            event_start_time=event_start_time,
            market_end_time=market_end_time,
            market_active=market_active,
            market_resolved=market_resolved,
            required_sources=required_sources,
            source_health=source_health,
        )
        normalized_blocked_reasons = normalize_blocked_reasons(
            blocked_reasons,
            freeze_reasons,
        )
        if normalized_blocked_reasons:
            return ProposalDecision(
                proposal=None,
                blocked_reasons=normalized_blocked_reasons,
                blocked_reason=normalized_blocked_reasons[0],
            )
        price = (
            opportunity.best_ask_yes
            if opportunity.side == "buy_yes"
            else opportunity.best_bid_yes
        )
        return ProposalDecision(
            proposal=OrderProposal(
                market_id=opportunity.market_id,
                side=opportunity.side,
                action="place",
                price=price,
                size=opportunity.fillable_size,
                tif="GTC",
                rationale=f"edge_after_costs_bps={opportunity.edge_after_costs_bps:.2f}",
            )
        )

    def proposal_for(
        self,
        opportunity: Opportunity,
        *,
        source_age_ms: int,
        book_dispersion: float,
        event_start_time: datetime | None = None,
        market_end_time: datetime | None = None,
        market_active: bool | None = True,
        market_resolved: bool | None = None,
        source_health: Mapping[str, Any] | None = None,
        required_sources: tuple[str, ...] = (),
        correlated_exposure: CorrelatedExposureDecision | None = None,
        now: datetime | None = None,
    ) -> OrderProposal | None:
        decision = self.evaluate(
            opportunity,
            source_age_ms=source_age_ms,
            book_dispersion=book_dispersion,
            event_start_time=event_start_time,
            market_end_time=market_end_time,
            market_active=market_active,
            market_resolved=market_resolved,
            source_health=source_health,
            required_sources=required_sources,
            correlated_exposure=correlated_exposure,
            now=now,
        )
        return decision.proposal

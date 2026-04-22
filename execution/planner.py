from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from execution.models import OrderProposal
from opportunity.models import Opportunity


@dataclass(frozen=True)
class PlannerThresholds:
    min_match_confidence: float = 0.95
    max_source_age_ms: int = 4000
    max_book_dispersion: float = 0.03
    entry_edge_bps: float = 150.0
    exit_edge_bps: float = 50.0
    freeze_minutes_before_start: int = 10
    cooldown_seconds_after_score_change: int = 15


class ExecutionPlanner:
    def __init__(self, thresholds: PlannerThresholds | None = None) -> None:
        self.thresholds = thresholds or PlannerThresholds()

    def proposal_for(
        self,
        opportunity: Opportunity,
        *,
        source_age_ms: int,
        book_dispersion: float,
        event_start_time: datetime | None = None,
        now: datetime | None = None,
    ) -> OrderProposal | None:
        if opportunity.blocked_reason:
            return None
        if opportunity.confidence < self.thresholds.min_match_confidence:
            return None
        if source_age_ms > self.thresholds.max_source_age_ms:
            return None
        if book_dispersion > self.thresholds.max_book_dispersion:
            return None
        if opportunity.edge_after_costs_bps < self.thresholds.entry_edge_bps:
            return None
        current = now or datetime.now(timezone.utc)
        if event_start_time is not None:
            if event_start_time.tzinfo is None:
                event_start_time = event_start_time.replace(tzinfo=timezone.utc)
            seconds_to_start = (event_start_time - current).total_seconds()
            if seconds_to_start <= self.thresholds.freeze_minutes_before_start * 60:
                return None
        price = opportunity.best_ask_yes if opportunity.side == "buy_yes" else opportunity.best_bid_yes
        return OrderProposal(
            market_id=opportunity.market_id,
            side=opportunity.side,
            action="place",
            price=price,
            size=opportunity.fillable_size,
            tif="GTC",
            rationale=f"edge_after_costs_bps={opportunity.edge_after_costs_bps:.2f}",
        )

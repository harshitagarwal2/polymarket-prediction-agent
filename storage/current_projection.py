from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from execution import ExecutionPlanner
from opportunity.models import Opportunity, normalize_blocked_reasons

from storage.current_read_adapter import (
    CurrentStateReadAdapter,
    FileCurrentStateReadAdapter,
)
from storage.current_selection import best_mapping_by_market


@dataclass(frozen=True)
class CurrentStateTables:
    opportunities: dict[str, object]
    mappings: dict[str, object]
    fair_values: dict[str, object]
    bbo_rows: dict[str, object]
    sportsbook_events: dict[str, object]
    source_health: dict[str, object]
    polymarket_markets: dict[str, object]


@dataclass(frozen=True)
class PreviewRuntimeContext:
    preview_order_proposals: tuple[dict[str, object], ...]
    blocked_preview_orders: tuple[dict[str, object], ...]


def _parse_event_start(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    return float(value)


def _coalesce_float(*values: object, default: float = 0.0) -> float:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return default


def load_current_state_tables(
    opportunity_root: str | Path | None,
    *,
    read_adapter: CurrentStateReadAdapter | None = None,
) -> CurrentStateTables:
    if read_adapter is None and opportunity_root is None:
        return CurrentStateTables({}, {}, {}, {}, {}, {}, {})
    adapter = read_adapter
    if adapter is None:
        assert opportunity_root is not None
        adapter = FileCurrentStateReadAdapter.from_opportunity_root(opportunity_root)
    return CurrentStateTables(
        opportunities=adapter.read_table("opportunities"),
        mappings=adapter.read_table("market_mappings"),
        fair_values=adapter.read_table("fair_values"),
        bbo_rows=adapter.read_table("polymarket_bbo"),
        sportsbook_events=adapter.read_table("sportsbook_events"),
        source_health=adapter.read_table("source_health"),
        polymarket_markets=adapter.read_table("polymarket_markets"),
    )


def build_preview_runtime_context(
    opportunity_root: str | Path | None,
    *,
    policy=None,
    read_adapter: CurrentStateReadAdapter | None = None,
) -> PreviewRuntimeContext:
    tables = load_current_state_tables(opportunity_root, read_adapter=read_adapter)
    if opportunity_root is None and read_adapter is None:
        return PreviewRuntimeContext((), ())

    planner = ExecutionPlanner(
        None if policy is None else policy.proposal_planner.build()
    )
    mapping_by_market = best_mapping_by_market(tables.mappings)
    required_sources = tuple(
        source_name
        for source_name in (
            "polymarket_market_channel",
            "sportsbook_odds",
            "market_mappings",
            "fair_values",
        )
        if source_name in tables.source_health
    )

    proposals: list[dict[str, object]] = []
    blocked: list[dict[str, object]] = []

    def _snapshot_payload(
        opportunity: Opportunity,
        *,
        blocked_reason: str | None,
        blocked_reasons: tuple[str, ...],
    ) -> dict[str, object]:
        return {
            "market_id": opportunity.market_id,
            "side": opportunity.side,
            "fair_yes_prob": opportunity.fair_yes_prob,
            "best_bid_yes": opportunity.best_bid_yes,
            "best_ask_yes": opportunity.best_ask_yes,
            "edge_buy_bps": opportunity.edge_buy_bps,
            "edge_sell_bps": opportunity.edge_sell_bps,
            "edge_buy_after_costs_bps": opportunity.edge_buy_after_costs_bps,
            "edge_sell_after_costs_bps": opportunity.edge_sell_after_costs_bps,
            "edge_after_costs_bps": opportunity.edge_after_costs_bps,
            "fillable_size": opportunity.fillable_size,
            "confidence": opportunity.confidence,
            "blocked_reason": blocked_reason,
            "blocked_reasons": list(blocked_reasons),
        }

    for row in tables.opportunities.values():
        if not isinstance(row, dict):
            continue
        market_id = str(row.get("market_id") or "")
        bbo = tables.bbo_rows.get(market_id)
        fair_value = tables.fair_values.get(market_id)
        mapping = mapping_by_market.get(market_id, {})
        market_row = tables.polymarket_markets.get(market_id)
        fair_value_age_ms = (
            int(fair_value.get("data_age_ms", 0)) if isinstance(fair_value, dict) else 0
        )
        bbo_source_age_ms = (
            int(bbo.get("source_age_ms", 0))
            if isinstance(bbo, dict) and bbo.get("source_age_ms") not in (None, "")
            else 0
        )
        source_age_ms = max(fair_value_age_ms, bbo_source_age_ms)
        book_dispersion = (
            float(fair_value.get("book_dispersion", 0.0))
            if isinstance(fair_value, dict)
            else 0.0
        )
        side = str(row.get("side") or "buy_yes")
        blocked_reasons = list(
            normalize_blocked_reasons(
                row.get("blocked_reasons"),
                row.get("blocked_reason"),
            )
        )
        if isinstance(bbo, dict):
            limit_price = (
                bbo.get("best_ask_yes")
                if side == "buy_yes"
                else bbo.get("best_bid_yes")
            )
        else:
            limit_price = None
        if limit_price in (None, ""):
            blocked_reasons = list(
                normalize_blocked_reasons(blocked_reasons, "missing executable bbo")
            )
        current_fillable_size = (
            _float_or_none(bbo.get("best_ask_yes_size"))
            if isinstance(bbo, dict)
            and side == "buy_yes"
            and bbo.get("best_ask_yes_size") not in (None, "")
            else _float_or_none(bbo.get("best_bid_yes_size"))
            if isinstance(bbo, dict)
            and side != "buy_yes"
            and bbo.get("best_bid_yes_size") not in (None, "")
            else None
        )
        stored_fillable_size = _float_or_none(row.get("fillable_size"))
        fillable_size = (
            stored_fillable_size if stored_fillable_size is not None else 0.0
        )
        if current_fillable_size is not None:
            fillable_size = (
                current_fillable_size
                if stored_fillable_size is None
                else min(fillable_size, current_fillable_size)
                if fillable_size > 0.0
                else fillable_size
            )
        fillable_size = max(fillable_size, 0.0)
        if fillable_size <= 0.0:
            blocked_reasons = list(
                normalize_blocked_reasons(blocked_reasons, "insufficient visible depth")
            )
        sportsbook_event = tables.sportsbook_events.get(
            str(mapping.get("sportsbook_event_id") or "")
        )
        event_start_time = (
            _parse_event_start(sportsbook_event.get("start_time"))
            if isinstance(sportsbook_event, dict)
            else None
        ) or (
            _parse_event_start(sportsbook_event.get("commence_time"))
            if isinstance(sportsbook_event, dict)
            else None
        )
        market_end_time = (
            _parse_event_start(market_row.get("end_time"))
            if isinstance(market_row, dict)
            else None
        )
        market_status = (
            str(market_row.get("status") or "").strip().lower()
            if isinstance(market_row, dict)
            else ""
        )
        opportunity = Opportunity(
            market_id=market_id,
            side=side,
            fair_yes_prob=_coalesce_float(
                row.get("fair_yes_prob"),
                fair_value.get("fair_yes_prob")
                if isinstance(fair_value, dict)
                else None,
            ),
            best_bid_yes=_coalesce_float(
                row.get("best_bid_yes"),
                bbo.get("best_bid_yes") if isinstance(bbo, dict) else None,
            ),
            best_ask_yes=_coalesce_float(
                row.get("best_ask_yes"),
                bbo.get("best_ask_yes") if isinstance(bbo, dict) else None,
            ),
            edge_buy_bps=_coalesce_float(row.get("edge_buy_bps")),
            edge_sell_bps=_coalesce_float(row.get("edge_sell_bps")),
            edge_buy_after_costs_bps=_coalesce_float(
                row.get("edge_buy_after_costs_bps"),
                row.get("edge_buy_bps"),
            ),
            edge_sell_after_costs_bps=_coalesce_float(
                row.get("edge_sell_after_costs_bps"),
                row.get("edge_sell_bps"),
            ),
            edge_after_costs_bps=_coalesce_float(row.get("edge_after_costs_bps")),
            fillable_size=fillable_size,
            confidence=_coalesce_float(row.get("confidence")),
            blocked_reasons=tuple(blocked_reasons),
            blocked_reason=blocked_reasons[0] if blocked_reasons else None,
        )
        decision = planner.evaluate(
            opportunity,
            source_age_ms=source_age_ms,
            book_dispersion=book_dispersion,
            event_start_time=event_start_time,
            market_end_time=market_end_time,
            market_active=market_status not in {"closed", "inactive", "resolved"},
            market_resolved=market_status in {"resolved", "settled"},
            source_health=tables.source_health,
            required_sources=required_sources,
        )
        if decision.proposal is None:
            blocked_payload = _snapshot_payload(
                opportunity,
                blocked_reason=decision.blocked_reason,
                blocked_reasons=decision.blocked_reasons,
            )
            blocked_payload["price"] = limit_price
            blocked.append(blocked_payload)
            continue
        proposal_payload = decision.proposal.__dict__.copy()
        proposal_payload.update(
            _snapshot_payload(
                opportunity,
                blocked_reason=decision.blocked_reason,
                blocked_reasons=decision.blocked_reasons,
            )
        )
        proposals.append(proposal_payload)
    return PreviewRuntimeContext(tuple(proposals), tuple(blocked))


__all__ = [
    "CurrentStateTables",
    "PreviewRuntimeContext",
    "build_preview_runtime_context",
    "load_current_state_tables",
]

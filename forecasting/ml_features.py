from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_feature_row(
    *,
    fair_value: dict[str, Any],
    opportunity: dict[str, Any] | None = None,
    bbo: dict[str, Any] | None = None,
    mapping: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, float]:
    current = now or datetime.now(timezone.utc)
    as_of_raw = fair_value.get("as_of")
    as_of = (
        datetime.fromisoformat(str(as_of_raw).replace("Z", "+00:00"))
        if as_of_raw not in (None, "")
        else current
    )
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    best_bid = float((bbo or {}).get("best_bid_yes") or 0.0)
    best_ask = float((bbo or {}).get("best_ask_yes") or 0.0)
    midpoint = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0
    return {
        "fair_yes_prob": float(fair_value.get("fair_yes_prob") or 0.0),
        "book_dispersion": float(fair_value.get("book_dispersion") or 0.0),
        "data_age_ms": float(fair_value.get("data_age_ms") or 0.0),
        "source_count": float(fair_value.get("source_count") or 0.0),
        "edge_after_costs_bps": float((opportunity or {}).get("edge_after_costs_bps") or 0.0),
        "fillable_size": float((opportunity or {}).get("fillable_size") or 0.0),
        "confidence": float((opportunity or {}).get("confidence") or 0.0),
        "best_bid_yes": best_bid,
        "best_ask_yes": best_ask,
        "spread_yes": max(0.0, best_ask - best_bid) if best_bid and best_ask else 0.0,
        "midpoint_yes": midpoint,
        "match_confidence": float((mapping or {}).get("match_confidence") or 0.0),
        "resolution_risk": float((mapping or {}).get("resolution_risk") or 0.0),
        "snapshot_age_seconds": max(0.0, (current - as_of).total_seconds()),
    }

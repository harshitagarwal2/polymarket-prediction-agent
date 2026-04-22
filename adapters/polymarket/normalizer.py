from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def normalize_market_row(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_id": str(
            payload.get("conditionId")
            or payload.get("condition_id")
            or payload.get("market")
            or payload.get("id")
            or ""
        ),
        "condition_id": payload.get("conditionId") or payload.get("condition_id"),
        "title": payload.get("question") or payload.get("title") or "",
        "description": payload.get("description"),
        "event_slug": payload.get("event_slug") or payload.get("eventSlug") or payload.get("slug"),
        "market_slug": payload.get("market_slug") or payload.get("marketSlug"),
        "category": payload.get("category"),
        "end_time": payload.get("endDate") or payload.get("end_date_iso"),
        "status": "open" if bool(payload.get("active", True)) else "closed",
        "raw_json": payload,
    }


def normalize_bbo_event(evt: dict[str, Any]) -> dict[str, Any]:
    book_ts = evt.get("timestamp") or evt.get("book_ts")
    if isinstance(book_ts, (int, float)):
        observed_at = datetime.fromtimestamp(float(book_ts) / 1000.0, tz=timezone.utc)
    else:
        observed_at = datetime.now(timezone.utc)
    best_bid = _to_float(evt.get("best_bid") or evt.get("bestBid") or evt.get("bid"))
    best_ask = _to_float(evt.get("best_ask") or evt.get("bestAsk") or evt.get("ask"))
    midpoint = (
        round((best_bid + best_ask) / 2, 6)
        if best_bid is not None and best_ask is not None
        else _to_float(evt.get("midpoint"))
    )
    spread = (
        round(best_ask - best_bid, 6)
        if best_bid is not None and best_ask is not None
        else None
    )
    source_age_ms = evt.get("source_age_ms")
    if source_age_ms in (None, ""):
        source_age_ms = max(0, int((datetime.now(timezone.utc) - observed_at).total_seconds() * 1000))
    return {
        "market_id": str(
            evt.get("market_id")
            or evt.get("conditionId")
            or evt.get("condition_id")
            or evt.get("asset_id")
            or evt.get("token_id")
            or ""
        ),
        "best_bid_yes": best_bid,
        "best_bid_yes_size": _to_float(evt.get("best_bid_size") or evt.get("bestBidSize")),
        "best_ask_yes": best_ask,
        "best_ask_yes_size": _to_float(evt.get("best_ask_size") or evt.get("bestAskSize")),
        "midpoint_yes": midpoint,
        "spread_yes": spread,
        "book_ts": observed_at.isoformat(),
        "source_age_ms": int(source_age_ms),
    }

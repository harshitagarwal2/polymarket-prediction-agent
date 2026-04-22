from __future__ import annotations


def mapping_priority(row: dict[str, object]) -> tuple[int, int, float, float, str]:
    is_active = bool(row.get("is_active", True))
    mismatch_reason = row.get("mismatch_reason")
    return (
        1 if is_active else 0,
        1 if mismatch_reason in (None, "") else 0,
        float(row.get("match_confidence") or 0.0),
        -float(row.get("resolution_risk") or 0.0),
        str(row.get("sportsbook_event_id") or ""),
    )


def best_mapping_rows(mapping: dict[str, object]) -> list[dict[str, object]]:
    best_by_market: dict[str, dict[str, object]] = {}
    for row in mapping.values():
        if not isinstance(row, dict):
            continue
        market_id = str(row.get("polymarket_market_id") or "")
        if not market_id:
            continue
        existing = best_by_market.get(market_id)
        if existing is None or mapping_priority(row) > mapping_priority(existing):
            best_by_market[market_id] = row
    rows = list(best_by_market.values())
    rows.sort(key=lambda row: str(row.get("polymarket_market_id") or ""))
    return rows


def best_mapping_by_market(
    mapping: dict[str, object],
) -> dict[str, dict[str, object]]:
    return {
        str(row.get("polymarket_market_id") or ""): row
        for row in best_mapping_rows(mapping)
    }

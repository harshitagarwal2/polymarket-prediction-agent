from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def american_to_decimal(odds: int) -> float:
    if odds == 0:
        raise ValueError("american odds must not be zero")
    if odds > 0:
        return 1.0 + (odds / 100.0)
    return 1.0 + (100.0 / abs(odds))


def implied_probability(*, american_odds: int | None = None, decimal_odds: float | None = None) -> float:
    if decimal_odds is None:
        if american_odds is None:
            raise ValueError("one of american_odds or decimal_odds is required")
        decimal_odds = american_to_decimal(int(american_odds))
    if decimal_odds <= 0:
        raise ValueError("decimal odds must be positive")
    return 1.0 / float(decimal_odds)


def normalize_odds_event(
    event: dict[str, Any],
    *,
    source: str,
    market_type: str,
    captured_at: datetime | None = None,
) -> list[dict[str, Any]]:
    observed_at = captured_at or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    for bookmaker in event.get("bookmakers") or []:
        for market in bookmaker.get("markets") or []:
            if market.get("key") != market_type:
                continue
            probabilities = []
            normalized_outcomes: list[dict[str, Any]] = []
            for outcome in market.get("outcomes") or []:
                price = outcome.get("price")
                if price in (None, ""):
                    continue
                if isinstance(price, int):
                    decimal_price = american_to_decimal(price)
                else:
                    decimal_price = float(price)
                prob = implied_probability(decimal_odds=decimal_price)
                probabilities.append(prob)
                normalized_outcomes.append(
                    {
                        "selection": str(outcome.get("name") or "").strip(),
                        "price_decimal": decimal_price,
                        "implied_prob": prob,
                    }
                )
            overround = sum(probabilities) if probabilities else None
            for outcome in normalized_outcomes:
                rows.append(
                    {
                        "sportsbook_event_id": str(event.get("id") or ""),
                        "source": source,
                        "market_type": market_type,
                        "selection": outcome["selection"],
                        "price_decimal": outcome["price_decimal"],
                        "implied_prob": outcome["implied_prob"],
                        "overround": overround,
                        "quote_ts": observed_at.isoformat(),
                        "source_age_ms": 0,
                        "raw_json": event,
                        "sport": event.get("sport_key"),
                        "league": event.get("sport_title"),
                        "home_team": event.get("home_team"),
                        "away_team": event.get("away_team"),
                        "start_time": event.get("commence_time"),
                    }
                )
    return rows

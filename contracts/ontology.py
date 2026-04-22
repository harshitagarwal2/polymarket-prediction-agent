from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from adapters import MarketSummary
from adapters.types import OutcomeSide, Venue


@dataclass(frozen=True)
class NormalizedContractIdentity:
    venue: Venue
    market_key: str
    group_key: str
    contract_type: str
    outcome: OutcomeSide
    title: str | None = None
    category: str | None = None
    sport: str | None = None
    series: str | None = None
    event_key: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    labels: tuple[str, ...] = ()


def contract_type_for_market(market: MarketSummary) -> str:
    for value in (
        market.sports_market_type,
        market.category,
        market.sport,
        market.series,
    ):
        if value not in (None, ""):
            return str(value).strip().lower()
    return "general"


def market_labels(market: MarketSummary) -> set[str]:
    labels: set[str] = set()
    for value in (
        market.category,
        market.sport,
        market.series,
        market.event_key,
        market.game_id,
        market.sports_market_type,
    ):
        if value not in (None, ""):
            labels.add(str(value).strip().lower())
    labels.update(tag.strip().lower() for tag in market.tags if tag.strip())
    return labels


def market_group_key(market: MarketSummary) -> str:
    raw = market.raw
    payload = raw
    if isinstance(raw, dict) and isinstance(raw.get("market"), dict):
        payload = raw["market"]
    if isinstance(payload, dict):
        for key in (
            "condition_id",
            "conditionId",
            "market_id",
            "marketId",
            "market",
            "slug",
            "question",
            "title",
        ):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
    if market.event_key:
        return market.event_key
    if market.title:
        return market.title
    if market.contract.title:
        return market.contract.title
    return market.contract.symbol


def market_hours_to_expiry(market: MarketSummary, *, now: datetime) -> float | None:
    if market.expires_at is None:
        return None
    expires_at = market.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return (expires_at - now).total_seconds() / 3600.0


def market_identity_from_market(market: MarketSummary) -> NormalizedContractIdentity:
    return NormalizedContractIdentity(
        venue=market.contract.venue,
        market_key=market.contract.market_key,
        group_key=market_group_key(market),
        contract_type=contract_type_for_market(market),
        outcome=market.contract.outcome,
        title=market.title or market.contract.title,
        category=market.category,
        sport=market.sport,
        series=market.series,
        event_key=market.event_key,
        game_id=market.game_id,
        sports_market_type=market.sports_market_type,
        labels=tuple(sorted(market_labels(market))),
    )

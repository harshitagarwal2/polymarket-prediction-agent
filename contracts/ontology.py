from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

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


class NormalizedMarketType(str, Enum):
    MONEYLINE_FULL_GAME = "moneyline_full_game"
    MONEYLINE_REGULATION = "moneyline_regulation"
    SPREAD_FULL_GAME = "spread_full_game"
    TOTAL_FULL_GAME = "total_full_game"
    GENERAL = "general"


def normalize_market_type(value: str | None) -> NormalizedMarketType:
    normalized = (value or "").strip().lower()
    if "regulation" in normalized:
        return NormalizedMarketType.MONEYLINE_REGULATION
    if "spread" in normalized:
        return NormalizedMarketType.SPREAD_FULL_GAME
    if "total" in normalized:
        return NormalizedMarketType.TOTAL_FULL_GAME
    if any(token in normalized for token in ("moneyline", "h2h", "winner", "win")):
        return NormalizedMarketType.MONEYLINE_FULL_GAME
    return NormalizedMarketType.GENERAL


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
        contract_type=normalize_market_type(contract_type_for_market(market)).value,
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

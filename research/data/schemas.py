from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    resolved = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return resolved.isoformat()


@dataclass(frozen=True)
class SportsInputRow:
    source: str
    captured_at: datetime = field(default_factory=_utc_now)
    source_event_id: str | None = None
    sport_key: str | None = None
    bookmaker: str | None = None
    decimal_odds: float | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    selection_name: str | None = None
    outcome: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    start_time: datetime | None = None
    implied_probability: float | None = None
    label: int | None = None
    raw: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "captured_at": self.captured_at.isoformat(),
            "source_event_id": self.source_event_id,
            "sport_key": self.sport_key,
            "bookmaker": self.bookmaker,
            "decimal_odds": self.decimal_odds,
            "event_key": self.event_key,
            "sport": self.sport,
            "series": self.series,
            "game_id": self.game_id,
            "sports_market_type": self.sports_market_type,
            "selection_name": self.selection_name,
            "outcome": self.outcome,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "start_time": _serialize_datetime(self.start_time),
            "implied_probability": self.implied_probability,
            "label": self.label,
            "raw": self.raw,
        }


@dataclass(frozen=True)
class PolymarketMarketRecord:
    layer: str
    captured_at: datetime = field(default_factory=_utc_now)
    contract: dict[str, object] | None = None
    market_key: str | None = None
    condition_id: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    title: str | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None
    midpoint: float | None = None
    volume: float | None = None
    start_time: datetime | None = None
    raw: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "layer": self.layer,
            "captured_at": self.captured_at.isoformat(),
            "contract": self.contract,
            "market_key": self.market_key,
            "condition_id": self.condition_id,
            "event_key": self.event_key,
            "sport": self.sport,
            "series": self.series,
            "game_id": self.game_id,
            "sports_market_type": self.sports_market_type,
            "title": self.title,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "best_bid_size": self.best_bid_size,
            "best_ask_size": self.best_ask_size,
            "midpoint": self.midpoint,
            "volume": self.volume,
            "start_time": _serialize_datetime(self.start_time),
            "raw": self.raw,
        }


@dataclass(frozen=True)
class TrainingSetRow:
    home_team: str
    away_team: str
    label: int
    record_id: str | None = None
    recorded_at: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    source: str | None = None
    market_key: str | None = None
    condition_id: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "home_team": self.home_team,
            "away_team": self.away_team,
            "label": int(self.label),
            "record_id": self.record_id,
            "recorded_at": self.recorded_at,
            "event_key": self.event_key,
            "sport": self.sport,
            "series": self.series,
            "game_id": self.game_id,
            "sports_market_type": self.sports_market_type,
            "source": self.source,
            "market_key": self.market_key,
            "condition_id": self.condition_id,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class InferenceDatasetRow:
    record_id: str
    recorded_at: str
    market_id: str
    sportsbook_event_id: str
    sportsbook_market_type: str | None = None
    normalized_market_type: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    condition_id: str | None = None
    market_title: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    commence_time: str | None = None
    bookmaker_count: int = 0
    sportsbook_source_age_ms: int | None = None
    polymarket_source_age_ms: int | None = None
    fair_value_age_ms: int | None = None
    source_age_ms: int | None = None
    has_polymarket_book: bool = False
    fair_yes_prob: float | None = None
    calibrated_fair_yes_prob: float | None = None
    lower_prob: float | None = None
    upper_prob: float | None = None
    book_dispersion: float | None = None
    best_bid_yes: float | None = None
    best_ask_yes: float | None = None
    midpoint_yes: float | None = None
    match_confidence: float | None = None
    resolution_risk: float | None = None
    inference_allowed: bool = False
    blocked_reason: str | None = None
    blocked_reasons: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "record_id": self.record_id,
            "recorded_at": self.recorded_at,
            "market_id": self.market_id,
            "sportsbook_event_id": self.sportsbook_event_id,
            "sportsbook_market_type": self.sportsbook_market_type,
            "normalized_market_type": self.normalized_market_type,
            "event_key": self.event_key,
            "sport": self.sport,
            "series": self.series,
            "game_id": self.game_id,
            "condition_id": self.condition_id,
            "market_title": self.market_title,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "commence_time": self.commence_time,
            "bookmaker_count": int(self.bookmaker_count),
            "sportsbook_source_age_ms": self.sportsbook_source_age_ms,
            "polymarket_source_age_ms": self.polymarket_source_age_ms,
            "fair_value_age_ms": self.fair_value_age_ms,
            "source_age_ms": self.source_age_ms,
            "has_polymarket_book": self.has_polymarket_book,
            "fair_yes_prob": self.fair_yes_prob,
            "calibrated_fair_yes_prob": self.calibrated_fair_yes_prob,
            "lower_prob": self.lower_prob,
            "upper_prob": self.upper_prob,
            "book_dispersion": self.book_dispersion,
            "best_bid_yes": self.best_bid_yes,
            "best_ask_yes": self.best_ask_yes,
            "midpoint_yes": self.midpoint_yes,
            "match_confidence": self.match_confidence,
            "resolution_risk": self.resolution_risk,
            "inference_allowed": self.inference_allowed,
            "blocked_reason": self.blocked_reason,
            "blocked_reasons": list(self.blocked_reasons),
        }

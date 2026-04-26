from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from adapters.types import MarketSummary, OrderBookSnapshot
from storage.postgres.bootstrap import (
    PostgresDsnNotConfiguredError,
    bootstrap_postgres,
    require_postgres_dsn,
    resolve_postgres_dsn,
)
from storage.postgres.models import (
    ExecutionFillRecord,
    ExecutionOrderRecord,
    FairValueRecord,
    MarketMappingRecord,
    ModelRegistryRecord,
    OpportunityRecord,
    PolymarketBalanceRecord,
    PolymarketBBORecord,
    PolymarketFillRecord,
    PolymarketMarketRecord,
    PolymarketOrderRecord,
    PolymarketPositionRecord,
    RuntimeCycleRecord,
    SportsbookEventRecord,
    SportsbookOddsRecord,
    TradeDecisionRecord,
    TradeAttributionRecord,
)
from storage.postgres.repositories import (
    BBORepository,
    ExecutionFillRepository,
    ExecutionOrderRepository,
    FairValueRepository,
    list_raw_capture_events,
    MappingRepository,
    MarketRepository,
    ModelRegistryRepository,
    OpportunityRepository,
    PolymarketBalanceRepository,
    PolymarketFillRepository,
    PolymarketOrderRepository,
    PolymarketPositionRepository,
    RuntimeCycleRepository,
    SourceHealthRepository,
    SportsbookEventRepository,
    SportsbookOddsRepository,
    TradeDecisionRepository,
    TradeAttributionRepository,
    append_raw_capture_event,
    read_capture_checkpoint,
    upsert_capture_checkpoint,
)


@dataclass(frozen=True)
class NormalizedMarketRow:
    market_key: str
    venue: str
    outcome: str
    title: str | None
    category: str | None
    sport: str | None
    series: str | None
    event_key: str | None
    game_id: str | None
    best_bid: float | None
    best_ask: float | None
    midpoint: float | None
    volume: float | None
    active: bool
    observed_at: str

    def to_payload(self) -> dict[str, object]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class NormalizedOrderBookRow:
    market_key: str
    best_bid: float | None
    best_ask: float | None
    midpoint: float | None
    bid_levels: int
    ask_levels: int
    observed_at: str

    def to_payload(self) -> dict[str, object]:
        return self.__dict__.copy()


def market_row_from_summary(
    market: MarketSummary,
    *,
    observed_at: datetime | None = None,
) -> NormalizedMarketRow:
    timestamp = observed_at or datetime.now(timezone.utc)
    return NormalizedMarketRow(
        market_key=market.contract.market_key,
        venue=market.contract.venue.value,
        outcome=market.contract.outcome.value,
        title=market.title or market.contract.title,
        category=market.category,
        sport=market.sport,
        series=market.series,
        event_key=market.event_key,
        game_id=market.game_id,
        best_bid=market.best_bid,
        best_ask=market.best_ask,
        midpoint=market.midpoint,
        volume=market.volume,
        active=market.active,
        observed_at=timestamp.isoformat(),
    )


def order_book_row_from_snapshot(book: OrderBookSnapshot) -> NormalizedOrderBookRow:
    return NormalizedOrderBookRow(
        market_key=book.contract.market_key,
        best_bid=book.best_bid,
        best_ask=book.best_ask,
        midpoint=book.midpoint,
        bid_levels=len(book.bids),
        ask_levels=len(book.asks),
        observed_at=book.observed_at.isoformat(),
    )


__all__ = [
    "BBORepository",
    "bootstrap_postgres",
    "ExecutionFillRecord",
    "ExecutionFillRepository",
    "ExecutionOrderRecord",
    "ExecutionOrderRepository",
    "FairValueRecord",
    "FairValueRepository",
    "MappingRepository",
    "MarketMappingRecord",
    "MarketRepository",
    "ModelRegistryRecord",
    "ModelRegistryRepository",
    "NormalizedMarketRow",
    "NormalizedOrderBookRow",
    "OpportunityRecord",
    "OpportunityRepository",
    "PostgresDsnNotConfiguredError",
    "PolymarketBalanceRecord",
    "PolymarketBalanceRepository",
    "PolymarketBBORecord",
    "PolymarketFillRecord",
    "PolymarketFillRepository",
    "PolymarketMarketRecord",
    "PolymarketOrderRecord",
    "PolymarketOrderRepository",
    "PolymarketPositionRecord",
    "PolymarketPositionRepository",
    "require_postgres_dsn",
    "RuntimeCycleRecord",
    "RuntimeCycleRepository",
    "SourceHealthRepository",
    "SportsbookEventRecord",
    "SportsbookEventRepository",
    "SportsbookOddsRecord",
    "SportsbookOddsRepository",
    "TradeDecisionRecord",
    "TradeDecisionRepository",
    "TradeAttributionRecord",
    "TradeAttributionRepository",
    "append_raw_capture_event",
    "list_raw_capture_events",
    "market_row_from_summary",
    "order_book_row_from_snapshot",
    "read_capture_checkpoint",
    "resolve_postgres_dsn",
    "upsert_capture_checkpoint",
]

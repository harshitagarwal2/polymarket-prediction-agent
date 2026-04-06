from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from adapters import MarketSummary
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderBookSnapshot,
    OrderIntent,
    PlacementResult,
    PositionSnapshot,
    Venue,
)


@dataclass(frozen=True)
class AdapterHealth:
    venue: Venue
    connected: bool
    message: str | None = None


class TradingAdapter(Protocol):
    venue: Venue

    def health(self) -> AdapterHealth: ...

    def get_order_book(self, contract: Contract) -> OrderBookSnapshot: ...

    def list_markets(self, limit: int = 100) -> list[MarketSummary]: ...

    def list_open_orders(
        self, contract: Contract | None = None
    ) -> list[NormalizedOrder]: ...

    def list_positions(
        self, contract: Contract | None = None
    ) -> list[PositionSnapshot]: ...

    def list_fills(self, contract: Contract | None = None) -> list[FillSnapshot]: ...

    def get_position(self, contract: Contract) -> PositionSnapshot: ...

    def get_balance(self) -> BalanceSnapshot: ...

    def get_account_snapshot(
        self, contract: Contract | None = None
    ) -> AccountSnapshot: ...

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult: ...

    def cancel_order(self, order_id: str) -> bool: ...

    def cancel_all(self, contract: Contract | None = None) -> int: ...

    def close(self) -> None: ...

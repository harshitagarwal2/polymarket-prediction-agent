from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from adapters.types import (
    BalanceSnapshot,
    Contract,
    NormalizedOrder,
    OrderBookSnapshot,
    OrderIntent,
    PositionSnapshot,
)


@dataclass(frozen=True)
class LinkedMarketRiskGraphSnapshot:
    market_key: str
    linked_event_key: str | None = None
    mutually_exclusive_group_key: str | None = None


@dataclass
class StrategyContext:
    contract: Contract
    book: OrderBookSnapshot
    position: PositionSnapshot
    balance: BalanceSnapshot
    open_orders: list[NormalizedOrder] = field(default_factory=list)
    fair_value: float | None = None
    metadata: dict = field(default_factory=dict)
    risk_graph: LinkedMarketRiskGraphSnapshot | None = None


class Strategy(Protocol):
    def generate_intents(self, context: StrategyContext) -> list[OrderIntent]: ...


class NoopStrategy:
    def generate_intents(self, context: StrategyContext) -> list[OrderIntent]:
        return []

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from adapters.types import Contract, OrderAction


@dataclass
class MarketSummary:
    contract: Contract
    title: str | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    midpoint: float | None = None
    volume: float | None = None
    category: str | None = None
    active: bool = True
    expires_at: datetime | None = None
    raw: Any | None = None


@dataclass(frozen=True)
class OpportunityCandidate:
    contract: Contract
    action: OrderAction
    fair_value: float
    market_price: float
    edge: float
    score: float
    rationale: str
    raw: Any | None = None

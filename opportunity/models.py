from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Opportunity:
    market_id: str
    side: str
    fair_yes_prob: float
    best_bid_yes: float
    best_ask_yes: float
    edge_buy_bps: float
    edge_sell_bps: float
    edge_after_costs_bps: float
    fillable_size: float
    confidence: float
    blocked_reason: Optional[str] = None

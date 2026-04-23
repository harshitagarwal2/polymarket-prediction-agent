from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional


def normalize_blocked_reasons(*values: object) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in (None, ""):
            continue
        candidates: Iterable[object]
        if isinstance(value, str):
            candidates = (value,)
        elif isinstance(value, Iterable):
            candidates = value
        else:
            candidates = (value,)
        for candidate in candidates:
            if candidate in (None, ""):
                continue
            reason = str(candidate).strip()
            if not reason or reason in seen:
                continue
            ordered.append(reason)
            seen.add(reason)
    return tuple(ordered)


@dataclass(frozen=True)
class Opportunity:
    market_id: str
    side: str
    fair_yes_prob: float
    best_bid_yes: float
    best_ask_yes: float
    edge_buy_bps: float
    edge_sell_bps: float
    edge_buy_after_costs_bps: float
    edge_sell_after_costs_bps: float
    edge_after_costs_bps: float
    fillable_size: float
    confidence: float
    blocked_reasons: tuple[str, ...] = ()
    blocked_reason: Optional[str] = None

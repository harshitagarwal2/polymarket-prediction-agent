from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderProposal:
    market_id: str
    side: str
    action: str
    price: float
    size: float
    tif: str
    rationale: str

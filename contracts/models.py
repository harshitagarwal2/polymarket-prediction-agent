from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ContractMatch:
    polymarket_market_id: str
    sportsbook_event_id: str
    sportsbook_market_type: str
    normalized_market_type: str
    match_confidence: float
    resolution_risk: float
    mismatch_reason: Optional[str] = None

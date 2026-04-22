from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from adapters import MarketSummary


@dataclass(frozen=True)
class FreezeWindowPolicy:
    freeze_minutes_before_start: int = 10


def freeze_reason_for_market(
    market: MarketSummary,
    *,
    policy: FreezeWindowPolicy,
    now: datetime | None = None,
) -> str | None:
    current = now or datetime.now(timezone.utc)
    if market.start_time is not None:
        seconds_to_start = (market.start_time - current).total_seconds()
        if seconds_to_start <= policy.freeze_minutes_before_start * 60:
            return "market within pre-start freeze window"
    if market.active is False:
        return "market inactive"
    return None

from __future__ import annotations

from datetime import datetime


def _minutes_until(
    *, captured_at: datetime | None, start_time: datetime | None
) -> float:
    if captured_at is None or start_time is None:
        return 0.0
    return (start_time - captured_at).total_seconds() / 60.0


def build_market_microstructure_features(
    *,
    best_bid: float | None,
    best_ask: float | None,
    volume: float | None = None,
    best_bid_size: float | None = None,
    best_ask_size: float | None = None,
    captured_at: datetime | None = None,
    start_time: datetime | None = None,
) -> dict[str, float]:
    bid = float(best_bid) if best_bid is not None else 0.0
    ask = float(best_ask) if best_ask is not None else 0.0
    bid_size = float(best_bid_size) if best_bid_size is not None else 0.0
    ask_size = float(best_ask_size) if best_ask_size is not None else 0.0
    midpoint = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else 0.0
    spread = max(ask - bid, 0.0) if ask > 0.0 and bid > 0.0 else 0.0
    quoted_liquidity = bid_size + ask_size
    imbalance = (
        (bid_size - ask_size) / quoted_liquidity if quoted_liquidity > 0.0 else 0.0
    )
    return {
        "best_bid": bid,
        "best_ask": ask,
        "midpoint": midpoint,
        "spread": spread,
        "best_bid_size": bid_size,
        "best_ask_size": ask_size,
        "quoted_liquidity": quoted_liquidity,
        "book_imbalance": imbalance,
        "time_to_start_minutes": _minutes_until(
            captured_at=captured_at,
            start_time=start_time,
        ),
        "volume": float(volume or 0.0),
    }

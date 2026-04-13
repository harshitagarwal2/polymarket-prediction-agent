from __future__ import annotations


def build_market_microstructure_features(
    *,
    best_bid: float | None,
    best_ask: float | None,
    volume: float | None = None,
) -> dict[str, float]:
    bid = float(best_bid) if best_bid is not None else 0.0
    ask = float(best_ask) if best_ask is not None else 0.0
    midpoint = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else 0.0
    spread = max(ask - bid, 0.0) if ask > 0.0 and bid > 0.0 else 0.0
    return {
        "best_bid": bid,
        "best_ask": ask,
        "midpoint": midpoint,
        "spread": spread,
        "volume": float(volume or 0.0),
    }

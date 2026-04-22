from __future__ import annotations

from dataclasses import dataclass

from adapters import MarketSummary
from adapters.types import OrderAction, OrderBookSnapshot


@dataclass(frozen=True)
class FillabilityEstimate:
    requested_quantity: float
    fillable_quantity: float
    completion_ratio: float
    spread: float | None
    visible_depth: float


def market_spread(market: MarketSummary) -> float | None:
    if market.best_bid is None or market.best_ask is None:
        return None
    return max(0.0, market.best_ask - market.best_bid)


def estimate_fillability_from_book(
    book: OrderBookSnapshot,
    *,
    action: OrderAction,
    quantity: float,
    limit_price: float | None = None,
    max_levels: int | None = None,
) -> FillabilityEstimate:
    estimate = book.estimate_fill(
        action,
        quantity,
        limit_price=limit_price,
        max_levels=max_levels,
    )
    return FillabilityEstimate(
        requested_quantity=quantity,
        fillable_quantity=estimate.filled_quantity,
        completion_ratio=(estimate.filled_quantity / quantity) if quantity > 0 else 0.0,
        spread=None,
        visible_depth=book.cumulative_quantity(action, limit_price=limit_price, max_levels=max_levels),
    )


def estimate_fillability_from_market(
    market: MarketSummary,
    *,
    action: OrderAction,
    quantity: float = 1.0,
) -> FillabilityEstimate:
    visible_depth = float(market.volume or 0.0)
    fillable_quantity = min(max(visible_depth, 0.0), quantity)
    return FillabilityEstimate(
        requested_quantity=quantity,
        fillable_quantity=fillable_quantity,
        completion_ratio=(fillable_quantity / quantity) if quantity > 0 else 0.0,
        spread=market_spread(market),
        visible_depth=visible_depth,
    )

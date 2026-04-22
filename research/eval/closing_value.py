from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClosingValueReport:
    signal_price: float
    closing_price: float
    fair_value: float | None
    side: str
    closing_edge_bps: float
    value_capture_bps: float


def evaluate_closing_value(
    *,
    signal_price: float,
    closing_price: float,
    side: str,
    fair_value: float | None = None,
) -> ClosingValueReport:
    if side == "sell_yes":
        closing_edge_bps = (signal_price - closing_price) * 10_000.0
        value_capture_bps = (
            (signal_price - fair_value) * 10_000.0 if fair_value is not None else 0.0
        )
    else:
        closing_edge_bps = (closing_price - signal_price) * 10_000.0
        value_capture_bps = (
            (fair_value - signal_price) * 10_000.0 if fair_value is not None else 0.0
        )
    return ClosingValueReport(
        signal_price=signal_price,
        closing_price=closing_price,
        fair_value=fair_value,
        side=side,
        closing_edge_bps=round(closing_edge_bps, 4),
        value_capture_bps=round(value_capture_bps, 4),
    )

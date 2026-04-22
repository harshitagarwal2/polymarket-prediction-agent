from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExchangeSimConfig:
    max_fill_ratio_per_step: float = 1.0
    cancel_latency_steps: int = 0
    stale_after_steps: int = 0
    price_move_bps_per_step: float = 0.0


def simulate_fillable_quantity(
    requested_quantity: float,
    visible_quantity: float,
    *,
    max_fill_ratio_per_step: float = 1.0,
) -> float:
    capped_visible = max(0.0, visible_quantity) * max(0.0, max_fill_ratio_per_step)
    return min(max(0.0, requested_quantity), capped_visible)


def cancel_effective_after_steps(
    current_step: int,
    *,
    cancel_requested_step: int,
    cancel_latency_steps: int,
) -> bool:
    return current_step >= cancel_requested_step + max(0, cancel_latency_steps)


def snapshot_is_stale(
    *,
    current_step: int,
    snapshot_step: int,
    stale_after_steps: int,
) -> bool:
    return (current_step - snapshot_step) > max(0, stale_after_steps)


def apply_wait_time_slippage(
    *,
    price: float,
    wait_steps: int,
    price_move_bps_per_step: float,
    is_buy: bool,
) -> float:
    drift = max(0.0, wait_steps) * max(0.0, price_move_bps_per_step) / 10_000.0
    if is_buy:
        return price * (1.0 + drift)
    return price * (1.0 - drift)

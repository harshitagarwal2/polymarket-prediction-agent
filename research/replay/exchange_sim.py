from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExchangeSimConfig:
    max_fill_ratio_per_step: float = 1.0
    cancel_latency_steps: int = 0
    stale_after_steps: int = 0


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

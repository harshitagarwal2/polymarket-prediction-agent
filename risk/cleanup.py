from __future__ import annotations

from dataclasses import dataclass
import time

from adapters.base import TradingAdapter
from adapters.types import Contract


@dataclass(frozen=True)
class CleanupResult:
    confirmed: bool
    polls: int
    consecutive_empty_polls: int
    remaining_order_ids: list[str]


class CleanupCoordinator:
    """Fail-closed cleanup helper inspired by upstream cancel/verify loops."""

    def __init__(self, adapter: TradingAdapter):
        self.adapter = adapter

    def cancel_all_and_verify(
        self,
        contract: Contract | None = None,
        *,
        stable_polls: int = 2,
        sleep_seconds: float = 0.5,
        max_wait_seconds: float = 10.0,
    ) -> CleanupResult:
        self.adapter.cancel_all(contract)
        consecutive_empty = 0
        polls = 0
        deadline = time.monotonic() + max(0.0, max_wait_seconds)
        while consecutive_empty < stable_polls:
            open_orders = self.adapter.list_open_orders(contract)
            polls += 1
            if open_orders:
                consecutive_empty = 0
            else:
                consecutive_empty += 1
            if consecutive_empty >= stable_polls:
                return CleanupResult(
                    confirmed=True,
                    polls=polls,
                    consecutive_empty_polls=consecutive_empty,
                    remaining_order_ids=[],
                )
            if time.monotonic() >= deadline:
                return CleanupResult(
                    confirmed=False,
                    polls=polls,
                    consecutive_empty_polls=consecutive_empty,
                    remaining_order_ids=[order.order_id for order in open_orders],
                )
            time.sleep(sleep_seconds)
        return CleanupResult(
            confirmed=True,
            polls=polls,
            consecutive_empty_polls=consecutive_empty,
            remaining_order_ids=[],
        )

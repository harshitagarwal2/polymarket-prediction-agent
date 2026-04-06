from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from adapters.base import TradingAdapter
from adapters.types import Contract, FillSnapshot, NormalizedOrder


@dataclass
class OrderState:
    """Simple in-memory projected order state inspired by upstream orderbook managers."""

    open_orders: dict[str, NormalizedOrder] = field(default_factory=dict)
    pending_place_ids: set[str] = field(default_factory=set)
    pending_cancel_ids: set[str] = field(default_factory=set)

    def sync(self, orders: list[NormalizedOrder]) -> None:
        self.open_orders = {order.order_id: order for order in orders}
        self.pending_place_ids.intersection_update(self.open_orders.keys())

    def mark_submitted(self, order: NormalizedOrder) -> None:
        self.open_orders[order.order_id] = order
        self.pending_place_ids.add(order.order_id)

    def mark_cancel_requested(self, order_id: str) -> None:
        self.pending_cancel_ids.add(order_id)

    def mark_cancelled(self, order_id: str) -> None:
        self.pending_cancel_ids.discard(order_id)
        self.pending_place_ids.discard(order_id)
        self.open_orders.pop(order_id, None)

    def restore_pending_cancels(self, order_ids: list[str] | set[str]) -> None:
        self.pending_cancel_ids = set(order_ids)

    def apply_live_order_upserts(self, orders: Iterable[NormalizedOrder]) -> int:
        applied = 0
        for order in orders:
            existing = self.open_orders.get(order.order_id)
            if existing == order:
                continue
            self.open_orders[order.order_id] = order
            applied += 1
        return applied

    def apply_live_terminal_orders(self, order_ids: Iterable[str]) -> int:
        applied = 0
        for order_id in order_ids:
            if order_id in self.open_orders:
                self.open_orders.pop(order_id, None)
                self.pending_place_ids.discard(order_id)
                self.pending_cancel_ids.discard(order_id)
                applied += 1
        return applied

    def resting_for_contract(self, market_key: str) -> list[NormalizedOrder]:
        return [
            order
            for order in self.open_orders.values()
            if order.contract.market_key == market_key
        ]


@dataclass(frozen=True)
class LifecycleDecision:
    order_id: str
    action: str
    reason: str
    contract_key: str | None = None


@dataclass
class OrderLifecyclePolicy:
    max_order_age_seconds: float = 30.0

    def evaluate(
        self, orders: list[NormalizedOrder], *, now: datetime | None = None
    ) -> list[LifecycleDecision]:
        now = now or datetime.now(timezone.utc)
        decisions: list[LifecycleDecision] = []
        for order in orders:
            age_seconds = (now - order.created_at).total_seconds()
            if age_seconds > self.max_order_age_seconds:
                decisions.append(
                    LifecycleDecision(
                        order_id=order.order_id,
                        action="cancel",
                        reason=(
                            f"order age {age_seconds:.2f}s exceeds {self.max_order_age_seconds:.2f}s"
                        ),
                        contract_key=order.contract.market_key,
                    )
                )
        return decisions


@dataclass
class OrderLifecycleManager:
    adapter: TradingAdapter
    policy: OrderLifecyclePolicy
    cancel_handler: Callable[[NormalizedOrder, str], Any] | None = None

    def cancel_stale_orders(
        self, contract: Contract | None = None, *, now: datetime | None = None
    ) -> list[LifecycleDecision]:
        orders = self.adapter.list_open_orders(contract)
        decisions = self.policy.evaluate(orders, now=now)
        orders_by_id = {order.order_id: order for order in orders}
        for decision in decisions:
            if decision.action == "cancel":
                order = orders_by_id.get(decision.order_id)
                if order is None:
                    continue
                if self.cancel_handler is not None:
                    self.cancel_handler(order, decision.reason)
                    continue
                self.adapter.cancel_order(decision.order_id)
        return decisions


@dataclass(frozen=True)
class OrderFillSummary:
    order_id: str
    filled_quantity: float
    remaining_quantity: float
    status: str


def summarize_fill_state(
    open_orders: list[NormalizedOrder], fills: list[FillSnapshot]
) -> list[OrderFillSummary]:
    fill_totals: dict[str, float] = {}
    for fill in fills:
        fill_totals[fill.order_id] = fill_totals.get(fill.order_id, 0.0) + fill.quantity

    summaries: list[OrderFillSummary] = []
    for order in open_orders:
        filled_quantity = fill_totals.get(order.order_id, 0.0)
        if filled_quantity > 0 and order.remaining_quantity > 0:
            status = "partial"
        elif filled_quantity > 0 and order.remaining_quantity <= 0:
            status = "filled"
        else:
            status = "open"
        summaries.append(
            OrderFillSummary(
                order_id=order.order_id,
                filled_quantity=filled_quantity,
                remaining_quantity=order.remaining_quantity,
                status=status,
            )
        )

    open_order_ids = {order.order_id for order in open_orders}
    for order_id, filled_quantity in fill_totals.items():
        if order_id in open_order_ids:
            continue
        summaries.append(
            OrderFillSummary(
                order_id=order_id,
                filled_quantity=filled_quantity,
                remaining_quantity=0.0,
                status="filled",
            )
        )
    return summaries

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    PositionSnapshot,
)


@dataclass
class AccountStateCache:
    balance: BalanceSnapshot | None = None
    positions: dict[str, PositionSnapshot] = field(default_factory=dict)
    open_orders: dict[str, NormalizedOrder] = field(default_factory=dict)
    fills: dict[str, FillSnapshot] = field(default_factory=dict)

    def sync_snapshot(
        self, snapshot: AccountSnapshot, contract: Contract | None = None
    ) -> None:
        self.balance = snapshot.balance

        if contract is None:
            self.positions = {
                position.contract.market_key: position
                for position in snapshot.positions
            }
            self.open_orders = {order.order_id: order for order in snapshot.open_orders}
            self.fills = {fill.fill_key: fill for fill in snapshot.fills}
            return

        self.positions = {
            key: value
            for key, value in self.positions.items()
            if key != contract.market_key
        }
        self.positions.update(
            {position.contract.market_key: position for position in snapshot.positions}
        )

        tracked_order_ids = {
            order_id
            for order_id, order in self.open_orders.items()
            if order.contract.market_key == contract.market_key
        }
        for order_id in tracked_order_ids:
            self.open_orders.pop(order_id, None)
        self.open_orders.update(
            {order.order_id: order for order in snapshot.open_orders}
        )

        tracked_fill_keys = {
            fill_key
            for fill_key, fill in self.fills.items()
            if fill.contract.market_key == contract.market_key
        }
        for fill_key in tracked_fill_keys:
            self.fills.pop(fill_key, None)
        self.fills.update({fill.fill_key: fill for fill in snapshot.fills})

    def position_for(self, contract: Contract) -> PositionSnapshot:
        return self.positions.get(
            contract.market_key, PositionSnapshot(contract=contract, quantity=0.0)
        )

    def open_orders_for(self, contract: Contract) -> list[NormalizedOrder]:
        return [
            order
            for order in self.open_orders.values()
            if order.contract.market_key == contract.market_key
        ]

    def fills_for(self, contract: Contract) -> list[FillSnapshot]:
        return [
            fill
            for fill in self.fills.values()
            if fill.contract.market_key == contract.market_key
        ]

    def record_submitted_order(self, order: NormalizedOrder) -> None:
        self.open_orders[order.order_id] = order

    def apply_live_order_upserts(self, orders: Iterable[NormalizedOrder]) -> int:
        applied = 0
        for order in orders:
            existing = self.open_orders.get(order.order_id)
            if existing == order:
                continue
            self.open_orders[order.order_id] = order
            applied += 1
        return applied

    def apply_live_fill_upserts(self, fills: Iterable[FillSnapshot]) -> int:
        applied = 0
        for fill in fills:
            existing = self.fills.get(fill.fill_key)
            if existing == fill:
                continue
            self.fills[fill.fill_key] = fill
            applied += 1
        return applied

    def apply_live_terminal_orders(self, order_ids: Iterable[str]) -> int:
        applied = 0
        for order_id in order_ids:
            if order_id in self.open_orders:
                self.open_orders.pop(order_id, None)
                applied += 1
        return applied


@dataclass(frozen=True)
class AccountTruthSummary:
    complete: bool
    issues: list[str]
    open_orders: int
    positions: int
    fills: int
    partial_fills: int
    balance_available: float | None
    balance_total: float | None
    open_order_notional: float
    reserved_buy_notional: float
    marked_position_notional: float
    observed_at: datetime | None


@dataclass(frozen=True)
class AccountTruthDriftReport:
    changed: bool
    open_orders_delta: int
    positions_delta: int
    fills_delta: int
    partial_fills_delta: int
    balance_available_delta: float | None
    balance_total_delta: float | None
    open_order_notional_delta: float
    reserved_buy_notional_delta: float
    marked_position_notional_delta: float


def summarize_account_snapshot(snapshot: AccountSnapshot) -> AccountTruthSummary:
    from engine.order_state import summarize_fill_state

    fill_summaries = summarize_fill_state(snapshot.open_orders, snapshot.fills)
    partial_fills = len(
        [summary for summary in fill_summaries if summary.status == "partial"]
    )
    open_order_notional = sum(
        order.remaining_quantity * order.price for order in snapshot.open_orders
    )
    reserved_buy_notional = sum(
        order.remaining_quantity * order.price
        for order in snapshot.open_orders
        if order.action.value == "buy"
    )
    marked_position_notional = 0.0
    for position in snapshot.positions:
        mark = position.mark_price
        if mark is None:
            mark = position.average_price
        if mark is None:
            mark = 0.0
        marked_position_notional += position.quantity * mark

    return AccountTruthSummary(
        complete=snapshot.complete,
        issues=list(snapshot.issues),
        open_orders=len(snapshot.open_orders),
        positions=len(snapshot.positions),
        fills=len(snapshot.fills),
        partial_fills=partial_fills,
        balance_available=snapshot.balance.available,
        balance_total=snapshot.balance.total,
        open_order_notional=open_order_notional,
        reserved_buy_notional=reserved_buy_notional,
        marked_position_notional=marked_position_notional,
        observed_at=snapshot.observed_at,
    )


def compare_truth_summaries(
    previous: AccountTruthSummary, current: AccountTruthSummary
) -> AccountTruthDriftReport:
    balance_available_delta = None
    if previous.balance_available is not None and current.balance_available is not None:
        balance_available_delta = current.balance_available - previous.balance_available

    balance_total_delta = None
    if previous.balance_total is not None and current.balance_total is not None:
        balance_total_delta = current.balance_total - previous.balance_total

    report = AccountTruthDriftReport(
        changed=(
            previous.complete != current.complete
            or previous.issues != current.issues
            or previous.open_orders != current.open_orders
            or previous.positions != current.positions
            or previous.fills != current.fills
            or previous.partial_fills != current.partial_fills
            or balance_available_delta not in (None, 0.0)
            or balance_total_delta not in (None, 0.0)
            or previous.open_order_notional != current.open_order_notional
            or previous.reserved_buy_notional != current.reserved_buy_notional
            or previous.marked_position_notional != current.marked_position_notional
        ),
        open_orders_delta=current.open_orders - previous.open_orders,
        positions_delta=current.positions - previous.positions,
        fills_delta=current.fills - previous.fills,
        partial_fills_delta=current.partial_fills - previous.partial_fills,
        balance_available_delta=balance_available_delta,
        balance_total_delta=balance_total_delta,
        open_order_notional_delta=current.open_order_notional
        - previous.open_order_notional,
        reserved_buy_notional_delta=current.reserved_buy_notional
        - previous.reserved_buy_notional,
        marked_position_notional_delta=current.marked_position_notional
        - previous.marked_position_notional,
    )
    return report

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import count
from typing import Any

from adapters.types import (
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderBookSnapshot,
    OrderIntent,
    OrderStatus,
    PositionSnapshot,
)


_PUBLIC_TRADE_METADATA_KEYS = frozenset({"mapping_risk", "replay_step_index"})


def _cancel_effective_after_steps(
    current_step: int,
    *,
    cancel_requested_step: int,
    cancel_latency_steps: int,
) -> bool:
    return current_step >= cancel_requested_step + max(0, cancel_latency_steps)


@dataclass
class PaperTrade:
    order_id: str
    contract: Contract
    action: OrderAction
    price: float
    quantity: float
    filled: bool
    reason: str
    requested_quantity: float | None = None
    remaining_quantity: float | None = None
    submitted_step: int | None = None
    fill_step: int | None = None
    wait_steps: int = 0
    resting: bool = False
    limit_price: float | None = None
    decision_best_bid: float | None = None
    decision_best_ask: float | None = None
    decision_midpoint: float | None = None
    decision_reference_price: float | None = None
    decision_fair_value: float | None = None
    visible_quantity: float | None = None
    levels_consumed: int = 0
    stale_data_flag: bool = False
    price_move_bps: float = 0.0
    cancel_requested_step: int | None = None
    cancel_effective_step: int | None = None
    cancel_race_fill: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def fill_ratio(self) -> float:
        requested = self.requested_quantity or 0.0
        if requested <= 0:
            return 0.0
        return self.quantity / requested

    @property
    def partial_fill(self) -> bool:
        return self.filled and (self.remaining_quantity or 0.0) > 0.0

    def _edge_bps(self, *, price: float | None) -> float | None:
        if price is None or price <= 0.0 or self.decision_fair_value is None:
            return None
        direction = 1.0 if self.action is OrderAction.BUY else -1.0
        return round(direction * (self.decision_fair_value - price) * 10_000.0, 4)

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "order_id": self.order_id,
            "market_id": self.contract.market_key,
            "action": self.action.value,
            "price": self.price,
            "filled_quantity": self.quantity,
            "filled": self.filled,
            "reason": self.reason,
            "fill_ratio": round(self.fill_ratio, 6),
            "partial_fill": self.partial_fill,
            "wait_steps": self.wait_steps,
            "resting": self.resting,
            "levels_consumed": self.levels_consumed,
            "stale_data_flag": self.stale_data_flag,
            "price_move_bps": self.price_move_bps,
        }
        optional_fields = {
            "requested_quantity": self.requested_quantity,
            "remaining_quantity": self.remaining_quantity,
            "submitted_step": self.submitted_step,
            "fill_step": self.fill_step,
            "limit_price": self.limit_price,
            "decision_best_bid": self.decision_best_bid,
            "decision_best_ask": self.decision_best_ask,
            "decision_midpoint": self.decision_midpoint,
            "decision_reference_price": self.decision_reference_price,
            "decision_fair_value": self.decision_fair_value,
            "visible_quantity": self.visible_quantity,
            "cancel_requested_step": self.cancel_requested_step,
            "cancel_effective_step": self.cancel_effective_step,
        }
        for key, value in optional_fields.items():
            if value is not None:
                payload[key] = value
        expected_edge_bps = self._edge_bps(price=self.decision_reference_price)
        realized_edge_bps = self._edge_bps(price=self.price) if self.filled else None
        if expected_edge_bps is not None:
            payload["expected_edge_bps"] = expected_edge_bps
        if realized_edge_bps is not None:
            payload["realized_edge_bps"] = realized_edge_bps
            if expected_edge_bps is not None:
                payload["slippage_bps"] = round(
                    realized_edge_bps - expected_edge_bps,
                    4,
                )
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        if self.cancel_race_fill:
            payload["cancel_race_fill"] = True
        return payload


@dataclass
class PaperExecutionConfig:
    max_fill_ratio_per_step: float = 1.0
    slippage_bps: float = 0.0
    resting_max_fill_ratio_per_step: float | None = None
    resting_fill_delay_steps: int = 0
    cancel_latency_steps: int = 0
    stale_after_steps: int = 0
    price_move_bps_per_step: float = 0.0


@dataclass
class PaperBroker:
    cash: float = 1000.0
    positions: dict[str, float] = field(default_factory=dict)
    trades: list[PaperTrade] = field(default_factory=list)
    open_orders: dict[str, NormalizedOrder] = field(default_factory=dict)
    pending_cancels: dict[str, int] = field(default_factory=dict)
    config: PaperExecutionConfig = field(default_factory=PaperExecutionConfig)
    contracts: dict[str, Contract] = field(default_factory=dict)
    current_step: int = 0
    _order_sequence: count = field(default_factory=lambda: count(1), repr=False)
    initial_cash: float = field(init=False)

    def __post_init__(self) -> None:
        self.initial_cash = self.cash

    def _remember_contract(self, contract: Contract) -> None:
        self.contracts[contract.market_key] = contract

    def position_for(self, contract: Contract) -> PositionSnapshot:
        self._remember_contract(contract)
        return PositionSnapshot(
            contract=contract, quantity=self.positions.get(contract.market_key, 0.0)
        )

    def open_orders_for(self, contract: Contract) -> list[NormalizedOrder]:
        self._remember_contract(contract)
        return [
            order
            for order in self.open_orders.values()
            if order.contract.market_key == contract.market_key
            and order.status in {OrderStatus.RESTING, OrderStatus.PARTIALLY_FILLED}
        ]

    def positions_snapshot(self) -> list[PositionSnapshot]:
        snapshots: list[PositionSnapshot] = []
        for market_key, quantity in self.positions.items():
            contract = self.contracts.get(market_key)
            if contract is None:
                continue
            snapshots.append(PositionSnapshot(contract=contract, quantity=quantity))
        return snapshots

    def open_order_snapshots(self) -> list[NormalizedOrder]:
        return list(self.open_orders.values())

    def _next_order_id(self) -> str:
        return f"paper-{next(self._order_sequence)}"

    def reserved_cash(self, *, exclude_order_id: str | None = None) -> float:
        return sum(
            order.remaining_quantity * order.price
            for order in self.open_orders.values()
            if order.action is OrderAction.BUY and order.order_id != exclude_order_id
        )

    def available_cash(self, *, exclude_order_id: str | None = None) -> float:
        return max(
            0.0, self.cash - self.reserved_cash(exclude_order_id=exclude_order_id)
        )

    def reserved_inventory(
        self, contract: Contract, *, exclude_order_id: str | None = None
    ) -> float:
        return sum(
            order.remaining_quantity
            for order in self.open_orders.values()
            if order.action is OrderAction.SELL
            and order.contract.market_key == contract.market_key
            and order.order_id != exclude_order_id
        )

    def available_inventory(
        self, contract: Contract, *, exclude_order_id: str | None = None
    ) -> float:
        position = self.positions.get(contract.market_key, 0.0)
        return max(
            0.0,
            position
            - self.reserved_inventory(contract, exclude_order_id=exclude_order_id),
        )

    def _record_fill(
        self,
        *,
        order: NormalizedOrder,
        book: OrderBookSnapshot,
        price: float,
        quantity: float,
        reason: str,
        requested_quantity: float,
        remaining_quantity: float,
        resting: bool,
        visible_quantity: float,
        levels_consumed: int,
        price_move_bps: float = 0.0,
    ) -> PaperTrade:
        contract = order.contract
        self._remember_contract(contract)
        raw = order.raw if isinstance(order.raw, dict) else {}
        submitted_step = raw.get("_submitted_step")
        cancel_requested_step = (
            raw.get("_cancel_requested_step")
            if isinstance(raw.get("_cancel_requested_step"), int)
            else None
        )
        cancel_effective_step = (
            raw.get("_cancel_effective_step")
            if isinstance(raw.get("_cancel_effective_step"), int)
            else None
        )
        wait_steps = 0
        if isinstance(submitted_step, int):
            wait_steps = max(0, self.current_step - submitted_step)
        trade = PaperTrade(
            order_id=order.order_id,
            contract=contract,
            action=order.action,
            price=price,
            quantity=quantity,
            filled=quantity > 0,
            reason=reason,
            requested_quantity=requested_quantity,
            remaining_quantity=remaining_quantity,
            submitted_step=submitted_step if isinstance(submitted_step, int) else None,
            fill_step=self.current_step,
            wait_steps=wait_steps,
            resting=resting,
            limit_price=order.price,
            decision_best_bid=_coerce_optional_float(raw.get("_decision_best_bid")),
            decision_best_ask=_coerce_optional_float(raw.get("_decision_best_ask")),
            decision_midpoint=_coerce_optional_float(raw.get("_decision_midpoint")),
            decision_reference_price=_coerce_optional_float(
                raw.get("_decision_reference_price")
            ),
            decision_fair_value=_coerce_optional_float(raw.get("_decision_fair_value")),
            visible_quantity=visible_quantity,
            levels_consumed=levels_consumed,
            stale_data_flag=_snapshot_is_stale(
                current_step=self.current_step,
                snapshot_step=submitted_step,
                stale_after_steps=self.config.stale_after_steps,
            )
            if isinstance(submitted_step, int)
            else False,
            price_move_bps=price_move_bps,
            cancel_requested_step=cancel_requested_step,
            cancel_effective_step=cancel_effective_step,
            cancel_race_fill=(
                quantity > 0
                and cancel_requested_step is not None
                and (
                    cancel_effective_step is None
                    or self.current_step < cancel_effective_step
                )
            ),
            metadata={
                key: value
                for key, value in raw.items()
                if isinstance(key, str) and key in _PUBLIC_TRADE_METADATA_KEYS
            },
        )
        self.trades.append(trade)
        return trade

    def _apply_fill(
        self, action: OrderAction, contract: Contract, price: float, quantity: float
    ) -> None:
        if quantity <= 0:
            return
        self._remember_contract(contract)
        if action is OrderAction.BUY:
            self.cash -= price * quantity
            self.positions[contract.market_key] = (
                self.positions.get(contract.market_key, 0.0) + quantity
            )
        else:
            self.cash += price * quantity
            self.positions[contract.market_key] = (
                self.positions.get(contract.market_key, 0.0) - quantity
            )

    def _walk_levels(
        self,
        action: OrderAction,
        book: OrderBookSnapshot,
        limit_price: float,
        quantity: float,
        *,
        fill_ratio: float,
    ) -> tuple[list[tuple[float, float]], float]:
        fills: list[tuple[float, float]] = []
        remaining = quantity
        levels = book.asks if action is OrderAction.BUY else book.bids
        for level in levels:
            crosses = (
                limit_price >= level.price
                if action is OrderAction.BUY
                else limit_price <= level.price
            )
            if not crosses or remaining <= 0:
                break
            effective_level_qty = _simulate_fillable_quantity(
                requested_quantity=remaining,
                visible_quantity=level.quantity,
                max_fill_ratio_per_step=fill_ratio,
            )
            fill_qty = min(remaining, effective_level_qty)
            fills.append((level.price, fill_qty))
            remaining -= fill_qty
        return fills, remaining

    def _resting_fill_ratio(self) -> float:
        if self.config.resting_max_fill_ratio_per_step is not None:
            return self.config.resting_max_fill_ratio_per_step
        return self.config.max_fill_ratio_per_step

    def _resting_order_ready(self, order: NormalizedOrder) -> bool:
        raw = order.raw if isinstance(order.raw, dict) else {}
        submitted_step = raw.get("_submitted_step")
        cancel_requested_step = (
            raw.get("_cancel_requested_step")
            if isinstance(raw.get("_cancel_requested_step"), int)
            else None
        )
        cancel_effective_step = (
            raw.get("_cancel_effective_step")
            if isinstance(raw.get("_cancel_effective_step"), int)
            else None
        )
        if not isinstance(submitted_step, int):
            return True
        return (
            self.current_step - submitted_step
        ) > self.config.resting_fill_delay_steps

    def _apply_slippage(self, action: OrderAction, price: float) -> float:
        multiplier = self.config.slippage_bps / 10000.0
        if action is OrderAction.BUY:
            return price * (1 + multiplier)
        return price * (1 - multiplier)

    def _apply_wait_time_slippage(
        self, *, order: NormalizedOrder, price: float, resting: bool
    ) -> tuple[float, float]:
        if not resting or self.config.price_move_bps_per_step <= 0.0:
            return price, 0.0
        raw = order.raw if isinstance(order.raw, dict) else {}
        submitted_step = raw.get("_submitted_step")
        if not isinstance(submitted_step, int):
            return price, 0.0
        wait_steps = max(0, self.current_step - submitted_step)
        if wait_steps <= 0:
            return price, 0.0
        price_move_bps = wait_steps * self.config.price_move_bps_per_step
        multiplier = price_move_bps / 10_000.0
        if order.action is OrderAction.BUY:
            return price * (1.0 + multiplier), price_move_bps
        return price * (1.0 - multiplier), price_move_bps

    def _execute_order(
        self, book: OrderBookSnapshot, order: NormalizedOrder, *, resting: bool
    ) -> list[PaperTrade]:
        if resting and not self._resting_order_ready(order):
            return []
        requested_quantity = order.remaining_quantity
        visible_quantity = book.cumulative_quantity(
            order.action,
            limit_price=order.price,
        )
        fills, remaining = self._walk_levels(
            order.action,
            book,
            order.price,
            requested_quantity,
            fill_ratio=(
                self._resting_fill_ratio()
                if resting
                else self.config.max_fill_ratio_per_step
            ),
        )
        if (
            order.action is OrderAction.SELL
            and self.available_inventory(
                order.contract, exclude_order_id=order.order_id
            )
            <= 0
        ):
            return [
                self._record_fill(
                    order=order,
                    book=book,
                    price=order.price,
                    quantity=0.0,
                    reason="insufficient paper inventory",
                    requested_quantity=requested_quantity,
                    remaining_quantity=requested_quantity,
                    resting=resting,
                    visible_quantity=visible_quantity,
                    levels_consumed=0,
                )
            ]

        results: list[PaperTrade] = []
        spent = 0.0
        for level_index, (fill_price, fill_qty) in enumerate(fills, start=1):
            effective_price = self._apply_slippage(order.action, fill_price)
            effective_price, price_move_bps = self._apply_wait_time_slippage(
                order=order,
                price=effective_price,
                resting=resting,
            )
            if order.action is OrderAction.BUY:
                affordable_qty = fill_qty
                if effective_price > 0:
                    affordable_qty = min(
                        fill_qty,
                        self.available_cash(exclude_order_id=order.order_id)
                        / effective_price,
                    )
                fill_qty = max(0.0, affordable_qty)
            else:
                available_inventory = self.available_inventory(
                    order.contract, exclude_order_id=order.order_id
                )
                fill_qty = min(fill_qty, available_inventory)
            if fill_qty <= 0:
                continue
            self._apply_fill(order.action, order.contract, effective_price, fill_qty)
            spent += fill_qty
            remaining_quantity = max(0.0, requested_quantity - spent)
            results.append(
                self._record_fill(
                    order=order,
                    book=book,
                    price=effective_price,
                    quantity=fill_qty,
                    reason="book crossed",
                    requested_quantity=requested_quantity,
                    remaining_quantity=remaining_quantity,
                    resting=resting,
                    visible_quantity=visible_quantity,
                    levels_consumed=level_index,
                    price_move_bps=price_move_bps,
                )
            )

        order.remaining_quantity = max(0.0, order.remaining_quantity - spent)
        if order.remaining_quantity <= 0:
            order.status = OrderStatus.FILLED
            self.open_orders.pop(order.order_id, None)
            self.pending_cancels.pop(order.order_id, None)
        elif spent > 0:
            order.status = OrderStatus.PARTIALLY_FILLED
            self.open_orders[order.order_id] = order
        else:
            order.status = OrderStatus.RESTING
            self.open_orders[order.order_id] = order
            results.append(
                self._record_fill(
                    order=order,
                    book=book,
                    price=order.price,
                    quantity=0.0,
                    reason="resting on book",
                    requested_quantity=requested_quantity,
                    remaining_quantity=order.remaining_quantity,
                    resting=resting,
                    visible_quantity=visible_quantity,
                    levels_consumed=0,
                )
            )
        return results

    def request_cancel(self, order_id: str) -> bool:
        order = self.open_orders.get(order_id)
        if order is None:
            return False
        self.pending_cancels[order_id] = self.current_step
        raw = order.raw if isinstance(order.raw, dict) else {}
        raw["_cancel_requested_step"] = self.current_step
        raw["_cancel_effective_step"] = self.current_step + max(
            0, self.config.cancel_latency_steps
        )
        order.raw = raw
        self.open_orders[order_id] = order
        return True

    def _apply_effective_cancels(self) -> None:
        to_remove: list[str] = []
        for order_id, requested_step in self.pending_cancels.items():
            if _cancel_effective_after_steps(
                self.current_step,
                cancel_requested_step=requested_step,
                cancel_latency_steps=self.config.cancel_latency_steps,
            ):
                to_remove.append(order_id)
        for order_id in to_remove:
            self.open_orders.pop(order_id, None)
            self.pending_cancels.pop(order_id, None)

    def submit_intents(
        self, book: OrderBookSnapshot, intents: list[OrderIntent]
    ) -> list[PaperTrade]:
        results: list[PaperTrade] = []
        for intent in intents:
            raw = dict(intent.metadata)
            raw["_submitted_step"] = self.current_step
            raw["_decision_fair_value"] = raw.get("fair_value")
            raw["_decision_best_bid"] = book.best_bid
            raw["_decision_best_ask"] = book.best_ask
            raw["_decision_midpoint"] = book.midpoint
            raw["_decision_reference_price"] = (
                book.best_ask if intent.action is OrderAction.BUY else book.best_bid
            )
            raw["_decision_visible_quantity"] = book.cumulative_quantity(
                intent.action,
                limit_price=intent.price,
            )
            order = NormalizedOrder(
                order_id=self._next_order_id(),
                contract=intent.contract,
                action=intent.action,
                price=intent.price,
                quantity=intent.quantity,
                remaining_quantity=intent.quantity,
                status=OrderStatus.PENDING,
                raw=raw,
            )
            results.extend(self._execute_order(book, order, resting=False))
        return results

    def advance(self, book: OrderBookSnapshot) -> list[PaperTrade]:
        self.current_step += 1
        self._apply_effective_cancels()
        results: list[PaperTrade] = []
        current_orders = list(self.open_orders.values())
        for order in current_orders:
            if order.contract.market_key != book.contract.market_key:
                continue
            results.extend(self._execute_order(book, order, resting=True))
        return results

    def execute(
        self, book: OrderBookSnapshot, intents: list[OrderIntent]
    ) -> list[PaperTrade]:
        return self.submit_intents(book, intents)

    def portfolio_value(self, mark_prices: dict[str, float]) -> float:
        value = self.cash
        for market_key, quantity in self.positions.items():
            value += quantity * mark_prices.get(market_key, 0.0)
        return value


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _snapshot_is_stale(
    *, current_step: int, snapshot_step: int | None, stale_after_steps: int
) -> bool:
    if snapshot_step is None or stale_after_steps <= 0:
        return False
    return (current_step - snapshot_step) > max(0, stale_after_steps)


def _simulate_fillable_quantity(
    *,
    requested_quantity: float,
    visible_quantity: float,
    max_fill_ratio_per_step: float,
) -> float:
    capped_visible = max(0.0, visible_quantity) * max(0.0, max_fill_ratio_per_step)
    return min(max(0.0, requested_quantity), capped_visible)

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import count

from adapters.types import (
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderBookSnapshot,
    OrderIntent,
    OrderStatus,
    PositionSnapshot,
)


@dataclass
class PaperTrade:
    order_id: str
    contract: Contract
    action: OrderAction
    price: float
    quantity: float
    filled: bool
    reason: str


@dataclass
class PaperExecutionConfig:
    max_fill_ratio_per_step: float = 1.0
    slippage_bps: float = 0.0


@dataclass
class PaperBroker:
    cash: float = 1000.0
    positions: dict[str, float] = field(default_factory=dict)
    trades: list[PaperTrade] = field(default_factory=list)
    open_orders: dict[str, NormalizedOrder] = field(default_factory=dict)
    config: PaperExecutionConfig = field(default_factory=PaperExecutionConfig)
    _order_sequence: count = field(default_factory=lambda: count(1), repr=False)
    initial_cash: float = field(init=False)

    def __post_init__(self) -> None:
        self.initial_cash = self.cash

    def position_for(self, contract: Contract) -> PositionSnapshot:
        return PositionSnapshot(
            contract=contract, quantity=self.positions.get(contract.market_key, 0.0)
        )

    def open_orders_for(self, contract: Contract) -> list[NormalizedOrder]:
        return [
            order
            for order in self.open_orders.values()
            if order.contract.market_key == contract.market_key
            and order.status in {OrderStatus.RESTING, OrderStatus.PARTIALLY_FILLED}
        ]

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
        order_id: str,
        contract: Contract,
        action: OrderAction,
        price: float,
        quantity: float,
        reason: str,
    ) -> PaperTrade:
        trade = PaperTrade(
            order_id=order_id,
            contract=contract,
            action=action,
            price=price,
            quantity=quantity,
            filled=quantity > 0,
            reason=reason,
        )
        self.trades.append(trade)
        return trade

    def _apply_fill(
        self, action: OrderAction, contract: Contract, price: float, quantity: float
    ) -> None:
        if quantity <= 0:
            return
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
            effective_level_qty = level.quantity * self.config.max_fill_ratio_per_step
            fill_qty = min(remaining, effective_level_qty)
            fills.append((level.price, fill_qty))
            remaining -= fill_qty
        return fills, remaining

    def _apply_slippage(self, action: OrderAction, price: float) -> float:
        multiplier = self.config.slippage_bps / 10000.0
        if action is OrderAction.BUY:
            return price * (1 + multiplier)
        return price * (1 - multiplier)

    def _execute_order(
        self, book: OrderBookSnapshot, order: NormalizedOrder
    ) -> list[PaperTrade]:
        fills, remaining = self._walk_levels(
            order.action, book, order.price, order.remaining_quantity
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
                    order_id=order.order_id,
                    contract=order.contract,
                    action=order.action,
                    price=order.price,
                    quantity=0.0,
                    reason="insufficient paper inventory",
                )
            ]

        results: list[PaperTrade] = []
        spent = 0.0
        for fill_price, fill_qty in fills:
            effective_price = self._apply_slippage(order.action, fill_price)
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
            results.append(
                self._record_fill(
                    order_id=order.order_id,
                    contract=order.contract,
                    action=order.action,
                    price=effective_price,
                    quantity=fill_qty,
                    reason="book crossed",
                )
            )

        order.remaining_quantity = max(0.0, order.remaining_quantity - spent)
        if order.remaining_quantity <= 0:
            order.status = OrderStatus.FILLED
            self.open_orders.pop(order.order_id, None)
        elif spent > 0:
            order.status = OrderStatus.PARTIALLY_FILLED
            self.open_orders[order.order_id] = order
        else:
            order.status = OrderStatus.RESTING
            self.open_orders[order.order_id] = order
            results.append(
                self._record_fill(
                    order_id=order.order_id,
                    contract=order.contract,
                    action=order.action,
                    price=order.price,
                    quantity=0.0,
                    reason="resting on book",
                )
            )
        return results

    def submit_intents(
        self, book: OrderBookSnapshot, intents: list[OrderIntent]
    ) -> list[PaperTrade]:
        results: list[PaperTrade] = []
        for intent in intents:
            order = NormalizedOrder(
                order_id=self._next_order_id(),
                contract=intent.contract,
                action=intent.action,
                price=intent.price,
                quantity=intent.quantity,
                remaining_quantity=intent.quantity,
                status=OrderStatus.PENDING,
                raw=intent.metadata,
            )
            results.extend(self._execute_order(book, order))
        return results

    def advance(self, book: OrderBookSnapshot) -> list[PaperTrade]:
        results: list[PaperTrade] = []
        current_orders = list(self.open_orders.values())
        for order in current_orders:
            if order.contract.market_key != book.contract.market_key:
                continue
            results.extend(self._execute_order(book, order))
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

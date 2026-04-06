from __future__ import annotations

from dataclasses import dataclass, field

from adapters.types import NormalizedOrder, OrderIntent, PositionSnapshot


@dataclass
class RiskLimits:
    max_global_contracts: int = 20
    max_contracts_per_market: int = 5
    reserve_contracts_buffer: int = 0
    max_order_notional: float | None = None
    min_price: float = 0.01
    max_price: float = 0.99
    max_daily_loss: float | None = None


@dataclass
class Rejection:
    intent: OrderIntent
    reason: str


@dataclass
class RiskDecision:
    approved: list[OrderIntent] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)


@dataclass
class RiskState:
    daily_realized_pnl: float = 0.0


class RiskEngine:
    def __init__(self, limits: RiskLimits, state: RiskState | None = None):
        self.limits = limits
        self.state = state or RiskState()

    def _order_exposure(self, order: NormalizedOrder) -> float:
        if order.action.value == "sell" and order.reduce_only:
            return 0.0
        return order.remaining_quantity

    def _current_market_exposure(
        self,
        market_key: str,
        positions: list[PositionSnapshot],
        open_orders: list[NormalizedOrder],
    ) -> float:
        position_quantity = sum(
            abs(position.quantity)
            for position in positions
            if position.contract.market_key == market_key
        )
        resting = sum(
            self._order_exposure(order)
            for order in open_orders
            if order.contract.market_key == market_key
        )
        return position_quantity + resting

    def _current_global_exposure(
        self,
        positions: list[PositionSnapshot],
        open_orders: list[NormalizedOrder],
    ) -> float:
        position_quantity = sum(abs(position.quantity) for position in positions)
        resting = sum(self._order_exposure(order) for order in open_orders)
        return position_quantity + resting

    def _resolve_positions(
        self,
        position: PositionSnapshot,
        positions: list[PositionSnapshot] | None,
    ) -> list[PositionSnapshot]:
        resolved = {
            existing.contract.market_key: existing for existing in positions or []
        }
        resolved[position.contract.market_key] = position
        return list(resolved.values())

    def evaluate(
        self,
        intents: list[OrderIntent],
        *,
        position: PositionSnapshot,
        open_orders: list[NormalizedOrder],
        positions: list[PositionSnapshot] | None = None,
    ) -> RiskDecision:
        decision = RiskDecision()
        current_positions = self._resolve_positions(position, positions)

        if (
            self.limits.max_daily_loss is not None
            and self.state.daily_realized_pnl <= -abs(self.limits.max_daily_loss)
        ):
            for intent in intents:
                decision.rejected.append(Rejection(intent, "daily loss limit reached"))
            return decision

        running_market_exposure = {
            current_position.contract.market_key: self._current_market_exposure(
                current_position.contract.market_key,
                current_positions,
                open_orders,
            )
            for current_position in current_positions
        }
        running_global_exposure = self._current_global_exposure(
            current_positions, open_orders
        )
        remaining_reduce_only_capacity = {
            current_position.contract.market_key: max(current_position.quantity, 0.0)
            for current_position in current_positions
        }

        for intent in intents:
            if (
                intent.price < self.limits.min_price
                or intent.price > self.limits.max_price
            ):
                decision.rejected.append(
                    Rejection(intent, "price outside allowed range")
                )
                continue
            if intent.quantity <= 0:
                decision.rejected.append(Rejection(intent, "quantity must be positive"))
                continue
            if (
                self.limits.max_order_notional is not None
                and intent.notional > self.limits.max_order_notional
            ):
                decision.rejected.append(
                    Rejection(intent, "order notional exceeds cap")
                )
                continue

            market_key = intent.contract.market_key
            market_exposure = running_market_exposure.get(
                market_key,
                self._current_market_exposure(
                    market_key, current_positions, open_orders
                ),
            )
            exposure_increase = intent.quantity
            if intent.action.value == "sell" and intent.reduce_only:
                reduce_only_capacity = remaining_reduce_only_capacity.get(
                    market_key, 0.0
                )
                exposure_increase = max(0.0, intent.quantity - reduce_only_capacity)
            projected_market = market_exposure + exposure_increase
            if projected_market > self.limits.max_contracts_per_market:
                decision.rejected.append(
                    Rejection(intent, "per-market exposure cap exceeded")
                )
                continue

            projected_global = running_global_exposure + exposure_increase
            max_global = max(
                0,
                self.limits.max_global_contracts - self.limits.reserve_contracts_buffer,
            )
            if projected_global > max_global:
                decision.rejected.append(
                    Rejection(intent, "global exposure cap exceeded")
                )
                continue

            running_market_exposure[market_key] = projected_market
            running_global_exposure = projected_global
            if intent.action.value == "buy":
                remaining_reduce_only_capacity[market_key] = (
                    remaining_reduce_only_capacity.get(market_key, 0.0)
                    + intent.quantity
                )
            elif intent.action.value == "sell" and intent.reduce_only:
                remaining_reduce_only_capacity[market_key] = max(
                    0.0,
                    remaining_reduce_only_capacity.get(market_key, 0.0)
                    - intent.quantity,
                )
            decision.approved.append(intent)

        return decision

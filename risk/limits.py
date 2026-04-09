from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Iterable

from adapters.types import MarketSummary, NormalizedOrder, OrderIntent, PositionSnapshot


@dataclass
class RiskLimits:
    max_global_contracts: int = 20
    max_contracts_per_market: int = 5
    max_contracts_per_event: int | None = None
    reserve_contracts_buffer: int = 0
    max_order_notional: float | None = None
    min_price: float = 0.01
    max_price: float = 0.99
    max_daily_loss: float | None = None
    enforce_atomic_batches: bool = True


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

    def daily_loss_limit_reached(self, max_daily_loss: float | None) -> bool:
        if max_daily_loss is None:
            return False
        return self.daily_realized_pnl <= -abs(max_daily_loss)


class RiskEngine:
    def __init__(self, limits: RiskLimits, state: RiskState | None = None):
        self.limits = limits
        self.state = state or RiskState()
        self.market_key_to_event_exposure_key: dict[str, str] = {}

    def _normalize_event_component(self, value: str | None) -> str | None:
        if value in (None, ""):
            return None
        normalized = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
        return normalized or None

    def _event_exposure_key(
        self,
        *,
        event_key: str | None = None,
        sport: str | None = None,
        series: str | None = None,
        game_id: str | None = None,
    ) -> str | None:
        normalized_event_key = self._normalize_event_component(event_key)
        if normalized_event_key is not None:
            return f"event:{normalized_event_key}"
        normalized_sport = self._normalize_event_component(sport)
        normalized_series = self._normalize_event_component(series)
        normalized_game_id = self._normalize_event_component(game_id)
        if (
            normalized_sport is None
            or normalized_series is None
            or normalized_game_id is None
        ):
            return None
        return f"composite:{normalized_sport}:{normalized_series}:{normalized_game_id}"

    def register_market_event(
        self,
        market_key: str,
        *,
        event_key: str | None = None,
        sport: str | None = None,
        series: str | None = None,
        game_id: str | None = None,
    ) -> str | None:
        exposure_key = self._event_exposure_key(
            event_key=event_key,
            sport=sport,
            series=series,
            game_id=game_id,
        )
        if exposure_key is None:
            return None
        self.market_key_to_event_exposure_key[str(market_key)] = exposure_key
        return exposure_key

    def register_markets(self, markets: Iterable[MarketSummary]) -> None:
        for market in markets:
            self.register_market_event(
                market.contract.market_key,
                event_key=market.event_key,
                sport=market.sport,
                series=market.series,
                game_id=market.game_id,
            )

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

    def _current_event_exposure(
        self,
        event_exposure_key: str,
        positions: list[PositionSnapshot],
        open_orders: list[NormalizedOrder],
    ) -> float:
        position_quantity = sum(
            abs(position.quantity)
            for position in positions
            if self.market_key_to_event_exposure_key.get(position.contract.market_key)
            == event_exposure_key
        )
        resting = sum(
            self._order_exposure(order)
            for order in open_orders
            if self.market_key_to_event_exposure_key.get(order.contract.market_key)
            == event_exposure_key
        )
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

    def _finalize_atomic_batch(
        self,
        intents: list[OrderIntent],
        decision: RiskDecision,
    ) -> RiskDecision:
        if (
            not self.limits.enforce_atomic_batches
            or len(intents) <= 1
            or not decision.approved
            or not decision.rejected
        ):
            return decision

        rejection_by_intent = {
            id(rejection.intent): rejection for rejection in decision.rejected
        }
        companion_reason = decision.rejected[0].reason
        batch_rejections: list[Rejection] = []
        for intent in intents:
            rejection = rejection_by_intent.get(id(intent))
            if rejection is not None:
                batch_rejections.append(rejection)
                continue
            batch_rejections.append(
                Rejection(
                    intent,
                    f"batched with rejected intent: {companion_reason}",
                )
            )
        return RiskDecision(approved=[], rejected=batch_rejections)

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

        if self.state.daily_loss_limit_reached(self.limits.max_daily_loss):
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
        running_event_exposure = {
            event_exposure_key: self._current_event_exposure(
                event_exposure_key,
                current_positions,
                open_orders,
            )
            for event_exposure_key in {
                self.market_key_to_event_exposure_key.get(
                    current_position.contract.market_key
                )
                for current_position in current_positions
            }.union(
                {
                    self.market_key_to_event_exposure_key.get(order.contract.market_key)
                    for order in open_orders
                }
            )
            if event_exposure_key is not None
        }
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

            event_exposure_key = self.market_key_to_event_exposure_key.get(market_key)
            projected_event = None
            if (
                self.limits.max_contracts_per_event is not None
                and event_exposure_key is not None
            ):
                current_event_exposure = running_event_exposure.get(
                    event_exposure_key,
                    self._current_event_exposure(
                        event_exposure_key,
                        current_positions,
                        open_orders,
                    ),
                )
                projected_event = current_event_exposure + exposure_increase
                if projected_event > self.limits.max_contracts_per_event:
                    decision.rejected.append(
                        Rejection(intent, "per-event exposure cap exceeded")
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
            if projected_event is not None and event_exposure_key is not None:
                running_event_exposure[event_exposure_key] = projected_event
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

        return self._finalize_atomic_batch(intents, decision)

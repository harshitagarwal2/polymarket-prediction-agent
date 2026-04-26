from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Iterable

from adapters.types import MarketSummary, NormalizedOrder, OrderIntent, PositionSnapshot
from engine.interfaces import LinkedMarketRiskGraphSnapshot
from risk.correlated_exposure import CorrelatedExposureGraph


@dataclass
class RiskLimits:
    max_global_contracts: int = 20
    max_contracts_per_market: int = 5
    max_contracts_per_event: int | None = None
    max_notional_per_event: float | None = None
    reserve_contracts_buffer: int = 0
    max_order_notional: float | None = None
    min_price: float = 0.01
    max_price: float = 0.99
    max_daily_loss: float | None = None
    max_weekly_loss: float | None = None
    max_cumulative_loss: float | None = None
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
    weekly_realized_pnl: float = 0.0
    cumulative_realized_pnl: float = 0.0

    def daily_loss_limit_reached(self, max_daily_loss: float | None) -> bool:
        if max_daily_loss is None:
            return False
        return self.daily_realized_pnl <= -abs(max_daily_loss)

    def weekly_loss_limit_reached(self, max_weekly_loss: float | None) -> bool:
        if max_weekly_loss is None:
            return False
        return self.weekly_realized_pnl <= -abs(max_weekly_loss)

    def cumulative_loss_limit_reached(self, max_cumulative_loss: float | None) -> bool:
        if max_cumulative_loss is None:
            return False
        return self.cumulative_realized_pnl <= -abs(max_cumulative_loss)


class RiskEngine:
    def __init__(self, limits: RiskLimits, state: RiskState | None = None):
        self.limits = limits
        self.state = state or RiskState()
        self.market_key_to_event_exposure_key: dict[str, str] = {}
        self.market_key_to_mutually_exclusive_group_key: dict[str, str] = {}
        self.correlated_graph = CorrelatedExposureGraph()

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
        mutually_exclusive_group_key: str | None = None,
    ) -> str | None:
        exposure_key = self._event_exposure_key(
            event_key=event_key,
            sport=sport,
            series=series,
            game_id=game_id,
        )
        if mutually_exclusive_group_key not in (None, ""):
            self.market_key_to_mutually_exclusive_group_key[str(market_key)] = str(
                mutually_exclusive_group_key
            )
        self.correlated_graph.register_market(
            str(market_key),
            cluster_key=exposure_key,
            mutually_exclusive_group_key=mutually_exclusive_group_key,
        )
        if exposure_key is None:
            return None
        self.market_key_to_event_exposure_key[str(market_key)] = exposure_key
        return exposure_key

    def _market_group_key_from_market(self, market: MarketSummary) -> str:
        raw = market.raw
        payload = raw
        if isinstance(raw, dict) and isinstance(raw.get("market"), dict):
            payload = raw["market"]
        if isinstance(payload, dict):
            for key in (
                "condition_id",
                "conditionId",
                "market_id",
                "marketId",
                "market",
                "slug",
                "question",
                "title",
            ):
                value = payload.get(key)
                if value not in (None, ""):
                    return str(value)
        if market.title not in (None, ""):
            return str(market.title)
        if market.contract.title not in (None, ""):
            return str(market.contract.title)
        return market.contract.symbol

    def register_markets(self, markets: Iterable[MarketSummary]) -> None:
        for market in markets:
            self.register_market_event(
                market.contract.market_key,
                event_key=market.event_key,
                sport=market.sport,
                series=market.series,
                game_id=market.game_id,
                mutually_exclusive_group_key=self._market_group_key_from_market(market),
            )

    def _mutually_exclusive_group_key_for_market_key(self, market_key: str) -> str:
        return self.correlated_graph.group_key_for(market_key)

    def graph_snapshot_for(
        self, market_key: str
    ) -> LinkedMarketRiskGraphSnapshot | None:
        linked_event_key = self.market_key_to_event_exposure_key.get(market_key)
        mutually_exclusive_group_key = (
            self.market_key_to_mutually_exclusive_group_key.get(market_key)
        )
        if linked_event_key is None and mutually_exclusive_group_key is None:
            return None
        return LinkedMarketRiskGraphSnapshot(
            market_key=market_key,
            linked_event_key=linked_event_key,
            mutually_exclusive_group_key=mutually_exclusive_group_key,
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
        market_keys = {
            position.contract.market_key
            for position in positions
            if self.market_key_to_event_exposure_key.get(position.contract.market_key)
            == event_exposure_key
        }.union(
            {
                order.contract.market_key
                for order in open_orders
                if self.market_key_to_event_exposure_key.get(order.contract.market_key)
                == event_exposure_key
            }
        )
        return self.correlated_graph.grouped_cluster_exposure(
            cluster_key=event_exposure_key,
            exposure_by_market={
                market_key: self._current_market_exposure(
                    market_key,
                    positions,
                    open_orders,
                )
                for market_key in market_keys
            },
        )

    def _position_notional(self, position: PositionSnapshot) -> float:
        reference_price = position.mark_price
        if reference_price is None:
            reference_price = position.average_price
        if reference_price is None:
            return 0.0
        return abs(position.quantity) * max(0.0, float(reference_price))

    def _order_notional(self, order: NormalizedOrder) -> float:
        if order.action.value == "sell" and order.reduce_only:
            return 0.0
        return max(0.0, order.remaining_quantity * order.price)

    def _current_event_notional_exposure(
        self,
        event_exposure_key: str,
        positions: list[PositionSnapshot],
        open_orders: list[NormalizedOrder],
    ) -> float:
        market_keys = {
            position.contract.market_key
            for position in positions
            if self.market_key_to_event_exposure_key.get(position.contract.market_key)
            == event_exposure_key
        }.union(
            {
                order.contract.market_key
                for order in open_orders
                if self.market_key_to_event_exposure_key.get(order.contract.market_key)
                == event_exposure_key
            }
        )
        return self.correlated_graph.grouped_cluster_exposure(
            cluster_key=event_exposure_key,
            exposure_by_market={
                market_key: sum(
                    self._position_notional(position)
                    for position in positions
                    if position.contract.market_key == market_key
                )
                + sum(
                    self._order_notional(order)
                    for order in open_orders
                    if order.contract.market_key == market_key
                )
                for market_key in market_keys
            },
        )

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
        if self.state.weekly_loss_limit_reached(self.limits.max_weekly_loss):
            for intent in intents:
                decision.rejected.append(Rejection(intent, "weekly loss limit reached"))
            return decision
        if self.state.cumulative_loss_limit_reached(self.limits.max_cumulative_loss):
            for intent in intents:
                decision.rejected.append(
                    Rejection(intent, "cumulative loss limit reached")
                )
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
        for order in open_orders:
            market_key = order.contract.market_key
            if market_key in running_market_exposure:
                continue
            running_market_exposure[market_key] = self._current_market_exposure(
                market_key,
                current_positions,
                open_orders,
            )
        running_event_group_exposure: dict[str, dict[str, float]] = {}
        running_event_exposure: dict[str, float] = {}
        running_event_group_notional: dict[str, dict[str, float]] = {}
        running_event_notional: dict[str, float] = {}
        for event_exposure_key in set(self.market_key_to_event_exposure_key.values()):
            running_event_exposure[event_exposure_key] = self._current_event_exposure(
                event_exposure_key,
                current_positions,
                open_orders,
            )
            running_event_notional[event_exposure_key] = (
                self._current_event_notional_exposure(
                    event_exposure_key,
                    current_positions,
                    open_orders,
                )
            )
        for market_key, exposure in running_market_exposure.items():
            event_exposure_key = self.market_key_to_event_exposure_key.get(market_key)
            if event_exposure_key is None:
                continue
            event_groups = running_event_group_exposure.setdefault(
                event_exposure_key, {}
            )
            group_key = self._mutually_exclusive_group_key_for_market_key(market_key)
            event_groups[group_key] = max(event_groups.get(group_key, 0.0), exposure)
            event_notional_groups = running_event_group_notional.setdefault(
                event_exposure_key, {}
            )
            event_notional_groups[group_key] = max(
                event_notional_groups.get(group_key, 0.0),
                sum(
                    self._position_notional(position)
                    for position in current_positions
                    if position.contract.market_key == market_key
                )
                + sum(
                    self._order_notional(order)
                    for order in open_orders
                    if order.contract.market_key == market_key
                ),
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
            market_exposure = running_market_exposure.get(market_key)
            if market_exposure is None:
                market_exposure = self._current_market_exposure(
                    market_key,
                    current_positions,
                    open_orders,
                )
                running_market_exposure[market_key] = market_exposure
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
                event_groups = running_event_group_exposure.setdefault(
                    event_exposure_key, {}
                )
                group_key = self._mutually_exclusive_group_key_for_market_key(
                    market_key
                )
                current_group_exposure = event_groups.get(group_key, 0.0)
                current_event_exposure = running_event_exposure.get(
                    event_exposure_key, 0.0
                )
                projected_group_exposure = max(current_group_exposure, projected_market)
                projected_event = (
                    current_event_exposure
                    - current_group_exposure
                    + projected_group_exposure
                )
                if projected_event > self.limits.max_contracts_per_event:
                    decision.rejected.append(
                        Rejection(intent, "per-event exposure cap exceeded")
                    )
                    continue

            projected_event_notional = None
            if (
                self.limits.max_notional_per_event is not None
                and event_exposure_key is not None
            ):
                notional_groups = running_event_group_notional.setdefault(
                    event_exposure_key, {}
                )
                group_key = self._mutually_exclusive_group_key_for_market_key(
                    market_key
                )
                current_group_notional = notional_groups.get(group_key, 0.0)
                current_event_notional = running_event_notional.get(
                    event_exposure_key, 0.0
                )
                proposed_market_notional = projected_market * intent.price
                projected_group_notional = max(
                    current_group_notional,
                    proposed_market_notional,
                )
                projected_event_notional = (
                    current_event_notional
                    - current_group_notional
                    + projected_group_notional
                )
                if projected_event_notional > self.limits.max_notional_per_event:
                    decision.rejected.append(
                        Rejection(intent, "per-event capital-at-risk cap exceeded")
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
                event_groups = running_event_group_exposure.setdefault(
                    event_exposure_key, {}
                )
                group_key = self._mutually_exclusive_group_key_for_market_key(
                    market_key
                )
                event_groups[group_key] = max(
                    event_groups.get(group_key, 0.0),
                    projected_market,
                )
                running_event_exposure[event_exposure_key] = projected_event
            if projected_event_notional is not None and event_exposure_key is not None:
                notional_groups = running_event_group_notional.setdefault(
                    event_exposure_key, {}
                )
                group_key = self._mutually_exclusive_group_key_for_market_key(
                    market_key
                )
                notional_groups[group_key] = max(
                    notional_groups.get(group_key, 0.0),
                    projected_market * intent.price,
                )
                running_event_notional[event_exposure_key] = projected_event_notional
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

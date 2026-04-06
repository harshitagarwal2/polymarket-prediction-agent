from __future__ import annotations

from dataclasses import dataclass, field

from adapters.base import TradingAdapter
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    PositionSnapshot,
)
from engine.accounting import AccountStateCache
from engine.order_state import OrderState


def _position_delta_for_fill(fill: FillSnapshot) -> float:
    return fill.quantity if fill.action.value == "buy" else -fill.quantity


def _balance_delta_for_fill(fill: FillSnapshot) -> float:
    notional = fill.price * fill.quantity
    if fill.action.value == "buy":
        return -(notional + fill.fee)
    return notional - fill.fee


@dataclass(frozen=True)
class ReconciliationIssue:
    code: str
    message: str


@dataclass(frozen=True)
class ReconciliationPolicy:
    action: str
    reason: str


@dataclass
class ReconciliationReport:
    contract: Contract
    local_orders: list[NormalizedOrder] = field(default_factory=list)
    observed_orders: list[NormalizedOrder] = field(default_factory=list)
    local_position: PositionSnapshot | None = None
    observed_position: PositionSnapshot | None = None
    local_balance: BalanceSnapshot | None = None
    observed_balance: BalanceSnapshot | None = None
    local_fills: list[FillSnapshot] = field(default_factory=list)
    observed_fills: list[FillSnapshot] = field(default_factory=list)
    missing_on_venue: list[str] = field(default_factory=list)
    cancel_acknowledged: list[str] = field(default_factory=list)
    unexpected_on_venue: list[str] = field(default_factory=list)
    diverged_orders: list[str] = field(default_factory=list)
    missing_fills_on_venue: list[str] = field(default_factory=list)
    unexpected_fills_on_venue: list[str] = field(default_factory=list)
    cancel_race_fills: list[str] = field(default_factory=list)
    position_drift: float = 0.0
    balance_drift: float = 0.0
    issues: list[ReconciliationIssue] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return not self.issues

    @property
    def policy(self) -> ReconciliationPolicy:
        halt_codes = {
            "missing_fill_on_venue",
            "unexpected_fill_on_venue",
            "position_drift",
            "balance_drift",
        }
        resync_codes = {
            "missing_on_venue",
            "unexpected_on_venue",
            "diverged_order",
        }
        seen_codes = {issue.code for issue in self.issues}
        if seen_codes & halt_codes:
            reason = "; ".join(
                issue.message for issue in self.issues if issue.code in halt_codes
            )
            return ReconciliationPolicy(action="halt", reason=reason)
        if seen_codes & resync_codes:
            reason = "; ".join(
                issue.message for issue in self.issues if issue.code in resync_codes
            )
            return ReconciliationPolicy(action="resync", reason=reason)
        return ReconciliationPolicy(action="ok", reason="")


class ReconciliationEngine:
    def __init__(
        self,
        adapter: TradingAdapter,
        order_state: OrderState,
        account_state: AccountStateCache,
    ):
        self.adapter = adapter
        self.order_state = order_state
        self.account_state = account_state

    def reconcile(
        self,
        contract: Contract,
        observed_snapshot: AccountSnapshot | None = None,
        *,
        local_orders: list[NormalizedOrder] | None = None,
        local_position: PositionSnapshot | None = None,
        local_balance: BalanceSnapshot | None = None,
        local_fills: list[FillSnapshot] | None = None,
        pending_cancel_order_ids: set[str] | None = None,
    ) -> ReconciliationReport:
        local_orders = local_orders or self.order_state.resting_for_contract(
            contract.market_key
        )
        pending_cancel_order_ids = pending_cancel_order_ids or set()
        snapshot = observed_snapshot or self.adapter.get_account_snapshot(contract)
        observed_orders = snapshot.open_orders
        local_position = local_position or self.account_state.position_for(contract)
        observed_position = next(
            (
                position
                for position in snapshot.positions
                if position.contract.market_key == contract.market_key
            ),
            PositionSnapshot(contract=contract, quantity=0.0),
        )
        local_balance = local_balance or self.account_state.balance
        observed_balance = snapshot.balance
        local_fills = local_fills or self.account_state.fills_for(contract)
        observed_fills = snapshot.fills

        local_by_id = {order.order_id: order for order in local_orders}
        observed_by_id = {order.order_id: order for order in observed_orders}
        local_fill_keys = {fill.fill_key for fill in local_fills}
        observed_fill_keys = {fill.fill_key for fill in observed_fills}

        raw_missing_on_venue = sorted(set(local_by_id) - set(observed_by_id))
        cancel_acknowledged = sorted(
            order_id
            for order_id in raw_missing_on_venue
            if order_id in pending_cancel_order_ids
        )
        missing_on_venue = [
            order_id
            for order_id in raw_missing_on_venue
            if order_id not in pending_cancel_order_ids
        ]
        unexpected_on_venue = sorted(set(observed_by_id) - set(local_by_id))
        diverged_orders: list[str] = []
        missing_fills_on_venue = sorted(local_fill_keys - observed_fill_keys)
        cancel_race_observed_fills = [
            fill
            for fill in observed_fills
            if fill.fill_key not in local_fill_keys
            and fill.order_id in pending_cancel_order_ids
        ]
        cancel_race_fills = sorted(fill.fill_key for fill in cancel_race_observed_fills)
        unexpected_fills_on_venue = sorted(
            fill.fill_key
            for fill in observed_fills
            if fill.fill_key not in local_fill_keys
            and fill.order_id not in pending_cancel_order_ids
        )
        expected_cancel_race_position_delta = sum(
            _position_delta_for_fill(fill)
            for fill in cancel_race_observed_fills
            if fill.contract.market_key == contract.market_key
        )
        position_drift = observed_position.quantity - (
            local_position.quantity + expected_cancel_race_position_delta
        )
        balance_drift = 0.0
        if local_balance is not None:
            expected_cancel_race_balance_delta = sum(
                _balance_delta_for_fill(fill) for fill in cancel_race_observed_fills
            )
            balance_drift = observed_balance.available - (
                local_balance.available + expected_cancel_race_balance_delta
            )
        issues: list[ReconciliationIssue] = []

        for order_id in sorted(set(local_by_id) & set(observed_by_id)):
            local = local_by_id[order_id]
            observed = observed_by_id[order_id]
            if (
                abs(local.price - observed.price) > 1e-9
                or abs(local.remaining_quantity - observed.remaining_quantity) > 1e-9
            ):
                diverged_orders.append(order_id)

        for order_id in missing_on_venue:
            issues.append(
                ReconciliationIssue(
                    "missing_on_venue",
                    f"Local order {order_id} is not visible on venue",
                )
            )
        for order_id in unexpected_on_venue:
            issues.append(
                ReconciliationIssue(
                    "unexpected_on_venue",
                    f"Venue order {order_id} is not tracked locally",
                )
            )
        for order_id in diverged_orders:
            issues.append(
                ReconciliationIssue(
                    "diverged_order",
                    f"Order {order_id} differs between local and venue state",
                )
            )
        for fill_key in missing_fills_on_venue:
            issues.append(
                ReconciliationIssue(
                    "missing_fill_on_venue",
                    f"Local fill {fill_key} is not visible on venue",
                )
            )
        for fill_key in unexpected_fills_on_venue:
            issues.append(
                ReconciliationIssue(
                    "unexpected_fill_on_venue",
                    f"Venue fill {fill_key} is not tracked locally",
                )
            )
        if abs(position_drift) > 1e-9:
            issues.append(
                ReconciliationIssue(
                    "position_drift",
                    f"Position drift detected: local={local_position.quantity} observed={observed_position.quantity}",
                )
            )
        if abs(balance_drift) > 1e-9:
            issues.append(
                ReconciliationIssue(
                    "balance_drift",
                    f"Balance drift detected: local={local_balance.available if local_balance else 'unknown'} observed={observed_balance.available}",
                )
            )

        return ReconciliationReport(
            contract=contract,
            local_orders=local_orders,
            observed_orders=observed_orders,
            local_position=local_position,
            observed_position=observed_position,
            local_balance=local_balance,
            observed_balance=observed_balance,
            local_fills=local_fills,
            observed_fills=observed_fills,
            missing_on_venue=missing_on_venue,
            cancel_acknowledged=cancel_acknowledged,
            unexpected_on_venue=unexpected_on_venue,
            diverged_orders=diverged_orders,
            missing_fills_on_venue=missing_fills_on_venue,
            unexpected_fills_on_venue=unexpected_fills_on_venue,
            cancel_race_fills=cancel_race_fills,
            position_drift=position_drift,
            balance_drift=balance_drift,
            issues=issues,
        )

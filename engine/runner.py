from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

from adapters.base import TradingAdapter
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderAction,
    OrderIntent,
    OrderStatus,
    PlacementResult,
    PositionSnapshot,
    deserialize_balance_snapshot,
    deserialize_contract,
    deserialize_fill_snapshot,
    deserialize_normalized_order,
    deserialize_position_snapshot,
    serialize_balance_snapshot,
    serialize_contract,
    serialize_fill_snapshot,
    serialize_normalized_order,
    serialize_position_snapshot,
)
from engine.accounting import AccountStateCache
from engine.interfaces import Strategy, StrategyContext
from engine.order_state import OrderState, summarize_fill_state
from engine.reconciliation import ReconciliationEngine, ReconciliationReport
from engine.safety_state import (
    EngineSafetyState,
    EngineStatusSnapshot,
    PendingCancelState,
    PendingRefreshRequestState,
    RecoveryItemState,
    PendingSubmissionState,
)
from engine.safety_store import SafetyStateStore
from risk.limits import Rejection, RiskDecision, RiskEngine


@dataclass
class EngineRunResult:
    context: StrategyContext
    proposed: list[OrderIntent]
    risk: RiskDecision
    placements: list[PlacementResult]
    reconciliation_before: ReconciliationReport | None = None
    reconciliation_after: ReconciliationReport | None = None


@dataclass
class RefreshRequest:
    scope: str
    reasons: set[str]
    priority: int
    requested_at: datetime
    due_at: datetime


@dataclass(frozen=True)
class ActionPolicyDecision:
    action: str
    reason: str | None = None
    scope: str | None = None


class TradingEngine:
    def __init__(
        self,
        adapter: TradingAdapter,
        strategy: Strategy,
        risk_engine: RiskEngine,
        resume_confirmation_required: int = 2,
        safety_state_path: str | Path | None = None,
        cancel_retry_interval_seconds: float = 5.0,
        cancel_retry_max_attempts: int = 3,
        cancel_attention_timeout_seconds: float = 30.0,
        overlay_max_age_seconds: float = 30.0,
        forced_refresh_debounce_seconds: float = 0.0,
        pending_submission_recovery_seconds: float = 5.0,
        pending_submission_expiry_seconds: float = 30.0,
    ):
        self.adapter = adapter
        self.strategy = strategy
        self.risk_engine = risk_engine
        self.resume_confirmation_required = max(1, resume_confirmation_required)
        self.order_state = OrderState()
        self.account_state = AccountStateCache()
        self.safety_store = (
            SafetyStateStore(safety_state_path)
            if safety_state_path is not None
            else None
        )
        self.safety_state = (
            self.safety_store.load()
            if self.safety_store is not None
            else EngineSafetyState()
        )
        self.reconciliation = ReconciliationEngine(
            adapter, self.order_state, self.account_state
        )
        self.cancel_retry_interval_seconds = max(0.0, cancel_retry_interval_seconds)
        self.cancel_retry_max_attempts = max(1, cancel_retry_max_attempts)
        self.cancel_attention_timeout_seconds = max(
            0.0, cancel_attention_timeout_seconds
        )
        self.overlay_max_age_seconds = max(1.0, overlay_max_age_seconds)
        self.forced_refresh_debounce_seconds = max(0.0, forced_refresh_debounce_seconds)
        self.pending_submission_recovery_seconds = max(
            0.0, pending_submission_recovery_seconds
        )
        self.pending_submission_expiry_seconds = max(
            self.pending_submission_recovery_seconds,
            pending_submission_expiry_seconds,
        )
        self.order_state.restore_pending_cancels(
            self.pending_cancel_order_ids(unresolved_only=True)
        )
        self._sync_pending_cancel_attention_halt()
        self._applied_live_terminal_markers: dict[str, tuple[datetime | None, str]] = {}
        self._refresh_requests: dict[str, RefreshRequest] = {}

    def _persist_safety_state(self) -> None:
        if self.safety_store is not None:
            self.safety_store.save(self.safety_state)

    def _sync_operator_control_state(self) -> None:
        if self.safety_store is None:
            return
        persisted = self.safety_store.load()
        self.safety_state.paused = persisted.paused
        self.safety_state.pause_reason = persisted.pause_reason
        self.safety_state.hold_new_orders = persisted.hold_new_orders
        self.safety_state.hold_reason = persisted.hold_reason
        self.safety_state.hold_since = persisted.hold_since
        if persisted.pending_refresh_requests:
            for request in persisted.pending_refresh_requests:
                self.request_authoritative_refresh(request.reason, scope=request.scope)
            self.safety_state.pending_refresh_requests = []
            self._persist_safety_state()

    def set_new_order_hold(self, reason: str = "new orders held by operator") -> None:
        self.safety_state.hold_new_orders = True
        self.safety_state.hold_reason = reason
        self.safety_state.hold_since = datetime.now(timezone.utc)
        self._persist_safety_state()

    def clear_new_order_hold(self) -> None:
        self.safety_state.hold_new_orders = False
        self.safety_state.hold_reason = None
        self.safety_state.hold_since = None
        self._persist_safety_state()

    def queue_authoritative_refresh_request(
        self, reason: str, *, scope: str = "account"
    ) -> None:
        for request in self.safety_state.pending_refresh_requests:
            if request.scope == scope and request.reason == reason:
                return
        self.safety_state.pending_refresh_requests.append(
            PendingRefreshRequestState(
                scope=scope,
                reason=reason,
                requested_at=datetime.now(timezone.utc),
            )
        )
        self._upsert_recovery_item(
            "account-refresh-needed" if scope == "account" else "market-refresh-needed",
            scope,
            reason,
            "authoritative_snapshot",
        )
        self._persist_safety_state()

    def _recovery_id(self, item_type: str, scope: str) -> str:
        return f"{item_type}:{scope}"

    def recovery_items(
        self, scope: str | None = None, *, open_only: bool = False
    ) -> list[RecoveryItemState]:
        items = list(self.safety_state.recovery_items)
        if scope is not None:
            items = [item for item in items if item.scope == scope]
        if open_only:
            items = [item for item in items if item.status == "open"]
        return items

    def _prune_recovery_items(self) -> None:
        open_items = [
            item for item in self.safety_state.recovery_items if item.status == "open"
        ]
        closed_items = [
            item for item in self.safety_state.recovery_items if item.status != "open"
        ]
        closed_items.sort(
            key=lambda item: item.cleared_at or item.opened_at, reverse=True
        )
        self.safety_state.recovery_items = open_items + closed_items[:20]

    def _upsert_recovery_item(
        self,
        item_type: str,
        scope: str,
        reason: str,
        clear_source: str,
        *,
        evidence: str | None = None,
        observed_at: datetime | None = None,
    ) -> RecoveryItemState:
        now = observed_at or datetime.now(timezone.utc)
        recovery_id = self._recovery_id(item_type, scope)
        item = next(
            (
                existing
                for existing in self.safety_state.recovery_items
                if existing.recovery_id == recovery_id
            ),
            None,
        )
        if item is None:
            item = RecoveryItemState(
                recovery_id=recovery_id,
                item_type=item_type,
                scope=scope,
                reason=reason,
                clear_source=clear_source,
                opened_at=now,
                last_evidence_at=now,
                last_evidence=evidence or reason,
            )
            self.safety_state.recovery_items.append(item)
        else:
            if item.status != "open":
                item.opened_at = now
                item.occurrence_count += 1
            item.status = "open"
            item.reason = reason
            item.clear_source = clear_source
            item.last_evidence_at = now
            item.last_evidence = evidence or reason
            item.cleared_at = None
            item.clear_reason = None
        self._prune_recovery_items()
        return item

    def _clear_recovery_item(
        self,
        item_type: str,
        scope: str,
        *,
        observed_at: datetime,
        clear_reason: str,
    ) -> None:
        recovery_id = self._recovery_id(item_type, scope)
        item = next(
            (
                existing
                for existing in self.safety_state.recovery_items
                if existing.recovery_id == recovery_id
            ),
            None,
        )
        if item is None:
            return
        item.status = "cleared"
        item.cleared_at = observed_at
        item.clear_reason = clear_reason
        item.last_evidence_at = observed_at
        item.last_evidence = clear_reason
        self._prune_recovery_items()

    def resume_trading_eligible(self) -> bool:
        return (
            not self.safety_state.halted
            and not self.safety_state.paused
            and not self.safety_state.hold_new_orders
            and not self.recovery_items(open_only=True)
        )

    def _refresh_priority_and_backoff(self, reason: str) -> tuple[int, float]:
        if reason in {
            "authoritative snapshot incomplete",
            "terminal marker reversed by snapshot",
        }:
            return 0, 0.0
        if reason == "pending cancel awaiting authoritative observation":
            return 1, 0.0
        if reason in {
            "live state became active",
            "live subscription changed",
        }:
            return 1, 0.0
        if (
            reason.startswith("live state error:")
            or reason == "overlay max age exceeded"
        ):
            return 2, 1.0
        return 1, 0.0

    def request_authoritative_refresh(
        self, reason: str, *, scope: str = "account"
    ) -> None:
        now = datetime.now(timezone.utc)
        priority, backoff_seconds = self._refresh_priority_and_backoff(reason)
        due_at = now + timedelta(
            seconds=max(self.forced_refresh_debounce_seconds, backoff_seconds)
        )
        existing = self._refresh_requests.get(scope)
        if existing is None:
            self._refresh_requests[scope] = RefreshRequest(
                scope=scope,
                reasons={reason},
                priority=priority,
                requested_at=now,
                due_at=due_at,
            )
            self.safety_state.overlay_forced_snapshot_count += 1
        else:
            existing.reasons.add(reason)
            existing.priority = min(existing.priority, priority)
            existing.requested_at = min(existing.requested_at, now)
            existing.due_at = min(existing.due_at, due_at)
        self.safety_state.overlay_last_forced_snapshot_reason = reason
        self.safety_state.overlay_last_forced_snapshot_scope = scope
        self._upsert_recovery_item(
            "account-refresh-needed" if scope == "account" else "market-refresh-needed",
            scope,
            reason,
            "authoritative_snapshot",
            observed_at=now,
        )
        self._persist_safety_state()

    def consume_authoritative_refresh_request(self) -> tuple[bool, str | None]:
        now = datetime.now(timezone.utc)
        due_requests = [
            request
            for request in self._refresh_requests.values()
            if request.due_at <= now
        ]
        if not due_requests:
            return False, None
        due_requests.sort(key=lambda request: (request.priority, request.requested_at))
        reason = "; ".join(
            sorted({part for request in due_requests for part in request.reasons})
        )
        for request in due_requests:
            self._refresh_requests.pop(request.scope, None)
        return True, reason

    def _record_action_policy(
        self, action: str, reason: str | None = None, *, persist: bool = False
    ) -> None:
        self.safety_state.last_action_gate_action = action
        self.safety_state.last_action_gate_reason = reason
        if persist:
            self._persist_safety_state()

    def _has_pending_refresh(self, scope: str) -> bool:
        return scope in self._refresh_requests or "account" in self._refresh_requests

    def evaluate_order_action_policy(
        self,
        action_type: str,
        *,
        contract: Contract,
        order_id: str | None = None,
        intent: OrderIntent | None = None,
    ) -> ActionPolicyDecision:
        scope = contract.market_key
        if action_type in {"submit", "replace", "retry_submit"}:
            if intent is not None:
                existing_submission = self._pending_submission_record(
                    self._intent_id(intent), contract.market_key
                )
                if (
                    existing_submission is not None
                    and not existing_submission.acknowledged
                ):
                    return ActionPolicyDecision(
                        "hold",
                        existing_submission.reason
                        or "pending submission still unresolved",
                        scope,
                    )
            if self.safety_state.halted:
                return ActionPolicyDecision("hold", self.safety_state.reason, scope)
            if self.safety_state.paused:
                return ActionPolicyDecision(
                    "hold",
                    self.safety_state.pause_reason or "engine paused by operator",
                    scope,
                )
            if self.safety_state.hold_new_orders:
                return ActionPolicyDecision(
                    "hold",
                    self.safety_state.hold_reason or "new orders held by operator",
                    scope,
                )
            pending_cancel_reason = self.pending_cancel_submission_guard_reason(
                contract
            )
            if pending_cancel_reason is not None:
                return ActionPolicyDecision(
                    "recover-first", pending_cancel_reason, scope
                )
        if action_type in {"cancel", "retry_cancel", "replace"}:
            if order_id is not None and any(
                item.order_id == order_id
                for item in self.pending_cancels(contract, unresolved_only=True)
            ):
                return ActionPolicyDecision(
                    "hold",
                    f"pending cancel already unresolved for order {order_id}",
                    scope,
                )
            if self.pending_submissions(contract, unresolved_only=True):
                return ActionPolicyDecision(
                    "recover-first",
                    "pending submission awaiting authoritative observation",
                    scope,
                )
        if self.safety_state.overlay_degraded:
            return ActionPolicyDecision(
                "recover-first",
                self.safety_state.overlay_degraded_reason or "runtime degraded",
                scope,
            )
        if self.safety_state.overlay_delta_suppressed:
            return ActionPolicyDecision(
                "recover-first",
                self.safety_state.overlay_degraded_reason
                or "overlay recovery in progress",
                scope,
            )
        if self._has_pending_refresh(scope):
            return ActionPolicyDecision(
                "recover-first",
                "authoritative refresh already pending",
                scope,
            )
        return ActionPolicyDecision("allow", None, scope)

    def _stop_live_heartbeat(self) -> None:
        stopper = getattr(self.adapter, "stop_heartbeat", None)
        if callable(stopper):
            stopper()
            self.sync_heartbeat_state()

    def sync_heartbeat_state(self) -> None:
        getter = getattr(self.adapter, "heartbeat_status", None)
        if not callable(getter):
            self.safety_state.heartbeat_required = False
            self.safety_state.heartbeat_active = False
            self.safety_state.heartbeat_running = False
            self.safety_state.heartbeat_healthy_for_trading = True
            self.safety_state.heartbeat_unhealthy = False
            self.safety_state.heartbeat_last_success_at = None
            self.safety_state.heartbeat_consecutive_failures = 0
            self.safety_state.heartbeat_last_error = None
            self.safety_state.heartbeat_last_id = None
            self._persist_safety_state()
            return

        status = getter()
        self.safety_state.heartbeat_required = bool(getattr(status, "required", False))
        self.safety_state.heartbeat_active = bool(getattr(status, "active", False))
        self.safety_state.heartbeat_running = bool(getattr(status, "running", False))
        self.safety_state.heartbeat_healthy_for_trading = bool(
            getattr(status, "healthy_for_trading", True)
        )
        self.safety_state.heartbeat_unhealthy = bool(
            getattr(status, "unhealthy", False)
        )
        self.safety_state.heartbeat_last_success_at = getattr(
            status, "last_success_at", None
        )
        self.safety_state.heartbeat_consecutive_failures = int(
            getattr(status, "consecutive_failures", 0) or 0
        )
        self.safety_state.heartbeat_last_error = getattr(status, "last_error", None)
        self.safety_state.heartbeat_last_id = getattr(status, "last_heartbeat_id", None)
        self._persist_safety_state()

    def _persist_detailed_truth(self, snapshot: AccountSnapshot) -> None:
        self.safety_state.persisted_open_orders = [
            serialize_normalized_order(order) for order in snapshot.open_orders
        ]
        self.safety_state.persisted_positions = [
            serialize_position_snapshot(position) for position in snapshot.positions
        ]
        self.safety_state.persisted_fills = [
            serialize_fill_snapshot(fill) for fill in snapshot.fills
        ]
        self.safety_state.persisted_balance = serialize_balance_snapshot(
            snapshot.balance
        )

    def _persisted_orders_for_contract(
        self, contract: Contract
    ) -> list[NormalizedOrder]:
        return [
            order
            for order in (
                deserialize_normalized_order(payload)
                for payload in self.safety_state.persisted_open_orders
            )
            if order.contract.market_key == contract.market_key
        ]

    def _persisted_fills_for_contract(self, contract: Contract) -> list[FillSnapshot]:
        return [
            fill
            for fill in (
                deserialize_fill_snapshot(payload)
                for payload in self.safety_state.persisted_fills
            )
            if fill.contract.market_key == contract.market_key
        ]

    def _persisted_position_for(self, contract: Contract) -> PositionSnapshot:
        for payload in self.safety_state.persisted_positions:
            position = deserialize_position_snapshot(payload)
            if position.contract.market_key == contract.market_key:
                return position
        return PositionSnapshot(contract=contract, quantity=0.0)

    def _persisted_balance(self) -> BalanceSnapshot | None:
        if self.safety_state.persisted_balance is None:
            return None
        return deserialize_balance_snapshot(self.safety_state.persisted_balance)

    def _persisted_truth_available(self) -> bool:
        return bool(
            self.safety_state.persisted_open_orders
            or self.safety_state.persisted_positions
            or self.safety_state.persisted_fills
            or self.safety_state.persisted_balance is not None
        )

    def reconcile_persisted_truth(
        self,
        contract: Contract,
        snapshot: AccountSnapshot | None = None,
    ) -> ReconciliationReport:
        if self._persisted_truth_available():
            local_orders = self._persisted_orders_for_contract(contract)
            local_position = self._persisted_position_for(contract)
            local_balance = self._persisted_balance()
            local_fills = self._persisted_fills_for_contract(contract)
        else:
            local_orders = self.order_state.resting_for_contract(contract.market_key)
            local_position = self.account_state.position_for(contract)
            local_balance = self.account_state.balance
            local_fills = self.account_state.fills_for(contract)
        return self.reconciliation.reconcile(
            contract,
            observed_snapshot=snapshot,
            local_orders=local_orders,
            local_position=local_position,
            local_balance=local_balance,
            local_fills=local_fills,
            pending_cancel_order_ids=self.pending_cancel_order_ids(
                contract, unresolved_only=True
            ),
        )

    def _contract_key(self, contract: Contract | str) -> str:
        return contract if isinstance(contract, str) else contract.market_key

    def _prune_pending_cancels(self) -> None:
        self.safety_state.pending_cancels = [
            item for item in self.safety_state.pending_cancels if not item.acknowledged
        ]

    def _pending_cancel_attention_records(
        self, contract: Contract | None = None
    ) -> list[PendingCancelState]:
        records = self.pending_cancels(contract, unresolved_only=True)
        return [item for item in records if item.operator_attention_required]

    def _sync_pending_cancel_attention_halt(self) -> None:
        records = self._pending_cancel_attention_records()
        if not records:
            return
        oldest = min(records, key=lambda item: item.requested_at)
        self.safety_state.halted = True
        self.safety_state.reason = self.pending_cancel_block_reason()
        self.safety_state.contract_key = oldest.contract_key
        self.safety_state.clean_resume_streak = 0
        self.safety_state.last_clean_resume_observed_at = None

    def pending_cancels(
        self, contract: Contract | str | None = None, *, unresolved_only: bool = False
    ) -> list[PendingCancelState]:
        records = list(self.safety_state.pending_cancels)
        if contract is not None:
            contract_key = self._contract_key(contract)
            records = [item for item in records if item.contract_key == contract_key]
        if unresolved_only:
            records = [item for item in records if not item.acknowledged]
        return records

    def pending_cancel_order_ids(
        self, contract: Contract | str | None = None, *, unresolved_only: bool = True
    ) -> set[str]:
        return {
            item.order_id
            for item in self.pending_cancels(contract, unresolved_only=unresolved_only)
        }

    def _intent_id(self, intent: OrderIntent) -> str:
        if intent.client_order_id not in (None, ""):
            return f"client:{intent.client_order_id}"
        return "|".join(
            [
                intent.contract.market_key,
                intent.action.value,
                f"{intent.price:.6f}",
                f"{intent.quantity:.6f}",
                "post" if intent.post_only else "taker",
                "reduce" if intent.reduce_only else "open",
                str(intent.expiration_ts or "none"),
            ]
        )

    def pending_submissions(
        self, contract: Contract | str | None = None, *, unresolved_only: bool = False
    ) -> list[PendingSubmissionState]:
        records = list(self.safety_state.pending_submissions)
        if contract is not None:
            contract_key = self._contract_key(contract)
            records = [item for item in records if item.contract_key == contract_key]
        if unresolved_only:
            records = [item for item in records if not item.acknowledged]
        return records

    def _prune_pending_submissions(self) -> None:
        self.safety_state.pending_submissions = [
            item
            for item in self.safety_state.pending_submissions
            if not item.acknowledged
        ]

    def _pending_submission_record(
        self, intent_id: str, contract_key: str
    ) -> PendingSubmissionState | None:
        return next(
            (
                item
                for item in self.safety_state.pending_submissions
                if item.intent_id == intent_id and item.contract_key == contract_key
            ),
            None,
        )

    def _submission_contract(self, record: PendingSubmissionState) -> Contract:
        return deserialize_contract(record.contract)

    def _submission_reservation_order(
        self, record: PendingSubmissionState
    ) -> NormalizedOrder:
        contract = self._submission_contract(record)
        return NormalizedOrder(
            order_id=record.order_id or f"pending-submit:{record.intent_id}",
            contract=contract,
            action=record.action,
            price=record.price,
            quantity=record.quantity,
            remaining_quantity=record.quantity,
            status=OrderStatus.PENDING,
            created_at=record.requested_at,
            updated_at=record.last_attempt_at or record.requested_at,
            post_only=record.post_only,
            reduce_only=record.reduce_only,
            expiration_ts=record.expiration_ts,
            client_order_id=record.client_order_id,
            raw={"pending_submission": True, "intent_id": record.intent_id},
        )

    def pending_submission_reservations(
        self, contract: Contract | str | None = None
    ) -> list[NormalizedOrder]:
        return [
            self._submission_reservation_order(record)
            for record in self.pending_submissions(contract, unresolved_only=True)
        ]

    def _ambiguous_submission_exception(self, exc: Exception) -> bool:
        message = str(exc).lower()
        return any(
            token in message
            for token in (
                "timeout",
                "timed out",
                "disconnect",
                "connection reset",
                "broken pipe",
                "connection aborted",
                "connection closed",
                "temporarily unavailable",
            )
        )

    def track_pending_submission(
        self,
        intent: OrderIntent,
        *,
        status: str,
        reason: str | None = None,
        order_id: str | None = None,
    ) -> PendingSubmissionState:
        now = datetime.now(timezone.utc)
        intent_id = self._intent_id(intent)
        contract_key = intent.contract.market_key
        record = self._pending_submission_record(intent_id, contract_key)
        if record is None:
            record = PendingSubmissionState(
                intent_id=intent_id,
                contract_key=contract_key,
                contract=serialize_contract(intent.contract),
                action=intent.action,
                price=intent.price,
                quantity=intent.quantity,
                requested_at=now,
                client_order_id=intent.client_order_id,
                post_only=intent.post_only,
                reduce_only=intent.reduce_only,
                expiration_ts=intent.expiration_ts,
            )
            self.safety_state.pending_submissions.append(record)
        record.last_attempt_at = now
        record.attempt_count += 1
        record.status = status
        record.reason = reason
        record.order_id = order_id or record.order_id
        record.acknowledged = False
        record.observed_at = None
        record.resolved_at = None
        self._upsert_recovery_item(
            "submit-uncertain",
            contract_key,
            reason or status,
            "authoritative_observation",
            observed_at=now,
        )
        self._prune_pending_submissions()
        self._persist_safety_state()
        return record

    def _resolve_pending_submission(
        self,
        record: PendingSubmissionState,
        *,
        status: str,
        observed_at: datetime,
        reason: str | None = None,
        order_id: str | None = None,
    ) -> None:
        record.status = status
        record.reason = reason or record.reason
        record.order_id = order_id or record.order_id
        record.acknowledged = True
        record.observed_at = observed_at
        record.resolved_at = observed_at
        self._clear_recovery_item(
            "submit-uncertain",
            record.contract_key,
            observed_at=observed_at,
            clear_reason=clear_reason
            if (clear_reason := (reason or status))
            else status,
        )

    def _submission_matches_snapshot(
        self, record: PendingSubmissionState, snapshot: AccountSnapshot
    ) -> tuple[str | None, bool]:
        contract = self._submission_contract(record)
        if record.order_id is not None:
            if any(order.order_id == record.order_id for order in snapshot.open_orders):
                return record.order_id, False
            if any(fill.order_id == record.order_id for fill in snapshot.fills):
                return record.order_id, True
        for order in snapshot.open_orders:
            if (
                order.contract.market_key == contract.market_key
                and order.action is record.action
                and order.price == record.price
                and order.quantity >= record.quantity
            ):
                return order.order_id, False
        for fill in snapshot.fills:
            if (
                fill.contract.market_key == contract.market_key
                and fill.action is record.action
                and fill.price == record.price
                and fill.quantity <= record.quantity
            ):
                return fill.order_id or record.order_id, True
        if record.action is OrderAction.BUY:
            for position in snapshot.positions:
                if (
                    position.contract.market_key == contract.market_key
                    and position.quantity >= record.quantity
                ):
                    return record.order_id, True
        return None, False

    def _reconcile_pending_submissions(self, snapshot: AccountSnapshot) -> None:
        now = snapshot.observed_at
        changed = False
        for record in self.pending_submissions(unresolved_only=True):
            matched_order_id, matched_fill = self._submission_matches_snapshot(
                record, snapshot
            )
            if matched_order_id is not None:
                self._resolve_pending_submission(
                    record,
                    status="filled" if matched_fill else "observed",
                    observed_at=now,
                    order_id=matched_order_id,
                )
                changed = True
                continue
            age_seconds = (now - record.requested_at).total_seconds()
            if age_seconds >= self.pending_submission_expiry_seconds:
                self._resolve_pending_submission(
                    record,
                    status="expired",
                    observed_at=now,
                    reason="pending submission expired without authoritative observation",
                )
                changed = True
                continue
            if age_seconds >= self.pending_submission_recovery_seconds:
                if record.status != "needs_recovery":
                    record.status = "needs_recovery"
                    record.reason = "pending submission still unresolved; authoritative refresh required"
                    changed = True
                self.request_authoritative_refresh(
                    record.reason or "pending submission unresolved",
                    scope=record.contract_key,
                )
        if changed:
            self._prune_pending_submissions()
            self._persist_safety_state()

    def track_cancel_request(
        self, order_id: str, contract: Contract | str, reason: str
    ) -> PendingCancelState:
        now = datetime.now(timezone.utc)
        contract_key = self._contract_key(contract)
        record = next(
            (
                item
                for item in self.safety_state.pending_cancels
                if item.order_id == order_id and item.contract_key == contract_key
            ),
            None,
        )
        if record is None:
            record = PendingCancelState(
                order_id=order_id,
                contract_key=contract_key,
                requested_at=now,
            )
            self.safety_state.pending_cancels.append(record)
        record.reason = reason
        record.last_attempt_at = now
        record.attempt_count += 1
        record.acknowledged = False
        record.status = "pending"
        record.resolved_at = None
        self.order_state.mark_cancel_requested(order_id)
        self._upsert_recovery_item(
            "cancel-uncertain",
            contract_key,
            reason,
            "authoritative_snapshot",
            observed_at=now,
        )
        self.request_authoritative_refresh(
            "pending cancel awaiting authoritative observation",
            scope=contract_key,
        )
        self._prune_pending_cancels()
        self._persist_safety_state()
        return record

    def request_cancel_order(
        self,
        order: NormalizedOrder,
        reason: str,
    ) -> PendingCancelState:
        policy = self.evaluate_order_action_policy(
            "cancel", contract=order.contract, order_id=order.order_id
        )
        self._record_action_policy(policy.action, policy.reason)
        if policy.action == "hold":
            existing = next(
                (
                    item
                    for item in self.pending_cancels(
                        order.contract, unresolved_only=True
                    )
                    if item.order_id == order.order_id
                ),
                None,
            )
            if existing is not None:
                existing.reason = policy.reason or existing.reason
                self._persist_safety_state()
                return existing
        record = self.track_cancel_request(order.order_id, order.contract, reason)
        if policy.action == "recover-first":
            record.status = "needs_recovery"
            record.reason = policy.reason or reason
            self.request_authoritative_refresh(
                record.reason or reason, scope=record.contract_key
            )
            self._persist_safety_state()
            return record
        try:
            self.adapter.cancel_order(order.order_id)
        except Exception:
            pass
        return record

    def request_cancel_all(
        self,
        contract: Contract | None = None,
        *,
        reason: str = "operator cancel requested",
    ) -> list[PendingCancelState]:
        records: list[PendingCancelState] = []
        for order in self.adapter.list_open_orders(contract):
            records.append(self.request_cancel_order(order, reason))
        return records

    def _post_cancel_fills_seen(self, record: PendingCancelState) -> bool:
        return bool(record.post_cancel_fill_seen)

    def _mark_post_cancel_fills_seen(self, record: PendingCancelState) -> None:
        record.post_cancel_fill_seen = True

    def _resolve_pending_cancel(self, record: PendingCancelState) -> None:
        record.acknowledged = True
        record.operator_attention_required = False
        record.status = "cancel_race" if record.post_cancel_fill_seen else "cancelled"
        record.resolved_at = datetime.now(timezone.utc)
        self.order_state.mark_cancelled(record.order_id)
        self._clear_recovery_item(
            "cancel-uncertain",
            record.contract_key,
            observed_at=record.resolved_at,
            clear_reason=(
                "cancel race resolved from authoritative truth"
                if record.post_cancel_fill_seen
                else "cancel resolved from authoritative truth"
            ),
        )

    def _reconcile_pending_cancels(
        self,
        snapshot: AccountSnapshot,
        *,
        allow_retry: bool,
    ) -> None:
        now = snapshot.observed_at
        open_order_ids = {order.order_id for order in snapshot.open_orders}
        fill_order_ids = {fill.order_id for fill in snapshot.fills if fill.order_id}
        changed = False
        for record in self.pending_cancels(unresolved_only=True):
            if record.order_id in fill_order_ids and not self._post_cancel_fills_seen(
                record
            ):
                self._mark_post_cancel_fills_seen(record)
                changed = True
            if record.order_id not in open_order_ids:
                self._resolve_pending_cancel(record)
                changed = True
                continue
            if allow_retry:
                retry_due = (
                    record.last_attempt_at is None
                    or (now - record.last_attempt_at).total_seconds()
                    >= self.cancel_retry_interval_seconds
                )
                if retry_due and record.attempt_count < self.cancel_retry_max_attempts:
                    try:
                        self.adapter.cancel_order(record.order_id)
                    except Exception:
                        pass
                    record.last_attempt_at = now
                    record.attempt_count += 1
                    self.order_state.mark_cancel_requested(record.order_id)
                    changed = True
            elapsed = (now - record.requested_at).total_seconds()
            if (
                record.attempt_count >= self.cancel_retry_max_attempts
                or elapsed >= self.cancel_attention_timeout_seconds
            ) and not record.operator_attention_required:
                record.operator_attention_required = True
                changed = True
        if changed:
            self._prune_pending_cancels()
            self._sync_pending_cancel_attention_halt()
            self._persist_safety_state()

    def observe_polled_snapshot(
        self,
        snapshot: AccountSnapshot,
        *,
        contract: Contract | None = None,
        allow_retry: bool = True,
        apply_live_delta: bool = False,
    ) -> None:
        self._reconcile_pending_cancels(snapshot, allow_retry=allow_retry)
        self._reconcile_pending_submissions(snapshot)
        self._record_snapshot_correction(snapshot, contract=contract)
        self.account_state.sync_snapshot(snapshot, contract)
        self.order_state.sync(snapshot.open_orders)
        self._update_overlay_health(snapshot, contract=contract)
        if snapshot.complete:
            scope = contract.market_key if contract is not None else "account"
            self._clear_recovery_item(
                "account-refresh-needed"
                if scope == "account"
                else "market-refresh-needed",
                scope,
                observed_at=snapshot.observed_at,
                clear_reason="authoritative snapshot observed",
            )
        if (
            apply_live_delta
            and snapshot.complete
            and not self.safety_state.overlay_delta_suppressed
        ):
            self._apply_live_user_state_delta(contract)
        self._record_truth_snapshot(snapshot)

    def _record_truth_snapshot(self, snapshot) -> None:
        partial_fills = len(
            [
                summary
                for summary in summarize_fill_state(
                    snapshot.open_orders, snapshot.fills
                )
                if summary.status == "partial"
            ]
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
        self.safety_state.last_truth_complete = snapshot.complete
        self.safety_state.last_truth_issues = list(snapshot.issues)
        self.safety_state.last_truth_open_orders = len(snapshot.open_orders)
        self.safety_state.last_truth_positions = len(snapshot.positions)
        self.safety_state.last_truth_fills = len(snapshot.fills)
        self.safety_state.last_truth_partial_fills = partial_fills
        self.safety_state.last_truth_balance_available = snapshot.balance.available
        self.safety_state.last_truth_balance_total = snapshot.balance.total
        self.safety_state.last_truth_open_order_notional = open_order_notional
        self.safety_state.last_truth_reserved_buy_notional = reserved_buy_notional
        self.safety_state.last_truth_marked_position_notional = marked_position_notional
        self.safety_state.last_truth_observed_at = snapshot.observed_at
        self._persist_detailed_truth(snapshot)
        self._persist_safety_state()

    def _order_signature(self, order: NormalizedOrder) -> tuple[Any, ...]:
        return (
            order.contract.market_key,
            order.action.value,
            order.price,
            order.quantity,
            order.remaining_quantity,
            order.status.value,
        )

    def _fill_signature(self, fill: FillSnapshot) -> tuple[Any, ...]:
        return (
            fill.contract.market_key,
            fill.order_id,
            fill.action.value,
            fill.price,
            fill.quantity,
            fill.fee,
            fill.fill_id,
        )

    def _record_snapshot_correction(
        self, snapshot: AccountSnapshot, *, contract: Contract | None
    ) -> None:
        if contract is None:
            cached_orders = self.account_state.open_orders
            cached_fills = self.account_state.fills
        else:
            cached_orders = {
                order.order_id: order
                for order in self.account_state.open_orders_for(contract)
            }
            cached_fills = {
                fill.fill_key: fill for fill in self.account_state.fills_for(contract)
            }
        snapshot_orders = {order.order_id: order for order in snapshot.open_orders}
        snapshot_fills = {fill.fill_key: fill for fill in snapshot.fills}
        order_ids = set(cached_orders) | set(snapshot_orders)
        fill_keys = set(cached_fills) | set(snapshot_fills)
        terminal_confirmations = 0
        terminal_reversals = 0
        if contract is None:
            snapshot_order_ids = {order.order_id for order in snapshot.open_orders}
            for order_id in list(self._applied_live_terminal_markers):
                if order_id in snapshot_order_ids:
                    terminal_reversals += 1
                else:
                    terminal_confirmations += 1
            self._applied_live_terminal_markers = {}
        else:
            contract_key = contract.market_key
            snapshot_order_ids = {order.order_id for order in snapshot.open_orders}
            remaining_terminal_markers: dict[str, tuple[datetime | None, str]] = {}
            for order_id, marker in self._applied_live_terminal_markers.items():
                marker_contract_key = marker[1]
                if marker_contract_key != contract_key:
                    remaining_terminal_markers[order_id] = marker
                    continue
                if order_id in snapshot_order_ids:
                    terminal_reversals += 1
                else:
                    terminal_confirmations += 1
            self._applied_live_terminal_markers = remaining_terminal_markers
        order_corrections = 0
        for order_id in order_ids:
            cached_order = cached_orders.get(order_id)
            snapshot_order = snapshot_orders.get(order_id)
            if cached_order is None or snapshot_order is None:
                order_corrections += 1
                continue
            if self._order_signature(cached_order) != self._order_signature(
                snapshot_order
            ):
                order_corrections += 1
        fill_corrections = 0
        for fill_key in fill_keys:
            cached_fill = cached_fills.get(fill_key)
            snapshot_fill = snapshot_fills.get(fill_key)
            if cached_fill is None or snapshot_fill is None:
                fill_corrections += 1
                continue
            if self._fill_signature(cached_fill) != self._fill_signature(snapshot_fill):
                fill_corrections += 1
        self.safety_state.last_snapshot_correction_at = snapshot.observed_at
        self.safety_state.last_snapshot_correction_order_count = order_corrections
        self.safety_state.last_snapshot_correction_fill_count = fill_corrections
        self.safety_state.last_snapshot_terminal_confirmation_count = (
            terminal_confirmations
        )
        self.safety_state.last_snapshot_terminal_reversal_count = terminal_reversals

    def _apply_live_user_state_delta(self, contract: Contract | None = None) -> None:
        getter = getattr(self.adapter, "live_user_state_delta", None)
        if not callable(getter):
            self.safety_state.last_live_delta_applied_at = None
            self.safety_state.last_live_delta_source = None
            self.safety_state.last_live_delta_order_upserts = 0
            self.safety_state.last_live_delta_fill_upserts = 0
            self.safety_state.last_live_delta_terminal_orders = 0
            self.safety_state.last_live_terminal_marker_applied_count = 0
            self._persist_safety_state()
            return
        delta = getter(contract)
        if delta is None:
            self.safety_state.last_live_delta_applied_at = None
            self.safety_state.last_live_delta_source = None
            self.safety_state.last_live_delta_order_upserts = 0
            self.safety_state.last_live_delta_fill_upserts = 0
            self.safety_state.last_live_delta_terminal_orders = 0
            self.safety_state.last_live_terminal_marker_applied_count = 0
            self._persist_safety_state()
            return

        terminal_order_ids = list(getattr(delta, "terminal_order_ids", ()) or ())
        applicable_terminal_order_ids: list[str] = []
        observed_at = getattr(delta, "observed_at", None)
        pending_cancel_order_ids = self.pending_cancel_order_ids(
            contract, unresolved_only=True
        )
        if contract is not None:
            contract_key = contract.market_key
            terminal_order_ids = [
                order_id
                for order_id in terminal_order_ids
                if (
                    (order := self.account_state.open_orders.get(order_id)) is not None
                    and order.contract.market_key == contract_key
                )
            ]
        for order_id in terminal_order_ids:
            if order_id in pending_cancel_order_ids:
                continue
            order = self.account_state.open_orders.get(order_id)
            if order is None:
                continue
            if observed_at is not None and order.updated_at > observed_at:
                continue
            applicable_terminal_order_ids.append(order_id)
        order_upserts = cast(
            list[NormalizedOrder], list(getattr(delta, "open_orders", ()) or ())
        )
        fill_upserts = cast(list[FillSnapshot], list(getattr(delta, "fills", ()) or ()))
        terminal_order_ids = cast(list[str], applicable_terminal_order_ids)
        applied_orders = self.order_state.apply_live_order_upserts(order_upserts)
        self.account_state.apply_live_order_upserts(order_upserts)
        applied_fills = self.account_state.apply_live_fill_upserts(fill_upserts)
        removed_orders = self.order_state.apply_live_terminal_orders(terminal_order_ids)
        self.account_state.apply_live_terminal_orders(terminal_order_ids)
        for order_id in terminal_order_ids:
            order = self.account_state.open_orders.get(order_id)
            contract_key = (
                contract.market_key
                if contract is not None
                else order.contract.market_key
                if order is not None
                else ""
            )
            self._applied_live_terminal_markers[order_id] = (observed_at, contract_key)
        self.safety_state.last_live_delta_applied_at = observed_at
        self.safety_state.last_live_delta_source = getattr(delta, "source", None)
        self.safety_state.last_live_delta_order_upserts = applied_orders
        self.safety_state.last_live_delta_fill_upserts = applied_fills
        self.safety_state.last_live_delta_terminal_orders = removed_orders
        self.safety_state.last_live_terminal_marker_applied_count = removed_orders
        self._persist_safety_state()

    def _update_overlay_health(
        self, snapshot: AccountSnapshot, *, contract: Contract | None = None
    ) -> None:
        scope = contract.market_key if contract is not None else "account"
        getter = getattr(self.adapter, "live_state_status", None)
        if not callable(getter):
            self.safety_state.overlay_degraded = False
            self.safety_state.overlay_degraded_since = None
            self.safety_state.overlay_degraded_reason = None
            self.safety_state.overlay_delta_suppressed = False
            self.safety_state.overlay_last_live_event_at = None
            self.safety_state.overlay_last_confirmed_snapshot_at = snapshot.observed_at
            self.safety_state.overlay_last_live_state_active = False
            self.safety_state.overlay_last_subscribed_markets = cast(list[str], [])
            self._clear_recovery_item(
                "snapshot-gap",
                scope,
                observed_at=snapshot.observed_at,
                clear_reason="authoritative snapshot complete",
            )
            self._clear_recovery_item(
                "market-overlay-stale",
                scope,
                observed_at=snapshot.observed_at,
                clear_reason="live overlay healthy",
            )
            self._persist_safety_state()
            return

        status = getter()
        live_mode = getattr(status, "mode", None)
        current_active = bool(getattr(status, "active", False))
        current_markets = cast(
            list[str], list(getattr(status, "subscribed_markets", ()) or ())
        )
        live_event_at = max(
            [
                value
                for value in (
                    getattr(status, "last_update_at", None),
                    getattr(status, "fills_last_update_at", None),
                )
                if value is not None
            ],
            default=None,
        )

        trigger_reasons: list[str] = []

        degraded_reason: str | None = None
        if not snapshot.complete:
            degraded_reason = "authoritative snapshot incomplete"
        elif live_mode == "degraded":
            degraded_detail = getattr(status, "degraded_reason", None) or getattr(
                status, "last_error", None
            )
            degraded_reason = (
                f"live state error: {degraded_detail}"
                if degraded_detail not in (None, "")
                else "live state degraded"
            )
        elif getattr(status, "last_error", None):
            degraded_reason = f"live state error: {getattr(status, 'last_error', None)}"
        elif self.safety_state.last_snapshot_terminal_reversal_count > 0:
            degraded_reason = "terminal marker reversed by snapshot"
        elif (
            current_active
            and live_event_at is not None
            and (snapshot.observed_at - live_event_at).total_seconds()
            > self.overlay_max_age_seconds
        ):
            degraded_reason = "overlay max age exceeded"

        previously_degraded = self.safety_state.overlay_degraded
        self.safety_state.overlay_last_live_event_at = live_event_at
        self.safety_state.overlay_last_live_state_active = current_active
        self.safety_state.overlay_last_subscribed_markets = current_markets

        if degraded_reason is not None:
            marker = getattr(self.adapter, "mark_live_state_degraded", None)
            if callable(marker):
                marker(degraded_reason)
            self.safety_state.overlay_degraded = True
            if not previously_degraded:
                self.safety_state.overlay_degraded_since = snapshot.observed_at
            self.safety_state.overlay_degraded_reason = degraded_reason
            self.safety_state.overlay_delta_suppressed = True
            trigger_reasons.append(degraded_reason)
            if degraded_reason == "authoritative snapshot incomplete":
                self._upsert_recovery_item(
                    "snapshot-gap",
                    scope,
                    degraded_reason,
                    "authoritative_snapshot",
                    observed_at=snapshot.observed_at,
                )
            else:
                self._upsert_recovery_item(
                    "market-overlay-stale",
                    scope,
                    degraded_reason,
                    "authoritative_snapshot",
                    observed_at=snapshot.observed_at,
                )
        else:
            confirmer = getattr(self.adapter, "confirm_live_state_recovery", None)
            recovery_confirmed = False
            if live_mode == "recovering" and callable(confirmer):
                confirmer(snapshot.observed_at)
                recovery_confirmed = True
            self.safety_state.overlay_degraded = False
            if previously_degraded:
                self.safety_state.overlay_last_recovery_outcome = "snapshot_confirmed"
                self.safety_state.overlay_last_recovery_scope = (
                    contract.market_key if contract is not None else "account"
                )
                self.safety_state.overlay_last_recovery_at = snapshot.observed_at
                if self.safety_state.overlay_degraded_since is not None:
                    self.safety_state.overlay_last_suppression_duration_seconds = (
                        snapshot.observed_at - self.safety_state.overlay_degraded_since
                    ).total_seconds()
            self.safety_state.overlay_degraded_since = None
            self.safety_state.overlay_degraded_reason = None
            self.safety_state.overlay_delta_suppressed = (
                live_mode == "recovering" and not recovery_confirmed
            )
            self.safety_state.overlay_last_confirmed_snapshot_at = snapshot.observed_at
            self._clear_recovery_item(
                "snapshot-gap",
                scope,
                observed_at=snapshot.observed_at,
                clear_reason="authoritative snapshot complete",
            )
            self._clear_recovery_item(
                "market-overlay-stale",
                scope,
                observed_at=snapshot.observed_at,
                clear_reason="live overlay healthy",
            )

        for reason in trigger_reasons:
            self.request_authoritative_refresh(
                reason,
                scope=scope,
            )
        self._persist_safety_state()

    def _normalized_order_from_intent(
        self,
        order_id: str,
        intent: OrderIntent,
        raw: Any | None = None,
    ) -> NormalizedOrder:
        return NormalizedOrder(
            order_id=order_id,
            contract=intent.contract,
            action=intent.action,
            price=intent.price,
            quantity=intent.quantity,
            remaining_quantity=intent.quantity,
            post_only=intent.post_only,
            reduce_only=intent.reduce_only,
            expiration_ts=intent.expiration_ts,
            client_order_id=intent.client_order_id,
            raw=raw,
        )

    def restore_from_venue(self, contract: Contract) -> None:
        snapshot = self.adapter.get_account_snapshot(contract)
        self.observe_polled_snapshot(snapshot, contract=contract, allow_retry=True)

    def build_context(
        self,
        contract: Contract,
        fair_value: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StrategyContext:
        self._sync_operator_control_state()
        self.sync_heartbeat_state()
        snapshot = self.adapter.get_account_snapshot(None)
        self.observe_polled_snapshot(snapshot, allow_retry=True, apply_live_delta=True)
        book = self.adapter.get_order_book(contract)
        pending_submission_orders = self.pending_submission_reservations(contract)
        open_orders = (
            self.account_state.open_orders_for(contract) + pending_submission_orders
        )
        position = self.account_state.position_for(contract)
        balance = self.account_state.balance or self.adapter.get_balance()
        contract_fill_summaries = summarize_fill_state(
            open_orders,
            self.account_state.fills_for(contract),
        )
        global_fill_summaries = summarize_fill_state(
            list(self.account_state.open_orders.values()),
            list(self.account_state.fills.values()),
        )
        merged_metadata = dict(metadata or {})
        merged_metadata["account_snapshot_complete"] = snapshot.complete
        merged_metadata["account_snapshot_issues"] = list(snapshot.issues)
        merged_metadata["engine_halted"] = self.safety_state.halted
        merged_metadata["engine_halt_reason"] = self.safety_state.reason
        merged_metadata["engine_paused"] = self.safety_state.paused
        merged_metadata["engine_pause_reason"] = self.safety_state.pause_reason
        merged_metadata["hold_new_orders"] = self.safety_state.hold_new_orders
        merged_metadata["hold_reason"] = self.safety_state.hold_reason
        merged_metadata["hold_since"] = self.safety_state.hold_since
        merged_metadata["last_action_gate_action"] = (
            self.safety_state.last_action_gate_action
        )
        merged_metadata["last_action_gate_reason"] = (
            self.safety_state.last_action_gate_reason
        )
        merged_metadata["resume_confirmation_required"] = (
            self.resume_confirmation_required
        )
        merged_metadata["clean_resume_streak"] = self.safety_state.clean_resume_streak
        merged_metadata["global_open_order_count"] = len(self.account_state.open_orders)
        merged_metadata["global_open_order_notional"] = sum(
            order.remaining_quantity * order.price
            for order in self.account_state.open_orders.values()
        )
        merged_metadata["pending_submission_count_contract"] = len(
            self.pending_submissions(contract, unresolved_only=True)
        )
        merged_metadata["pending_submission_count_global"] = len(
            self.pending_submissions(unresolved_only=True)
        )
        merged_metadata["pending_submission_reserved_notional_contract"] = sum(
            order.remaining_quantity * order.price
            for order in pending_submission_orders
        )
        merged_metadata["pending_submission_reserved_notional_global"] = sum(
            order.remaining_quantity * order.price
            for order in self.pending_submission_reservations()
        )
        merged_metadata["partial_fill_count_contract"] = len(
            [
                summary
                for summary in contract_fill_summaries
                if summary.status == "partial"
            ]
        )
        merged_metadata["partial_fill_count_global"] = len(
            [
                summary
                for summary in global_fill_summaries
                if summary.status == "partial"
            ]
        )
        merged_metadata["heartbeat_required"] = self.safety_state.heartbeat_required
        merged_metadata["heartbeat_active"] = self.safety_state.heartbeat_active
        merged_metadata["heartbeat_running"] = self.safety_state.heartbeat_running
        merged_metadata["heartbeat_healthy_for_trading"] = (
            self.safety_state.heartbeat_healthy_for_trading
        )
        merged_metadata["heartbeat_unhealthy"] = self.safety_state.heartbeat_unhealthy
        merged_metadata["heartbeat_last_success_at"] = (
            self.safety_state.heartbeat_last_success_at
        )
        merged_metadata["heartbeat_consecutive_failures"] = (
            self.safety_state.heartbeat_consecutive_failures
        )
        merged_metadata["heartbeat_last_error"] = self.safety_state.heartbeat_last_error
        merged_metadata["heartbeat_last_id"] = self.safety_state.heartbeat_last_id
        merged_metadata["pending_cancel_count_contract"] = len(
            self.pending_cancels(contract, unresolved_only=True)
        )
        merged_metadata["pending_cancel_count_global"] = len(
            self.pending_cancels(unresolved_only=True)
        )
        merged_metadata["pending_refresh_request_count"] = len(
            self.safety_state.pending_refresh_requests
        )
        merged_metadata["last_live_delta_applied_at"] = (
            self.safety_state.last_live_delta_applied_at
        )
        merged_metadata["last_live_delta_source"] = (
            self.safety_state.last_live_delta_source
        )
        merged_metadata["last_live_delta_order_upserts"] = (
            self.safety_state.last_live_delta_order_upserts
        )
        merged_metadata["last_live_delta_fill_upserts"] = (
            self.safety_state.last_live_delta_fill_upserts
        )
        merged_metadata["last_live_delta_terminal_orders"] = (
            self.safety_state.last_live_delta_terminal_orders
        )
        merged_metadata["last_live_terminal_marker_applied_count"] = (
            self.safety_state.last_live_terminal_marker_applied_count
        )
        merged_metadata["last_snapshot_correction_at"] = (
            self.safety_state.last_snapshot_correction_at
        )
        merged_metadata["last_snapshot_correction_order_count"] = (
            self.safety_state.last_snapshot_correction_order_count
        )
        merged_metadata["last_snapshot_correction_fill_count"] = (
            self.safety_state.last_snapshot_correction_fill_count
        )
        merged_metadata["last_snapshot_terminal_confirmation_count"] = (
            self.safety_state.last_snapshot_terminal_confirmation_count
        )
        merged_metadata["last_snapshot_terminal_reversal_count"] = (
            self.safety_state.last_snapshot_terminal_reversal_count
        )
        merged_metadata["overlay_degraded"] = self.safety_state.overlay_degraded
        merged_metadata["overlay_degraded_since"] = (
            self.safety_state.overlay_degraded_since
        )
        merged_metadata["overlay_degraded_reason"] = (
            self.safety_state.overlay_degraded_reason
        )
        merged_metadata["overlay_delta_suppressed"] = (
            self.safety_state.overlay_delta_suppressed
        )
        merged_metadata["overlay_last_live_event_at"] = (
            self.safety_state.overlay_last_live_event_at
        )
        merged_metadata["overlay_last_confirmed_snapshot_at"] = (
            self.safety_state.overlay_last_confirmed_snapshot_at
        )
        merged_metadata["overlay_forced_snapshot_count"] = (
            self.safety_state.overlay_forced_snapshot_count
        )
        merged_metadata["overlay_last_forced_snapshot_reason"] = (
            self.safety_state.overlay_last_forced_snapshot_reason
        )
        merged_metadata["overlay_last_forced_snapshot_scope"] = (
            self.safety_state.overlay_last_forced_snapshot_scope
        )
        merged_metadata["overlay_last_recovery_outcome"] = (
            self.safety_state.overlay_last_recovery_outcome
        )
        merged_metadata["overlay_last_recovery_scope"] = (
            self.safety_state.overlay_last_recovery_scope
        )
        merged_metadata["overlay_last_recovery_at"] = (
            self.safety_state.overlay_last_recovery_at
        )
        merged_metadata["overlay_last_suppression_duration_seconds"] = (
            self.safety_state.overlay_last_suppression_duration_seconds
        )
        return StrategyContext(
            contract=contract,
            book=book,
            position=position,
            balance=balance,
            open_orders=open_orders,
            fair_value=fair_value,
            metadata=merged_metadata,
        )

    def _block_for_incomplete_snapshot(
        self, preview: EngineRunResult
    ) -> EngineRunResult:
        self._stop_live_heartbeat()
        issues = preview.context.metadata.get("account_snapshot_issues", [])
        reason = "venue account snapshot incomplete"
        if issues:
            reason = f"{reason}: {'; '.join(issues)}"
        blocked_rejections = list(preview.risk.rejected)
        blocked_rejections.extend(
            Rejection(intent, reason) for intent in preview.risk.approved
        )
        return EngineRunResult(
            context=preview.context,
            proposed=preview.proposed,
            risk=RiskDecision(approved=[], rejected=blocked_rejections),
            placements=[],
            reconciliation_before=preview.reconciliation_before,
            reconciliation_after=None,
        )

    def _block_for_engine_halt(self, preview: EngineRunResult) -> EngineRunResult:
        reason = (
            self.safety_state.reason
            or "engine halted after unsafe reconciliation drift"
        )
        intents = preview.proposed or [
            rejection.intent for rejection in preview.risk.rejected
        ]
        blocked_rejections = [Rejection(intent, reason) for intent in intents]
        return EngineRunResult(
            context=preview.context,
            proposed=preview.proposed,
            risk=RiskDecision(approved=[], rejected=blocked_rejections),
            placements=[],
            reconciliation_before=preview.reconciliation_before,
            reconciliation_after=None,
        )

    def _block_for_pause(self, preview: EngineRunResult) -> EngineRunResult:
        self._stop_live_heartbeat()
        reason = self.safety_state.pause_reason or "engine paused by operator"
        blocked_rejections = list(preview.risk.rejected)
        blocked_rejections.extend(
            Rejection(intent, reason) for intent in preview.risk.approved
        )
        return EngineRunResult(
            context=preview.context,
            proposed=preview.proposed,
            risk=RiskDecision(approved=[], rejected=blocked_rejections),
            placements=[],
            reconciliation_before=preview.reconciliation_before,
            reconciliation_after=None,
        )

    def _block_for_heartbeat(
        self, preview: EngineRunResult, reason: str
    ) -> EngineRunResult:
        blocked_rejections = list(preview.risk.rejected)
        blocked_rejections.extend(
            Rejection(intent, reason) for intent in preview.risk.approved
        )
        return EngineRunResult(
            context=preview.context,
            proposed=preview.proposed,
            risk=RiskDecision(approved=[], rejected=blocked_rejections),
            placements=[],
            reconciliation_before=preview.reconciliation_before,
            reconciliation_after=None,
        )

    def _halt_engine(self, contract: Contract, reason: str) -> None:
        self.safety_state.halted = True
        self.safety_state.reason = reason
        self.safety_state.contract_key = contract.market_key
        self.safety_state.clean_resume_streak = 0
        self.safety_state.last_clean_resume_observed_at = None
        self._persist_safety_state()

    def halt(self, reason: str, contract: Contract | None = None) -> None:
        self._stop_live_heartbeat()
        if contract is None:
            self.safety_state.halted = True
            self.safety_state.reason = reason
            self.safety_state.clean_resume_streak = 0
            self.safety_state.last_clean_resume_observed_at = None
            self._persist_safety_state()
            return
        self._halt_engine(contract, reason)

    def heartbeat_block_reason(self) -> str | None:
        self.sync_heartbeat_state()
        if not self.safety_state.heartbeat_required:
            return None
        if self.safety_state.heartbeat_unhealthy:
            detail = self.safety_state.heartbeat_last_error or "heartbeat unhealthy"
            return f"Polymarket heartbeat unhealthy for trading: {detail}"
        if not self.safety_state.heartbeat_active:
            detail = self.safety_state.heartbeat_last_error
            if detail:
                return f"Polymarket heartbeat is not active for live trading: {detail}"
            return "Polymarket heartbeat is not active for live trading"
        if not self.safety_state.heartbeat_healthy_for_trading:
            detail = self.safety_state.heartbeat_last_error
            if detail:
                return f"awaiting first successful Polymarket heartbeat: {detail}"
            return "awaiting first successful Polymarket heartbeat"
        return None

    def pending_cancel_block_reason(
        self, contract: Contract | None = None
    ) -> str | None:
        records = self._pending_cancel_attention_records(contract)
        if not records:
            return None
        oldest = min(records, key=lambda item: item.requested_at)
        age_seconds = (datetime.now(timezone.utc) - oldest.requested_at).total_seconds()
        fill_suffix = (
            "; post-cancel fills seen" if self._post_cancel_fills_seen(oldest) else ""
        )
        return (
            f"unresolved cancel requires operator attention for order {oldest.order_id} "
            f"(attempts={oldest.attempt_count}, age={age_seconds:.1f}s{fill_suffix})"
        )

    def pending_cancel_submission_guard_reason(
        self, contract: Contract | None = None
    ) -> str | None:
        records = self.pending_cancels(contract, unresolved_only=True)
        if not records:
            return None
        oldest = min(records, key=lambda item: item.requested_at)
        age_seconds = (datetime.now(timezone.utc) - oldest.requested_at).total_seconds()
        return (
            f"pending cancel awaiting authoritative observation for order {oldest.order_id} "
            f"(age={age_seconds:.1f}s)"
        )

    def _placement_acknowledgement_failures(
        self,
        placements: list[PlacementResult],
        observed_snapshot,
    ) -> list[str]:
        observed_order_ids = {order.order_id for order in observed_snapshot.open_orders}
        observed_fill_order_ids = {
            fill.order_id for fill in observed_snapshot.fills if fill.order_id
        }
        failures: list[str] = []
        for placement in placements:
            if not placement.accepted:
                continue
            if placement.order_id is not None and any(
                record.order_id == placement.order_id
                for record in self.pending_submissions(unresolved_only=True)
            ):
                continue
            if not placement.order_id:
                if any(
                    record.order_id is None
                    for record in self.pending_submissions(unresolved_only=True)
                ):
                    continue
                failures.append(
                    "venue accepted placement without an order_id; cannot acknowledge safely"
                )
                continue
            if placement.order_id in observed_order_ids:
                continue
            if placement.order_id in observed_fill_order_ids:
                continue
            failures.append(
                f"placed order {placement.order_id} was not acknowledged by the venue snapshot"
            )
        return failures

    def _schedule_pending_submission_refreshes(
        self, contract: Contract, placements: list[PlacementResult]
    ) -> None:
        for placement in placements:
            if not placement.accepted:
                continue
            matching_records = [
                record
                for record in self.pending_submissions(contract, unresolved_only=True)
                if record.order_id == placement.order_id
                or (placement.order_id is None and record.order_id is None)
            ]
            for record in matching_records:
                if record.status == "pending":
                    record.status = "needs_recovery"
                    record.reason = (
                        "pending submission awaiting authoritative observation"
                    )
                self.request_authoritative_refresh(
                    record.reason
                    or "pending submission awaiting authoritative observation",
                    scope=record.contract_key,
                )
        self._persist_safety_state()

    def pause(self, reason: str = "paused by operator") -> None:
        self._stop_live_heartbeat()
        self.safety_state.paused = True
        self.safety_state.pause_reason = reason
        self._persist_safety_state()

    def clear_pause(self) -> None:
        self.safety_state.paused = False
        self.safety_state.pause_reason = None
        self._persist_safety_state()

    def status_snapshot(self) -> EngineStatusSnapshot:
        self._sync_operator_control_state()
        self.sync_heartbeat_state()
        return EngineStatusSnapshot(
            halted=self.safety_state.halted,
            halt_reason=self.safety_state.reason,
            paused=self.safety_state.paused,
            pause_reason=self.safety_state.pause_reason,
            hold_new_orders=self.safety_state.hold_new_orders,
            hold_reason=self.safety_state.hold_reason,
            hold_since=self.safety_state.hold_since,
            last_action_gate_action=self.safety_state.last_action_gate_action,
            last_action_gate_reason=self.safety_state.last_action_gate_reason,
            contract_key=self.safety_state.contract_key,
            clean_resume_streak=self.safety_state.clean_resume_streak,
            last_clean_resume_observed_at=self.safety_state.last_clean_resume_observed_at,
            last_truth_complete=self.safety_state.last_truth_complete,
            last_truth_issues=self.safety_state.last_truth_issues,
            last_truth_open_orders=self.safety_state.last_truth_open_orders,
            last_truth_positions=self.safety_state.last_truth_positions,
            last_truth_fills=self.safety_state.last_truth_fills,
            last_truth_partial_fills=self.safety_state.last_truth_partial_fills,
            last_truth_balance_available=self.safety_state.last_truth_balance_available,
            last_truth_balance_total=self.safety_state.last_truth_balance_total,
            last_truth_open_order_notional=self.safety_state.last_truth_open_order_notional,
            last_truth_reserved_buy_notional=self.safety_state.last_truth_reserved_buy_notional,
            last_truth_marked_position_notional=self.safety_state.last_truth_marked_position_notional,
            last_truth_observed_at=self.safety_state.last_truth_observed_at,
            heartbeat_required=self.safety_state.heartbeat_required,
            heartbeat_active=self.safety_state.heartbeat_active,
            heartbeat_running=self.safety_state.heartbeat_running,
            heartbeat_healthy_for_trading=self.safety_state.heartbeat_healthy_for_trading,
            heartbeat_unhealthy=self.safety_state.heartbeat_unhealthy,
            heartbeat_last_success_at=self.safety_state.heartbeat_last_success_at,
            heartbeat_consecutive_failures=self.safety_state.heartbeat_consecutive_failures,
            heartbeat_last_error=self.safety_state.heartbeat_last_error,
            heartbeat_last_id=self.safety_state.heartbeat_last_id,
            last_live_delta_applied_at=self.safety_state.last_live_delta_applied_at,
            last_live_delta_source=self.safety_state.last_live_delta_source,
            last_live_delta_order_upserts=self.safety_state.last_live_delta_order_upserts,
            last_live_delta_fill_upserts=self.safety_state.last_live_delta_fill_upserts,
            last_live_delta_terminal_orders=self.safety_state.last_live_delta_terminal_orders,
            last_live_terminal_marker_applied_count=self.safety_state.last_live_terminal_marker_applied_count,
            last_snapshot_correction_at=self.safety_state.last_snapshot_correction_at,
            last_snapshot_correction_order_count=self.safety_state.last_snapshot_correction_order_count,
            last_snapshot_correction_fill_count=self.safety_state.last_snapshot_correction_fill_count,
            last_snapshot_terminal_confirmation_count=self.safety_state.last_snapshot_terminal_confirmation_count,
            last_snapshot_terminal_reversal_count=self.safety_state.last_snapshot_terminal_reversal_count,
            overlay_degraded=self.safety_state.overlay_degraded,
            overlay_degraded_since=self.safety_state.overlay_degraded_since,
            overlay_degraded_reason=self.safety_state.overlay_degraded_reason,
            overlay_delta_suppressed=self.safety_state.overlay_delta_suppressed,
            overlay_last_live_event_at=self.safety_state.overlay_last_live_event_at,
            overlay_last_confirmed_snapshot_at=self.safety_state.overlay_last_confirmed_snapshot_at,
            overlay_forced_snapshot_count=self.safety_state.overlay_forced_snapshot_count,
            overlay_last_forced_snapshot_reason=self.safety_state.overlay_last_forced_snapshot_reason,
            overlay_last_forced_snapshot_scope=self.safety_state.overlay_last_forced_snapshot_scope,
            overlay_last_recovery_outcome=self.safety_state.overlay_last_recovery_outcome,
            overlay_last_recovery_scope=self.safety_state.overlay_last_recovery_scope,
            overlay_last_recovery_at=self.safety_state.overlay_last_recovery_at,
            overlay_last_suppression_duration_seconds=self.safety_state.overlay_last_suppression_duration_seconds,
            overlay_last_live_state_active=self.safety_state.overlay_last_live_state_active,
            overlay_last_subscribed_markets=self.safety_state.overlay_last_subscribed_markets,
            pending_cancels=self.pending_cancels(unresolved_only=True),
            pending_submissions=self.pending_submissions(unresolved_only=True),
            pending_refresh_requests=list(self.safety_state.pending_refresh_requests),
            recovery_items=self.recovery_items(open_only=True),
            resume_trading_eligible=self.resume_trading_eligible(),
        )

    def clear_halt(self) -> None:
        self.safety_state.halted = False
        self.safety_state.reason = None
        self.safety_state.contract_key = None
        self.safety_state.clean_resume_streak = 0
        self.safety_state.last_clean_resume_observed_at = None
        self._persist_safety_state()

    def try_resume(self, contract: Contract) -> ReconciliationReport:
        self._sync_operator_control_state()
        if (
            self.safety_state.contract_key is not None
            and self.safety_state.contract_key != contract.market_key
        ):
            report = ReconciliationReport(contract=contract)
            self.safety_state.halted = True
            self.safety_state.reason = (
                self.safety_state.reason
                or f"engine halted for different contract: {self.safety_state.contract_key}"
            )
            self.safety_state.clean_resume_streak = 0
            self.safety_state.last_clean_resume_observed_at = None
            self._persist_safety_state()
            return report
        snapshot = self.adapter.get_account_snapshot(contract)
        report = self.reconcile_persisted_truth(contract, snapshot)
        self._reconcile_pending_cancels(snapshot, allow_retry=False)
        cancel_block_reason = self.pending_cancel_block_reason(contract)
        if snapshot.complete and report.policy.action == "ok":
            if cancel_block_reason is not None:
                self.safety_state.halted = True
                self.safety_state.reason = cancel_block_reason
                self.safety_state.contract_key = contract.market_key
                self.safety_state.clean_resume_streak = 0
                self.safety_state.last_clean_resume_observed_at = None
                self._persist_safety_state()
                return report
            self.observe_polled_snapshot(
                snapshot,
                contract=contract,
                allow_retry=False,
                apply_live_delta=False,
            )
            previous = self.safety_state.last_clean_resume_observed_at
            if previous is None or snapshot.observed_at > previous:
                self.safety_state.clean_resume_streak += 1
                self.safety_state.last_clean_resume_observed_at = snapshot.observed_at
                if (
                    self.safety_state.clean_resume_streak
                    >= self.resume_confirmation_required
                ):
                    self.clear_halt()
                else:
                    self.safety_state.halted = True
                    self.safety_state.contract_key = contract.market_key
                    self._persist_safety_state()
            else:
                self.safety_state.halted = True
                self.safety_state.reason = "resume evidence is not fresh"
                self.safety_state.contract_key = contract.market_key
                self.safety_state.clean_resume_streak = 0
                self.safety_state.last_clean_resume_observed_at = None
                self._persist_safety_state()
        else:
            self.safety_state.halted = True
            reason = report.policy.reason or "; ".join(snapshot.issues)
            self.safety_state.reason = reason or "unable to resume safely"
            self.safety_state.contract_key = contract.market_key
            self.safety_state.clean_resume_streak = 0
            self.safety_state.last_clean_resume_observed_at = None
            self._persist_safety_state()
        return report

    def preview_once(
        self,
        contract: Contract,
        fair_value: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> EngineRunResult:
        context = self.build_context(contract, fair_value=fair_value, metadata=metadata)
        reconciliation_before = self.reconciliation.reconcile(
            contract,
            pending_cancel_order_ids=self.pending_cancel_order_ids(
                contract, unresolved_only=True
            ),
        )
        proposed = self.strategy.generate_intents(context)
        risk_open_orders = (
            list(self.account_state.open_orders.values())
            + self.pending_submission_reservations()
        )
        risk = self.risk_engine.evaluate(
            proposed,
            position=context.position,
            positions=list(self.account_state.positions.values()),
            open_orders=risk_open_orders,
        )
        return EngineRunResult(
            context=context,
            proposed=proposed,
            risk=risk,
            placements=[],
            reconciliation_before=reconciliation_before,
        )

    def run_once(
        self,
        contract: Contract,
        fair_value: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> EngineRunResult:
        preview = self.preview_once(contract, fair_value=fair_value, metadata=metadata)
        if self.safety_state.paused:
            return self._block_for_pause(preview)
        if self.safety_state.halted:
            return self._block_for_engine_halt(preview)
        if not preview.context.metadata.get("account_snapshot_complete", True):
            return self._block_for_incomplete_snapshot(preview)
        heartbeat_reason = self.heartbeat_block_reason()
        if heartbeat_reason is not None:
            if self.safety_state.heartbeat_unhealthy:
                self.halt(heartbeat_reason, contract)
            return self._block_for_heartbeat(preview, heartbeat_reason)
        cancel_reason = self.pending_cancel_block_reason(contract)
        if cancel_reason is not None:
            self.halt(cancel_reason, contract)
            return self._block_for_heartbeat(preview, cancel_reason)
        if self.safety_state.hold_new_orders and preview.risk.approved:
            reason = self.safety_state.hold_reason or "new orders held by operator"
            return self._block_for_heartbeat(preview, reason)
        pending_cancel_submission_reason = self.pending_cancel_submission_guard_reason(
            contract
        )
        if pending_cancel_submission_reason is not None and preview.risk.approved:
            self.request_authoritative_refresh(
                "pending cancel awaiting authoritative observation",
                scope=contract.market_key,
            )
            return self._block_for_heartbeat(preview, pending_cancel_submission_reason)
        placements: list[PlacementResult] = []
        fail_closed_reasons: list[str] = []
        for intent in preview.risk.approved:
            policy = self.evaluate_order_action_policy(
                "submit", contract=intent.contract, intent=intent
            )
            self._record_action_policy(policy.action, policy.reason)
            if policy.action == "recover-first":
                self.request_authoritative_refresh(
                    policy.reason or "recovery required before submit",
                    scope=policy.scope or intent.contract.market_key,
                )
                placements.append(
                    PlacementResult(
                        False,
                        status=OrderStatus.REJECTED,
                        message=(
                            "placement deferred pending authoritative refresh: "
                            f"{policy.reason or 'recovery required before submit'}"
                        ),
                        raw={
                            "action_policy": policy.action,
                            "reason": policy.reason,
                            "scope": policy.scope or intent.contract.market_key,
                        },
                    )
                )
                continue
            if policy.action == "hold":
                placements.append(
                    PlacementResult(
                        False,
                        status=OrderStatus.REJECTED,
                        message=(
                            "placement held by action policy: "
                            f"{policy.reason or 'action currently held'}"
                        ),
                        raw={
                            "action_policy": policy.action,
                            "reason": policy.reason,
                            "scope": policy.scope or intent.contract.market_key,
                        },
                    )
                )
                continue
            admission_getter = getattr(self.adapter, "admit_limit_order", None)
            if callable(admission_getter):
                admission = admission_getter(intent)
                action = getattr(admission, "action", "allow")
                reason = (
                    getattr(admission, "reason", None)
                    or "admission guard blocked order"
                )
                scope = getattr(admission, "scope", None) or intent.contract.market_key
                if action == "refresh_then_retry":
                    self.request_authoritative_refresh(reason, scope=scope)
                    placements.append(
                        PlacementResult(
                            False,
                            status=OrderStatus.REJECTED,
                            message=f"placement deferred pending authoritative refresh: {reason}",
                            raw={
                                "admission_action": action,
                                "reason": reason,
                                "scope": scope,
                            },
                        )
                    )
                    break
                if action == "deny":
                    placements.append(
                        PlacementResult(
                            False,
                            status=OrderStatus.REJECTED,
                            message=f"placement denied by live admission guard: {reason}",
                            raw={
                                "admission_action": action,
                                "reason": reason,
                                "scope": scope,
                            },
                        )
                    )
                    break
            try:
                result = self.adapter.place_limit_order(intent)
            except Exception as exc:
                if self._ambiguous_submission_exception(exc):
                    record = self.track_pending_submission(
                        intent,
                        status="needs_recovery",
                        reason=f"ambiguous submission outcome: {exc}",
                    )
                    self.request_authoritative_refresh(
                        record.reason or "ambiguous submission outcome",
                        scope=record.contract_key,
                    )
                    placements.append(
                        PlacementResult(
                            False,
                            status=OrderStatus.PENDING,
                            message=(
                                "placement uncertain; authoritative refresh scheduled: "
                                f"{record.reason}"
                            ),
                            raw={
                                "exception": repr(exc),
                                "pending_submission": record.intent_id,
                            },
                        )
                    )
                    continue
                result = PlacementResult(
                    False,
                    status=OrderStatus.REJECTED,
                    message=f"placement exception: {exc}",
                    raw={"exception": repr(exc)},
                )
                fail_closed_reasons.append(
                    f"order placement raised an exception for {intent.contract.market_key}: {exc}"
                )
                placements.append(result)
                break
            placements.append(result)
            if result.accepted and result.order_id:
                self.track_pending_submission(
                    intent,
                    status="pending",
                    order_id=result.order_id,
                    reason="awaiting authoritative observation",
                )
                normalized = self._normalized_order_from_intent(
                    result.order_id,
                    intent,
                    raw=result.raw,
                )
                self.order_state.mark_submitted(normalized)
                self.account_state.record_submitted_order(normalized)
            if result.accepted and not result.order_id:
                record = self.track_pending_submission(
                    intent,
                    status="needs_recovery",
                    reason="venue accepted placement without an order_id; authoritative refresh required",
                )
                self.request_authoritative_refresh(
                    record.reason or "venue accepted placement without an order_id",
                    scope=record.contract_key,
                )
                continue
            if not result.accepted:
                existing_submission = self._pending_submission_record(
                    self._intent_id(intent), intent.contract.market_key
                )
                if existing_submission is not None:
                    self._resolve_pending_submission(
                        existing_submission,
                        status="rejected",
                        observed_at=datetime.now(timezone.utc),
                        reason=result.message or "venue rejected placement",
                    )
        observed_after = self.adapter.get_account_snapshot(contract)
        pending_cancel_order_ids = self.pending_cancel_order_ids(
            contract, unresolved_only=True
        )
        reconciliation_after = self.reconciliation.reconcile(
            contract,
            observed_snapshot=observed_after,
            pending_cancel_order_ids=pending_cancel_order_ids,
        )
        self.observe_polled_snapshot(
            observed_after,
            contract=contract,
            allow_retry=False,
            apply_live_delta=True,
        )
        self._schedule_pending_submission_refreshes(contract, placements)
        fail_closed_reasons.extend(
            self._placement_acknowledgement_failures(placements, observed_after)
        )
        if reconciliation_after.policy.action == "halt":
            fail_closed_reasons.append(reconciliation_after.policy.reason)
        if fail_closed_reasons:
            unique_reasons = list(
                dict.fromkeys(reason for reason in fail_closed_reasons if reason)
            )
            self._halt_engine(contract, "; ".join(unique_reasons))
        return EngineRunResult(
            context=preview.context,
            proposed=preview.proposed,
            risk=preview.risk,
            placements=placements,
            reconciliation_before=preview.reconciliation_before,
            reconciliation_after=reconciliation_after,
        )

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from adapters.types import OrderAction
from engine.safety_state import (
    EngineSafetyState,
    PendingCancelState,
    PendingRefreshRequestState,
    RecoveryItemState,
    PendingSubmissionState,
)


def _serialize_pending_cancel(state: PendingCancelState) -> dict:
    return {
        "order_id": state.order_id,
        "contract_key": state.contract_key,
        "requested_at": state.requested_at.isoformat(),
        "reason": state.reason,
        "last_attempt_at": (
            state.last_attempt_at.isoformat()
            if state.last_attempt_at is not None
            else None
        ),
        "attempt_count": state.attempt_count,
        "acknowledged": state.acknowledged,
        "operator_attention_required": state.operator_attention_required,
        "post_cancel_fill_seen": state.post_cancel_fill_seen,
    }


def _deserialize_pending_cancel(payload: dict) -> PendingCancelState:
    return PendingCancelState(
        order_id=str(payload["order_id"]),
        contract_key=str(payload["contract_key"]),
        requested_at=datetime.fromisoformat(payload["requested_at"]),
        reason=payload.get("reason"),
        last_attempt_at=(
            datetime.fromisoformat(payload["last_attempt_at"])
            if payload.get("last_attempt_at")
            else None
        ),
        attempt_count=int(payload.get("attempt_count", 0)),
        acknowledged=bool(payload.get("acknowledged", False)),
        operator_attention_required=bool(
            payload.get("operator_attention_required", False)
        ),
        post_cancel_fill_seen=bool(
            payload.get(
                "post_cancel_fill_seen",
                payload.get("post_cancel_fill_seen", False),
            )
        ),
    )


def _serialize_pending_submission(state: PendingSubmissionState) -> dict:
    return {
        "intent_id": state.intent_id,
        "contract_key": state.contract_key,
        "pair_id": state.pair_id,
        "contract": state.contract,
        "action": state.action,
        "price": state.price,
        "quantity": state.quantity,
        "requested_at": state.requested_at.isoformat(),
        "last_attempt_at": (
            state.last_attempt_at.isoformat()
            if state.last_attempt_at is not None
            else None
        ),
        "attempt_count": state.attempt_count,
        "order_id": state.order_id,
        "client_order_id": state.client_order_id,
        "post_only": state.post_only,
        "reduce_only": state.reduce_only,
        "expiration_ts": state.expiration_ts,
        "status": state.status,
        "reason": state.reason,
        "acknowledged": state.acknowledged,
        "observed_at": (
            state.observed_at.isoformat() if state.observed_at is not None else None
        ),
        "resolved_at": (
            state.resolved_at.isoformat() if state.resolved_at is not None else None
        ),
    }


def _deserialize_pending_submission(payload: dict) -> PendingSubmissionState:
    return PendingSubmissionState(
        intent_id=str(payload["intent_id"]),
        contract_key=str(payload["contract_key"]),
        pair_id=payload.get("pair_id"),
        contract=dict(payload["contract"]),
        action=OrderAction(payload["action"]),
        price=float(payload["price"]),
        quantity=float(payload["quantity"]),
        requested_at=datetime.fromisoformat(payload["requested_at"]),
        last_attempt_at=(
            datetime.fromisoformat(payload["last_attempt_at"])
            if payload.get("last_attempt_at")
            else None
        ),
        attempt_count=int(payload.get("attempt_count", 0)),
        order_id=payload.get("order_id"),
        client_order_id=payload.get("client_order_id"),
        post_only=bool(payload.get("post_only", False)),
        reduce_only=bool(payload.get("reduce_only", False)),
        expiration_ts=payload.get("expiration_ts"),
        status=str(payload.get("status", "pending")),
        reason=payload.get("reason"),
        acknowledged=bool(payload.get("acknowledged", False)),
        observed_at=(
            datetime.fromisoformat(payload["observed_at"])
            if payload.get("observed_at")
            else None
        ),
        resolved_at=(
            datetime.fromisoformat(payload["resolved_at"])
            if payload.get("resolved_at")
            else None
        ),
    )


def _serialize_pending_refresh_request(state: PendingRefreshRequestState) -> dict:
    return {
        "scope": state.scope,
        "reason": state.reason,
        "requested_at": state.requested_at.isoformat(),
    }


def _deserialize_pending_refresh_request(payload: dict) -> PendingRefreshRequestState:
    return PendingRefreshRequestState(
        scope=str(payload["scope"]),
        reason=str(payload["reason"]),
        requested_at=datetime.fromisoformat(payload["requested_at"]),
    )


def _serialize_recovery_item(state: RecoveryItemState) -> dict:
    return {
        "recovery_id": state.recovery_id,
        "item_type": state.item_type,
        "scope": state.scope,
        "reason": state.reason,
        "clear_source": state.clear_source,
        "opened_at": state.opened_at.isoformat(),
        "last_evidence_at": (
            state.last_evidence_at.isoformat()
            if state.last_evidence_at is not None
            else None
        ),
        "last_evidence": state.last_evidence,
        "status": state.status,
        "occurrence_count": state.occurrence_count,
        "cleared_at": (
            state.cleared_at.isoformat() if state.cleared_at is not None else None
        ),
        "clear_reason": state.clear_reason,
    }


def _deserialize_recovery_item(payload: dict) -> RecoveryItemState:
    return RecoveryItemState(
        recovery_id=str(payload["recovery_id"]),
        item_type=str(payload["item_type"]),
        scope=str(payload["scope"]),
        reason=str(payload["reason"]),
        clear_source=str(payload["clear_source"]),
        opened_at=datetime.fromisoformat(payload["opened_at"]),
        last_evidence_at=(
            datetime.fromisoformat(payload["last_evidence_at"])
            if payload.get("last_evidence_at")
            else None
        ),
        last_evidence=payload.get("last_evidence"),
        status=str(payload.get("status", "open")),
        occurrence_count=int(payload.get("occurrence_count", 1)),
        cleared_at=(
            datetime.fromisoformat(payload["cleared_at"])
            if payload.get("cleared_at")
            else None
        ),
        clear_reason=payload.get("clear_reason"),
    )


class SafetyStateStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> EngineSafetyState:
        if not self.path.exists():
            return EngineSafetyState()
        payload = json.loads(self.path.read_text())
        observed_at = payload.get("last_clean_resume_observed_at")
        heartbeat_success_at = payload.get("heartbeat_last_success_at")
        live_delta_applied_at = payload.get("last_live_delta_applied_at")
        snapshot_correction_at = payload.get("last_snapshot_correction_at")
        overlay_degraded_since = payload.get("overlay_degraded_since")
        overlay_last_live_event_at = payload.get("overlay_last_live_event_at")
        overlay_last_confirmed_snapshot_at = payload.get(
            "overlay_last_confirmed_snapshot_at"
        )
        overlay_last_recovery_at = payload.get("overlay_last_recovery_at")
        return EngineSafetyState(
            halted=bool(payload.get("halted", False)),
            reason=payload.get("reason"),
            contract_key=payload.get("contract_key"),
            clean_resume_streak=int(payload.get("clean_resume_streak", 0)),
            last_clean_resume_observed_at=(
                datetime.fromisoformat(observed_at) if observed_at else None
            ),
            paused=bool(payload.get("paused", False)),
            pause_reason=payload.get("pause_reason"),
            hold_new_orders=bool(payload.get("hold_new_orders", False)),
            hold_reason=payload.get("hold_reason"),
            hold_since=(
                datetime.fromisoformat(payload["hold_since"])
                if payload.get("hold_since")
                else None
            ),
            last_action_gate_action=payload.get("last_action_gate_action"),
            last_action_gate_reason=payload.get("last_action_gate_reason"),
            last_depth_assessment=payload.get("last_depth_assessment"),
            last_truth_complete=bool(payload.get("last_truth_complete", False)),
            last_truth_issues=payload.get("last_truth_issues"),
            last_truth_open_orders=int(payload.get("last_truth_open_orders", 0)),
            last_truth_positions=int(payload.get("last_truth_positions", 0)),
            last_truth_fills=int(payload.get("last_truth_fills", 0)),
            last_truth_partial_fills=int(payload.get("last_truth_partial_fills", 0)),
            last_truth_balance_available=payload.get("last_truth_balance_available"),
            last_truth_balance_total=payload.get("last_truth_balance_total"),
            last_truth_open_order_notional=float(
                payload.get("last_truth_open_order_notional", 0.0)
            ),
            last_truth_reserved_buy_notional=float(
                payload.get("last_truth_reserved_buy_notional", 0.0)
            ),
            last_truth_marked_position_notional=float(
                payload.get("last_truth_marked_position_notional", 0.0)
            ),
            last_truth_observed_at=(
                datetime.fromisoformat(payload["last_truth_observed_at"])
                if payload.get("last_truth_observed_at")
                else None
            ),
            heartbeat_required=bool(payload.get("heartbeat_required", False)),
            heartbeat_active=bool(payload.get("heartbeat_active", False)),
            heartbeat_running=bool(payload.get("heartbeat_running", False)),
            heartbeat_healthy_for_trading=bool(
                payload.get("heartbeat_healthy_for_trading", True)
            ),
            heartbeat_unhealthy=bool(payload.get("heartbeat_unhealthy", False)),
            heartbeat_last_success_at=(
                datetime.fromisoformat(heartbeat_success_at)
                if heartbeat_success_at
                else None
            ),
            heartbeat_consecutive_failures=int(
                payload.get("heartbeat_consecutive_failures", 0)
            ),
            heartbeat_last_error=payload.get("heartbeat_last_error"),
            heartbeat_last_id=payload.get("heartbeat_last_id"),
            last_live_delta_applied_at=(
                datetime.fromisoformat(live_delta_applied_at)
                if live_delta_applied_at
                else None
            ),
            last_live_delta_source=payload.get("last_live_delta_source"),
            last_live_delta_order_upserts=int(
                payload.get("last_live_delta_order_upserts", 0)
            ),
            last_live_delta_fill_upserts=int(
                payload.get("last_live_delta_fill_upserts", 0)
            ),
            last_live_delta_terminal_orders=int(
                payload.get("last_live_delta_terminal_orders", 0)
            ),
            last_live_terminal_marker_applied_count=int(
                payload.get("last_live_terminal_marker_applied_count", 0)
            ),
            last_snapshot_correction_at=(
                datetime.fromisoformat(snapshot_correction_at)
                if snapshot_correction_at
                else None
            ),
            last_snapshot_correction_order_count=int(
                payload.get("last_snapshot_correction_order_count", 0)
            ),
            last_snapshot_correction_fill_count=int(
                payload.get("last_snapshot_correction_fill_count", 0)
            ),
            last_snapshot_terminal_confirmation_count=int(
                payload.get("last_snapshot_terminal_confirmation_count", 0)
            ),
            last_snapshot_terminal_reversal_count=int(
                payload.get("last_snapshot_terminal_reversal_count", 0)
            ),
            overlay_degraded=bool(payload.get("overlay_degraded", False)),
            overlay_degraded_since=(
                datetime.fromisoformat(overlay_degraded_since)
                if overlay_degraded_since
                else None
            ),
            overlay_degraded_reason=payload.get("overlay_degraded_reason"),
            overlay_delta_suppressed=bool(
                payload.get("overlay_delta_suppressed", False)
            ),
            overlay_last_live_event_at=(
                datetime.fromisoformat(overlay_last_live_event_at)
                if overlay_last_live_event_at
                else None
            ),
            overlay_last_confirmed_snapshot_at=(
                datetime.fromisoformat(overlay_last_confirmed_snapshot_at)
                if overlay_last_confirmed_snapshot_at
                else None
            ),
            overlay_forced_snapshot_count=int(
                payload.get("overlay_forced_snapshot_count", 0)
            ),
            overlay_last_forced_snapshot_reason=payload.get(
                "overlay_last_forced_snapshot_reason"
            ),
            overlay_last_forced_snapshot_scope=payload.get(
                "overlay_last_forced_snapshot_scope"
            ),
            overlay_last_recovery_outcome=payload.get("overlay_last_recovery_outcome"),
            overlay_last_recovery_scope=payload.get("overlay_last_recovery_scope"),
            overlay_last_recovery_at=(
                datetime.fromisoformat(overlay_last_recovery_at)
                if overlay_last_recovery_at
                else None
            ),
            overlay_last_suppression_duration_seconds=payload.get(
                "overlay_last_suppression_duration_seconds"
            ),
            overlay_last_live_state_active=bool(
                payload.get("overlay_last_live_state_active", False)
            ),
            overlay_last_subscribed_markets=list(
                payload.get("overlay_last_subscribed_markets") or []
            ),
            persisted_open_orders=list(payload.get("persisted_open_orders") or []),
            persisted_positions=list(payload.get("persisted_positions") or []),
            persisted_fills=list(payload.get("persisted_fills") or []),
            persisted_balance=payload.get("persisted_balance"),
            pending_cancels=[
                _deserialize_pending_cancel(item)
                for item in payload.get("pending_cancels") or []
            ],
            pending_submissions=[
                _deserialize_pending_submission(item)
                for item in payload.get("pending_submissions") or []
            ],
            pending_refresh_requests=[
                _deserialize_pending_refresh_request(item)
                for item in payload.get("pending_refresh_requests") or []
            ],
            recovery_items=[
                _deserialize_recovery_item(item)
                for item in payload.get("recovery_items") or []
            ],
        )

    def save(self, state: EngineSafetyState) -> None:
        payload = asdict(state)
        observed_at = payload.get("last_clean_resume_observed_at")
        if isinstance(observed_at, datetime):
            payload["last_clean_resume_observed_at"] = observed_at.isoformat()
        truth_observed_at = payload.get("last_truth_observed_at")
        if isinstance(truth_observed_at, datetime):
            payload["last_truth_observed_at"] = truth_observed_at.isoformat()
        heartbeat_success_at = payload.get("heartbeat_last_success_at")
        if isinstance(heartbeat_success_at, datetime):
            payload["heartbeat_last_success_at"] = heartbeat_success_at.isoformat()
        live_delta_applied_at = payload.get("last_live_delta_applied_at")
        if isinstance(live_delta_applied_at, datetime):
            payload["last_live_delta_applied_at"] = live_delta_applied_at.isoformat()
        snapshot_correction_at = payload.get("last_snapshot_correction_at")
        if isinstance(snapshot_correction_at, datetime):
            payload["last_snapshot_correction_at"] = snapshot_correction_at.isoformat()
        hold_since = payload.get("hold_since")
        if isinstance(hold_since, datetime):
            payload["hold_since"] = hold_since.isoformat()
        overlay_degraded_since = payload.get("overlay_degraded_since")
        if isinstance(overlay_degraded_since, datetime):
            payload["overlay_degraded_since"] = overlay_degraded_since.isoformat()
        overlay_last_live_event_at = payload.get("overlay_last_live_event_at")
        if isinstance(overlay_last_live_event_at, datetime):
            payload["overlay_last_live_event_at"] = (
                overlay_last_live_event_at.isoformat()
            )
        overlay_last_confirmed_snapshot_at = payload.get(
            "overlay_last_confirmed_snapshot_at"
        )
        if isinstance(overlay_last_confirmed_snapshot_at, datetime):
            payload["overlay_last_confirmed_snapshot_at"] = (
                overlay_last_confirmed_snapshot_at.isoformat()
            )
        overlay_last_recovery_at = payload.get("overlay_last_recovery_at")
        if isinstance(overlay_last_recovery_at, datetime):
            payload["overlay_last_recovery_at"] = overlay_last_recovery_at.isoformat()
        payload["pending_cancels"] = [
            _serialize_pending_cancel(item) for item in state.pending_cancels
        ]
        payload["pending_submissions"] = [
            _serialize_pending_submission(item) for item in state.pending_submissions
        ]
        payload["pending_refresh_requests"] = [
            _serialize_pending_refresh_request(item)
            for item in state.pending_refresh_requests
        ]
        payload["recovery_items"] = [
            _serialize_recovery_item(item) for item in state.recovery_items
        ]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True))

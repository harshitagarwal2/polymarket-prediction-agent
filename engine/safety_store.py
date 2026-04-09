from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from adapters.types import OrderAction
from engine.safety_state import (
    EngineSafetyState,
    PendingCancelState,
    PendingRefreshRequestState,
    RecoveryItemState,
    PendingSubmissionState,
)


def _load_failure_item(message: str, *, observed_at: datetime) -> RecoveryItemState:
    return RecoveryItemState(
        recovery_id="safety-state-load-failure:account",
        item_type="safety-state-load-failure",
        scope="account",
        reason=message,
        clear_source="operator_review",
        opened_at=observed_at,
        last_evidence_at=observed_at,
        last_evidence=message,
    )


def _load_failure_state(message: str) -> EngineSafetyState:
    observed_at = datetime.now().astimezone()
    return EngineSafetyState(
        halted=True,
        reason=message,
        recovery_items=[_load_failure_item(message, observed_at=observed_at)],
    )


def _append_load_warnings(
    state: EngineSafetyState, warnings: list[str]
) -> EngineSafetyState:
    if not warnings:
        return state
    message = "safety state recovered with warnings: " + "; ".join(warnings)
    observed_at = datetime.now().astimezone()
    state.halted = True
    state.clean_resume_streak = 0
    state.last_clean_resume_observed_at = None
    state.reason = f"{state.reason}; {message}" if state.reason else message
    state.recovery_items = [
        item
        for item in state.recovery_items
        if item.recovery_id != "safety-state-load-failure:account"
    ]
    state.recovery_items.append(_load_failure_item(message, observed_at=observed_at))
    return state


def _safe_bool(value: Any, field_name: str, warnings: list[str], default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    warnings.append(f"{field_name} invalid; using {default!r}")
    return default


def _safe_int(value: Any, field_name: str, warnings: list[str], default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        warnings.append(f"{field_name} invalid; using {default!r}")
        return default


def _safe_float(
    value: Any, field_name: str, warnings: list[str], default: float | None
) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        warnings.append(f"{field_name} invalid; using {default!r}")
        return default


def _safe_str(
    value: Any, field_name: str, warnings: list[str], default: str | None = None
) -> str | None:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        warnings.append(f"{field_name} invalid; using {default!r}")
        return default


def _safe_list(
    value: Any,
    field_name: str,
    warnings: list[str],
    *,
    default: list[Any] | None = None,
) -> list[Any]:
    if value is None:
        return list(default or [])
    if isinstance(value, list):
        return list(value)
    warnings.append(f"{field_name} invalid; using {default or []!r}")
    return list(default or [])


def _safe_dict(
    value: Any,
    field_name: str,
    warnings: list[str],
    *,
    default: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if value is None:
        return default
    if isinstance(value, dict):
        return dict(value)
    warnings.append(f"{field_name} invalid; using {default!r}")
    return default


def _safe_datetime(
    value: Any,
    field_name: str,
    warnings: list[str],
    *,
    default: datetime | None = None,
) -> datetime | None:
    if value in (None, ""):
        return default
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        warnings.append(f"{field_name} invalid; using {default!r}")
        return default


def _safe_deserialize_items(
    value: Any,
    field_name: str,
    warnings: list[str],
    deserializer: Callable[[dict[str, Any]], Any],
) -> list[Any]:
    items = _safe_list(value, field_name, warnings)
    deserialized: list[Any] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            warnings.append(f"{field_name}[{index}] invalid; dropping entry")
            continue
        try:
            deserialized.append(deserializer(item))
        except (KeyError, TypeError, ValueError) as exc:
            warnings.append(f"{field_name}[{index}] invalid ({exc}); dropping entry")
    return deserialized


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
        "status": state.status,
        "resolved_at": (
            state.resolved_at.isoformat() if state.resolved_at is not None else None
        ),
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
        post_cancel_fill_seen=bool(payload.get("post_cancel_fill_seen", False)),
        status=str(payload.get("status", "pending")),
        resolved_at=(
            datetime.fromisoformat(payload["resolved_at"])
            if payload.get("resolved_at")
            else None
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

    def _temp_path(self) -> Path:
        return self.path.with_name(f"{self.path.name}.tmp")

    def _fsync_parent_dir(self) -> None:
        try:
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _write_payload_atomically(self, encoded_payload: str) -> None:
        temp_path = self._temp_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with temp_path.open("w", encoding="utf-8") as handle:
                handle.write(encoded_payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            self._fsync_parent_dir()
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

    def _candidate_paths(self) -> list[Path]:
        return [
            candidate
            for candidate in (self.path, self._temp_path())
            if candidate.exists()
        ]

    def _recover_candidate(self, candidate: Path) -> None:
        if candidate == self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(candidate, self.path)
            self._fsync_parent_dir()
        except OSError:
            return

    def _deserialize_state_payload(self, payload: dict[str, Any]) -> EngineSafetyState:
        warnings: list[str] = []
        state = EngineSafetyState(
            halted=_safe_bool(payload.get("halted"), "halted", warnings, False),
            reason=_safe_str(payload.get("reason"), "reason", warnings),
            contract_key=_safe_str(
                payload.get("contract_key"), "contract_key", warnings
            ),
            clean_resume_streak=_safe_int(
                payload.get("clean_resume_streak"),
                "clean_resume_streak",
                warnings,
                0,
            ),
            last_clean_resume_observed_at=_safe_datetime(
                payload.get("last_clean_resume_observed_at"),
                "last_clean_resume_observed_at",
                warnings,
            ),
            paused=_safe_bool(payload.get("paused"), "paused", warnings, False),
            pause_reason=_safe_str(
                payload.get("pause_reason"), "pause_reason", warnings
            ),
            hold_new_orders=_safe_bool(
                payload.get("hold_new_orders"), "hold_new_orders", warnings, False
            ),
            hold_reason=_safe_str(payload.get("hold_reason"), "hold_reason", warnings),
            hold_since=_safe_datetime(
                payload.get("hold_since"), "hold_since", warnings
            ),
            last_action_gate_action=_safe_str(
                payload.get("last_action_gate_action"),
                "last_action_gate_action",
                warnings,
            ),
            last_action_gate_reason=_safe_str(
                payload.get("last_action_gate_reason"),
                "last_action_gate_reason",
                warnings,
            ),
            last_depth_assessment=_safe_dict(
                payload.get("last_depth_assessment"),
                "last_depth_assessment",
                warnings,
            ),
            last_truth_complete=_safe_bool(
                payload.get("last_truth_complete"),
                "last_truth_complete",
                warnings,
                False,
            ),
            last_truth_issues=_safe_list(
                payload.get("last_truth_issues"), "last_truth_issues", warnings
            ),
            last_truth_open_orders=_safe_int(
                payload.get("last_truth_open_orders"),
                "last_truth_open_orders",
                warnings,
                0,
            ),
            last_truth_positions=_safe_int(
                payload.get("last_truth_positions"),
                "last_truth_positions",
                warnings,
                0,
            ),
            last_truth_fills=_safe_int(
                payload.get("last_truth_fills"), "last_truth_fills", warnings, 0
            ),
            last_truth_partial_fills=_safe_int(
                payload.get("last_truth_partial_fills"),
                "last_truth_partial_fills",
                warnings,
                0,
            ),
            last_truth_balance_available=_safe_float(
                payload.get("last_truth_balance_available"),
                "last_truth_balance_available",
                warnings,
                None,
            ),
            last_truth_balance_total=_safe_float(
                payload.get("last_truth_balance_total"),
                "last_truth_balance_total",
                warnings,
                None,
            ),
            last_truth_open_order_notional=float(
                _safe_float(
                    payload.get("last_truth_open_order_notional"),
                    "last_truth_open_order_notional",
                    warnings,
                    0.0,
                )
                or 0.0
            ),
            last_truth_reserved_buy_notional=float(
                _safe_float(
                    payload.get("last_truth_reserved_buy_notional"),
                    "last_truth_reserved_buy_notional",
                    warnings,
                    0.0,
                )
                or 0.0
            ),
            last_truth_marked_position_notional=float(
                _safe_float(
                    payload.get("last_truth_marked_position_notional"),
                    "last_truth_marked_position_notional",
                    warnings,
                    0.0,
                )
                or 0.0
            ),
            last_truth_observed_at=_safe_datetime(
                payload.get("last_truth_observed_at"),
                "last_truth_observed_at",
                warnings,
            ),
            heartbeat_required=_safe_bool(
                payload.get("heartbeat_required"),
                "heartbeat_required",
                warnings,
                False,
            ),
            heartbeat_active=_safe_bool(
                payload.get("heartbeat_active"), "heartbeat_active", warnings, False
            ),
            heartbeat_running=_safe_bool(
                payload.get("heartbeat_running"),
                "heartbeat_running",
                warnings,
                False,
            ),
            heartbeat_healthy_for_trading=_safe_bool(
                payload.get("heartbeat_healthy_for_trading"),
                "heartbeat_healthy_for_trading",
                warnings,
                True,
            ),
            heartbeat_unhealthy=_safe_bool(
                payload.get("heartbeat_unhealthy"),
                "heartbeat_unhealthy",
                warnings,
                False,
            ),
            heartbeat_last_success_at=_safe_datetime(
                payload.get("heartbeat_last_success_at"),
                "heartbeat_last_success_at",
                warnings,
            ),
            heartbeat_consecutive_failures=_safe_int(
                payload.get("heartbeat_consecutive_failures"),
                "heartbeat_consecutive_failures",
                warnings,
                0,
            ),
            heartbeat_last_error=_safe_str(
                payload.get("heartbeat_last_error"),
                "heartbeat_last_error",
                warnings,
            ),
            heartbeat_last_id=_safe_str(
                payload.get("heartbeat_last_id"), "heartbeat_last_id", warnings
            ),
            last_live_delta_applied_at=_safe_datetime(
                payload.get("last_live_delta_applied_at"),
                "last_live_delta_applied_at",
                warnings,
            ),
            last_live_delta_source=_safe_str(
                payload.get("last_live_delta_source"),
                "last_live_delta_source",
                warnings,
            ),
            last_live_delta_order_upserts=_safe_int(
                payload.get("last_live_delta_order_upserts"),
                "last_live_delta_order_upserts",
                warnings,
                0,
            ),
            last_live_delta_fill_upserts=_safe_int(
                payload.get("last_live_delta_fill_upserts"),
                "last_live_delta_fill_upserts",
                warnings,
                0,
            ),
            last_live_delta_terminal_orders=_safe_int(
                payload.get("last_live_delta_terminal_orders"),
                "last_live_delta_terminal_orders",
                warnings,
                0,
            ),
            last_live_terminal_marker_applied_count=_safe_int(
                payload.get("last_live_terminal_marker_applied_count"),
                "last_live_terminal_marker_applied_count",
                warnings,
                0,
            ),
            last_snapshot_correction_at=_safe_datetime(
                payload.get("last_snapshot_correction_at"),
                "last_snapshot_correction_at",
                warnings,
            ),
            last_snapshot_correction_order_count=_safe_int(
                payload.get("last_snapshot_correction_order_count"),
                "last_snapshot_correction_order_count",
                warnings,
                0,
            ),
            last_snapshot_correction_fill_count=_safe_int(
                payload.get("last_snapshot_correction_fill_count"),
                "last_snapshot_correction_fill_count",
                warnings,
                0,
            ),
            last_snapshot_terminal_confirmation_count=_safe_int(
                payload.get("last_snapshot_terminal_confirmation_count"),
                "last_snapshot_terminal_confirmation_count",
                warnings,
                0,
            ),
            last_snapshot_terminal_reversal_count=_safe_int(
                payload.get("last_snapshot_terminal_reversal_count"),
                "last_snapshot_terminal_reversal_count",
                warnings,
                0,
            ),
            overlay_degraded=_safe_bool(
                payload.get("overlay_degraded"), "overlay_degraded", warnings, False
            ),
            overlay_degraded_since=_safe_datetime(
                payload.get("overlay_degraded_since"),
                "overlay_degraded_since",
                warnings,
            ),
            overlay_degraded_reason=_safe_str(
                payload.get("overlay_degraded_reason"),
                "overlay_degraded_reason",
                warnings,
            ),
            overlay_delta_suppressed=_safe_bool(
                payload.get("overlay_delta_suppressed"),
                "overlay_delta_suppressed",
                warnings,
                False,
            ),
            overlay_last_live_event_at=_safe_datetime(
                payload.get("overlay_last_live_event_at"),
                "overlay_last_live_event_at",
                warnings,
            ),
            overlay_last_confirmed_snapshot_at=_safe_datetime(
                payload.get("overlay_last_confirmed_snapshot_at"),
                "overlay_last_confirmed_snapshot_at",
                warnings,
            ),
            overlay_forced_snapshot_count=_safe_int(
                payload.get("overlay_forced_snapshot_count"),
                "overlay_forced_snapshot_count",
                warnings,
                0,
            ),
            overlay_last_forced_snapshot_reason=_safe_str(
                payload.get("overlay_last_forced_snapshot_reason"),
                "overlay_last_forced_snapshot_reason",
                warnings,
            ),
            overlay_last_forced_snapshot_scope=_safe_str(
                payload.get("overlay_last_forced_snapshot_scope"),
                "overlay_last_forced_snapshot_scope",
                warnings,
            ),
            overlay_last_recovery_outcome=_safe_str(
                payload.get("overlay_last_recovery_outcome"),
                "overlay_last_recovery_outcome",
                warnings,
            ),
            overlay_last_recovery_scope=_safe_str(
                payload.get("overlay_last_recovery_scope"),
                "overlay_last_recovery_scope",
                warnings,
            ),
            overlay_last_recovery_at=_safe_datetime(
                payload.get("overlay_last_recovery_at"),
                "overlay_last_recovery_at",
                warnings,
            ),
            overlay_last_suppression_duration_seconds=_safe_float(
                payload.get("overlay_last_suppression_duration_seconds"),
                "overlay_last_suppression_duration_seconds",
                warnings,
                None,
            ),
            overlay_last_live_state_active=_safe_bool(
                payload.get("overlay_last_live_state_active"),
                "overlay_last_live_state_active",
                warnings,
                False,
            ),
            overlay_last_subscribed_markets=[
                str(item)
                for item in _safe_list(
                    payload.get("overlay_last_subscribed_markets"),
                    "overlay_last_subscribed_markets",
                    warnings,
                )
            ],
            persisted_open_orders=_safe_list(
                payload.get("persisted_open_orders"),
                "persisted_open_orders",
                warnings,
            ),
            persisted_positions=_safe_list(
                payload.get("persisted_positions"),
                "persisted_positions",
                warnings,
            ),
            persisted_fills=_safe_list(
                payload.get("persisted_fills"), "persisted_fills", warnings
            ),
            persisted_balance=_safe_dict(
                payload.get("persisted_balance"), "persisted_balance", warnings
            ),
            daily_loss_date=_safe_str(
                payload.get("daily_loss_date"), "daily_loss_date", warnings
            ),
            daily_loss_baseline_balance=_safe_float(
                payload.get("daily_loss_baseline_balance"),
                "daily_loss_baseline_balance",
                warnings,
                None,
            ),
            daily_loss_current_balance=_safe_float(
                payload.get("daily_loss_current_balance"),
                "daily_loss_current_balance",
                warnings,
                None,
            ),
            daily_loss_source=_safe_str(
                payload.get("daily_loss_source"), "daily_loss_source", warnings
            ),
            daily_loss_approximation=_safe_str(
                payload.get("daily_loss_approximation"),
                "daily_loss_approximation",
                warnings,
            ),
            daily_realized_pnl=float(
                _safe_float(
                    payload.get("daily_realized_pnl"),
                    "daily_realized_pnl",
                    warnings,
                    0.0,
                )
                or 0.0
            ),
            daily_loss_last_updated_at=_safe_datetime(
                payload.get("daily_loss_last_updated_at"),
                "daily_loss_last_updated_at",
                warnings,
            ),
            pending_cancels=_safe_deserialize_items(
                payload.get("pending_cancels"),
                "pending_cancels",
                warnings,
                _deserialize_pending_cancel,
            ),
            pending_submissions=_safe_deserialize_items(
                payload.get("pending_submissions"),
                "pending_submissions",
                warnings,
                _deserialize_pending_submission,
            ),
            pending_refresh_requests=_safe_deserialize_items(
                payload.get("pending_refresh_requests"),
                "pending_refresh_requests",
                warnings,
                _deserialize_pending_refresh_request,
            ),
            recovery_items=_safe_deserialize_items(
                payload.get("recovery_items"),
                "recovery_items",
                warnings,
                _deserialize_recovery_item,
            ),
        )
        return _append_load_warnings(state, warnings)

    def load(self) -> EngineSafetyState:
        candidates = self._candidate_paths()
        if not candidates:
            return EngineSafetyState()
        failures: list[str] = []
        for candidate in candidates:
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("top-level payload must be an object")
                state = self._deserialize_state_payload(payload)
                self._recover_candidate(candidate)
                return state
            except (
                OSError,
                UnicodeDecodeError,
                json.JSONDecodeError,
                ValueError,
            ) as exc:
                failures.append(f"{candidate.name}: {exc}")
        return _load_failure_state(
            "safety state load failed; manual recovery required ("
            + "; ".join(failures)
            + ")"
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
        daily_loss_last_updated_at = payload.get("daily_loss_last_updated_at")
        if isinstance(daily_loss_last_updated_at, datetime):
            payload["daily_loss_last_updated_at"] = (
                daily_loss_last_updated_at.isoformat()
            )
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
        self._write_payload_atomically(json.dumps(payload, indent=2, sort_keys=True))

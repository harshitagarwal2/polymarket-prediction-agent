from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from adapters.types import OrderAction


@dataclass
class PendingCancelState:
    order_id: str
    contract_key: str
    requested_at: datetime
    reason: str | None = None
    last_attempt_at: datetime | None = None
    attempt_count: int = 0
    acknowledged: bool = False
    operator_attention_required: bool = False
    post_cancel_fill_seen: bool = False
    status: str = "pending"
    resolved_at: datetime | None = None


@dataclass
class PendingSubmissionState:
    intent_id: str
    contract_key: str
    contract: dict[str, Any]
    action: OrderAction
    price: float
    quantity: float
    requested_at: datetime
    last_attempt_at: datetime | None = None
    attempt_count: int = 0
    order_id: str | None = None
    client_order_id: str | None = None
    post_only: bool = False
    reduce_only: bool = False
    expiration_ts: int | None = None
    status: str = "pending"
    reason: str | None = None
    acknowledged: bool = False
    observed_at: datetime | None = None
    resolved_at: datetime | None = None
    pair_id: str | None = None


@dataclass
class PendingRefreshRequestState:
    scope: str
    reason: str
    requested_at: datetime


@dataclass
class RecoveryItemState:
    recovery_id: str
    item_type: str
    scope: str
    reason: str
    clear_source: str
    opened_at: datetime
    last_evidence_at: datetime | None = None
    last_evidence: str | None = None
    status: str = "open"
    occurrence_count: int = 1
    cleared_at: datetime | None = None
    clear_reason: str | None = None


@dataclass
class EngineSafetyState:
    halted: bool = False
    reason: str | None = None
    contract_key: str | None = None
    clean_resume_streak: int = 0
    last_clean_resume_observed_at: datetime | None = None
    paused: bool = False
    pause_reason: str | None = None
    hold_new_orders: bool = False
    hold_reason: str | None = None
    hold_since: datetime | None = None
    last_action_gate_action: str | None = None
    last_action_gate_reason: str | None = None
    last_depth_assessment: dict[str, Any] | None = None
    last_truth_complete: bool = False
    last_truth_issues: list[str] | None = None
    last_truth_open_orders: int = 0
    last_truth_positions: int = 0
    last_truth_fills: int = 0
    last_truth_partial_fills: int = 0
    last_truth_balance_available: float | None = None
    last_truth_balance_total: float | None = None
    last_truth_open_order_notional: float = 0.0
    last_truth_reserved_buy_notional: float = 0.0
    last_truth_marked_position_notional: float = 0.0
    last_truth_observed_at: datetime | None = None
    heartbeat_required: bool = False
    heartbeat_active: bool = False
    heartbeat_running: bool = False
    heartbeat_healthy_for_trading: bool = True
    heartbeat_unhealthy: bool = False
    heartbeat_last_success_at: datetime | None = None
    heartbeat_consecutive_failures: int = 0
    heartbeat_last_error: str | None = None
    heartbeat_last_id: str | None = None
    last_live_delta_applied_at: datetime | None = None
    last_live_delta_source: str | None = None
    last_live_delta_order_upserts: int = 0
    last_live_delta_fill_upserts: int = 0
    last_live_delta_terminal_orders: int = 0
    last_live_terminal_marker_applied_count: int = 0
    last_snapshot_correction_at: datetime | None = None
    last_snapshot_correction_order_count: int = 0
    last_snapshot_correction_fill_count: int = 0
    last_snapshot_terminal_confirmation_count: int = 0
    last_snapshot_terminal_reversal_count: int = 0
    overlay_degraded: bool = False
    overlay_degraded_since: datetime | None = None
    overlay_degraded_reason: str | None = None
    overlay_delta_suppressed: bool = False
    overlay_last_live_event_at: datetime | None = None
    overlay_last_confirmed_snapshot_at: datetime | None = None
    overlay_forced_snapshot_count: int = 0
    overlay_last_forced_snapshot_reason: str | None = None
    overlay_last_forced_snapshot_scope: str | None = None
    overlay_last_recovery_outcome: str | None = None
    overlay_last_recovery_scope: str | None = None
    overlay_last_recovery_at: datetime | None = None
    overlay_last_suppression_duration_seconds: float | None = None
    overlay_last_live_state_active: bool = False
    overlay_last_subscribed_markets: list[str] = field(default_factory=list)
    persisted_open_orders: list[dict[str, Any]] = field(default_factory=list)
    persisted_positions: list[dict[str, Any]] = field(default_factory=list)
    persisted_fills: list[dict[str, Any]] = field(default_factory=list)
    persisted_balance: dict[str, Any] | None = None
    daily_loss_date: str | None = None
    daily_loss_baseline_balance: float | None = None
    daily_loss_current_balance: float | None = None
    daily_loss_source: str | None = None
    daily_loss_approximation: str | None = None
    daily_realized_pnl: float = 0.0
    daily_loss_last_updated_at: datetime | None = None
    pending_cancels: list[PendingCancelState] = field(default_factory=list)
    pending_submissions: list[PendingSubmissionState] = field(default_factory=list)
    pending_refresh_requests: list[PendingRefreshRequestState] = field(
        default_factory=list
    )
    recovery_items: list[RecoveryItemState] = field(default_factory=list)


@dataclass(frozen=True)
class EngineStatusSnapshot:
    halted: bool
    halt_reason: str | None
    paused: bool
    pause_reason: str | None
    hold_new_orders: bool
    hold_reason: str | None
    hold_since: datetime | None
    last_action_gate_action: str | None
    last_action_gate_reason: str | None
    last_depth_assessment: dict[str, Any] | None
    contract_key: str | None
    clean_resume_streak: int
    last_clean_resume_observed_at: datetime | None
    last_truth_complete: bool
    last_truth_issues: list[str] | None
    last_truth_open_orders: int
    last_truth_positions: int
    last_truth_fills: int
    last_truth_partial_fills: int
    last_truth_balance_available: float | None
    last_truth_balance_total: float | None
    last_truth_open_order_notional: float
    last_truth_reserved_buy_notional: float
    last_truth_marked_position_notional: float
    last_truth_observed_at: datetime | None
    heartbeat_required: bool
    heartbeat_active: bool
    heartbeat_running: bool
    heartbeat_healthy_for_trading: bool
    heartbeat_unhealthy: bool
    heartbeat_last_success_at: datetime | None
    heartbeat_consecutive_failures: int
    heartbeat_last_error: str | None
    heartbeat_last_id: str | None
    last_live_delta_applied_at: datetime | None
    last_live_delta_source: str | None
    last_live_delta_order_upserts: int
    last_live_delta_fill_upserts: int
    last_live_delta_terminal_orders: int
    last_live_terminal_marker_applied_count: int
    last_snapshot_correction_at: datetime | None
    last_snapshot_correction_order_count: int
    last_snapshot_correction_fill_count: int
    last_snapshot_terminal_confirmation_count: int
    last_snapshot_terminal_reversal_count: int
    overlay_degraded: bool
    overlay_degraded_since: datetime | None
    overlay_degraded_reason: str | None
    overlay_delta_suppressed: bool
    overlay_last_live_event_at: datetime | None
    overlay_last_confirmed_snapshot_at: datetime | None
    overlay_forced_snapshot_count: int
    overlay_last_forced_snapshot_reason: str | None
    overlay_last_forced_snapshot_scope: str | None
    overlay_last_recovery_outcome: str | None
    overlay_last_recovery_scope: str | None
    overlay_last_recovery_at: datetime | None
    overlay_last_suppression_duration_seconds: float | None
    overlay_last_live_state_active: bool
    overlay_last_subscribed_markets: list[str]
    daily_loss_date: str | None
    daily_loss_baseline_balance: float | None
    daily_loss_current_balance: float | None
    daily_loss_source: str | None
    daily_loss_approximation: str | None
    daily_realized_pnl: float
    daily_loss_last_updated_at: datetime | None
    daily_loss_limit_reached: bool
    pending_cancels: list[PendingCancelState]
    pending_submissions: list[PendingSubmissionState]
    pending_refresh_requests: list[PendingRefreshRequestState]
    recovery_items: list[RecoveryItemState]
    resume_trading_eligible: bool

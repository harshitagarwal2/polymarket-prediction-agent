from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
import uuid
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderBookSnapshot,
    OrderIntent,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    Venue,
)
from engine.accounting import (
    AccountTruthSummary,
    compare_truth_summaries,
    summarize_account_snapshot,
)
from engine.alerting import (
    build_runtime_alerts,
    build_runtime_heartbeat,
    load_alerts,
    load_heartbeat,
    send_alerts,
    send_heartbeat,
    write_alerts,
    write_heartbeat,
)
from engine.config_loader import load_config_file, nested_config_value
from engine import (
    OrderLifecycleManager,
    OrderLifecyclePolicy,
    summarize_fill_state,
)
from engine.cli_output import add_quiet_flag, emit_json, emit_lines
from engine.interfaces import NoopStrategy
from engine.runtime_policy import load_runtime_policy
from engine.runtime_bootstrap import build_current_state_read_adapter
from engine.runtime_bootstrap import build_adapter as _build_adapter
from engine.model_drift import build_model_drift_report, write_model_drift_report
from engine.model_drift import (
    build_model_drift_report,
    write_model_drift_report,
)
from engine.runner import TradingEngine
from engine.safety_state import (
    EngineSafetyState,
    PendingRefreshRequestState,
    RecoveryItemState,
)
from engine.safety_store import SafetyStateStore
from storage.journal import (
    EventJournal,
    read_jsonl_events,
    summarize_recent_runtime,
    summarize_scan_cycle_events,
)
from storage import (
    record_operator_sync_quote_result,
    sync_execution_fills_from_projected_state,
)
from storage.postgres import ExecutionFillRepository, bootstrap_postgres, require_postgres_dsn
from llm import (
    advisory_summary_payload,
    build_llm_advisory_artifact,
    load_llm_advisory_artifact,
    load_llm_advisory_contract_rows,
    render_llm_advisory_markdown,
    write_llm_advisory_artifacts,
)
from llm.advisory_context import build_preview_runtime_context
from execution import OrderProposal, QuoteManager
from risk.kill_switch import extract_kill_switch_reasons
from risk.cleanup import CleanupCoordinator
from risk.limits import RiskEngine, RiskLimits


DEFAULT_LLM_ADVISORY_PATH = "runtime/data/current/llm_advisory.json"


def _load_operator_policy(args):
    policy_file = getattr(args, "policy_file", None)
    if policy_file in (None, ""):
        config_file = getattr(args, "config_file", None)
        if config_file not in (None, ""):
            payload = load_config_file(config_file)
            policy_value = nested_config_value(payload, "runtime", "policy_file")
            if isinstance(policy_value, str) and policy_value.strip():
                policy_file = policy_value.strip()
    return load_runtime_policy(policy_file) if policy_file else None


def _operator_risk_limits(policy) -> RiskLimits:
    if policy is not None:
        return policy.risk_limits.build()
    return RiskLimits()


def _operator_engine_kwargs(policy) -> dict[str, Any]:
    if policy is None:
        return {}
    supported_keys = {
        "cancel_retry_interval_seconds",
        "cancel_retry_max_attempts",
        "cancel_attention_timeout_seconds",
        "overlay_max_age_seconds",
        "max_active_wallet_balance",
        "forced_refresh_debounce_seconds",
        "pending_submission_recovery_seconds",
        "pending_submission_expiry_seconds",
    }
    return {
        key: value
        for key, value in dict(policy.trading_engine.build_kwargs()).items()
        if key in supported_keys
    }


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_payload(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json_output(path: str | Path, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=output_path.parent,
        prefix=f"{output_path.name}.",
        suffix=".tmp",
        delete=False,
        encoding="utf-8",
    ) as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True))
        temp_path = Path(handle.name)
    temp_path.replace(output_path)


def _state_payload(state: EngineSafetyState) -> dict[str, Any]:
    return _normalize_payload(asdict(state))


def _pending_cancel_payloads(state: EngineSafetyState) -> list[dict[str, Any]]:
    now = datetime.now().astimezone()
    payloads: list[dict[str, Any]] = []
    for item in state.pending_cancels:
        age_seconds = (now - item.requested_at).total_seconds()
        payloads.append(
            {
                "order_id": item.order_id,
                "contract_key": item.contract_key,
                "reason": item.reason,
                "requested_at": item.requested_at,
                "last_attempt_at": item.last_attempt_at,
                "attempt_count": item.attempt_count,
                "acknowledged": item.acknowledged,
                "operator_attention_required": item.operator_attention_required,
                "post_cancel_fill_seen": item.post_cancel_fill_seen,
                "status": item.status,
                "resolved_at": item.resolved_at,
                "age_seconds": age_seconds,
            }
        )
    return _normalize_payload(payloads)


def _pending_submission_payloads(state: EngineSafetyState) -> list[dict[str, Any]]:
    now = datetime.now().astimezone()
    payloads: list[dict[str, Any]] = []
    for item in state.pending_submissions:
        age_seconds = (now - item.requested_at).total_seconds()
        payloads.append(
            {
                "intent_id": item.intent_id,
                "contract_key": item.contract_key,
                "action": item.action.value,
                "price": item.price,
                "quantity": item.quantity,
                "requested_at": item.requested_at,
                "last_attempt_at": item.last_attempt_at,
                "attempt_count": item.attempt_count,
                "order_id": item.order_id,
                "status": item.status,
                "reason": item.reason,
                "acknowledged": item.acknowledged,
                "observed_at": item.observed_at,
                "resolved_at": item.resolved_at,
                "age_seconds": age_seconds,
            }
        )
    return _normalize_payload(payloads)


def _pending_refresh_payloads(state: EngineSafetyState) -> list[dict[str, Any]]:
    now = datetime.now().astimezone()
    payloads: list[dict[str, Any]] = []
    for item in state.pending_refresh_requests:
        age_seconds = (now - item.requested_at).total_seconds()
        payloads.append(
            {
                "scope": item.scope,
                "reason": item.reason,
                "requested_at": item.requested_at,
                "age_seconds": age_seconds,
            }
        )
    return _normalize_payload(payloads)


def _recovery_item_payloads(state: EngineSafetyState) -> list[dict[str, Any]]:
    now = datetime.now().astimezone()
    payloads: list[dict[str, Any]] = []
    for item in state.recovery_items:
        reference = item.cleared_at or now
        age_seconds = (reference - item.opened_at).total_seconds()
        payloads.append(
            {
                "recovery_id": item.recovery_id,
                "item_type": item.item_type,
                "scope": item.scope,
                "reason": item.reason,
                "clear_source": item.clear_source,
                "opened_at": item.opened_at,
                "last_evidence_at": item.last_evidence_at,
                "last_evidence": item.last_evidence,
                "status": item.status,
                "occurrence_count": item.occurrence_count,
                "cleared_at": item.cleared_at,
                "clear_reason": item.clear_reason,
                "age_seconds": age_seconds,
            }
        )
    return _normalize_payload(payloads)


def _runtime_health_payload(state: EngineSafetyState) -> dict[str, Any]:
    open_recovery_items = [
        item for item in state.recovery_items if item.status == "open"
    ]
    reasons: list[str] = []
    watcher_mode = bool(
        state.hold_new_orders
        and isinstance(state.hold_reason, str)
        and state.hold_reason.startswith("watcher mode:")
    )
    if state.halted and state.reason:
        reasons.append(state.reason)
    if state.paused and state.pause_reason:
        reasons.append(state.pause_reason)
    if state.hold_new_orders and state.hold_reason:
        reasons.append(state.hold_reason)
    if state.overlay_degraded and state.overlay_degraded_reason:
        reasons.append(state.overlay_degraded_reason)
    if state.pending_submissions:
        reasons.append("pending submissions unresolved")
    if state.pending_cancels:
        reasons.append("pending cancels unresolved")
    if state.pending_refresh_requests:
        reasons.append("authoritative refresh queued")
    for item in open_recovery_items:
        reasons.append(f"{item.item_type}: {item.reason}")

    if state.halted:
        runtime_state = "halted"
    elif watcher_mode:
        runtime_state = "watcher"
    elif state.paused:
        runtime_state = "paused"
    elif state.hold_new_orders:
        runtime_state = "hold_new_orders"
    elif state.overlay_degraded:
        runtime_state = "degraded"
    elif open_recovery_items:
        runtime_state = "recovering"
    elif (
        state.overlay_delta_suppressed
        or state.pending_submissions
        or state.pending_cancels
        or state.pending_refresh_requests
    ):
        runtime_state = "recovering"
    else:
        runtime_state = "healthy"

    kill_switch_reasons = extract_kill_switch_reasons(state.reason)
    kill_switch_active = bool(kill_switch_reasons)
    hold_reason = state.hold_reason or ""

    return _normalize_payload(
        {
            "state": runtime_state,
            "reasons": reasons,
            "kill_switch_active": kill_switch_active,
            "kill_switch_reasons": list(kill_switch_reasons),
            "pending_cancel_count": len(state.pending_cancels),
            "pending_submission_count": len(state.pending_submissions),
            "pending_refresh_count": len(state.pending_refresh_requests),
            "open_recovery_count": len(open_recovery_items),
            "overlay_degraded": state.overlay_degraded,
            "overlay_delta_suppressed": state.overlay_delta_suppressed,
            "watcher_mode": watcher_mode,
            "daily_realized_pnl": state.daily_realized_pnl,
            "weekly_realized_pnl": float(
                getattr(state, "weekly_realized_pnl", 0.0) or 0.0
            ),
            "cumulative_realized_pnl": float(
                getattr(state, "cumulative_realized_pnl", 0.0) or 0.0
            ),
            "daily_loss_hold": "daily loss limit reached" in hold_reason,
            "weekly_loss_hold": "weekly loss limit reached" in hold_reason,
            "cumulative_loss_hold": "cumulative loss limit reached" in hold_reason,
            "wallet_balance_cap_hold": "active wallet balance exceeds cap"
            in hold_reason,
            "resume_trading_eligible": (
                not state.halted
                and not state.paused
                and not state.hold_new_orders
                and not open_recovery_items
            ),
        }
    )


def _filtered_advisory_artifact(artifact, contract_id: str | None):
    if contract_id in (None, ""):
        return artifact
    selected = tuple(
        row for row in artifact.contracts if row.contract_id == contract_id
    )
    if not selected:
        raise ValueError(f"contract not found in advisory artifact: {contract_id}")
    selected_market_ids = {
        row.market_id for row in selected if row.market_id is not None
    }
    return build_llm_advisory_artifact(
        selected,
        preview_order_proposals=[
            proposal
            for proposal in artifact.preview_order_proposals
            if proposal.market_id in selected_market_ids
        ],
        blocked_preview_orders=[
            blocked
            for blocked in artifact.blocked_preview_orders
            if blocked.market_id in selected_market_ids
        ],
        source=artifact.source,
        provider_name=artifact.provider_name,
        provider_model=artifact.provider_model,
        prompt_version=artifact.prompt_version,
        runtime_health=artifact.runtime_health,
        generated_at=artifact.generated_at,
    )


def _live_state_payload(adapter) -> dict[str, Any] | None:
    getter = getattr(adapter, "live_state_status", None)
    if not callable(getter):
        return None
    return _normalize_payload(getter().__dict__)


def _market_state_payload(adapter) -> dict[str, Any] | None:
    getter = getattr(adapter, "market_state_status", None)
    if not callable(getter):
        return None
    return _normalize_payload(getter().__dict__)


def _tracking_engine(args, adapter, *, policy=None) -> TradingEngine:
    return TradingEngine(
        adapter=adapter,
        strategy=NoopStrategy(),
        risk_engine=RiskEngine(_operator_risk_limits(policy)),
        safety_state_path=getattr(args, "state_file", "runtime/safety-state.json"),
        **_operator_engine_kwargs(policy),
    )


class _StateOnlyAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self._contract = Contract(
            venue=self.venue,
            symbol="state-only",
            outcome=OutcomeSide.UNKNOWN,
        )

    def health(self):
        return AdapterHealth(self.venue, True)

    def get_order_book(self, contract: Contract):
        raise RuntimeError("state-only adapter does not provide order books")

    def list_markets(self, limit: int = 100):
        return []

    def list_open_orders(self, contract: Contract | None = None):
        return []

    def list_positions(self, contract: Contract | None = None):
        return []

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract):
        return PositionSnapshot(contract=contract, quantity=0.0)

    def get_balance(self):
        return BalanceSnapshot(venue=self.venue, available=0.0, total=0.0)

    def get_account_snapshot(self, contract: Contract | None = None):
        target_contract = contract or self._contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=target_contract, quantity=0.0)],
            open_orders=[],
            fills=[],
        )

    def place_limit_order(self, intent: OrderIntent):
        raise RuntimeError("state-only adapter does not place orders")

    def cancel_order(self, order_id: str):
        return False

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


def _control_engine(args) -> TradingEngine:
    return TradingEngine(
        adapter=_StateOnlyAdapter(),
        strategy=NoopStrategy(),
        risk_engine=RiskEngine(RiskLimits()),
        safety_state_path=getattr(args, "state_file", "runtime/safety-state.json"),
    )


def _journal_action(args, action: str, payload: dict) -> None:
    journal_path = getattr(args, "journal", None)
    if not journal_path:
        return
    if "cycle_id" not in payload:
        payload = {"cycle_id": uuid.uuid4().hex, **payload}
    EventJournal(journal_path).append(action, payload)


def _preflight_execution_ledger(args, *, context: str) -> None:
    opportunity_root = getattr(args, "opportunity_root", None)
    if opportunity_root in (None, ""):
        return
    dsn = require_postgres_dsn(Path(opportunity_root) / "postgres", context=context)
    bootstrap_postgres(dsn)


def _recent_execution_status(recent_runtime: dict, snapshot) -> dict:
    recent_order_ids = list(recent_runtime.get("last_execution_order_ids") or [])
    current_open_order_ids = {order.order_id for order in snapshot.open_orders}
    current_fill_order_ids = {fill.order_id for fill in snapshot.fills}
    acknowledged_order_ids = sorted(
        order_id
        for order_id in recent_order_ids
        if order_id in current_open_order_ids or order_id in current_fill_order_ids
    )
    unresolved_order_ids = sorted(
        order_id
        for order_id in recent_order_ids
        if order_id not in current_open_order_ids
        and order_id not in current_fill_order_ids
    )
    return {
        "recent_order_ids": recent_order_ids,
        "acknowledged_order_ids": acknowledged_order_ids,
        "unresolved_order_ids": unresolved_order_ids,
    }


def _load_store(path: str) -> SafetyStateStore:
    return SafetyStateStore(path)


def _parse_contract(args, venue: Venue) -> Contract | None:
    symbol = getattr(args, "symbol", None)
    if not symbol:
        return None
    outcome_text = (getattr(args, "outcome", None) or "unknown").lower()
    outcome = (
        OutcomeSide.YES
        if outcome_text == "yes"
        else OutcomeSide.NO
        if outcome_text == "no"
        else OutcomeSide.UNKNOWN
    )
    return Contract(venue=venue, symbol=symbol, outcome=outcome)


def _require_contract(args, venue: Venue) -> Contract:
    contract = _parse_contract(args, venue)
    if contract is None:
        raise ValueError("resume requires --symbol and --outcome")
    if contract.outcome is OutcomeSide.UNKNOWN:
        raise ValueError("resume requires a contract outcome of yes or no")
    return contract


def _reconciliation_payload(report) -> dict:
    return {
        "contract_key": report.contract.market_key,
        "healthy": report.healthy,
        "policy": {
            "action": report.policy.action,
            "reason": report.policy.reason,
        },
        "missing_on_venue": list(report.missing_on_venue),
        "cancel_acknowledged": list(report.cancel_acknowledged),
        "unexpected_on_venue": list(report.unexpected_on_venue),
        "diverged_orders": list(report.diverged_orders),
        "missing_fills_on_venue": list(report.missing_fills_on_venue),
        "unexpected_fills_on_venue": list(report.unexpected_fills_on_venue),
        "cancel_race_fills": list(report.cancel_race_fills),
        "position_drift": report.position_drift,
        "balance_drift": report.balance_drift,
        "issues": [issue.__dict__ for issue in report.issues],
    }


def cmd_build_llm_advisory(args) -> int:
    policy = load_runtime_policy(args.policy_file) if args.policy_file else None
    state = _load_store(args.state_file).load()
    preview_context = build_preview_runtime_context(
        args.opportunity_root, policy=policy
    )
    artifact = build_llm_advisory_artifact(
        load_llm_advisory_contract_rows(args.llm_input),
        preview_order_proposals=preview_context.preview_order_proposals,
        blocked_preview_orders=preview_context.blocked_preview_orders,
        source="operator_cli",
        provider_name=args.provider_name,
        provider_model=args.provider_model,
        prompt_version=args.prompt_version,
        runtime_health=_runtime_health_payload(state),
    )
    json_path, markdown_path = write_llm_advisory_artifacts(artifact, args.output)
    summary = advisory_summary_payload(artifact)
    _journal_action(
        args,
        "operator_build_llm_advisory",
        {
            "output": str(json_path),
            "markdown_output": str(markdown_path),
            **summary,
        },
    )
    emit_json(
        {
            "json_output": str(json_path),
            "markdown_output": str(markdown_path),
            **summary,
        },
        quiet=args.quiet,
    )
    return 0


def cmd_show_llm_advisory(args) -> int:
    artifact = _filtered_advisory_artifact(
        load_llm_advisory_artifact(args.llm_advisory_file),
        args.contract_id,
    )
    if args.format == "markdown":
        emit_lines(render_llm_advisory_markdown(artifact), quiet=args.quiet)
    else:
        emit_json(artifact.to_payload(), quiet=args.quiet)
    return 0


def cmd_status(args) -> int:
    store = _load_store(args.state_file)
    state = store.load()
    persisted_truth = AccountTruthSummary(
        complete=state.last_truth_complete,
        issues=list(state.last_truth_issues or []),
        open_orders=state.last_truth_open_orders,
        positions=state.last_truth_positions,
        fills=state.last_truth_fills,
        partial_fills=state.last_truth_partial_fills,
        balance_available=state.last_truth_balance_available,
        balance_total=state.last_truth_balance_total,
        open_order_notional=state.last_truth_open_order_notional,
        reserved_buy_notional=state.last_truth_reserved_buy_notional,
        marked_position_notional=state.last_truth_marked_position_notional,
        observed_at=state.last_truth_observed_at,
    )
    payload: dict = {
        "safety_state": _state_payload(state),
        "depth_assessment": _normalize_payload(state.last_depth_assessment),
        "pending_cancels": _pending_cancel_payloads(state),
        "pending_submissions": _pending_submission_payloads(state),
        "pending_refresh_requests": _pending_refresh_payloads(state),
        "recovery_items": _recovery_item_payloads(state),
        "pending_cancel_operator_attention_required": any(
            item.operator_attention_required for item in state.pending_cancels
        ),
        "runtime_health": _runtime_health_payload(state),
        "last_truth_summary": {
            "complete": persisted_truth.complete,
            "issues": persisted_truth.issues,
            "open_orders": persisted_truth.open_orders,
            "positions": persisted_truth.positions,
            "fills": persisted_truth.fills,
            "partial_fills": persisted_truth.partial_fills,
            "balance_available": persisted_truth.balance_available,
            "balance_total": persisted_truth.balance_total,
            "open_order_notional": persisted_truth.open_order_notional,
            "reserved_buy_notional": persisted_truth.reserved_buy_notional,
            "marked_position_notional": persisted_truth.marked_position_notional,
            "observed_at": persisted_truth.observed_at.isoformat()
            if persisted_truth.observed_at is not None
            else None,
        },
    }
    if getattr(args, "journal", None):
        events = read_jsonl_events(args.journal)
        payload["journal_summary"] = summarize_scan_cycle_events(events)
        payload["recent_runtime"] = summarize_recent_runtime(events)
    else:
        payload["recent_runtime"] = None
    if getattr(args, "llm_advisory_file", None):
        payload["llm_advisory_summary"] = advisory_summary_payload(
            load_llm_advisory_artifact(args.llm_advisory_file)
        )
    if getattr(args, "venue", None):
        adapter = _build_adapter(args.venue)
        venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
        contract = _parse_contract(args, venue)
        snapshot = adapter.get_account_snapshot(contract)
        engine = _tracking_engine(args, adapter)
        current_truth = summarize_account_snapshot(snapshot)
        fill_summaries = summarize_fill_state(snapshot.open_orders, snapshot.fills)
        payload["venue_snapshot"] = {
            "complete": snapshot.complete,
            "issues": snapshot.issues,
            "open_orders": len(snapshot.open_orders),
            "positions": len(snapshot.positions),
            "fills": len(snapshot.fills),
            "partial_fills": len(
                [summary for summary in fill_summaries if summary.status == "partial"]
            ),
            "balance_available": snapshot.balance.available,
            "balance_total": snapshot.balance.total,
        }
        payload["truth_drift"] = compare_truth_summaries(
            persisted_truth, current_truth
        ).__dict__
        payload["live_state"] = _live_state_payload(adapter)
        payload["market_state"] = _market_state_payload(adapter)
        payload["reconciliation"] = (
            _reconciliation_payload(
                engine.reconcile_persisted_truth(contract, snapshot)
            )
            if contract is not None
            else None
        )
        recent_runtime = payload.get("recent_runtime") or {}
        payload["recent_execution_status"] = _recent_execution_status(
            recent_runtime, snapshot
        )
    if getattr(args, "output", None):
        _write_json_output(args.output, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def cmd_build_alerts(args) -> int:
    status_payload = json.loads(
        Path(args.runtime_status_file).read_text(encoding="utf-8")
    )
    if not isinstance(status_payload, dict):
        raise ValueError("runtime status payload must be a JSON object")
    payload = build_runtime_alerts(status_payload)
    if getattr(args, "output", None):
        write_alerts(args.output, payload)
    emit_json(payload, quiet=args.quiet)
    return 0


def cmd_send_alerts(args) -> int:
    payload = load_alerts(args.alerts_file)
    result = send_alerts(
        payload,
        webhook_url=args.webhook_url,
        minimum_severity=args.minimum_severity,
        dedupe_state_file=args.dedupe_state_file,
        dry_run=args.dry_run,
    )
    emit_json(result, quiet=args.quiet)
    return 0


def cmd_build_heartbeat(args) -> int:
    status_payload = json.loads(
        Path(args.runtime_status_file).read_text(encoding="utf-8")
    )
    if not isinstance(status_payload, dict):
        raise ValueError("runtime status payload must be a JSON object")
    payload = build_runtime_heartbeat(status_payload)
    if getattr(args, "output", None):
        write_heartbeat(args.output, payload)
    emit_json(payload, quiet=args.quiet)
    return 0


def cmd_send_heartbeat(args) -> int:
    payload = load_heartbeat(args.heartbeat_file)
    result = send_heartbeat(
        payload,
        webhook_url=args.webhook_url,
        dry_run=args.dry_run,
    )
    emit_json(result, quiet=args.quiet)
    return 0


def cmd_export_tax_audit(args) -> int:
    if args.opportunity_root in (None, ""):
        raise RuntimeError("tax audit export requires --opportunity-root")
    _preflight_execution_ledger(args, context="operator-cli export-tax-audit")
    synced_fill_count = sync_execution_fills_from_projected_state(args.opportunity_root)
    fill_rows = ExecutionFillRepository(Path(args.opportunity_root) / "postgres").read_all()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "fill_id",
        "order_id",
        "venue",
        "symbol",
        "outcome",
        "action",
        "price",
        "quantity",
        "fee",
        "fill_ts",
        "snapshot_observed_at",
        "snapshot_cohort_id",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for payload in fill_rows.values():
            if not isinstance(payload, dict):
                continue
            contract_key = str(payload.get("contract_key") or "")
            symbol = contract_key.split(":", 1)[0] if contract_key else None
            outcome = (
                contract_key.split(":", 1)[1]
                if contract_key and ":" in contract_key
                else None
            )
            raw_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
            writer.writerow(
                {
                    "fill_id": payload.get("fill_key") or payload.get("fill_id"),
                    "order_id": payload.get("order_id"),
                    "venue": "polymarket",
                    "symbol": symbol,
                    "outcome": outcome,
                    "action": raw_payload.get("action") if isinstance(raw_payload, dict) else None,
                    "price": payload.get("price"),
                    "quantity": payload.get("quantity"),
                    "fee": payload.get("fee"),
                    "fill_ts": payload.get("fill_ts"),
                    "snapshot_observed_at": payload.get("snapshot_observed_at"),
                    "snapshot_cohort_id": payload.get("snapshot_cohort_id"),
                }
            )
    emit_json(
        {
            "ok": True,
            "output": str(output_path),
            "row_count": len(fill_rows),
            "synced_fill_count": synced_fill_count,
        },
        quiet=args.quiet,
    )
    return 0


def cmd_build_model_drift(args) -> int:
    benchmark_payload = json.loads(
        Path(args.benchmark_report_file).read_text(encoding="utf-8")
    )
    if not isinstance(benchmark_payload, dict):
        raise ValueError("benchmark report payload must be a JSON object")
    payload = build_model_drift_report(
        benchmark_payload,
        max_brier_score=args.max_brier_score,
        max_expected_calibration_error=args.max_expected_calibration_error,
    )
    if getattr(args, "output", None):
        write_model_drift_report(args.output, payload)
    emit_json(payload, quiet=args.quiet)
    return 0


def cmd_pause(args) -> int:
    engine = _control_engine(args)
    engine.pause(args.reason)
    _journal_action(
        args,
        "operator_pause",
        {"reason": args.reason, "state": _state_payload(engine.safety_state)},
    )
    print(f"paused: {args.reason}")
    return 0


def cmd_unpause(args) -> int:
    engine = _control_engine(args)
    engine.clear_pause()
    _journal_action(
        args,
        "operator_unpause",
        {"state": _state_payload(engine.safety_state)},
    )
    print("unpaused")
    return 0


def cmd_hold_new_orders(args) -> int:
    engine = _control_engine(args)
    engine.set_new_order_hold(args.reason)
    _journal_action(
        args,
        "operator_hold_new_orders",
        {"reason": args.reason, "state": _state_payload(engine.safety_state)},
    )
    print(f"holding new orders: {args.reason}")
    return 0


def cmd_clear_hold_new_orders(args) -> int:
    engine = _control_engine(args)
    engine.clear_new_order_hold()
    _journal_action(
        args,
        "operator_clear_hold_new_orders",
        {"state": _state_payload(engine.safety_state)},
    )
    print("cleared new-order hold")
    return 0


def cmd_force_refresh(args) -> int:
    engine = _control_engine(args)
    scope = "account"
    if getattr(args, "venue", None):
        venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
        contract = _parse_contract(args, venue)
        if contract is not None:
            scope = contract.market_key
    engine.queue_authoritative_refresh_request(args.reason, scope=scope)
    _journal_action(
        args,
        "operator_force_refresh",
        {
            "scope": scope,
            "reason": args.reason,
            "state": _state_payload(engine.safety_state),
        },
    )
    print(json.dumps({"scope": scope, "reason": args.reason}, indent=2))
    return 0


def cmd_resume(args) -> int:
    adapter = _build_adapter(args.venue)
    venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
    contract = _require_contract(args, venue)
    engine = TradingEngine(
        adapter=adapter,
        strategy=NoopStrategy(),
        risk_engine=RiskEngine(_operator_risk_limits(None)),
        resume_confirmation_required=args.resume_confirmation_required,
        safety_state_path=args.state_file,
    )
    report = engine.try_resume(contract)
    status = engine.status_snapshot()
    payload = {
        "venue": args.venue,
        "resume_report": _reconciliation_payload(report),
        "safety_state": {
            "halted": status.halted,
            "halt_reason": status.halt_reason,
            "paused": status.paused,
            "pause_reason": status.pause_reason,
            "contract_key": status.contract_key,
            "clean_resume_streak": status.clean_resume_streak,
        },
        "pending_cancels": _pending_cancel_payloads(engine.safety_state),
    }
    _journal_action(
        args,
        "operator_resume",
        {
            "venue": args.venue,
            "symbol": contract.symbol,
            "outcome": contract.outcome.value,
            **payload,
        },
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def cmd_cancel_all(args) -> int:
    adapter = _build_adapter(args.venue)
    venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
    contract = _parse_contract(args, venue)
    engine = _tracking_engine(args, adapter, policy=None)
    open_orders = adapter.list_open_orders(contract)
    for order in open_orders:
        engine.track_cancel_request(
            order.order_id, order.contract, "operator cancel all"
        )
    verification = CleanupCoordinator(adapter).cancel_all_and_verify(
        contract,
        stable_polls=args.stable_polls,
        sleep_seconds=args.verify_sleep_seconds,
        max_wait_seconds=args.max_wait_seconds,
    )
    _journal_action(
        args,
        "operator_cancel_all",
        {
            "venue": args.venue,
            "symbol": getattr(contract, "symbol", None),
            "outcome": contract.outcome.value if contract is not None else None,
            "cancelled": len(open_orders),
            "pending_cancel_order_ids": [order.order_id for order in open_orders],
            "verification": verification.__dict__,
        },
    )
    print(
        json.dumps(
            {
                "venue": args.venue,
                "cancelled": len(open_orders),
                "pending_cancel_order_ids": [order.order_id for order in open_orders],
                "verification": verification.__dict__,
            },
            indent=2,
        )
    )
    return 0


def cmd_cancel_stale(args) -> int:
    policy = _load_operator_policy(args)
    adapter = _build_adapter(args.venue, args, policy=policy)
    venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
    contract = _parse_contract(args, venue)
    engine = _tracking_engine(args, adapter, policy=policy)
    lifecycle_policy = (
        policy.order_lifecycle_policy.build()
        if policy is not None and args.max_order_age_seconds == 30.0
        else OrderLifecyclePolicy(max_order_age_seconds=args.max_order_age_seconds)
    )
    manager = OrderLifecycleManager(
        adapter=adapter,
        policy=lifecycle_policy,
        cancel_handler=engine.request_cancel_order,
    )
    decisions = manager.cancel_stale_orders(contract)
    _journal_action(
        args,
        "operator_cancel_stale",
        {
            "venue": args.venue,
            "symbol": getattr(contract, "symbol", None),
            "outcome": contract.outcome.value if contract is not None else None,
            "count": len(decisions),
            "decisions": [decision.__dict__ for decision in decisions],
            "pending_cancel_order_ids": [decision.order_id for decision in decisions],
        },
    )
    print(
        json.dumps(
            {
                "venue": args.venue,
                "count": len(decisions),
                "decisions": [decision.__dict__ for decision in decisions],
                "pending_cancel_order_ids": [
                    decision.order_id for decision in decisions
                ],
            },
            indent=2,
        )
    )
    return 0


def cmd_sync_quote(args) -> int:
    policy = _load_operator_policy(args)
    adapter = _build_adapter(args.venue, args, policy=policy)
    _preflight_execution_ledger(args, context="operator-cli sync-quote")
    contract = Contract(
        venue=Venue(args.venue),
        symbol=args.symbol,
        outcome=OutcomeSide(args.outcome),
    )
    engine = TradingEngine(
        adapter=adapter,
        strategy=NoopStrategy(),
        risk_engine=RiskEngine(_operator_risk_limits(policy)),
        safety_state_path=args.state_file,
        **_operator_engine_kwargs(policy),
    )
    proposal = OrderProposal(
        market_id=args.symbol,
        side=args.side,
        action=args.action,
        price=args.price,
        size=args.quantity,
        tif=args.tif,
        rationale=args.rationale,
    )
    result = QuoteManager(engine).sync_quote(
        contract,
        proposal,
        reason=args.rationale,
    )
    cycle_id = uuid.uuid4().hex
    ledger_summary = None
    ledger_error = False
    opportunity_root = getattr(args, "opportunity_root", None)
    if opportunity_root not in (None, ""):
        try:
            ledger_summary = record_operator_sync_quote_result(
                opportunity_root,
                cycle_id=cycle_id,
                contract=contract,
                proposal=proposal,
                quote_result=result,
            )
        except Exception as exc:
            ledger_error = True
            ledger_summary = {
                "error_kind": exc.__class__.__name__,
                "error_message": str(exc),
            }
    payload = {
        "venue": args.venue,
        "symbol": args.symbol,
        "outcome": args.outcome,
        "cycle_id": cycle_id,
        "shell_action": result.action,
        "cancelled_order_ids": list(result.cancelled_order_ids),
        "submitted_order_ids": list(result.submitted_order_ids),
        "placement_count": len(result.placements),
        "ledger_summary": ledger_summary,
    }
    _journal_action(args, "operator_sync_quote", payload)
    print(json.dumps(payload, indent=2))
    return 1 if ledger_error else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction-market operator CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--state-file", default="runtime/safety-state.json")
    status.add_argument("--journal", default=None)
    status.add_argument("--llm-advisory-file", default=None)
    status.add_argument("--output", default=None)
    status.add_argument("--venue", choices=["polymarket", "kalshi"], default=None)
    status.add_argument("--symbol", default=None)
    status.add_argument(
        "--outcome", choices=["yes", "no", "unknown"], default="unknown"
    )
    add_quiet_flag(status)
    status.set_defaults(func=cmd_status)

    build_alerts = subparsers.add_parser("build-alerts")
    build_alerts.add_argument("--runtime-status-file", required=True)
    build_alerts.add_argument("--output", default=None)
    add_quiet_flag(build_alerts)
    build_alerts.set_defaults(func=cmd_build_alerts)

    send_alerts_cmd = subparsers.add_parser("send-alerts")
    send_alerts_cmd.add_argument("--alerts-file", required=True)
    send_alerts_cmd.add_argument("--webhook-url", required=True)
    send_alerts_cmd.add_argument(
        "--minimum-severity",
        choices=["info", "warning", "critical"],
        default="warning",
    )
    send_alerts_cmd.add_argument(
        "--dedupe-state-file", default="runtime/alert_dedupe_state.json"
    )
    send_alerts_cmd.add_argument("--dry-run", action="store_true")
    add_quiet_flag(send_alerts_cmd)
    send_alerts_cmd.set_defaults(func=cmd_send_alerts)

    build_heartbeat = subparsers.add_parser("build-heartbeat")
    build_heartbeat.add_argument("--runtime-status-file", required=True)
    build_heartbeat.add_argument("--output", default=None)
    add_quiet_flag(build_heartbeat)
    build_heartbeat.set_defaults(func=cmd_build_heartbeat)

    send_heartbeat_cmd = subparsers.add_parser("send-heartbeat")
    send_heartbeat_cmd.add_argument("--heartbeat-file", required=True)
    send_heartbeat_cmd.add_argument("--webhook-url", required=True)
    send_heartbeat_cmd.add_argument("--dry-run", action="store_true")
    add_quiet_flag(send_heartbeat_cmd)
    send_heartbeat_cmd.set_defaults(func=cmd_send_heartbeat)

    export_tax_audit = subparsers.add_parser("export-tax-audit")
    export_tax_audit.add_argument("--opportunity-root", required=True)
    export_tax_audit.add_argument("--output", required=True)
    export_tax_audit.add_argument("--require-postgres", action="store_true")
    add_quiet_flag(export_tax_audit)
    export_tax_audit.set_defaults(func=cmd_export_tax_audit)

    build_model_drift_cmd = subparsers.add_parser("build-model-drift")
    build_model_drift_cmd.add_argument("--benchmark-report-file", required=True)
    build_model_drift_cmd.add_argument("--output", default=None)
    build_model_drift_cmd.add_argument("--max-brier-score", type=float, default=None)
    build_model_drift_cmd.add_argument(
        "--max-expected-calibration-error", type=float, default=None
    )
    add_quiet_flag(build_model_drift_cmd)
    build_model_drift_cmd.set_defaults(func=cmd_build_model_drift)

    pause = subparsers.add_parser("pause")
    pause.add_argument("--state-file", default="runtime/safety-state.json")
    pause.add_argument("--reason", default="paused by operator")
    pause.add_argument("--journal", default=None)
    pause.set_defaults(func=cmd_pause)

    unpause = subparsers.add_parser("unpause")
    unpause.add_argument("--state-file", default="runtime/safety-state.json")
    unpause.add_argument("--journal", default=None)
    unpause.set_defaults(func=cmd_unpause)

    hold = subparsers.add_parser("hold-new-orders")
    hold.add_argument("--state-file", default="runtime/safety-state.json")
    hold.add_argument("--reason", default="new orders held by operator")
    hold.add_argument("--journal", default=None)
    hold.set_defaults(func=cmd_hold_new_orders)

    clear_hold = subparsers.add_parser("clear-hold-new-orders")
    clear_hold.add_argument("--state-file", default="runtime/safety-state.json")
    clear_hold.add_argument("--journal", default=None)
    clear_hold.set_defaults(func=cmd_clear_hold_new_orders)

    force_refresh = subparsers.add_parser("force-refresh")
    force_refresh.add_argument("--state-file", default="runtime/safety-state.json")
    force_refresh.add_argument("--journal", default=None)
    force_refresh.add_argument(
        "--venue", choices=["polymarket", "kalshi"], default=None
    )
    force_refresh.add_argument("--symbol", default=None)
    force_refresh.add_argument(
        "--outcome", choices=["yes", "no", "unknown"], default="unknown"
    )
    force_refresh.add_argument("--reason", default="operator requested refresh")
    force_refresh.set_defaults(func=cmd_force_refresh)

    resume = subparsers.add_parser("resume")
    resume.add_argument("--venue", choices=["polymarket", "kalshi"], required=True)
    resume.add_argument("--symbol", required=True)
    resume.add_argument("--outcome", choices=["yes", "no"], required=True)
    resume.add_argument("--state-file", default="runtime/safety-state.json")
    resume.add_argument("--journal", default=None)
    resume.add_argument("--resume-confirmation-required", type=int, default=2)
    resume.set_defaults(func=cmd_resume)

    cancel_all = subparsers.add_parser("cancel-all")
    cancel_all.add_argument("--venue", choices=["polymarket", "kalshi"], required=True)
    cancel_all.add_argument("--symbol", default=None)
    cancel_all.add_argument(
        "--outcome", choices=["yes", "no", "unknown"], default="unknown"
    )
    cancel_all.add_argument("--journal", default=None)
    cancel_all.add_argument("--state-file", default="runtime/safety-state.json")
    cancel_all.add_argument("--stable-polls", type=int, default=2)
    cancel_all.add_argument("--verify-sleep-seconds", type=float, default=0.5)
    cancel_all.add_argument("--max-wait-seconds", type=float, default=10.0)
    cancel_all.set_defaults(func=cmd_cancel_all)

    cancel_stale = subparsers.add_parser("cancel-stale")
    cancel_stale.add_argument(
        "--venue", choices=["polymarket", "kalshi"], required=True
    )
    cancel_stale.add_argument("--symbol", default=None)
    cancel_stale.add_argument(
        "--outcome", choices=["yes", "no", "unknown"], default="unknown"
    )
    cancel_stale.add_argument("--max-order-age-seconds", type=float, default=30.0)
    cancel_stale.add_argument("--journal", default=None)
    cancel_stale.add_argument("--state-file", default="runtime/safety-state.json")
    cancel_stale.add_argument("--policy-file", default=None)
    cancel_stale.add_argument("--config-file", default=None)
    cancel_stale.set_defaults(func=cmd_cancel_stale)

    sync_quote = subparsers.add_parser("sync-quote")
    sync_quote.add_argument("--venue", choices=["polymarket", "kalshi"], required=True)
    sync_quote.add_argument("--symbol", required=True)
    sync_quote.add_argument("--outcome", choices=["yes", "no"], required=True)
    sync_quote.add_argument(
        "--side",
        choices=["buy_yes", "sell_yes", "buy_no", "sell_no"],
        required=True,
    )
    sync_quote.add_argument(
        "--action",
        choices=["place", "replace", "amend", "cancel"],
        default="place",
    )
    sync_quote.add_argument("--price", type=float, default=0.0)
    sync_quote.add_argument("--quantity", type=float, default=0.0)
    sync_quote.add_argument("--tif", default="GTC")
    sync_quote.add_argument("--rationale", default="operator quote sync")
    sync_quote.add_argument("--journal", default=None)
    sync_quote.add_argument("--state-file", default="runtime/safety-state.json")
    sync_quote.add_argument("--opportunity-root", default="runtime/data")
    sync_quote.add_argument("--policy-file", default=None)
    sync_quote.add_argument("--config-file", default=None)
    sync_quote.set_defaults(func=cmd_sync_quote)

    build_llm_advisory = subparsers.add_parser("build-llm-advisory")
    build_llm_advisory.add_argument("--llm-input", required=True)
    build_llm_advisory.add_argument("--opportunity-root", default="runtime/data")
    build_llm_advisory.add_argument("--policy-file", default=None)
    build_llm_advisory.add_argument("--state-file", default="runtime/safety-state.json")
    build_llm_advisory.add_argument("--journal", default=None)
    build_llm_advisory.add_argument(
        "--output",
        default=DEFAULT_LLM_ADVISORY_PATH,
    )
    build_llm_advisory.add_argument("--provider-name", default="offline")
    build_llm_advisory.add_argument("--provider-model", default=None)
    build_llm_advisory.add_argument("--prompt-version", default=None)
    add_quiet_flag(build_llm_advisory)
    build_llm_advisory.set_defaults(func=cmd_build_llm_advisory)

    show_llm_advisory = subparsers.add_parser("show-llm-advisory")
    show_llm_advisory.add_argument(
        "--llm-advisory-file",
        default=DEFAULT_LLM_ADVISORY_PATH,
    )
    show_llm_advisory.add_argument("--contract-id", default=None)
    show_llm_advisory.add_argument(
        "--format",
        choices=["json", "markdown"],
        default="json",
    )
    add_quiet_flag(show_llm_advisory)
    show_llm_advisory.set_defaults(func=cmd_show_llm_advisory)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

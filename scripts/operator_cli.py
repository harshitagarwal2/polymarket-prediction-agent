from __future__ import annotations

import argparse
import json
import os
import sys
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
from engine import (
    OrderLifecycleManager,
    OrderLifecyclePolicy,
    summarize_fill_state,
)
from engine.cli_output import add_quiet_flag, emit_json, emit_lines
from engine.interfaces import NoopStrategy
from engine.runtime_policy import load_runtime_policy
from engine.runtime_bootstrap import build_adapter as _build_adapter
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


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_payload(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


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


def _tracking_engine(args, adapter) -> TradingEngine:
    return TradingEngine(
        adapter=adapter,
        strategy=NoopStrategy(),
        risk_engine=RiskEngine(RiskLimits()),
        safety_state_path=getattr(args, "state_file", "runtime/safety-state.json"),
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
    print(json.dumps(payload, indent=2, sort_keys=True))
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
        risk_engine=RiskEngine(RiskLimits()),
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
    engine = _tracking_engine(args, adapter)
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
    adapter = _build_adapter(args.venue)
    venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
    contract = _parse_contract(args, venue)
    engine = _tracking_engine(args, adapter)
    manager = OrderLifecycleManager(
        adapter=adapter,
        policy=OrderLifecyclePolicy(max_order_age_seconds=args.max_order_age_seconds),
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
    adapter = _build_adapter(args.venue)
    contract = Contract(
        venue=Venue(args.venue),
        symbol=args.symbol,
        outcome=OutcomeSide(args.outcome),
    )
    engine = TradingEngine(
        adapter=adapter,
        strategy=NoopStrategy(),
        risk_engine=RiskEngine(RiskLimits()),
        safety_state_path=args.state_file,
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
    payload = {
        "venue": args.venue,
        "symbol": args.symbol,
        "outcome": args.outcome,
        "shell_action": result.action,
        "cancelled_order_ids": list(result.cancelled_order_ids),
        "submitted_order_ids": list(result.submitted_order_ids),
        "placement_count": len(result.placements),
    }
    _journal_action(args, "operator_sync_quote", payload)
    print(json.dumps(payload, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction-market operator CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--state-file", default="runtime/safety-state.json")
    status.add_argument("--journal", default=None)
    status.add_argument("--llm-advisory-file", default=None)
    status.add_argument("--venue", choices=["polymarket", "kalshi"], default=None)
    status.add_argument("--symbol", default=None)
    status.add_argument(
        "--outcome", choices=["yes", "no", "unknown"], default="unknown"
    )
    status.set_defaults(func=cmd_status)

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

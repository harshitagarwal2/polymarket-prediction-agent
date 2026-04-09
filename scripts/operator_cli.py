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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adapters.kalshi import KalshiAdapter, KalshiConfig
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import Contract, OutcomeSide, Venue
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
from engine.interfaces import NoopStrategy
from engine.runner import TradingEngine
from engine.safety_state import (
    EngineSafetyState,
    PendingRefreshRequestState,
    RecoveryItemState,
)
from engine.safety_store import SafetyStateStore
from research.storage import (
    EventJournal,
    read_jsonl_events,
    summarize_recent_runtime,
    summarize_scan_cycle_events,
)
from risk.cleanup import CleanupCoordinator
from risk.limits import RiskEngine, RiskLimits


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

    return _normalize_payload(
        {
            "state": runtime_state,
            "reasons": reasons,
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


def _parse_comma_separated(value: str | None) -> list[str] | None:
    if value in (None, ""):
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


def _build_adapter(venue_name: str):
    if venue_name == "polymarket":
        return PolymarketAdapter(
            PolymarketConfig(
                private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
                funder=os.getenv("POLYMARKET_FUNDER"),
                account_address=os.getenv("POLYMARKET_ACCOUNT_ADDRESS"),
                user_ws_host=(
                    os.getenv("POLYMARKET_USER_WS_HOST")
                    or PolymarketConfig.user_ws_host
                ),
                live_user_markets=_parse_comma_separated(
                    os.getenv("POLYMARKET_LIVE_USER_MARKETS")
                ),
            )
        )
    if venue_name == "kalshi":
        return KalshiAdapter(
            KalshiConfig(
                api_key_id=os.getenv("KALSHI_API_KEY_ID"),
                private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH"),
            )
        )
    raise ValueError(f"unsupported venue: {venue_name}")


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
    store = _load_store(args.state_file)
    state = store.load()
    state.paused = True
    state.pause_reason = args.reason
    store.save(state)
    _journal_action(
        args, "operator_pause", {"reason": args.reason, "state": _state_payload(state)}
    )
    print(f"paused: {args.reason}")
    return 0


def cmd_unpause(args) -> int:
    store = _load_store(args.state_file)
    state = store.load()
    state.paused = False
    state.pause_reason = None
    store.save(state)
    _journal_action(args, "operator_unpause", {"state": _state_payload(state)})
    print("unpaused")
    return 0


def cmd_hold_new_orders(args) -> int:
    store = _load_store(args.state_file)
    state = store.load()
    state.hold_new_orders = True
    state.hold_reason = args.reason
    state.hold_since = datetime.now().astimezone()
    store.save(state)
    _journal_action(
        args,
        "operator_hold_new_orders",
        {"reason": args.reason, "state": _state_payload(state)},
    )
    print(f"holding new orders: {args.reason}")
    return 0


def cmd_clear_hold_new_orders(args) -> int:
    store = _load_store(args.state_file)
    state = store.load()
    state.hold_new_orders = False
    state.hold_reason = None
    state.hold_since = None
    store.save(state)
    _journal_action(
        args,
        "operator_clear_hold_new_orders",
        {"state": _state_payload(state)},
    )
    print("cleared new-order hold")
    return 0


def cmd_force_refresh(args) -> int:
    store = _load_store(args.state_file)
    state = store.load()
    scope = "account"
    if getattr(args, "venue", None):
        venue = Venue.POLYMARKET if args.venue == "polymarket" else Venue.KALSHI
        contract = _parse_contract(args, venue)
        if contract is not None:
            scope = contract.market_key
    if not any(
        item.scope == scope and item.reason == args.reason
        for item in state.pending_refresh_requests
    ):
        state.pending_refresh_requests.append(
            PendingRefreshRequestState(
                scope=scope,
                reason=args.reason,
                requested_at=datetime.now().astimezone(),
            )
        )
    recovery_id = (
        f"account-refresh-needed:{scope}"
        if scope == "account"
        else f"market-refresh-needed:{scope}"
    )
    existing_recovery = next(
        (item for item in state.recovery_items if item.recovery_id == recovery_id),
        None,
    )
    now = datetime.now().astimezone()
    if existing_recovery is None:
        state.recovery_items.append(
            RecoveryItemState(
                recovery_id=recovery_id,
                item_type=(
                    "account-refresh-needed"
                    if scope == "account"
                    else "market-refresh-needed"
                ),
                scope=scope,
                reason=args.reason,
                clear_source="authoritative_snapshot",
                opened_at=now,
                last_evidence_at=now,
                last_evidence=args.reason,
            )
        )
    else:
        existing_recovery.status = "open"
        existing_recovery.reason = args.reason
        existing_recovery.last_evidence_at = now
        existing_recovery.last_evidence = args.reason
        existing_recovery.cleared_at = None
        existing_recovery.clear_reason = None
    store.save(state)
    _journal_action(
        args,
        "operator_force_refresh",
        {"scope": scope, "reason": args.reason, "state": _state_payload(state)},
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prediction-market operator CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--state-file", default="runtime/safety-state.json")
    status.add_argument("--journal", default=None)
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

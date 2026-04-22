from __future__ import annotations

import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _normalize_for_json(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: _normalize_for_json(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _normalize_for_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def normalize_for_json(value: Any) -> Any:
    return _normalize_for_json(value)


def write_json(path: str | Path, payload: Any) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            normalize_for_json(payload),
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    return output_path


def write_jsonl_records(path: str | Path, records: list[Any]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(
                json.dumps(
                    normalize_for_json(record),
                    sort_keys=True,
                    allow_nan=False,
                )
                + "\n"
            )
    return output_path


def read_jsonl_records(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError("jsonl record must deserialize to an object")
        records.append(payload)
    return records


class EventJournal:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def append(self, event_type: str, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        envelope = {
            "event_id": uuid.uuid4().hex,
            "ts": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "payload": normalize_for_json(payload),
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(envelope, sort_keys=True) + "\n")


def read_jsonl_events(path: str | Path) -> list[dict[str, Any]]:
    return read_jsonl_records(path)


def _runtime_summary(payload: dict[str, Any]) -> dict[str, Any] | None:
    runtime_summary = payload.get("runtime_summary")
    if isinstance(runtime_summary, dict):
        return runtime_summary
    if payload.get("engine_halted"):
        return {"state": "halted"}
    if payload.get("engine_paused"):
        return {"state": "paused"}
    return None


def _cycle_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    cycle_metrics = payload.get("cycle_metrics")
    if isinstance(cycle_metrics, dict):
        merged = dict(cycle_metrics)
        if merged.get("selected_market_key") is None:
            merged["selected_market_key"] = payload.get("selected_market_key")
        return merged
    execution = payload.get("execution") or {}
    placements = execution.get("placements") or []
    accepted_placements = [
        placement for placement in placements if placement.get("accepted")
    ]
    gate_trace = payload.get("gate_trace") or []
    return {
        "market_count": int(payload.get("market_count", 0) or 0),
        "candidate_count": int(payload.get("candidate_count", 0) or 0),
        "skipped_candidate_count": len(payload.get("skipped_candidates") or []),
        "gate_trace_count": len(gate_trace),
        "allowed_gate_count": len(
            [entry for entry in gate_trace if entry.get("allowed") is True]
        ),
        "rejected_gate_count": len(
            [entry for entry in gate_trace if entry.get("allowed") is False]
        ),
        "selected_market_key": payload.get("selected_market_key"),
        "placement_count": len(placements),
        "accepted_placement_count": len(accepted_placements),
        "rejected_placement_count": len(placements) - len(accepted_placements),
    }


def _gate_stage_counts(payload: dict[str, Any]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for entry in payload.get("gate_trace") or []:
        stage = str(entry.get("stage") or "unknown")
        bucket = counts.setdefault(stage, {"allowed": 0, "rejected": 0})
        if entry.get("allowed") is False:
            bucket["rejected"] += 1
        else:
            bucket["allowed"] += 1
    return counts


def _lifecycle_action_counts(payload: dict[str, Any]) -> dict[str, int]:
    action_counts = payload.get("action_counts")
    if isinstance(action_counts, dict):
        return {str(key): int(value or 0) for key, value in action_counts.items()}
    counts: dict[str, int] = {}
    for decision in payload.get("decisions") or []:
        if not isinstance(decision, dict):
            continue
        action = decision.get("action")
        if action is None:
            continue
        key = str(action)
        counts[key] = counts.get(key, 0) + 1
    return counts


def summarize_scan_cycle_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    scan_cycles = [event for event in events if event.get("event_type") == "scan_cycle"]
    skipped_cycles = [
        event for event in events if event.get("event_type") == "scan_cycle_skipped"
    ]
    policy_allowed = 0
    policy_rejected = 0
    halted_cycles = 0
    paused_cycles = 0
    total_candidates = 0
    truth_blocked_cycles = 0
    total_skipped_candidates = 0
    total_gate_trace_entries = 0
    cycles_with_selection = 0
    cycles_with_execution_attempt = 0
    lifecycle_action_batches = 0
    lifecycle_action_counts: dict[str, int] = {}
    gate_stage_counts: dict[str, dict[str, int]] = {}
    runtime_state_counts: dict[str, int] = {}
    skip_reason_categories: dict[str, int] = {}

    for event in events:
        payload = event.get("payload", {})
        event_type = event.get("event_type")
        runtime_summary = _runtime_summary(payload)
        runtime_state = (
            runtime_summary.get("state")
            if isinstance(runtime_summary, dict)
            else None
        )
        if runtime_state is not None:
            runtime_state_counts[str(runtime_state)] = (
                runtime_state_counts.get(str(runtime_state), 0) + 1
            )
        elif payload.get("engine_halted"):
            runtime_state_counts["halted"] = runtime_state_counts.get("halted", 0) + 1
        elif payload.get("engine_paused"):
            runtime_state_counts["paused"] = runtime_state_counts.get("paused", 0) + 1

        if event_type == "scan_cycle":
            metrics = _cycle_metrics(payload)
            total_candidates += int(metrics.get("candidate_count", 0) or 0)
            total_skipped_candidates += int(
                metrics.get("skipped_candidate_count", 0) or 0
            )
            total_gate_trace_entries += int(metrics.get("gate_trace_count", 0) or 0)
            if payload.get("policy_allowed") is True:
                policy_allowed += 1
            if payload.get("policy_allowed") is False:
                policy_rejected += 1
            if payload.get("engine_halted"):
                halted_cycles += 1
            if payload.get("engine_paused"):
                paused_cycles += 1
            if (
                metrics.get("selected_market_key") is not None
                or payload.get("selected") is not None
            ):
                cycles_with_selection += 1
            if payload.get("execution") is not None:
                cycles_with_execution_attempt += 1
            for stage, counts in _gate_stage_counts(payload).items():
                bucket = gate_stage_counts.setdefault(
                    stage, {"allowed": 0, "rejected": 0}
                )
                bucket["allowed"] += counts["allowed"]
                bucket["rejected"] += counts["rejected"]
        elif event_type == "scan_cycle_blocked":
            truth_blocked_cycles += 1
        elif event_type == "scan_cycle_skipped":
            category = str(payload.get("reason_category") or "other")
            skip_reason_categories[category] = (
                skip_reason_categories.get(category, 0) + 1
            )
        elif event_type == "lifecycle_actions":
            lifecycle_action_batches += 1
            for action, count in _lifecycle_action_counts(payload).items():
                lifecycle_action_counts[action] = (
                    lifecycle_action_counts.get(action, 0) + count
                )

    return {
        "total_events": len(events),
        "scan_cycles": len(scan_cycles),
        "skipped_cycles": len(skipped_cycles),
        "truth_blocked_cycles": truth_blocked_cycles,
        "policy_allowed_cycles": policy_allowed,
        "policy_rejected_cycles": policy_rejected,
        "halted_cycles": halted_cycles,
        "paused_cycles": paused_cycles,
        "total_candidates_seen": total_candidates,
        "total_skipped_candidates": total_skipped_candidates,
        "total_gate_trace_entries": total_gate_trace_entries,
        "cycles_with_selection": cycles_with_selection,
        "cycles_with_execution_attempt": cycles_with_execution_attempt,
        "lifecycle_action_batches": lifecycle_action_batches,
        "lifecycle_action_counts": lifecycle_action_counts,
        "gate_stage_counts": gate_stage_counts,
        "runtime_state_counts": runtime_state_counts,
        "skip_reason_categories": skip_reason_categories,
    }


def summarize_recent_runtime(events: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "last_event_ts": None,
        "last_cycle_id": None,
        "last_scan_mode": None,
        "last_skip_reason": None,
        "last_truth_block_issues": [],
        "last_operator_action": None,
        "last_lifecycle_action_count": 0,
        "last_selected_market_key": None,
        "last_policy_allowed": None,
        "last_policy_reasons": [],
        "last_execution_attempt_mode": None,
        "last_execution_selected_market_key": None,
        "last_execution_placement_count": 0,
        "last_execution_accepted_placement_count": 0,
        "last_execution_order_ids": [],
        "last_runtime_summary": None,
        "last_cycle_metrics": None,
        "last_skip_reason_category": None,
        "last_truth_issue_count": 0,
        "last_lifecycle_action_counts": {},
        "last_lifecycle_order_ids": [],
        "last_gate_trace": [],
        "last_blocking_gate": None,
    }

    for event in events:
        summary["last_event_ts"] = event.get("ts")
        event_type = event.get("event_type")
        payload = event.get("payload", {})
        runtime_summary = _runtime_summary(payload)
        if runtime_summary is not None:
            summary["last_runtime_summary"] = runtime_summary
        if payload.get("cycle_id") is not None:
            summary["last_cycle_id"] = payload.get("cycle_id")

        if event_type == "scan_cycle":
            summary["last_scan_mode"] = payload.get("mode")
            market_key = payload.get("selected_market_key")
            if market_key is None and isinstance(payload.get("selected"), dict):
                selected = payload["selected"]
                contract_payload = selected.get("contract", {})
                if isinstance(contract_payload, dict):
                    symbol = contract_payload.get("symbol")
                    outcome = contract_payload.get("outcome")
                    if symbol is not None and outcome is not None:
                        market_key = f"{symbol}:{outcome}"
                    elif symbol is not None:
                        market_key = symbol
            summary["last_selected_market_key"] = market_key
            summary["last_policy_allowed"] = payload.get("policy_allowed")
            summary["last_policy_reasons"] = list(payload.get("policy_reasons") or [])
            summary["last_cycle_metrics"] = _cycle_metrics(payload)
            summary["last_gate_trace"] = list(payload.get("gate_trace") or [])
            summary["last_blocking_gate"] = payload.get("blocking_gate")

            execution = payload.get("execution") or {}
            placements = execution.get("placements") or []
            accepted = [placement for placement in placements if placement.get("accepted")]
            if payload.get("execution") is not None:
                summary["last_execution_attempt_mode"] = payload.get("mode")
                summary["last_execution_selected_market_key"] = market_key
            summary["last_execution_placement_count"] = len(placements)
            summary["last_execution_accepted_placement_count"] = len(accepted)
            summary["last_execution_order_ids"] = [
                placement.get("order_id")
                for placement in placements
                if placement.get("order_id") is not None
            ]

        elif event_type == "scan_cycle_skipped":
            summary["last_scan_mode"] = payload.get("mode")
            summary["last_skip_reason"] = payload.get("reason")
            summary["last_skip_reason_category"] = payload.get("reason_category")

        elif event_type == "scan_cycle_blocked":
            summary["last_scan_mode"] = payload.get("mode")
            summary["last_truth_block_issues"] = list(payload.get("issues") or [])
            summary["last_truth_issue_count"] = int(
                payload.get("truth_issue_count", len(summary["last_truth_block_issues"]))
                or 0
            )

        elif event_type == "lifecycle_actions":
            summary["last_lifecycle_action_count"] = int(payload.get("count", 0) or 0)
            summary["last_lifecycle_action_counts"] = _lifecycle_action_counts(payload)
            summary["last_lifecycle_order_ids"] = [
                decision.get("order_id")
                for decision in payload.get("decisions") or []
                if isinstance(decision, dict) and decision.get("order_id") is not None
            ]

        elif isinstance(event_type, str) and event_type.startswith("operator_"):
            summary["last_operator_action"] = event_type

    if summary["last_blocking_gate"] is None and summary["last_gate_trace"]:
        reversed_trace = list(reversed(summary["last_gate_trace"]))
        summary["last_blocking_gate"] = next(
            (
                entry
                for entry in reversed_trace
                if isinstance(entry, dict) and entry.get("allowed") is False
            ),
            None,
        )

    return summary

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ParquetStorage:
    """Small optional Parquet writer influenced by upstream chunked market storage."""

    def __init__(self, data_dir: Path | str = "research/data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def append_records(self, name: str, records: list[Any]) -> Path:
        if not records:
            raise ValueError("records must not be empty")
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("pandas is required for ParquetStorage") from exc

        normalized = []
        fetched_at = datetime.now(timezone.utc)
        for record in records:
            if is_dataclass(record) and not isinstance(record, type):
                payload = asdict(record)
            elif isinstance(record, dict):
                payload = dict(record)
            else:
                payload = {"value": record}
            payload["_fetched_at"] = fetched_at.isoformat()
            normalized.append(payload)

        df = pd.DataFrame(normalized)
        output_path = self.data_dir / f"{name}_{int(fetched_at.timestamp())}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path


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

    for event in scan_cycles:
        payload = event.get("payload", {})
        total_candidates += int(payload.get("candidate_count", 0) or 0)
        if payload.get("policy_allowed") is True:
            policy_allowed += 1
        if payload.get("policy_allowed") is False:
            policy_rejected += 1
        if payload.get("engine_halted"):
            halted_cycles += 1
        if payload.get("engine_paused"):
            paused_cycles += 1

    return {
        "total_events": len(events),
        "scan_cycles": len(scan_cycles),
        "skipped_cycles": len(skipped_cycles),
        "policy_allowed_cycles": policy_allowed,
        "policy_rejected_cycles": policy_rejected,
        "halted_cycles": halted_cycles,
        "paused_cycles": paused_cycles,
        "total_candidates_seen": total_candidates,
    }


def summarize_recent_runtime(events: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "last_event_ts": None,
        "last_cycle_id": None,
        "last_scan_mode": None,
        "last_selected_market_key": None,
        "last_policy_allowed": None,
        "last_policy_reasons": [],
        "last_skip_reason": None,
        "last_truth_block_issues": [],
        "last_operator_action": None,
        "last_lifecycle_action_count": 0,
        "last_execution_attempt_mode": None,
        "last_execution_selected_market_key": None,
        "last_execution_placement_count": 0,
        "last_execution_accepted_placement_count": 0,
        "last_execution_order_ids": [],
        "last_gate_trace": [],
        "last_blocking_gate": None,
    }

    for event in events:
        summary["last_event_ts"] = event.get("ts")
        event_type = event.get("event_type")
        payload = event.get("payload", {})
        if payload.get("cycle_id") is not None:
            summary["last_cycle_id"] = payload.get("cycle_id")

        if event_type == "scan_cycle":
            summary["last_scan_mode"] = payload.get("mode")
            selected = payload.get("selected") or {}
            contract = selected.get("contract") or {}
            market_key = (
                f"{contract.get('symbol')}:{contract.get('outcome')}"
                if contract.get("symbol")
                and contract.get("outcome") not in (None, "unknown")
                else contract.get("symbol")
            )
            summary["last_selected_market_key"] = market_key
            summary["last_policy_allowed"] = payload.get("policy_allowed")
            summary["last_policy_reasons"] = list(payload.get("policy_reasons") or [])
            summary["last_gate_trace"] = list(payload.get("gate_trace") or [])
            summary["last_blocking_gate"] = payload.get("blocking_gate")

            execution = payload.get("execution") or {}
            placements = execution.get("placements") or []
            if payload.get("mode") == "run":
                summary["last_execution_attempt_mode"] = payload.get("mode")
                summary["last_execution_selected_market_key"] = market_key
                summary["last_execution_placement_count"] = len(placements)
                summary["last_execution_accepted_placement_count"] = len(
                    [placement for placement in placements if placement.get("accepted")]
                )
                summary["last_execution_order_ids"] = [
                    placement.get("order_id")
                    for placement in placements
                    if placement.get("order_id") is not None
                ]

            if summary["last_blocking_gate"] is None:
                for gate in reversed(summary["last_gate_trace"]):
                    if gate.get("allowed") is False:
                        summary["last_blocking_gate"] = gate
                        break

        elif event_type == "scan_cycle_skipped":
            summary["last_scan_mode"] = payload.get("mode")
            summary["last_skip_reason"] = payload.get("reason")

        elif event_type == "scan_cycle_blocked":
            summary["last_scan_mode"] = payload.get("mode")
            summary["last_truth_block_issues"] = list(payload.get("issues") or [])

        elif event_type == "lifecycle_actions":
            summary["last_lifecycle_action_count"] = int(payload.get("count", 0) or 0)

        elif isinstance(event_type, str) and event_type.startswith("operator_"):
            summary["last_operator_action"] = event_type

    return summary

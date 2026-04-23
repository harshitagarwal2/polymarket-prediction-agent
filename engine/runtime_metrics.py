from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RuntimeMetricEvent:
    ts: str
    component: str
    action: str
    status: str
    trace_id: str | None
    latency_ms: float | None
    fields: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ts": self.ts,
            "component": self.component,
            "action": self.action,
            "status": self.status,
            "trace_id": self.trace_id,
            "latency_ms": self.latency_ms,
        }
        payload.update(self.fields)
        return payload


class RuntimeMetricsCollector:
    def __init__(
        self,
        path: str | Path = "runtime/data/current/runtime_metrics.json",
        *,
        max_recent_events: int = 50,
    ) -> None:
        self.path = Path(path)
        self.max_recent_events = max(1, max_recent_events)

    def record(
        self,
        *,
        component: str,
        action: str,
        status: str,
        trace_id: str | None = None,
        latency_ms: float | None = None,
        **fields: Any,
    ) -> dict[str, Any]:
        payload = self._load()
        metrics = payload.setdefault("metrics", {})
        metric_key = f"{component}:{action}"
        metric = metrics.setdefault(
            metric_key,
            {
                "component": component,
                "action": action,
                "count": 0,
                "ok_count": 0,
                "error_count": 0,
                "last_status": None,
                "last_trace_id": None,
                "last_latency_ms": None,
                "updated_at": None,
            },
        )
        metric["count"] = int(metric.get("count", 0)) + 1
        if status == "ok":
            metric["ok_count"] = int(metric.get("ok_count", 0)) + 1
        else:
            metric["error_count"] = int(metric.get("error_count", 0)) + 1
        metric["last_status"] = status
        metric["last_trace_id"] = trace_id
        metric["last_latency_ms"] = latency_ms
        metric["updated_at"] = _utc_now()

        recent_events = list(payload.get("recent_events") or [])
        event = RuntimeMetricEvent(
            ts=_utc_now(),
            component=component,
            action=action,
            status=status,
            trace_id=trace_id,
            latency_ms=latency_ms,
            fields=dict(fields),
        )
        recent_events.append(event.to_payload())
        payload["recent_events"] = recent_events[-self.max_recent_events :]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return event.to_payload()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"metrics": {}, "recent_events": []}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def snapshot(self) -> dict[str, Any]:
        return self._load()


class RuntimeProposalJournal:
    def __init__(
        self,
        path: str | Path = "runtime/data/current/preview_order_context.json",
    ) -> None:
        self.path = Path(path)

    def write_preview_snapshot(
        self,
        *,
        proposals: list[dict[str, object]],
        blocked: list[dict[str, object]],
    ) -> Path:
        payload = {
            "generated_at": _utc_now(),
            "preview_order_proposal_count": len(proposals),
            "preview_order_proposals": proposals,
            "preview_order_blocked_count": len(blocked),
            "preview_order_blocked": blocked,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return self.path

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request


_SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}


def _alert_payload(
    *, severity: str, category: str, summary: str, details: dict[str, Any]
) -> dict[str, Any]:
    key = f"{severity}:{category}:{summary}"
    dedupe_hash = hashlib.sha256(
        json.dumps(
            {
                "severity": severity,
                "category": category,
                "summary": summary,
                "details": details,
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "key": key,
        "severity": severity,
        "category": category,
        "summary": summary,
        "details": details,
        "dedupe_hash": dedupe_hash,
    }


def build_runtime_alerts(status_payload: dict[str, Any]) -> dict[str, Any]:
    alerts: list[dict[str, Any]] = []
    runtime_health = status_payload.get("runtime_health") or {}
    runtime_state = str(runtime_health.get("state") or "unknown")
    reasons = list(runtime_health.get("reasons") or [])

    if runtime_health.get("kill_switch_active"):
        alerts.append(
            _alert_payload(
                severity="critical",
                category="runtime_health",
                summary="Kill switch active",
                details={
                    "kill_switch_reasons": list(
                        runtime_health.get("kill_switch_reasons") or []
                    )
                },
            )
        )
    if runtime_state == "halted":
        alerts.append(
            _alert_payload(
                severity="critical",
                category="runtime_health",
                summary="Runtime halted",
                details={"reasons": reasons},
            )
        )
    elif runtime_state in {"hold_new_orders", "paused", "degraded", "recovering"}:
        alerts.append(
            _alert_payload(
                severity="warning",
                category="runtime_health",
                summary=f"Runtime state is {runtime_state}",
                details={"reasons": reasons},
            )
        )

    if status_payload.get("pending_cancel_operator_attention_required"):
        alerts.append(
            _alert_payload(
                severity="critical",
                category="execution",
                summary="Pending cancel requires operator attention",
                details={
                    "pending_cancel_count": int(
                        runtime_health.get("pending_cancel_count") or 0
                    )
                },
            )
        )

    truth_summary = status_payload.get("last_truth_summary") or {}
    if not bool(truth_summary.get("complete", True)):
        alerts.append(
            _alert_payload(
                severity="warning",
                category="account_truth",
                summary="Persisted account truth incomplete",
                details={"issues": list(truth_summary.get("issues") or [])},
            )
        )

    recent_execution = status_payload.get("recent_execution_status") or {}
    unresolved = list(recent_execution.get("unresolved_order_ids") or [])
    if unresolved:
        alerts.append(
            _alert_payload(
                severity="critical",
                category="execution",
                summary="Unresolved recent execution order IDs",
                details={"order_ids": unresolved},
            )
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "alert_count": len(alerts),
        "alerts": alerts,
    }


def write_alerts(path: str | Path, payload: dict[str, Any]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    return output_path


def load_alerts(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("alert payload must be a JSON object")
    return payload


def _load_dedupe_state(path: str | Path) -> dict[str, str]:
    target = Path(path)
    if not target.exists():
        return {}
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("alert dedupe state must be a JSON object")
    return {str(key): str(value) for key, value in payload.items()}


def _write_dedupe_state(path: str | Path, state: dict[str, str]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    return target


def _severity_allowed(severity: str, minimum: str) -> bool:
    return _SEVERITY_ORDER.get(severity, -1) >= _SEVERITY_ORDER.get(minimum, -1)


def send_alerts(
    alert_payload: dict[str, Any],
    *,
    webhook_url: str,
    minimum_severity: str = "warning",
    dedupe_state_file: str | Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    alerts = [
        alert
        for alert in list(alert_payload.get("alerts") or [])
        if isinstance(alert, dict)
        and _severity_allowed(str(alert.get("severity") or "info"), minimum_severity)
    ]
    state = (
        _load_dedupe_state(dedupe_state_file) if dedupe_state_file is not None else {}
    )
    to_send = [
        alert
        for alert in alerts
        if state.get(str(alert.get("key") or "")) != str(alert.get("dedupe_hash") or "")
    ]
    if not dry_run:
        body = json.dumps({"alerts": to_send}, sort_keys=True).encode("utf-8")
        req = request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=10) as response:  # noqa: S310
            response.read()
    sent_at = datetime.now(timezone.utc).isoformat()
    if dedupe_state_file is not None:
        for alert in to_send:
            key = str(alert.get("key") or "")
            dedupe_hash = str(alert.get("dedupe_hash") or "")
            if key and dedupe_hash:
                state[key] = dedupe_hash
        _write_dedupe_state(dedupe_state_file, state)
    return {
        "ok": True,
        "webhook_url": webhook_url,
        "minimum_severity": minimum_severity,
        "dry_run": dry_run,
        "candidate_alert_count": len(alerts),
        "sent_alert_count": len(to_send),
        "sent_alert_keys": [str(alert.get("key") or "") for alert in to_send],
        "sent_at": sent_at,
    }


def build_runtime_heartbeat(status_payload: dict[str, Any]) -> dict[str, Any]:
    runtime_health = status_payload.get("runtime_health") or {}
    safety_state = status_payload.get("safety_state") or {}
    journal_summary = status_payload.get("journal_summary") or {}
    recent_runtime = status_payload.get("recent_runtime") or {}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_state": runtime_health.get("state"),
        "resume_trading_eligible": runtime_health.get("resume_trading_eligible"),
        "heartbeat_required": safety_state.get("heartbeat_required"),
        "heartbeat_active": safety_state.get("heartbeat_active"),
        "heartbeat_running": safety_state.get("heartbeat_running"),
        "heartbeat_healthy_for_trading": safety_state.get(
            "heartbeat_healthy_for_trading"
        ),
        "heartbeat_last_success_at": safety_state.get("heartbeat_last_success_at"),
        "heartbeat_last_error": safety_state.get("heartbeat_last_error"),
        "heartbeat_last_id": safety_state.get("heartbeat_last_id"),
        "market_count": journal_summary.get("market_count"),
        "candidate_count": journal_summary.get("candidate_count"),
        "selected_market_key": recent_runtime.get("selected_market_key"),
    }


def write_heartbeat(path: str | Path, payload: dict[str, Any]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    return output_path


def load_heartbeat(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("heartbeat payload must be a JSON object")
    return payload


def send_heartbeat(
    heartbeat_payload: dict[str, Any], *, webhook_url: str, dry_run: bool = False
) -> dict[str, Any]:
    if not dry_run:
        body = json.dumps({"heartbeat": heartbeat_payload}, sort_keys=True).encode(
            "utf-8"
        )
        req = request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=10) as response:  # noqa: S310
            response.read()
    return {
        "ok": True,
        "webhook_url": webhook_url,
        "dry_run": dry_run,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "runtime_state": heartbeat_payload.get("runtime_state"),
    }


__all__ = [
    "build_runtime_heartbeat",
    "build_runtime_alerts",
    "load_heartbeat",
    "load_alerts",
    "send_heartbeat",
    "send_alerts",
    "write_heartbeat",
    "write_alerts",
]

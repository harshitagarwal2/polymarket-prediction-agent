from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _fair_value_report(payload: dict[str, Any]) -> dict[str, Any] | None:
    report = payload.get("fair_value_report")
    return report if isinstance(report, dict) else None


def build_model_drift_report(
    benchmark_payload: dict[str, Any],
    *,
    max_brier_score: float | None = None,
    max_expected_calibration_error: float | None = None,
) -> dict[str, Any]:
    fair_value_report = _fair_value_report(benchmark_payload)
    reasons: list[str] = []
    metrics: dict[str, Any] = {}
    if fair_value_report is None:
        reasons.append("benchmark payload missing fair_value_report")
    else:
        forecast_score = fair_value_report.get("forecast_score") or {}
        if isinstance(forecast_score, dict):
            metrics["brier_score"] = forecast_score.get("brier_score")
            metrics["expected_calibration_error"] = forecast_score.get(
                "expected_calibration_error"
            )
        calibration = fair_value_report.get("calibration") or {}
        if isinstance(calibration, dict):
            metric_delta = calibration.get("metric_delta") or {}
            if isinstance(metric_delta, dict):
                metrics["expected_calibration_error_improvement"] = metric_delta.get(
                    "expected_calibration_error_improvement"
                )

    brier_score = metrics.get("brier_score")
    if (
        max_brier_score is not None
        and isinstance(brier_score, (int, float))
        and not isinstance(brier_score, bool)
        and float(brier_score) > max_brier_score
    ):
        reasons.append(
            f"brier score exceeds threshold ({float(brier_score):.6f} > {max_brier_score:.6f})"
        )

    ece = metrics.get("expected_calibration_error")
    if (
        max_expected_calibration_error is not None
        and isinstance(ece, (int, float))
        and not isinstance(ece, bool)
        and float(ece) > max_expected_calibration_error
    ):
        reasons.append(
            "expected calibration error exceeds threshold "
            f"({float(ece):.6f} > {max_expected_calibration_error:.6f})"
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": not reasons,
        "reasons": reasons,
        "metrics": metrics,
        "thresholds": {
            "max_brier_score": max_brier_score,
            "max_expected_calibration_error": max_expected_calibration_error,
        },
    }


def write_model_drift_report(path: str | Path, payload: dict[str, Any]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    return output_path


def load_model_drift_report(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("model drift report must be a JSON object")
    return payload


__all__ = [
    "build_model_drift_report",
    "load_model_drift_report",
    "write_model_drift_report",
]

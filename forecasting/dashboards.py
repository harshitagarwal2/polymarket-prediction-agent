from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from forecasting.contracts import (
    ContractConsistencySurface,
    ContractEvidence,
    build_contract_consistency_surface,
)
from forecasting.scoring import ForecastScore, score_binary_forecasts


def _coerce_probability(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{field_name} must be numeric")
    probability = float(value)
    if not math.isfinite(probability):
        raise ValueError(f"{field_name} must be finite")
    if probability < 0.0 or probability > 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return probability


def _coerce_optional_binary(value: object, *, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{field_name} must be 0 or 1")
    parsed = int(value)
    if parsed not in {0, 1} or float(value) != float(parsed):
        raise ValueError(f"{field_name} must be 0 or 1")
    return parsed


def _coerce_optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _sanitize_markdown_text(value: object) -> str:
    text = str(value)
    sanitized = "".join(
        character
        for character in text
        if character in ("\n", "\t") or ord(character) >= 32
    )
    sanitized = (
        sanitized.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\\", "\\\\")
    )
    for character in ("`", "*", "[", "]", "|"):
        sanitized = sanitized.replace(character, f"\\{character}")
    return sanitized


def _sanitize_inline_text(value: object) -> str:
    return _sanitize_markdown_text(value).replace("\n", " ").replace("\t", " ")


@dataclass(frozen=True)
class ModelVsMarketRow:
    contract_id: str
    model_probability: float
    market_probability: float
    outcome_label: int | None = None
    domain: str | None = None
    segment: str | None = None
    market_source: str | None = None
    metadata: dict[str, object] | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "ModelVsMarketRow":
        contract_id = _coerce_optional_text(
            payload.get("contract_id") or payload.get("market_key")
        )
        if contract_id is None:
            raise ValueError("model-vs-market rows require contract_id or market_key")
        model_probability = _coerce_probability(
            payload.get(
                "model_probability",
                payload.get("fair_value", payload.get("prediction")),
            ),
            field_name="model_probability",
        )
        market_probability = _coerce_probability(
            payload.get("market_probability", payload.get("market_midpoint")),
            field_name="market_probability",
        )
        metadata_payload = payload.get("metadata")
        metadata = (
            dict(metadata_payload) if isinstance(metadata_payload, Mapping) else None
        )
        return cls(
            contract_id=contract_id,
            model_probability=model_probability,
            market_probability=market_probability,
            outcome_label=_coerce_optional_binary(
                payload.get("outcome_label"), field_name="outcome_label"
            ),
            domain=_coerce_optional_text(payload.get("domain")),
            segment=_coerce_optional_text(payload.get("segment")),
            market_source=_coerce_optional_text(payload.get("market_source")),
            metadata=metadata,
        )

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "contract_id": self.contract_id,
            "model_probability": self.model_probability,
            "market_probability": self.market_probability,
        }
        if self.outcome_label is not None:
            payload["outcome_label"] = self.outcome_label
        if self.domain is not None:
            payload["domain"] = self.domain
        if self.segment is not None:
            payload["segment"] = self.segment
        if self.market_source is not None:
            payload["market_source"] = self.market_source
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload


@dataclass(frozen=True)
class ModelVsMarketDashboard:
    title: str
    summary: dict[str, object]
    rows: tuple[ModelVsMarketRow, ...]
    consistency_surfaces: tuple[ContractConsistencySurface, ...]
    model_score: ForecastScore | None = None
    market_score: ForecastScore | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "title": self.title,
            "summary": self.summary,
            "rows": [row.to_payload() for row in self.rows],
            "consistency_surfaces": [
                surface.to_payload() for surface in self.consistency_surfaces
            ],
        }
        if self.model_score is not None:
            payload["model_score"] = self.model_score.to_payload()
        if self.market_score is not None:
            payload["market_score"] = self.market_score.to_payload()
        return payload


def _load_dashboard_payload(
    source: str | Path | Sequence[Mapping[str, object]] | Mapping[str, object],
) -> object:
    if isinstance(source, (str, Path)):
        return json.loads(Path(source).read_text())
    return source


def load_model_vs_market_rows(
    source: str | Path | Sequence[Mapping[str, object]] | Mapping[str, object],
) -> tuple[ModelVsMarketRow, ...]:
    payload = _load_dashboard_payload(source)
    raw_rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(raw_rows, Sequence) or isinstance(
        raw_rows, (str, bytes, bytearray)
    ):
        raise ValueError("model-vs-market input must be a list or object with rows")
    return tuple(
        ModelVsMarketRow.from_payload(row)
        for row in raw_rows
        if isinstance(row, Mapping)
    )


def build_model_vs_market_dashboard(
    rows: Sequence[ModelVsMarketRow],
    *,
    title: str = "Model vs Market Dashboard",
    evidence_by_contract: Mapping[str, ContractEvidence] | None = None,
    calibration_bin_count: int = 5,
) -> ModelVsMarketDashboard:
    if not rows:
        raise ValueError("rows must not be empty")
    evidence_lookup = dict(evidence_by_contract or {})
    contract_count = len(rows)
    domain_counts = Counter(row.domain or "unknown" for row in rows)
    segment_counts = Counter(row.segment or "default" for row in rows)
    average_model_probability = (
        sum(row.model_probability for row in rows) / contract_count
    )
    average_market_probability = (
        sum(row.market_probability for row in rows) / contract_count
    )
    average_gap = (
        sum(row.model_probability - row.market_probability for row in rows)
        / contract_count
    )
    average_abs_gap = (
        sum(abs(row.model_probability - row.market_probability) for row in rows)
        / contract_count
    )
    top_contract_edges = [
        {
            "contract_id": row.contract_id,
            "model_probability": row.model_probability,
            "market_probability": row.market_probability,
            "edge_vs_market": row.model_probability - row.market_probability,
            "domain": row.domain,
            "segment": row.segment,
        }
        for row in sorted(
            rows,
            key=lambda item: abs(item.model_probability - item.market_probability),
            reverse=True,
        )[:5]
    ]

    consistency_surfaces = tuple(
        build_contract_consistency_surface(
            row.contract_id,
            model_probability=row.model_probability,
            market_probability=row.market_probability,
            evidence=evidence_lookup.get(row.contract_id),
        )
        for row in rows
    )
    fallback_count = sum(
        1
        for surface in consistency_surfaces
        if surface.mode == "deterministic_fallback"
    )
    warn_count = sum(1 for surface in consistency_surfaces if surface.status == "warn")

    model_score = None
    market_score = None
    all_outcomes_available = all(row.outcome_label is not None for row in rows)
    calibration_comparison: dict[str, object] = {"available": all_outcomes_available}
    if all_outcomes_available:
        outcomes = {row.contract_id: int(row.outcome_label or 0) for row in rows}
        model_score = score_binary_forecasts(
            {row.contract_id: row.model_probability for row in rows},
            outcomes,
            bin_count=calibration_bin_count,
        )
        market_score = score_binary_forecasts(
            {row.contract_id: row.market_probability for row in rows},
            outcomes,
            bin_count=calibration_bin_count,
        )
        calibration_comparison = {
            "available": True,
            "bin_count": calibration_bin_count,
            "model_minus_market_brier": (
                model_score.brier_score - market_score.brier_score
            ),
            "model_minus_market_log_loss": (
                model_score.log_loss - market_score.log_loss
            ),
            "model_minus_market_accuracy": (
                model_score.accuracy - market_score.accuracy
            ),
            "model_minus_market_ece": (
                model_score.expected_calibration_error
                - market_score.expected_calibration_error
            ),
        }

    summary: dict[str, object] = {
        "contract_count": contract_count,
        "domain_counts": dict(sorted(domain_counts.items())),
        "segment_counts": dict(sorted(segment_counts.items())),
        "average_model_probability": average_model_probability,
        "average_market_probability": average_market_probability,
        "average_model_minus_market": average_gap,
        "average_abs_model_minus_market": average_abs_gap,
        "top_contract_edges": top_contract_edges,
        "consistency": {
            "surface_count": len(consistency_surfaces),
            "fallback_count": fallback_count,
            "warn_count": warn_count,
            "llm_evidence_count": len(evidence_lookup),
        },
        "calibration_comparison": calibration_comparison,
    }
    return ModelVsMarketDashboard(
        title=title,
        summary=summary,
        rows=tuple(rows),
        consistency_surfaces=consistency_surfaces,
        model_score=model_score,
        market_score=market_score,
    )


def _format_float(value: float | None, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def _summary_float(summary: Mapping[str, object], key: str) -> float | None:
    value = summary.get(key)
    return float(value) if isinstance(value, (int, float)) else None


def render_model_vs_market_markdown(dashboard: ModelVsMarketDashboard) -> str:
    summary = dashboard.summary
    consistency = summary.get("consistency") if isinstance(summary, dict) else None
    top_contract_edges = (
        summary.get("top_contract_edges") if isinstance(summary, dict) else None
    )
    lines = [f"# {_sanitize_inline_text(dashboard.title)}", ""]
    lines.extend(
        [
            "## Summary",
            "",
            f"- Contracts: {summary['contract_count']}",
            f"- Average model probability: {_format_float(_summary_float(summary, 'average_model_probability'))}",
            f"- Average market probability: {_format_float(_summary_float(summary, 'average_market_probability'))}",
            f"- Average model-market edge: {_format_float(_summary_float(summary, 'average_model_minus_market'))}",
            f"- Average absolute edge: {_format_float(_summary_float(summary, 'average_abs_model_minus_market'))}",
        ]
    )
    if isinstance(consistency, dict):
        lines.extend(
            [
                f"- LLM evidence rows: {consistency['llm_evidence_count']}",
                f"- Deterministic fallbacks: {consistency['fallback_count']}",
                f"- Consistency warnings: {consistency['warn_count']}",
                "",
            ]
        )
    calibration = (
        summary.get("calibration_comparison") if isinstance(summary, dict) else None
    )
    if isinstance(calibration, dict) and calibration.get("available"):
        lines.extend(
            [
                "## Calibration comparison",
                "",
                "| Metric | Model | Market | Delta (model - market) |",
                "|---|---:|---:|---:|",
                "| Brier score | {model_brier} | {market_brier} | {delta_brier} |".format(
                    model_brier=_format_float(
                        dashboard.model_score.brier_score
                        if dashboard.model_score
                        else None
                    ),
                    market_brier=_format_float(
                        dashboard.market_score.brier_score
                        if dashboard.market_score
                        else None
                    ),
                    delta_brier=_format_float(
                        calibration.get("model_minus_market_brier")
                        if isinstance(
                            calibration.get("model_minus_market_brier"), (int, float)
                        )
                        else None
                    ),
                ),
                "| Log loss | {model_log_loss} | {market_log_loss} | {delta_log_loss} |".format(
                    model_log_loss=_format_float(
                        dashboard.model_score.log_loss
                        if dashboard.model_score
                        else None
                    ),
                    market_log_loss=_format_float(
                        dashboard.market_score.log_loss
                        if dashboard.market_score
                        else None
                    ),
                    delta_log_loss=_format_float(
                        calibration.get("model_minus_market_log_loss")
                        if isinstance(
                            calibration.get("model_minus_market_log_loss"), (int, float)
                        )
                        else None
                    ),
                ),
                "| Accuracy | {model_accuracy} | {market_accuracy} | {delta_accuracy} |".format(
                    model_accuracy=_format_float(
                        dashboard.model_score.accuracy
                        if dashboard.model_score
                        else None
                    ),
                    market_accuracy=_format_float(
                        dashboard.market_score.accuracy
                        if dashboard.market_score
                        else None
                    ),
                    delta_accuracy=_format_float(
                        calibration.get("model_minus_market_accuracy")
                        if isinstance(
                            calibration.get("model_minus_market_accuracy"), (int, float)
                        )
                        else None
                    ),
                ),
                "| ECE | {model_ece} | {market_ece} | {delta_ece} |".format(
                    model_ece=_format_float(
                        dashboard.model_score.expected_calibration_error
                        if dashboard.model_score
                        else None
                    ),
                    market_ece=_format_float(
                        dashboard.market_score.expected_calibration_error
                        if dashboard.market_score
                        else None
                    ),
                    delta_ece=_format_float(
                        calibration.get("model_minus_market_ece")
                        if isinstance(
                            calibration.get("model_minus_market_ece"), (int, float)
                        )
                        else None
                    ),
                ),
                "",
            ]
        )
    lines.extend(
        [
            "## Top contract edges",
            "",
            "| Contract | Model | Market | Edge | Domain | Segment |",
            "|---|---:|---:|---:|---|---|",
        ]
    )
    for row in top_contract_edges if isinstance(top_contract_edges, Sequence) else ():
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {contract_id} | {model_probability} | {market_probability} | {edge} | {domain} | {segment} |".format(
                contract_id=_sanitize_inline_text(row["contract_id"]),
                model_probability=_format_float(float(row["model_probability"])),
                market_probability=_format_float(float(row["market_probability"])),
                edge=_format_float(float(row["edge_vs_market"])),
                domain=_sanitize_inline_text(row.get("domain") or "unknown"),
                segment=_sanitize_inline_text(row.get("segment") or "default"),
            )
        )
    lines.append("")
    lines.extend(["## Contract consistency surfaces", ""])
    for surface in dashboard.consistency_surfaces:
        lines.append(
            f"- `{_sanitize_inline_text(surface.contract_id)}` — {surface.mode}/{surface.status}; "
            + "; ".join(_sanitize_inline_text(note) for note in surface.notes[:2])
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def write_model_vs_market_dashboard(
    dashboard: ModelVsMarketDashboard,
    output_dir: str | Path,
) -> tuple[Path, Path]:
    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    json_path = resolved_output_dir / "model_vs_market_dashboard.json"
    markdown_path = resolved_output_dir / "model_vs_market_dashboard.md"
    json_path.write_text(json.dumps(dashboard.to_payload(), indent=2, sort_keys=True))
    markdown_path.write_text(render_model_vs_market_markdown(dashboard))
    return json_path, markdown_path

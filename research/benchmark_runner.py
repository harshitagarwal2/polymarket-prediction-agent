from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Mapping, Sequence, Union

from engine.strategies import FairValueBandStrategy
from research.blend import blend_binary_probabilities
from research.calibration import (
    HistogramCalibrator,
    fit_histogram_calibrator,
    load_calibration_artifact,
)
from research.baselines import (
    FairValueBaselineReport,
    ReplayBaselineReport,
    evaluate_fair_value_baselines,
    evaluate_replay_baselines,
)
from research.attribution import TradeAttribution, attribute_replay_result
from research.attribution.pnl_attribution import (
    ReplayAttributionSummary,
    summarize_trade_attributions,
)
from research.eval.execution_metrics import (
    ExecutionMetricsSummary,
    summarize_execution_metrics,
)
from research.fair_values import build_fair_value_manifest, resolve_rows_to_markets
from research.paper import PaperBroker, PaperExecutionConfig
from research.replay import ReplayResult, ReplayRunner
from research.schemas import (
    FairValueBenchmarkCase,
    ReplayBenchmarkCase,
    SportsBenchmarkCase,
    load_benchmark_case,
)
from research.scoring import (
    ForecastScore,
    ReplayScore,
    score_binary_forecasts,
    score_replay_result,
)
from risk.limits import RiskEngine, RiskLimits, RiskState

CalibrationArtifactSource = Union[
    HistogramCalibrator,
    Mapping[str, object],
    Sequence[Mapping[str, object]],
    str,
    Path,
]


@dataclass(frozen=True)
class FairValueEvaluationRow:
    market_key: str
    outcome_label: int
    fair_value: float
    fair_value_minus_outcome: float
    absolute_error: float
    brier_error: float
    log_loss: float
    correct: bool
    bookmaker: str | None = None
    source_bookmaker: str | None = None
    generated_at: str | None = None
    source_captured_at: str | None = None
    outcome: str | None = None
    condition_id: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    source: str | None = None
    match_strategy: str | None = None
    model_fair_value: float | None = None
    model_fair_value_minus_outcome: float | None = None
    model_absolute_error: float | None = None
    model_brier_error: float | None = None
    model_log_loss: float | None = None
    model_correct: bool | None = None
    blended_fair_value: float | None = None
    blended_fair_value_minus_outcome: float | None = None
    blended_absolute_error: float | None = None
    blended_brier_error: float | None = None
    blended_log_loss: float | None = None
    blended_correct: bool | None = None
    calibrated_fair_value: float | None = None
    calibrated_fair_value_minus_outcome: float | None = None
    calibrated_absolute_error: float | None = None
    calibrated_brier_error: float | None = None
    calibrated_log_loss: float | None = None
    calibrated_correct: bool | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "market_key": self.market_key,
            "outcome_label": self.outcome_label,
            "fair_value": _json_float(self.fair_value),
            "fair_value_minus_outcome": _json_float(self.fair_value_minus_outcome),
            "absolute_error": _json_float(self.absolute_error),
            "brier_error": _json_float(self.brier_error),
            "log_loss": _json_float(self.log_loss),
            "correct": self.correct,
        }
        optional_fields = {
            "bookmaker": self.bookmaker,
            "source_bookmaker": self.source_bookmaker,
            "generated_at": self.generated_at,
            "source_captured_at": self.source_captured_at,
            "outcome": self.outcome,
            "condition_id": self.condition_id,
            "event_key": self.event_key,
            "sport": self.sport,
            "series": self.series,
            "game_id": self.game_id,
            "sports_market_type": self.sports_market_type,
            "source": self.source,
            "match_strategy": self.match_strategy,
            "model_fair_value": self.model_fair_value,
            "model_fair_value_minus_outcome": self.model_fair_value_minus_outcome,
            "model_absolute_error": self.model_absolute_error,
            "model_brier_error": self.model_brier_error,
            "model_log_loss": self.model_log_loss,
            "model_correct": self.model_correct,
            "blended_fair_value": self.blended_fair_value,
            "blended_fair_value_minus_outcome": self.blended_fair_value_minus_outcome,
            "blended_absolute_error": self.blended_absolute_error,
            "blended_brier_error": self.blended_brier_error,
            "blended_log_loss": self.blended_log_loss,
            "blended_correct": self.blended_correct,
            "calibrated_fair_value": self.calibrated_fair_value,
            "calibrated_fair_value_minus_outcome": self.calibrated_fair_value_minus_outcome,
            "calibrated_absolute_error": self.calibrated_absolute_error,
            "calibrated_brier_error": self.calibrated_brier_error,
            "calibrated_log_loss": self.calibrated_log_loss,
            "calibrated_correct": self.calibrated_correct,
        }
        for key, value in optional_fields.items():
            if value is not None:
                payload[key] = _json_float(value) if isinstance(value, float) else value
        return payload


@dataclass(frozen=True)
class FairValueBenchmarkReport:
    input_row_count: int
    resolved_row_count: int
    skipped_group_count: int
    resolved_market_keys: tuple[str, ...]
    missing_market_keys: tuple[str, ...]
    manifest: dict[str, object]
    forecast_score: ForecastScore | None = None
    calibrated_forecast_score: ForecastScore | None = None
    calibration: dict[str, object] | None = None
    evaluation_rows: tuple[FairValueEvaluationRow, ...] = ()
    baselines: tuple[FairValueBaselineReport, ...] = ()

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "input_row_count": self.input_row_count,
            "resolved_row_count": self.resolved_row_count,
            "skipped_group_count": self.skipped_group_count,
            "resolved_market_keys": list(self.resolved_market_keys),
            "missing_market_keys": list(self.missing_market_keys),
            "manifest": self.manifest,
        }
        if self.forecast_score is not None:
            payload["forecast_score"] = self.forecast_score.to_payload()
        if self.calibrated_forecast_score is not None:
            payload["calibrated_forecast_score"] = (
                self.calibrated_forecast_score.to_payload()
            )
        if self.calibration is not None:
            payload["calibration"] = self.calibration
        if self.evaluation_rows:
            payload["evaluation_rows"] = [
                row.to_payload() for row in self.evaluation_rows
            ]
        if self.baselines:
            payload["baselines"] = [
                baseline.to_payload() for baseline in self.baselines
            ]
        return payload


@dataclass(frozen=True)
class ReplayBenchmarkReport:
    score: ReplayScore
    ending_positions: dict[str, float]
    mark_prices: dict[str, float]
    replay_result: ReplayResult
    trade_attributions: tuple[TradeAttribution, ...] = ()
    execution_metrics: ExecutionMetricsSummary | None = None
    attribution_summary: ReplayAttributionSummary | None = None
    baselines: tuple[ReplayBaselineReport, ...] = ()

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "score": self.score.to_payload(),
            "ending_positions": self.ending_positions,
            "mark_prices": self.mark_prices,
            "ending_cash": self.replay_result.ending_cash,
            "ending_portfolio_value": self.replay_result.ending_portfolio_value,
            "net_pnl": self.replay_result.net_pnl,
        }
        if self.replay_result.execution_ledger:
            payload["execution_ledger"] = [
                trade.to_payload() for trade in self.replay_result.execution_ledger
            ]
        if self.trade_attributions:
            payload["trade_attributions"] = [
                attribution.to_payload() for attribution in self.trade_attributions
            ]
        if self.execution_metrics is not None:
            payload["execution_metrics"] = self.execution_metrics.to_payload()
        if self.attribution_summary is not None:
            payload["attribution_summary"] = self.attribution_summary.to_payload()
        if self.baselines:
            payload["baselines"] = [
                baseline.to_payload() for baseline in self.baselines
            ]
        return payload


@dataclass(frozen=True)
class SportsBenchmarkReport:
    case_name: str
    description: str | None = None
    fair_value_report: FairValueBenchmarkReport | None = None
    replay_report: ReplayBenchmarkReport | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {"case_name": self.case_name}
        if self.description is not None:
            payload["description"] = self.description
        if self.fair_value_report is not None:
            payload["fair_value"] = self.fair_value_report.to_payload()
        if self.replay_report is not None:
            payload["replay"] = self.replay_report.to_payload()
        return payload


def _ensure_required_market_keys(
    *,
    expected_market_keys: tuple[str, ...],
    resolved_market_keys: tuple[str, ...],
) -> tuple[str, ...]:
    missing_market_keys = tuple(
        sorted(set(expected_market_keys) - set(resolved_market_keys))
    )
    if missing_market_keys:
        raise ValueError(
            "benchmark fair-value case missing expected market keys: "
            + ", ".join(missing_market_keys)
        )
    return missing_market_keys


def _ensure_outcome_labels_resolved(
    *,
    outcome_labels: dict[str, int],
    resolved_market_keys: tuple[str, ...],
) -> None:
    missing_label_keys = tuple(sorted(set(outcome_labels) - set(resolved_market_keys)))
    if missing_label_keys:
        raise ValueError(
            "benchmark fair-value case missing labeled market keys: "
            + ", ".join(missing_label_keys)
        )


def _coerce_probability(record: object) -> float:
    if not isinstance(record, dict):
        raise ValueError("manifest record must be a dictionary")
    value = record.get("fair_value")
    if not isinstance(value, (int, float, str)):
        raise ValueError("manifest record fair_value must be numeric")
    probability = float(value)
    if not math.isfinite(probability):
        raise ValueError("manifest record fair_value must be finite")
    return probability


def _manifest_prediction_map(
    manifest_values: dict[str, dict[str, object]],
    outcome_labels: dict[str, int],
) -> dict[str, float]:
    return {
        market_key: _coerce_probability(record)
        for market_key, record in manifest_values.items()
        if market_key in outcome_labels
    }


def _manifest_probability_map(
    manifest_values: dict[str, dict[str, object]],
) -> dict[str, float]:
    return {
        market_key: _coerce_probability(record)
        for market_key, record in manifest_values.items()
    }


def _json_float(value: float) -> float:
    return round(float(value), 12)


def _binary_log_loss(*, prediction: float, outcome: int) -> float:
    bounded_prediction = min(max(prediction, 1e-12), 1.0 - 1e-12)
    return -(
        outcome * math.log(bounded_prediction)
        + (1 - outcome) * math.log(1.0 - bounded_prediction)
    )


def _binary_probability_metrics(
    *,
    prediction: float,
    outcome: int,
) -> tuple[float, float, float, float, bool]:
    prediction_minus_outcome = prediction - outcome
    return (
        prediction_minus_outcome,
        abs(prediction_minus_outcome),
        prediction_minus_outcome**2,
        _binary_log_loss(prediction=prediction, outcome=outcome),
        (prediction >= 0.5) == bool(outcome),
    )


def _build_model_market_probabilities(
    case: FairValueBenchmarkCase,
    resolved_market_keys: tuple[str, ...],
) -> dict[str, float]:
    return {
        market_key: float(probability)
        for market_key, probability in case.model_fair_values.items()
        if market_key in resolved_market_keys
    }


def _build_blended_market_probabilities(
    *,
    manifest_probabilities: dict[str, float],
    model_market_probabilities: dict[str, float],
    model_blend_weight: float | None,
) -> dict[str, float]:
    if model_blend_weight is None:
        return {}
    return {
        market_key: blend_binary_probabilities(
            sportsbook_probability=sportsbook_probability,
            model_probability=model_market_probabilities[market_key],
            model_weight=model_blend_weight,
        )
        for market_key, sportsbook_probability in manifest_probabilities.items()
        if market_key in model_market_probabilities
    }


def _enrich_manifest_payload_with_optional_probabilities(
    *,
    manifest_payload: dict[str, object],
    model_market_probabilities: dict[str, float],
    blended_market_probabilities: dict[str, float],
) -> dict[str, object]:
    manifest_values = manifest_payload.get("values")
    if not isinstance(manifest_values, dict):
        return manifest_payload
    for market_key, record in manifest_values.items():
        if not isinstance(record, dict):
            continue
        if market_key in model_market_probabilities:
            record["model_fair_value"] = _json_float(
                model_market_probabilities[market_key]
            )
        if market_key in blended_market_probabilities:
            record["blended_fair_value"] = _json_float(
                blended_market_probabilities[market_key]
            )
    return manifest_payload


def _build_fair_value_evaluation_rows(
    *,
    manifest_values: dict[str, dict[str, object]],
    outcome_labels: dict[str, int],
    model_market_probabilities: dict[str, float] | None = None,
    blended_market_probabilities: dict[str, float] | None = None,
    calibrated_market_probabilities: dict[str, float] | None = None,
) -> tuple[FairValueEvaluationRow, ...]:
    rows: list[FairValueEvaluationRow] = []
    for market_key in sorted(outcome_labels):
        record = manifest_values[market_key]
        outcome_label = int(outcome_labels[market_key])
        fair_value = _coerce_probability(record)
        (
            fair_value_minus_outcome,
            absolute_error,
            brier_error,
            log_loss,
            correct,
        ) = _binary_probability_metrics(
            prediction=fair_value,
            outcome=outcome_label,
        )
        model_fair_value = None
        model_fair_value_minus_outcome = None
        model_absolute_error = None
        model_brier_error = None
        model_log_loss = None
        model_correct = None
        if (
            model_market_probabilities is not None
            and market_key in model_market_probabilities
        ):
            model_fair_value = float(model_market_probabilities[market_key])
            (
                model_fair_value_minus_outcome,
                model_absolute_error,
                model_brier_error,
                model_log_loss,
                model_correct,
            ) = _binary_probability_metrics(
                prediction=model_fair_value,
                outcome=outcome_label,
            )
        blended_fair_value = None
        blended_fair_value_minus_outcome = None
        blended_absolute_error = None
        blended_brier_error = None
        blended_log_loss = None
        blended_correct = None
        if (
            blended_market_probabilities is not None
            and market_key in blended_market_probabilities
        ):
            blended_fair_value = float(blended_market_probabilities[market_key])
            (
                blended_fair_value_minus_outcome,
                blended_absolute_error,
                blended_brier_error,
                blended_log_loss,
                blended_correct,
            ) = _binary_probability_metrics(
                prediction=blended_fair_value,
                outcome=outcome_label,
            )
        calibrated_fair_value = None
        calibrated_fair_value_minus_outcome = None
        calibrated_absolute_error = None
        calibrated_brier_error = None
        calibrated_log_loss = None
        calibrated_correct = None
        if calibrated_market_probabilities is not None:
            calibrated_fair_value = float(calibrated_market_probabilities[market_key])
            (
                calibrated_fair_value_minus_outcome,
                calibrated_absolute_error,
                calibrated_brier_error,
                calibrated_log_loss,
                calibrated_correct,
            ) = _binary_probability_metrics(
                prediction=calibrated_fair_value,
                outcome=outcome_label,
            )
        rows.append(
            FairValueEvaluationRow(
                market_key=market_key,
                outcome_label=outcome_label,
                fair_value=fair_value,
                fair_value_minus_outcome=fair_value_minus_outcome,
                absolute_error=absolute_error,
                brier_error=brier_error,
                log_loss=log_loss,
                correct=correct,
                bookmaker=(
                    str(record["bookmaker"])
                    if record.get("bookmaker") is not None
                    else None
                ),
                source_bookmaker=(
                    str(record["source_bookmaker"])
                    if record.get("source_bookmaker") is not None
                    else None
                ),
                generated_at=(
                    str(record["generated_at"])
                    if record.get("generated_at") is not None
                    else None
                ),
                source_captured_at=(
                    str(record["source_captured_at"])
                    if record.get("source_captured_at") is not None
                    else None
                ),
                outcome=(
                    str(record["outcome"])
                    if record.get("outcome") is not None
                    else None
                ),
                condition_id=(
                    str(record["condition_id"])
                    if record.get("condition_id") is not None
                    else None
                ),
                event_key=(
                    str(record["event_key"])
                    if record.get("event_key") is not None
                    else None
                ),
                sport=(
                    str(record["sport"]) if record.get("sport") is not None else None
                ),
                series=(
                    str(record["series"]) if record.get("series") is not None else None
                ),
                game_id=(
                    str(record["game_id"])
                    if record.get("game_id") is not None
                    else None
                ),
                sports_market_type=(
                    str(record["sports_market_type"])
                    if record.get("sports_market_type") is not None
                    else None
                ),
                source=(
                    str(record["source"]) if record.get("source") is not None else None
                ),
                match_strategy=(
                    str(record["match_strategy"])
                    if record.get("match_strategy") is not None
                    else None
                ),
                model_fair_value=model_fair_value,
                model_fair_value_minus_outcome=model_fair_value_minus_outcome,
                model_absolute_error=model_absolute_error,
                model_brier_error=model_brier_error,
                model_log_loss=model_log_loss,
                model_correct=model_correct,
                blended_fair_value=blended_fair_value,
                blended_fair_value_minus_outcome=blended_fair_value_minus_outcome,
                blended_absolute_error=blended_absolute_error,
                blended_brier_error=blended_brier_error,
                blended_log_loss=blended_log_loss,
                blended_correct=blended_correct,
                calibrated_fair_value=calibrated_fair_value,
                calibrated_fair_value_minus_outcome=calibrated_fair_value_minus_outcome,
                calibrated_absolute_error=calibrated_absolute_error,
                calibrated_brier_error=calibrated_brier_error,
                calibrated_log_loss=calibrated_log_loss,
                calibrated_correct=calibrated_correct,
            )
        )
    return tuple(rows)


def _resolve_prefit_calibrator(
    prefit_calibration: CalibrationArtifactSource | None,
) -> HistogramCalibrator | None:
    if prefit_calibration is None:
        return None
    if isinstance(prefit_calibration, HistogramCalibrator):
        return prefit_calibration
    return load_calibration_artifact(prefit_calibration)


def _extract_report_fair_values(
    report: FairValueBenchmarkReport | None,
) -> dict[str, float]:
    if report is None:
        return {}
    fair_values = {
        row.market_key: float(row.fair_value)
        for row in report.evaluation_rows
        if row.fair_value is not None
    }
    manifest_values = report.manifest.get("values")
    if isinstance(manifest_values, dict):
        for market_key, payload in manifest_values.items():
            if not isinstance(market_key, str):
                continue
            if isinstance(payload, (int, float)):
                fair_values.setdefault(market_key, float(payload))
                continue
            if isinstance(payload, dict) and isinstance(
                payload.get("fair_value"),
                (int, float),
            ):
                fair_values.setdefault(market_key, float(payload["fair_value"]))
    return fair_values


def run_fair_value_benchmark(
    case: FairValueBenchmarkCase,
    *,
    prefit_calibration: CalibrationArtifactSource | None = None,
) -> FairValueBenchmarkReport:
    rows = case.materialize_rows()
    resolved_rows = rows
    skipped_rows: list[dict[str, object]] = []
    if case.markets:
        resolved_rows, skipped_rows = resolve_rows_to_markets(
            rows, case.materialize_markets()
        )

    manifest = build_fair_value_manifest(
        resolved_rows,
        method=case.devig_method,
        source=case.source,
        max_age_seconds=case.max_age_seconds,
        aggregation=case.book_aggregation,
    )
    manifest_values = dict(manifest.values or {})
    skipped_groups = list(manifest.skipped_groups or [])
    if skipped_rows:
        skipped_groups.extend(skipped_rows)
        manifest = replace(manifest, skipped_groups=skipped_groups)
    manifest_payload = manifest.to_payload()
    resolved_market_keys = tuple(sorted(manifest_values.keys()))
    missing_market_keys = _ensure_required_market_keys(
        expected_market_keys=case.expected_market_keys,
        resolved_market_keys=resolved_market_keys,
    )

    manifest_probabilities = _manifest_probability_map(manifest_values)
    model_market_probabilities = _build_model_market_probabilities(
        case,
        resolved_market_keys,
    )
    blended_market_probabilities = _build_blended_market_probabilities(
        manifest_probabilities=manifest_probabilities,
        model_market_probabilities=model_market_probabilities,
        model_blend_weight=case.model_blend_weight,
    )
    manifest_payload = _enrich_manifest_payload_with_optional_probabilities(
        manifest_payload=manifest_payload,
        model_market_probabilities=model_market_probabilities,
        blended_market_probabilities=blended_market_probabilities,
    )
    forecast_score = None
    calibrated_forecast_score = None
    calibration_payload = None
    calibrated_market_probabilities: dict[str, float] | None = None
    evaluation_rows: tuple[FairValueEvaluationRow, ...] = ()
    calibrator = _resolve_prefit_calibrator(prefit_calibration)
    if case.outcome_labels:
        _ensure_outcome_labels_resolved(
            outcome_labels=case.outcome_labels,
            resolved_market_keys=resolved_market_keys,
        )
        predictions = {
            market_key: manifest_probabilities[market_key]
            for market_key in case.outcome_labels
        }
        forecast_score = score_binary_forecasts(
            predictions,
            case.outcome_labels,
        )

    if calibrator is None and case.calibration_samples:
        calibrator = fit_histogram_calibrator(
            case.calibration_samples,
            bin_count=case.calibration_bin_count,
        )
    if calibrator is not None:
        calibrated_market_probabilities = calibrator.apply_mapping(
            manifest_probabilities
        )
        calibration_payload = {
            "sample_count": calibrator.sample_count,
            "artifact": calibrator.to_payload(),
            "calibrated_market_probabilities": calibrated_market_probabilities,
            "source": "prefit"
            if prefit_calibration is not None
            else "case_calibration_samples",
        }
        if forecast_score is not None:
            calibrated_predictions = {
                market_key: calibrated_market_probabilities[market_key]
                for market_key in case.outcome_labels
            }
            calibrated_forecast_score = score_binary_forecasts(
                calibrated_predictions,
                case.outcome_labels,
            )
            calibration_payload["metric_delta"] = {
                "brier_improvement": (
                    forecast_score.brier_score - calibrated_forecast_score.brier_score
                ),
                "log_loss_improvement": (
                    forecast_score.log_loss - calibrated_forecast_score.log_loss
                ),
                "accuracy_delta": (
                    calibrated_forecast_score.accuracy - forecast_score.accuracy
                ),
                "expected_calibration_error_improvement": (
                    forecast_score.expected_calibration_error
                    - calibrated_forecast_score.expected_calibration_error
                ),
            }

    if case.outcome_labels:
        evaluation_rows = _build_fair_value_evaluation_rows(
            manifest_values=manifest_values,
            outcome_labels=case.outcome_labels,
            model_market_probabilities=model_market_probabilities,
            blended_market_probabilities=blended_market_probabilities,
            calibrated_market_probabilities=calibrated_market_probabilities,
        )

    return FairValueBenchmarkReport(
        input_row_count=len(rows),
        resolved_row_count=len(resolved_rows),
        skipped_group_count=len(skipped_groups),
        resolved_market_keys=resolved_market_keys,
        missing_market_keys=missing_market_keys,
        manifest=manifest_payload,
        forecast_score=forecast_score,
        calibrated_forecast_score=calibrated_forecast_score,
        calibration=calibration_payload,
        evaluation_rows=evaluation_rows,
        baselines=evaluate_fair_value_baselines(case=case, resolved_rows=resolved_rows),
    )


def run_replay_benchmark(
    case: ReplayBenchmarkCase,
    *,
    fair_value_by_market: Mapping[str, float] | None = None,
) -> ReplayBenchmarkReport:
    runner = ReplayRunner(
        strategy=FairValueBandStrategy(
            quantity=case.strategy.quantity,
            edge_threshold=case.strategy.edge_threshold,
            aggressive=case.strategy.aggressive,
        ),
        risk_engine=RiskEngine(
            RiskLimits(
                max_global_contracts=case.risk_limits.max_global_contracts,
                max_contracts_per_market=case.risk_limits.max_contracts_per_market,
                reserve_contracts_buffer=case.risk_limits.reserve_contracts_buffer,
                max_order_notional=case.risk_limits.max_order_notional,
                min_price=case.risk_limits.min_price,
                max_price=case.risk_limits.max_price,
                max_daily_loss=case.risk_limits.max_daily_loss,
                enforce_atomic_batches=case.risk_limits.enforce_atomic_batches,
            ),
            state=RiskState(daily_realized_pnl=case.risk_limits.daily_realized_pnl),
        ),
        broker=PaperBroker(
            cash=case.broker.cash,
            config=PaperExecutionConfig(
                max_fill_ratio_per_step=case.broker.max_fill_ratio_per_step,
                slippage_bps=case.broker.slippage_bps,
                resting_max_fill_ratio_per_step=(
                    case.broker.resting_max_fill_ratio_per_step
                ),
                resting_fill_delay_steps=case.broker.resting_fill_delay_steps,
                stale_after_steps=case.broker.stale_after_steps,
                price_move_bps_per_step=case.broker.price_move_bps_per_step,
            ),
        ),
    )
    result = runner.run(case.materialize_steps())
    execution_ledger_payloads = [
        trade.to_payload() for trade in result.execution_ledger
    ]
    trade_attributions = attribute_replay_result(
        result,
        fair_value_by_market=fair_value_by_market,
    )
    return ReplayBenchmarkReport(
        score=score_replay_result(result),
        ending_positions=dict(result.ending_positions),
        mark_prices=dict(result.mark_prices),
        replay_result=result,
        trade_attributions=trade_attributions,
        execution_metrics=summarize_execution_metrics(execution_ledger_payloads),
        attribution_summary=summarize_trade_attributions(trade_attributions),
        baselines=evaluate_replay_baselines(case),
    )


def run_benchmark_case(
    case: SportsBenchmarkCase,
    *,
    prefit_calibration: CalibrationArtifactSource | None = None,
) -> SportsBenchmarkReport:
    fair_value_report = (
        run_fair_value_benchmark(
            case.fair_value_case,
            prefit_calibration=prefit_calibration,
        )
        if case.fair_value_case is not None
        else None
    )
    return SportsBenchmarkReport(
        case_name=case.name,
        description=case.description,
        fair_value_report=fair_value_report,
        replay_report=(
            run_replay_benchmark(
                case.replay_case,
                fair_value_by_market=_extract_report_fair_values(fair_value_report),
            )
            if case.replay_case is not None
            else None
        ),
    )


def load_and_run_benchmark_case(
    path: str | Path,
    *,
    prefit_calibration: CalibrationArtifactSource | None = None,
) -> SportsBenchmarkReport:
    return run_benchmark_case(
        load_benchmark_case(path),
        prefit_calibration=prefit_calibration,
    )


def write_benchmark_report(
    report: SportsBenchmarkReport,
    path: str | Path,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_payload(), indent=2, sort_keys=True, allow_nan=False)
    )

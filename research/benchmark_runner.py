from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from engine.strategies import FairValueBandStrategy
from research.calibration import fit_histogram_calibrator
from research.baselines import (
    FairValueBaselineReport,
    ReplayBaselineReport,
    evaluate_fair_value_baselines,
    evaluate_replay_baselines,
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


def _build_fair_value_evaluation_rows(
    *,
    manifest_values: dict[str, dict[str, object]],
    outcome_labels: dict[str, int],
    calibrated_market_probabilities: dict[str, float] | None = None,
) -> tuple[FairValueEvaluationRow, ...]:
    rows: list[FairValueEvaluationRow] = []
    for market_key in sorted(outcome_labels):
        record = manifest_values[market_key]
        outcome_label = int(outcome_labels[market_key])
        fair_value = _coerce_probability(record)
        fair_value_minus_outcome = fair_value - outcome_label
        calibrated_fair_value = None
        calibrated_fair_value_minus_outcome = None
        calibrated_absolute_error = None
        calibrated_brier_error = None
        calibrated_log_loss = None
        calibrated_correct = None
        if calibrated_market_probabilities is not None:
            calibrated_fair_value = float(calibrated_market_probabilities[market_key])
            calibrated_fair_value_minus_outcome = calibrated_fair_value - outcome_label
            calibrated_absolute_error = abs(calibrated_fair_value_minus_outcome)
            calibrated_brier_error = calibrated_fair_value_minus_outcome**2
            calibrated_log_loss = _binary_log_loss(
                prediction=calibrated_fair_value,
                outcome=outcome_label,
            )
            calibrated_correct = (calibrated_fair_value >= 0.5) == bool(outcome_label)
        rows.append(
            FairValueEvaluationRow(
                market_key=market_key,
                outcome_label=outcome_label,
                fair_value=fair_value,
                fair_value_minus_outcome=fair_value_minus_outcome,
                absolute_error=abs(fair_value_minus_outcome),
                brier_error=fair_value_minus_outcome**2,
                log_loss=_binary_log_loss(
                    prediction=fair_value,
                    outcome=outcome_label,
                ),
                correct=(fair_value >= 0.5) == bool(outcome_label),
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
                calibrated_fair_value=calibrated_fair_value,
                calibrated_fair_value_minus_outcome=calibrated_fair_value_minus_outcome,
                calibrated_absolute_error=calibrated_absolute_error,
                calibrated_brier_error=calibrated_brier_error,
                calibrated_log_loss=calibrated_log_loss,
                calibrated_correct=calibrated_correct,
            )
        )
    return tuple(rows)


def run_fair_value_benchmark(case: FairValueBenchmarkCase) -> FairValueBenchmarkReport:
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
    if skipped_rows:
        manifest.skipped_groups.extend(skipped_rows)
    manifest_payload = manifest.to_payload()
    resolved_market_keys = tuple(sorted(manifest.values.keys()))
    missing_market_keys = _ensure_required_market_keys(
        expected_market_keys=case.expected_market_keys,
        resolved_market_keys=resolved_market_keys,
    )
    skipped_groups = list(manifest.skipped_groups)

    manifest_probabilities = _manifest_probability_map(manifest.values)
    forecast_score = None
    calibrated_forecast_score = None
    calibration_payload = None
    calibrated_market_probabilities: dict[str, float] | None = None
    evaluation_rows: tuple[FairValueEvaluationRow, ...] = ()
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

    if case.calibration_samples:
        calibrator = fit_histogram_calibrator(
            case.calibration_samples,
            bin_count=case.calibration_bin_count,
        )
        calibrated_market_probabilities = calibrator.apply_mapping(
            manifest_probabilities
        )
        calibration_payload = {
            "sample_count": len(case.calibration_samples),
            "artifact": calibrator.to_payload(),
            "calibrated_market_probabilities": calibrated_market_probabilities,
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
            manifest_values=manifest.values,
            outcome_labels=case.outcome_labels,
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


def run_replay_benchmark(case: ReplayBenchmarkCase) -> ReplayBenchmarkReport:
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
            ),
        ),
    )
    result = runner.run(case.materialize_steps())
    return ReplayBenchmarkReport(
        score=score_replay_result(result),
        ending_positions=dict(result.ending_positions),
        mark_prices=dict(result.mark_prices),
        replay_result=result,
        baselines=evaluate_replay_baselines(case),
    )


def run_benchmark_case(case: SportsBenchmarkCase) -> SportsBenchmarkReport:
    return SportsBenchmarkReport(
        case_name=case.name,
        description=case.description,
        fair_value_report=(
            run_fair_value_benchmark(case.fair_value_case)
            if case.fair_value_case is not None
            else None
        ),
        replay_report=(
            run_replay_benchmark(case.replay_case)
            if case.replay_case is not None
            else None
        ),
    )


def load_and_run_benchmark_case(path: str | Path) -> SportsBenchmarkReport:
    return run_benchmark_case(load_benchmark_case(path))


def write_benchmark_report(
    report: SportsBenchmarkReport,
    path: str | Path,
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_payload(), indent=2, sort_keys=True, allow_nan=False)
    )

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Sequence

from research.calibration import load_calibration_artifact
from research.benchmark_runner import (
    CalibrationArtifactSource,
    SportsBenchmarkReport,
    run_benchmark_case,
    write_benchmark_report,
)
from research.datasets import (
    DatasetRegistry,
    WalkForwardSplit,
    generate_walk_forward_splits,
)
from research.models.bradley_terry import (
    BradleyTerryArtifact,
    fit_bradley_terry_from_cases,
    generate_model_fair_values as generate_bt_model_fair_values,
)
from research.models.elo import (
    EloModelArtifact,
    fit_elo_model,
    generate_model_fair_values,
)
from research.scoring import (
    ForecastScore,
    compare_paired_loss_differentials,
    score_binary_forecasts,
)
from research.schemas import (
    FairValueBenchmarkCase,
    SportsBenchmarkCase,
    load_benchmark_case,
    packaged_benchmark_fixture_names,
)


@dataclass(frozen=True)
class BenchmarkSuiteCaseResult:
    case_path: str
    report: SportsBenchmarkReport

    def to_payload(self) -> dict[str, object]:
        return {
            "case_path": self.case_path,
            "report": self.report.to_payload(),
        }


@dataclass(frozen=True)
class BenchmarkSuiteFailure:
    case_path: str
    error: str

    def to_payload(self) -> dict[str, str]:
        return {"case_path": self.case_path, "error": self.error}


@dataclass(frozen=True)
class BenchmarkSuiteAggregate:
    total_cases: int
    successful_cases: int
    failed_cases: int
    fair_value_case_count: int
    replay_case_count: int
    average_brier_score: float | None
    average_log_loss: float | None
    average_accuracy: float | None
    average_expected_calibration_error: float | None
    average_calibrated_brier_score: float | None
    average_calibrated_log_loss: float | None
    average_calibrated_accuracy: float | None
    average_calibrated_expected_calibration_error: float | None
    calibrated_case_count: int
    average_calibrated_brier_improvement: float | None
    average_calibrated_log_loss_improvement: float | None
    average_calibrated_accuracy_delta: float | None
    average_calibrated_expected_calibration_error_improvement: float | None
    average_replay_net_pnl: float | None
    average_replay_return_pct: float | None
    edge_ledger_row_count: int
    fair_value_baseline_deltas: dict[str, dict[str, float | int]]
    fair_value_comparison_stats: dict[str, dict[str, object]]
    replay_baseline_deltas: dict[str, dict[str, float | int]]

    def to_payload(self) -> dict[str, object]:
        return {
            "total_cases": self.total_cases,
            "successful_cases": self.successful_cases,
            "failed_cases": self.failed_cases,
            "fair_value_case_count": self.fair_value_case_count,
            "replay_case_count": self.replay_case_count,
            "average_brier_score": self.average_brier_score,
            "average_log_loss": self.average_log_loss,
            "average_accuracy": self.average_accuracy,
            "average_expected_calibration_error": self.average_expected_calibration_error,
            "average_calibrated_brier_score": self.average_calibrated_brier_score,
            "average_calibrated_log_loss": self.average_calibrated_log_loss,
            "average_calibrated_accuracy": self.average_calibrated_accuracy,
            "average_calibrated_expected_calibration_error": self.average_calibrated_expected_calibration_error,
            "calibrated_case_count": self.calibrated_case_count,
            "average_calibrated_brier_improvement": self.average_calibrated_brier_improvement,
            "average_calibrated_log_loss_improvement": self.average_calibrated_log_loss_improvement,
            "average_calibrated_accuracy_delta": self.average_calibrated_accuracy_delta,
            "average_calibrated_expected_calibration_error_improvement": self.average_calibrated_expected_calibration_error_improvement,
            "average_replay_net_pnl": self.average_replay_net_pnl,
            "average_replay_return_pct": self.average_replay_return_pct,
            "edge_ledger_row_count": self.edge_ledger_row_count,
            "fair_value_baseline_deltas": self.fair_value_baseline_deltas,
            "fair_value_comparison_stats": self.fair_value_comparison_stats,
            "replay_baseline_deltas": self.replay_baseline_deltas,
        }


@dataclass(frozen=True)
class BenchmarkSuiteEdgeLedger:
    rows: tuple[dict[str, object], ...]

    def to_payload(self) -> dict[str, object]:
        return {
            "row_count": len(self.rows),
            "rows": [dict(row) for row in self.rows],
        }


@dataclass(frozen=True)
class BenchmarkSuiteReport:
    case_results: tuple[BenchmarkSuiteCaseResult, ...]
    failures: tuple[BenchmarkSuiteFailure, ...]
    aggregate: BenchmarkSuiteAggregate
    edge_ledger: BenchmarkSuiteEdgeLedger

    def to_payload(self) -> dict[str, object]:
        return {
            "aggregate": self.aggregate.to_payload(),
            "case_results": [case.to_payload() for case in self.case_results],
            "failures": [failure.to_payload() for failure in self.failures],
            "edge_ledger": self.edge_ledger.to_payload(),
        }


@dataclass(frozen=True)
class WalkForwardBenchmarkSplitResult:
    split_id: str
    split: WalkForwardSplit
    train_case_paths: tuple[str, ...]
    test_case_paths: tuple[str, ...]
    calibration_artifact: dict[str, object]
    report: BenchmarkSuiteReport
    model_artifact: dict[str, object] | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "split_id": self.split_id,
            "split": self.split.to_payload(),
            "train_case_paths": list(self.train_case_paths),
            "test_case_paths": list(self.test_case_paths),
            "calibration": self.calibration_artifact,
            "model": self.model_artifact,
            "aggregate": self.report.aggregate.to_payload(),
        }


@dataclass(frozen=True)
class WalkForwardBenchmarkSuiteReport:
    dataset_root: str
    dataset_name: str
    dataset_version: str
    min_train_size: int
    test_size: int
    step_size: int
    max_splits: int | None
    calibration_bin_count: int | None
    model_generator: str | None
    splits: tuple[WalkForwardBenchmarkSplitResult, ...]

    def to_payload(self) -> dict[str, object]:
        pooled_report = _build_walk_forward_root_report(self.splits)
        total_test_cases = sum(
            split.report.aggregate.total_cases for split in self.splits
        )
        total_successful_cases = sum(
            split.report.aggregate.successful_cases for split in self.splits
        )
        total_failed_cases = sum(
            split.report.aggregate.failed_cases for split in self.splits
        )
        return {
            "dataset": {
                "root_dir": self.dataset_root,
                "dataset_name": self.dataset_name,
                "version": self.dataset_version,
            },
            "aggregate": pooled_report.aggregate.to_payload(),
            "walk_forward": {
                "min_train_size": self.min_train_size,
                "test_size": self.test_size,
                "step_size": self.step_size,
                "max_splits": self.max_splits,
                "calibration_bin_count": self.calibration_bin_count,
                "model_generator": self.model_generator,
                "split_count": len(self.splits),
                "total_test_cases": total_test_cases,
                "total_successful_cases": total_successful_cases,
                "total_failed_cases": total_failed_cases,
            },
            "splits": [split.to_payload() for split in self.splits],
        }


def _average(values: list[float]) -> float | None:
    return (sum(values) / len(values)) if values else None


def _binary_log_loss(*, prediction: float, outcome: int) -> float:
    bounded_prediction = min(max(float(prediction), 1e-12), 1.0 - 1e-12)
    return -(
        outcome * math.log(bounded_prediction)
        + (1 - outcome) * math.log(1.0 - bounded_prediction)
    )


def _comparison_loss_metrics(*, prediction: float, outcome: int) -> tuple[float, float]:
    probability = float(prediction)
    if not math.isfinite(probability):
        raise ValueError("comparison prediction must be finite")
    prediction_minus_outcome = probability - outcome
    return (
        prediction_minus_outcome**2,
        _binary_log_loss(prediction=probability, outcome=outcome),
    )


def _comparison_description_from_baseline(description: str) -> str:
    baseline_description = description.rstrip(".")
    if baseline_description:
        baseline_description = (
            baseline_description[0].lower() + baseline_description[1:]
        )
    return (
        f"Primary fair value versus {baseline_description} on paired evaluation rows."
    )


def _float_payload_metric(
    payload: object,
    key: str,
) -> float | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if value is None:
        return None
    return float(value)


def _nested_float_payload_metric(
    payload: object,
    *,
    parent_key: str,
    key: str,
) -> float | None:
    if not isinstance(payload, dict):
        return None
    return _float_payload_metric(payload.get(parent_key), key)


def _collect_fair_value_baseline_deltas(
    reports: list[SportsBenchmarkReport],
) -> dict[str, dict[str, float | int]]:
    deltas: dict[str, list[tuple[float, float]]] = {}
    for report in reports:
        fair_value_report = report.fair_value_report
        if fair_value_report is None or fair_value_report.forecast_score is None:
            continue
        primary = fair_value_report.forecast_score
        for baseline in fair_value_report.baselines:
            if baseline.forecast_score is None:
                continue
            deltas.setdefault(baseline.name, []).append(
                (
                    primary.brier_score - baseline.forecast_score.brier_score,
                    primary.log_loss - baseline.forecast_score.log_loss,
                )
            )
    summary: dict[str, dict[str, float | int]] = {}
    for name, pairs in deltas.items():
        summary[name] = {
            "case_count": len(pairs),
            "average_brier_delta": sum(pair[0] for pair in pairs) / len(pairs),
            "average_log_loss_delta": sum(pair[1] for pair in pairs) / len(pairs),
        }
    return summary


def _collect_replay_baseline_deltas(
    reports: list[SportsBenchmarkReport],
) -> dict[str, dict[str, float | int]]:
    deltas: dict[str, list[tuple[float, float]]] = {}
    for report in reports:
        replay_report = report.replay_report
        if replay_report is None:
            continue
        primary = replay_report.score
        for baseline in replay_report.baselines:
            if baseline.score is None:
                continue
            deltas.setdefault(baseline.name, []).append(
                (
                    primary.net_pnl - baseline.score.net_pnl,
                    primary.return_pct - baseline.score.return_pct,
                )
            )
    summary: dict[str, dict[str, float | int]] = {}
    for name, pairs in deltas.items():
        summary[name] = {
            "case_count": len(pairs),
            "average_net_pnl_delta": sum(pair[0] for pair in pairs) / len(pairs),
            "average_return_pct_delta": sum(pair[1] for pair in pairs) / len(pairs),
        }
    return summary


_FAIR_VALUE_COMPARISON_SPECS: tuple[
    tuple[str, str, str, str],
    ...,
] = (
    (
        "calibrated_fair_value",
        "Primary fair value versus calibrated fair value on paired evaluation rows.",
        "calibrated_brier_error",
        "calibrated_log_loss",
    ),
    (
        "model_fair_value",
        "Primary fair value versus case-provided model fair value on paired evaluation rows.",
        "model_brier_error",
        "model_log_loss",
    ),
    (
        "blended_fair_value",
        "Primary fair value versus blended fair value on paired evaluation rows.",
        "blended_brier_error",
        "blended_log_loss",
    ),
)

_FAIR_VALUE_BASELINE_PREDICTION_MAP_COMPARISON_NAMES: tuple[str, ...] = (
    "bookmaker_multiplicative_independent",
    "bookmaker_power_independent",
    "bookmaker_multiplicative_best_line",
    "market_midpoint",
)


def _collect_fair_value_comparison_stats(
    reports: list[SportsBenchmarkReport],
) -> dict[str, dict[str, object]]:
    comparison_series: dict[str, dict[str, list[float]]] = {}
    comparison_case_counts: dict[str, int] = {}
    comparison_descriptions = {
        name: description for name, description, _, _ in _FAIR_VALUE_COMPARISON_SPECS
    }
    comparison_sources = {
        name: "evaluation_rows" for name, *_ in _FAIR_VALUE_COMPARISON_SPECS
    }
    for report in reports:
        fair_value_report = report.fair_value_report
        if fair_value_report is None:
            continue
        comparisons_seen_in_case: set[str] = set()
        for row in fair_value_report.evaluation_rows:
            for name, _, brier_attr, log_loss_attr in _FAIR_VALUE_COMPARISON_SPECS:
                comparison_brier_error = getattr(row, brier_attr)
                comparison_log_loss = getattr(row, log_loss_attr)
                if comparison_brier_error is None or comparison_log_loss is None:
                    continue
                metric_series = comparison_series.setdefault(
                    name,
                    {"brier_error": [], "log_loss": []},
                )
                metric_series["brier_error"].append(
                    row.brier_error - float(comparison_brier_error)
                )
                metric_series["log_loss"].append(
                    row.log_loss - float(comparison_log_loss)
                )
                comparisons_seen_in_case.add(name)
        baseline_reports = {
            baseline.name: baseline for baseline in fair_value_report.baselines
        }
        for name in _FAIR_VALUE_BASELINE_PREDICTION_MAP_COMPARISON_NAMES:
            baseline = baseline_reports.get(name)
            if baseline is None or baseline.prediction_map is None:
                continue
            case_brier_series: list[float] = []
            case_log_loss_series: list[float] = []
            for row in fair_value_report.evaluation_rows:
                baseline_prediction = baseline.prediction_map.get(row.market_key)
                if baseline_prediction is None:
                    case_brier_series = []
                    case_log_loss_series = []
                    break
                baseline_brier_error, baseline_log_loss = _comparison_loss_metrics(
                    prediction=baseline_prediction,
                    outcome=row.outcome_label,
                )
                case_brier_series.append(row.brier_error - baseline_brier_error)
                case_log_loss_series.append(row.log_loss - baseline_log_loss)
            if not case_brier_series or not case_log_loss_series:
                continue
            metric_series = comparison_series.setdefault(
                name,
                {"brier_error": [], "log_loss": []},
            )
            metric_series["brier_error"].extend(case_brier_series)
            metric_series["log_loss"].extend(case_log_loss_series)
            comparison_descriptions.setdefault(
                name,
                _comparison_description_from_baseline(baseline.description),
            )
            comparison_sources[name] = "baseline_prediction_map"
            comparisons_seen_in_case.add(name)
        for name in comparisons_seen_in_case:
            comparison_case_counts[name] = comparison_case_counts.get(name, 0) + 1

    summary: dict[str, dict[str, object]] = {}
    for name, metric_series in comparison_series.items():
        brier_series = metric_series["brier_error"]
        log_loss_series = metric_series["log_loss"]
        if not brier_series or not log_loss_series:
            continue
        summary[name] = {
            "description": comparison_descriptions[name],
            "source": comparison_sources[name],
            "case_count": comparison_case_counts.get(name, 0),
            "row_count": len(brier_series),
            "loss_differential_direction": "primary_metric_minus_comparison_metric",
            "metrics": {
                "brier_error": compare_paired_loss_differentials(
                    brier_series
                ).to_payload(),
                "log_loss": compare_paired_loss_differentials(
                    log_loss_series
                ).to_payload(),
            },
        }
    return summary


def _build_aggregate(
    case_results: list[BenchmarkSuiteCaseResult],
    failures: list[BenchmarkSuiteFailure],
) -> BenchmarkSuiteAggregate:
    reports = [case.report for case in case_results]
    fair_value_score = _build_pooled_forecast_score(case_results)
    calibrated_fair_value_score = _build_pooled_forecast_score(
        case_results,
        calibrated=True,
        only_calibrated_rows=True,
    )
    paired_raw_fair_value_score = _build_pooled_forecast_score(
        case_results,
        only_calibrated_rows=True,
    )
    replay_scores = [
        report.replay_report.score
        for report in reports
        if report.replay_report is not None
    ]
    return BenchmarkSuiteAggregate(
        total_cases=len(case_results) + len(failures),
        successful_cases=len(case_results),
        failed_cases=len(failures),
        fair_value_case_count=sum(
            1 for report in reports if report.fair_value_report is not None
        ),
        replay_case_count=sum(
            1 for report in reports if report.replay_report is not None
        ),
        average_brier_score=(
            fair_value_score.brier_score if fair_value_score is not None else None
        ),
        average_log_loss=(
            fair_value_score.log_loss if fair_value_score is not None else None
        ),
        average_accuracy=(
            fair_value_score.accuracy if fair_value_score is not None else None
        ),
        average_expected_calibration_error=(
            fair_value_score.expected_calibration_error
            if fair_value_score is not None
            else None
        ),
        average_calibrated_brier_score=(
            calibrated_fair_value_score.brier_score
            if calibrated_fair_value_score is not None
            else None
        ),
        average_calibrated_log_loss=(
            calibrated_fair_value_score.log_loss
            if calibrated_fair_value_score is not None
            else None
        ),
        average_calibrated_accuracy=(
            calibrated_fair_value_score.accuracy
            if calibrated_fair_value_score is not None
            else None
        ),
        average_calibrated_expected_calibration_error=(
            calibrated_fair_value_score.expected_calibration_error
            if calibrated_fair_value_score is not None
            else None
        ),
        calibrated_case_count=sum(
            1
            for report in reports
            if report.fair_value_report is not None
            and report.fair_value_report.calibrated_forecast_score is not None
        ),
        average_calibrated_brier_improvement=(
            paired_raw_fair_value_score.brier_score
            - calibrated_fair_value_score.brier_score
            if paired_raw_fair_value_score is not None
            and calibrated_fair_value_score is not None
            else None
        ),
        average_calibrated_log_loss_improvement=(
            paired_raw_fair_value_score.log_loss - calibrated_fair_value_score.log_loss
            if paired_raw_fair_value_score is not None
            and calibrated_fair_value_score is not None
            else None
        ),
        average_calibrated_accuracy_delta=(
            calibrated_fair_value_score.accuracy - paired_raw_fair_value_score.accuracy
            if paired_raw_fair_value_score is not None
            and calibrated_fair_value_score is not None
            else None
        ),
        average_calibrated_expected_calibration_error_improvement=(
            paired_raw_fair_value_score.expected_calibration_error
            - calibrated_fair_value_score.expected_calibration_error
            if paired_raw_fair_value_score is not None
            and calibrated_fair_value_score is not None
            else None
        ),
        average_replay_net_pnl=_average([score.net_pnl for score in replay_scores]),
        average_replay_return_pct=_average(
            [score.return_pct for score in replay_scores]
        ),
        edge_ledger_row_count=sum(
            len(report.fair_value_report.evaluation_rows)
            for report in reports
            if report.fair_value_report is not None
        ),
        fair_value_baseline_deltas=_collect_fair_value_baseline_deltas(reports),
        fair_value_comparison_stats=_collect_fair_value_comparison_stats(reports),
        replay_baseline_deltas=_collect_replay_baseline_deltas(reports),
    )


def _build_pooled_forecast_score(
    case_results: Sequence[BenchmarkSuiteCaseResult],
    *,
    calibrated: bool = False,
    only_calibrated_rows: bool = False,
) -> ForecastScore | None:
    predictions: dict[str, float] = {}
    outcomes: dict[str, int] = {}
    for case_index, case in enumerate(case_results):
        fair_value_report = case.report.fair_value_report
        if fair_value_report is None:
            continue
        for row_index, row in enumerate(fair_value_report.evaluation_rows):
            if only_calibrated_rows and row.calibrated_fair_value is None:
                continue
            prediction = row.calibrated_fair_value if calibrated else row.fair_value
            if prediction is None:
                continue
            pooled_key = f"{case_index}:{row_index}:{row.market_key}"
            predictions[pooled_key] = float(prediction)
            outcomes[pooled_key] = int(row.outcome_label)
    if not predictions:
        return None
    return score_binary_forecasts(predictions, outcomes)


def _build_edge_ledger(
    case_results: list[BenchmarkSuiteCaseResult],
) -> BenchmarkSuiteEdgeLedger:
    rows: list[dict[str, object]] = []
    for case in case_results:
        fair_value_report = case.report.fair_value_report
        if fair_value_report is None:
            continue
        for evaluation_row in fair_value_report.evaluation_rows:
            payload = evaluation_row.to_payload()
            payload["case_name"] = case.report.case_name
            payload["case_path"] = case.case_path
            rows.append(payload)
    return BenchmarkSuiteEdgeLedger(rows=tuple(rows))


def _build_walk_forward_root_report(
    splits: Sequence[WalkForwardBenchmarkSplitResult],
) -> BenchmarkSuiteReport:
    case_results: list[BenchmarkSuiteCaseResult] = []
    failures: list[BenchmarkSuiteFailure] = []
    for split in splits:
        case_results.extend(split.report.case_results)
        failures.extend(split.report.failures)
    return BenchmarkSuiteReport(
        case_results=tuple(case_results),
        failures=tuple(failures),
        aggregate=_build_aggregate(case_results, failures),
        edge_ledger=_build_edge_ledger(case_results),
    )


def _strip_case_calibration_samples(
    case: SportsBenchmarkCase,
) -> SportsBenchmarkCase:
    if case.fair_value_case is None or not case.fair_value_case.calibration_samples:
        return case
    return replace(
        case,
        fair_value_case=replace(case.fair_value_case, calibration_samples=()),
    )


def _inject_generated_model_probabilities(
    case: SportsBenchmarkCase,
    *,
    prefit_elo_model: EloModelArtifact | None,
    prefit_bt_model: BradleyTerryArtifact | None,
) -> SportsBenchmarkCase:
    if case.fair_value_case is None:
        return case
    if case.fair_value_case.model_fair_values:
        return case
    generated_model_fair_values: dict[str, float] = {}
    if prefit_elo_model is not None:
        generated_model_fair_values = generate_model_fair_values(
            case.fair_value_case,
            prefit_elo_model,
        )
    elif prefit_bt_model is not None:
        generated_model_fair_values = generate_bt_model_fair_values(
            case.fair_value_case,
            prefit_bt_model,
        )
    if not generated_model_fair_values:
        return case
    return replace(
        case,
        fair_value_case=replace(
            case.fair_value_case,
            model_fair_values=generated_model_fair_values,
        ),
    )


def run_benchmark_suite(
    case_paths: Sequence[str | Path],
    *,
    prefit_calibration: CalibrationArtifactSource | None = None,
    prefit_elo_model: EloModelArtifact | None = None,
    prefit_bt_model: BradleyTerryArtifact | None = None,
    strip_case_calibration_samples: bool = False,
) -> BenchmarkSuiteReport:
    case_results: list[BenchmarkSuiteCaseResult] = []
    failures: list[BenchmarkSuiteFailure] = []
    for case_path in case_paths:
        resolved_path = Path(case_path)
        try:
            case = load_benchmark_case(resolved_path)
            if strip_case_calibration_samples:
                case = _strip_case_calibration_samples(case)
            case = _inject_generated_model_probabilities(
                case,
                prefit_elo_model=prefit_elo_model,
                prefit_bt_model=prefit_bt_model,
            )
            report = run_benchmark_case(
                case,
                prefit_calibration=prefit_calibration,
            )
        except Exception as exc:  # noqa: BLE001
            failures.append(
                BenchmarkSuiteFailure(case_path=str(resolved_path), error=str(exc))
            )
            continue
        case_results.append(
            BenchmarkSuiteCaseResult(case_path=str(resolved_path), report=report)
        )
    return BenchmarkSuiteReport(
        case_results=tuple(case_results),
        failures=tuple(failures),
        aggregate=_build_aggregate(case_results, failures),
        edge_ledger=_build_edge_ledger(case_results),
    )


def packaged_benchmark_case_paths() -> tuple[Path, ...]:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    return tuple(fixtures_dir / name for name in packaged_benchmark_fixture_names())


def _split_id(split_index: int) -> str:
    return f"split-{split_index:03d}"


def run_walk_forward_benchmark_suite(
    *,
    dataset_name: str,
    dataset_root: str | Path = "research/datasets",
    version: str | None = None,
    min_train_size: int,
    test_size: int,
    step_size: int | None = None,
    max_splits: int | None = None,
    calibration_bin_count: int | None = None,
    model_generator: str | None = None,
) -> WalkForwardBenchmarkSuiteReport:
    registry = DatasetRegistry(dataset_root)
    snapshot = registry.load_snapshot(dataset_name, version)
    if snapshot.kind != "benchmark_cases":
        raise ValueError(
            f"dataset snapshot is not a benchmark case snapshot: {dataset_name}"
        )
    splits = generate_walk_forward_splits(
        snapshot,
        min_train_size=min_train_size,
        test_size=test_size,
        step_size=step_size,
        max_splits=max_splits,
    )
    if not splits:
        raise ValueError("walk-forward configuration produced no splits")

    split_results: list[WalkForwardBenchmarkSplitResult] = []
    for split in splits:
        split_id = _split_id(split.split_index)
        train_case_paths = registry.benchmark_case_paths_by_record_ids(
            dataset_name,
            split.train_record_ids,
            version=version,
        )
        train_cases = [load_benchmark_case(path) for path in train_case_paths]
        train_report = run_benchmark_suite(
            train_case_paths,
            strip_case_calibration_samples=True,
        )
        train_edge_ledger_payload = train_report.edge_ledger.to_payload()
        train_row_count = train_edge_ledger_payload["row_count"]
        if not isinstance(train_row_count, int) or train_row_count <= 0:
            raise ValueError(
                f"walk-forward split {split_id} produced no training edge-ledger rows"
            )
        calibrator = load_calibration_artifact(
            train_edge_ledger_payload,
            bin_count=calibration_bin_count,
        )
        test_case_paths = registry.benchmark_case_paths_by_record_ids(
            dataset_name,
            split.test_record_ids,
            version=version,
        )
        elo_model_artifact = None
        bt_model_artifact = None
        if model_generator == "elo":
            elo_model_artifact = fit_elo_model(train_cases)
        elif model_generator == "bt":
            bt_model_artifact = fit_bradley_terry_from_cases(train_cases)
        test_report = run_benchmark_suite(
            test_case_paths,
            prefit_calibration=calibrator,
            prefit_elo_model=elo_model_artifact,
            prefit_bt_model=bt_model_artifact,
            strip_case_calibration_samples=True,
        )
        split_results.append(
            WalkForwardBenchmarkSplitResult(
                split_id=split_id,
                split=split,
                train_case_paths=tuple(str(path) for path in train_case_paths),
                test_case_paths=tuple(str(path) for path in test_case_paths),
                calibration_artifact={
                    "sample_count": calibrator.sample_count,
                    "artifact": calibrator.to_payload(),
                    "training_edge_ledger_row_count": train_row_count,
                },
                model_artifact=(
                    elo_model_artifact.to_payload()
                    if elo_model_artifact is not None
                    else {
                        "model_generator": "bt",
                        "skill_by_team": bt_model_artifact.skill_by_team,
                    }
                    if bt_model_artifact is not None
                    else None
                ),
                report=test_report,
            )
        )
    resolved_step_size = step_size or test_size
    resolved_version = snapshot.version
    return WalkForwardBenchmarkSuiteReport(
        dataset_root=str(Path(dataset_root)),
        dataset_name=dataset_name,
        dataset_version=resolved_version,
        min_train_size=min_train_size,
        test_size=test_size,
        step_size=resolved_step_size,
        max_splits=max_splits,
        calibration_bin_count=calibration_bin_count,
        model_generator=model_generator,
        splits=tuple(split_results),
    )


def _safe_case_filename(case_name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", case_name).strip("._-")
    return slug or "benchmark-case"


def write_suite_report(
    report: BenchmarkSuiteReport, output_dir: str | Path
) -> tuple[Path, Path]:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    cases_dir = target_dir / "cases"
    cases_dir.mkdir(parents=True, exist_ok=True)
    cases_dir_resolved = cases_dir.resolve()
    for case in report.case_results:
        case_path = (
            cases_dir / f"{_safe_case_filename(case.report.case_name)}.json"
        ).resolve()
        if cases_dir_resolved not in case_path.parents:
            raise ValueError("suite case artifact path escaped cases directory")
        write_benchmark_report(case.report, case_path)
    summary_path = target_dir / "benchmark_suite_summary.json"
    summary_path.write_text(
        json.dumps(report.to_payload(), indent=2, sort_keys=True, allow_nan=False)
    )
    markdown_path = target_dir / "benchmark_suite_summary.md"
    from research.benchmark_reporting import render_suite_markdown

    markdown_path.write_text(render_suite_markdown(report))
    edge_ledger_path = target_dir / "benchmark_suite_edge_ledger.json"
    edge_ledger_path.write_text(
        json.dumps(
            report.edge_ledger.to_payload(), indent=2, sort_keys=True, allow_nan=False
        )
    )
    return summary_path, markdown_path


def write_walk_forward_suite_report(
    report: WalkForwardBenchmarkSuiteReport,
    output_dir: str | Path,
) -> Path:
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    splits_dir = target_dir / "splits"
    splits_dir.mkdir(parents=True, exist_ok=True)

    payload = report.to_payload()
    split_payloads: list[dict[str, object]] = []
    for split_result in report.splits:
        split_dir = splits_dir / split_result.split_id
        summary_path, markdown_path = write_suite_report(split_result.report, split_dir)
        edge_ledger_path = split_dir / "benchmark_suite_edge_ledger.json"
        split_payload = dict(split_result.to_payload())
        split_payload["report_artifacts"] = {
            "summary_json": str(summary_path.relative_to(target_dir)),
            "summary_markdown": str(markdown_path.relative_to(target_dir)),
            "edge_ledger_json": str(edge_ledger_path.relative_to(target_dir)),
            "cases_dir": str((split_dir / "cases").relative_to(target_dir)),
        }
        split_payloads.append(split_payload)
    payload["splits"] = split_payloads

    summary_path = target_dir / "walk_forward_benchmark_summary.json"
    summary_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False)
    )
    return summary_path

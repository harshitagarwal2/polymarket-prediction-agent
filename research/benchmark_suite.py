from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from research.benchmark_runner import (
    SportsBenchmarkReport,
    run_benchmark_case,
    write_benchmark_report,
)
from research.schemas import (
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


def _average(values: list[float]) -> float | None:
    return (sum(values) / len(values)) if values else None


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


def _build_aggregate(
    case_results: list[BenchmarkSuiteCaseResult],
    failures: list[BenchmarkSuiteFailure],
) -> BenchmarkSuiteAggregate:
    reports = [case.report for case in case_results]
    fair_value_scores = [
        report.fair_value_report.forecast_score
        for report in reports
        if report.fair_value_report is not None
        and report.fair_value_report.forecast_score is not None
    ]
    calibrated_fair_value_scores = [
        report.fair_value_report.calibrated_forecast_score
        for report in reports
        if report.fair_value_report is not None
        and report.fair_value_report.calibrated_forecast_score is not None
    ]
    calibration_payloads = [
        report.fair_value_report.calibration
        for report in reports
        if report.fair_value_report is not None
        and report.fair_value_report.calibration is not None
    ]
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
        average_brier_score=_average(
            [score.brier_score for score in fair_value_scores]
        ),
        average_log_loss=_average([score.log_loss for score in fair_value_scores]),
        average_accuracy=_average([score.accuracy for score in fair_value_scores]),
        average_expected_calibration_error=_average(
            [score.expected_calibration_error for score in fair_value_scores]
        ),
        average_calibrated_brier_score=_average(
            [score.brier_score for score in calibrated_fair_value_scores]
        ),
        average_calibrated_log_loss=_average(
            [score.log_loss for score in calibrated_fair_value_scores]
        ),
        average_calibrated_accuracy=_average(
            [score.accuracy for score in calibrated_fair_value_scores]
        ),
        average_calibrated_expected_calibration_error=_average(
            [score.expected_calibration_error for score in calibrated_fair_value_scores]
        ),
        calibrated_case_count=len(calibrated_fair_value_scores),
        average_calibrated_brier_improvement=_average(
            [
                value
                for payload in calibration_payloads
                if (
                    value := _nested_float_payload_metric(
                        payload,
                        parent_key="metric_delta",
                        key="brier_improvement",
                    )
                )
                is not None
            ]
        ),
        average_calibrated_log_loss_improvement=_average(
            [
                value
                for payload in calibration_payloads
                if (
                    value := _nested_float_payload_metric(
                        payload,
                        parent_key="metric_delta",
                        key="log_loss_improvement",
                    )
                )
                is not None
            ]
        ),
        average_calibrated_accuracy_delta=_average(
            [
                value
                for payload in calibration_payloads
                if (
                    value := _nested_float_payload_metric(
                        payload,
                        parent_key="metric_delta",
                        key="accuracy_delta",
                    )
                )
                is not None
            ]
        ),
        average_calibrated_expected_calibration_error_improvement=_average(
            [
                value
                for payload in calibration_payloads
                if (
                    value := _nested_float_payload_metric(
                        payload,
                        parent_key="metric_delta",
                        key="expected_calibration_error_improvement",
                    )
                )
                is not None
            ]
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
        replay_baseline_deltas=_collect_replay_baseline_deltas(reports),
    )


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


def run_benchmark_suite(case_paths: Sequence[str | Path]) -> BenchmarkSuiteReport:
    case_results: list[BenchmarkSuiteCaseResult] = []
    failures: list[BenchmarkSuiteFailure] = []
    for case_path in case_paths:
        resolved_path = Path(case_path)
        try:
            report = run_benchmark_case(load_benchmark_case(resolved_path))
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

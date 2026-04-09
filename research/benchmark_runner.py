from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from engine.strategies import FairValueBandStrategy
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
from risk.limits import RiskEngine, RiskLimits


@dataclass(frozen=True)
class FairValueBenchmarkReport:
    input_row_count: int
    resolved_row_count: int
    skipped_group_count: int
    resolved_market_keys: tuple[str, ...]
    missing_market_keys: tuple[str, ...]
    manifest: dict[str, object]
    forecast_score: ForecastScore | None = None
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
    manifest_payload = manifest.to_payload()
    resolved_market_keys = tuple(sorted(manifest.values.keys()))
    missing_market_keys = _ensure_required_market_keys(
        expected_market_keys=case.expected_market_keys,
        resolved_market_keys=resolved_market_keys,
    )
    skipped_groups = [*manifest.skipped_groups, *skipped_rows]
    if skipped_groups:
        manifest_payload["skipped_groups"] = skipped_groups

    forecast_score = None
    if case.outcome_labels:
        _ensure_outcome_labels_resolved(
            outcome_labels=case.outcome_labels,
            resolved_market_keys=resolved_market_keys,
        )
        forecast_score = score_binary_forecasts(
            {
                market_key: _coerce_probability(record)
                for market_key, record in manifest.values.items()
                if market_key in case.outcome_labels
            },
            case.outcome_labels,
        )

    return FairValueBenchmarkReport(
        input_row_count=len(rows),
        resolved_row_count=len(resolved_rows),
        skipped_group_count=len(skipped_groups),
        resolved_market_keys=resolved_market_keys,
        missing_market_keys=missing_market_keys,
        manifest=manifest_payload,
        forecast_score=forecast_score,
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
            )
        ),
        broker=PaperBroker(
            cash=case.broker.cash,
            config=PaperExecutionConfig(
                max_fill_ratio_per_step=case.broker.max_fill_ratio_per_step,
                slippage_bps=case.broker.slippage_bps,
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

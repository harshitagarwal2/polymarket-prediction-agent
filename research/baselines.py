from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

from engine.interfaces import NoopStrategy
from research.blend import blend_binary_probabilities
from research.fair_values import (
    BookAggregation,
    DevigMethod,
    SportsbookFairValueRow,
    build_fair_value_manifest,
)
from research.paper import PaperBroker, PaperExecutionConfig
from research.replay import ReplayRunner
from research.scoring import (
    ForecastScore,
    ReplayScore,
    score_binary_forecasts,
    score_replay_result,
)
from research.schemas import FairValueBenchmarkCase, ReplayBenchmarkCase
from risk.limits import RiskEngine, RiskLimits, RiskState


@dataclass(frozen=True)
class FairValueBaselineReport:
    name: str
    description: str
    forecast_score: ForecastScore | None = None
    skipped_reason: str | None = None
    prediction_map: dict[str, float] | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "name": self.name,
            "description": self.description,
        }
        if self.forecast_score is not None:
            payload["forecast_score"] = self.forecast_score.to_payload()
        if self.skipped_reason is not None:
            payload["skipped_reason"] = self.skipped_reason
        if self.prediction_map is not None:
            payload["prediction_map"] = {
                market_key: float(probability)
                for market_key, probability in self.prediction_map.items()
            }
        return payload


@dataclass(frozen=True)
class ReplayBaselineReport:
    name: str
    description: str
    score: ReplayScore | None = None
    skipped_reason: str | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "name": self.name,
            "description": self.description,
        }
        if self.score is not None:
            payload["score"] = self.score.to_payload()
        if self.skipped_reason is not None:
            payload["skipped_reason"] = self.skipped_reason
        return payload


def _safe_forecast_score(
    predictions: dict[str, float],
    labels: dict[str, int],
) -> ForecastScore | None:
    if not predictions or set(predictions) != set(labels):
        return None
    return score_binary_forecasts(predictions, labels)


def _missing_label_keys_reason(
    *,
    predictions: dict[str, float],
    labels: dict[str, int],
    prefix: str,
) -> str | None:
    if set(predictions) == set(labels):
        return None
    missing = sorted(set(labels) - set(predictions))
    return f"{prefix}: {', '.join(missing)}"


def _market_midpoint_predictions(
    case: FairValueBenchmarkCase,
) -> dict[str, float] | None:
    predictions: dict[str, float] = {}
    for market in case.materialize_markets():
        probability = market.midpoint
        if probability is None:
            if market.best_bid is not None and market.best_ask is not None:
                probability = (market.best_bid + market.best_ask) / 2.0
            else:
                probability = market.best_bid or market.best_ask
        if probability is None:
            continue
        predictions[market.contract.market_key] = float(probability)
    return predictions or None


def _build_bookmaker_baseline_predictions(
    *,
    rows: list[SportsbookFairValueRow],
    method: DevigMethod,
    aggregation: BookAggregation,
    labels: dict[str, int],
) -> tuple[dict[str, float] | None, str | None]:
    try:
        manifest = build_fair_value_manifest(
            rows, method=method, aggregation=aggregation
        )
    except ValueError as exc:
        return None, str(exc)
    predictions: dict[str, float] = {}
    for market_key, record in manifest.values.items():
        if market_key not in labels:
            continue
        value = record.get("fair_value")
        if not isinstance(value, (int, float, str)):
            return None, f"non-numeric fair value for {market_key}"
        probability = float(value)
        if not math.isfinite(probability):
            return None, f"non-finite fair value for {market_key}"
        predictions[market_key] = probability
    if set(predictions) != set(labels):
        missing = sorted(set(labels) - set(predictions))
        return None, f"missing labeled keys: {', '.join(missing)}"
    return predictions, None


def _build_model_baseline_predictions(
    *,
    case: FairValueBenchmarkCase,
) -> tuple[dict[str, float] | None, str | None]:
    if not case.model_fair_values:
        return None, "case does not define model fair values"
    predictions = {
        market_key: float(probability)
        for market_key, probability in case.model_fair_values.items()
        if market_key in case.outcome_labels
    }
    skipped_reason = _missing_label_keys_reason(
        predictions=predictions,
        labels=case.outcome_labels,
        prefix="missing labeled model fair values",
    )
    if skipped_reason is not None:
        return None, skipped_reason
    return predictions, None


def _build_blended_baseline_predictions(
    *,
    case: FairValueBenchmarkCase,
    resolved_rows: list[SportsbookFairValueRow],
) -> tuple[dict[str, float] | None, str | None]:
    if case.model_blend_weight is None:
        return None, "case does not define model blend weight"
    model_predictions, model_skipped_reason = _build_model_baseline_predictions(
        case=case
    )
    if model_predictions is None:
        return None, model_skipped_reason
    sportsbook_predictions, sportsbook_skipped_reason = (
        _build_bookmaker_baseline_predictions(
            rows=resolved_rows,
            method=case.devig_method,
            aggregation=case.book_aggregation,
            labels=case.outcome_labels,
        )
    )
    if sportsbook_predictions is None:
        return None, sportsbook_skipped_reason
    return (
        {
            market_key: blend_binary_probabilities(
                sportsbook_probability=sportsbook_predictions[market_key],
                model_probability=model_predictions[market_key],
                model_weight=case.model_blend_weight,
            )
            for market_key in case.outcome_labels
        },
        None,
    )


def evaluate_fair_value_baselines(
    *,
    case: FairValueBenchmarkCase,
    resolved_rows: list[SportsbookFairValueRow],
) -> tuple[FairValueBaselineReport, ...]:
    if not case.outcome_labels:
        return ()

    baselines: list[FairValueBaselineReport] = []
    specs: tuple[tuple[str, str, DevigMethod, BookAggregation], ...] = (
        (
            "bookmaker_multiplicative_independent",
            "Bookmaker de-vig baseline with multiplicative method and independent grouping.",
            "multiplicative",
            "independent",
        ),
        (
            "bookmaker_power_independent",
            "Bookmaker de-vig baseline with power method and independent grouping.",
            "power",
            "independent",
        ),
        (
            "bookmaker_multiplicative_best_line",
            "Bookmaker de-vig baseline with multiplicative method and best-line aggregation.",
            "multiplicative",
            "best-line",
        ),
    )
    for name, description, method, aggregation in specs:
        predictions, skipped_reason = _build_bookmaker_baseline_predictions(
            rows=resolved_rows,
            method=method,
            aggregation=aggregation,
            labels=case.outcome_labels,
        )
        baselines.append(
            FairValueBaselineReport(
                name=name,
                description=description,
                forecast_score=(
                    _safe_forecast_score(predictions, case.outcome_labels)
                    if predictions is not None
                    else None
                ),
                skipped_reason=skipped_reason,
                prediction_map=predictions,
            )
        )

    midpoint_predictions = _market_midpoint_predictions(case)
    midpoint_score = (
        _safe_forecast_score(midpoint_predictions, case.outcome_labels)
        if midpoint_predictions is not None
        else None
    )
    baselines.append(
        FairValueBaselineReport(
            name="market_midpoint",
            description="Market midpoint baseline derived from provided market snapshots.",
            forecast_score=midpoint_score,
            skipped_reason=(
                None
                if midpoint_score is not None
                else "market snapshots do not provide complete midpoint coverage"
            ),
            prediction_map=(
                midpoint_predictions if midpoint_score is not None else None
            ),
        )
    )

    model_predictions, model_skipped_reason = _build_model_baseline_predictions(
        case=case,
    )
    baselines.append(
        FairValueBaselineReport(
            name="model_fair_value",
            description="Model-only baseline derived from case-provided model fair values.",
            forecast_score=(
                _safe_forecast_score(model_predictions, case.outcome_labels)
                if model_predictions is not None
                else None
            ),
            skipped_reason=model_skipped_reason,
            prediction_map=model_predictions,
        )
    )

    blended_predictions, blended_skipped_reason = _build_blended_baseline_predictions(
        case=case,
        resolved_rows=resolved_rows,
    )
    baselines.append(
        FairValueBaselineReport(
            name="blended_fair_value",
            description="Logit blend of sportsbook fair values and case-provided model fair values.",
            forecast_score=(
                _safe_forecast_score(blended_predictions, case.outcome_labels)
                if blended_predictions is not None
                else None
            ),
            skipped_reason=blended_skipped_reason,
            prediction_map=blended_predictions,
        )
    )
    return tuple(baselines)


def evaluate_replay_baselines(
    case: ReplayBenchmarkCase,
) -> tuple[ReplayBaselineReport, ...]:
    runner = ReplayRunner(
        strategy=NoopStrategy(),
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
    return (
        ReplayBaselineReport(
            name="noop_strategy",
            description="No-trade replay baseline for strategy comparison.",
            score=score_replay_result(result),
        ),
    )


def available_baseline_names(
    fair_value_baselines: Iterable[FairValueBaselineReport],
    replay_baselines: Iterable[ReplayBaselineReport],
) -> tuple[str, ...]:
    names = [baseline.name for baseline in fair_value_baselines]
    names.extend(baseline.name for baseline in replay_baselines)
    return tuple(names)

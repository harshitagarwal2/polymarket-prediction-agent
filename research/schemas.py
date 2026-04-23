from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Any

from adapters.types import (
    MarketSummary,
    OrderBookSnapshot,
    PriceLevel,
    deserialize_contract,
    deserialize_market_summary,
    serialize_contract,
)
from research.fair_values import (
    BookAggregation,
    DevigMethod,
    SportsbookFairValueRow,
    parse_sportsbook_rows,
    parse_timestamp,
)
from research.calibration import CalibrationSample
from research.replay import ReplayStep


def _read_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def _require_list_of_dicts(name: str, value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{name}[{index}] must be an object")
        normalized.append(item)
    return normalized


def _require_price_levels(name: str, value: object) -> list[dict[str, Any]]:
    return _require_list_of_dicts(name, [] if value is None else value)


def _coerce_finite_float(name: str, value: object) -> float:
    if not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def _coerce_optional_finite_float(name: str, value: object) -> float | None:
    if value is None:
        return None
    return _coerce_finite_float(name, value)


def _coerce_unit_probability(name: str, value: object) -> float:
    probability = _coerce_finite_float(name, value)
    if probability < 0.0 or probability > 1.0:
        raise ValueError(f"{name} must be between 0 and 1")
    return probability


def _coerce_binary_outcome(name: str, value: object) -> int:
    if not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} must be 0 or 1")
    outcome = int(value)
    if outcome not in {0, 1}:
        raise ValueError(f"{name} must be 0 or 1")
    return outcome


def _serialize_price_level(level: PriceLevel) -> dict[str, float]:
    return {"price": level.price, "quantity": level.quantity}


def _deserialize_price_level(payload: dict[str, Any]) -> PriceLevel:
    return PriceLevel(
        price=_coerce_finite_float("price_level.price", payload["price"]),
        quantity=_coerce_finite_float("price_level.quantity", payload["quantity"]),
    )


def serialize_order_book_snapshot(book: OrderBookSnapshot) -> dict[str, Any]:
    return {
        "contract": serialize_contract(book.contract),
        "bids": [_serialize_price_level(level) for level in book.bids],
        "asks": [_serialize_price_level(level) for level in book.asks],
        "midpoint": book.midpoint,
        "last_price": book.last_price,
        "observed_at": book.observed_at.isoformat(),
        "raw": book.raw,
    }


def deserialize_order_book_snapshot(payload: dict[str, Any]) -> OrderBookSnapshot:
    bids = _require_price_levels(
        "replay_case.steps[].book.bids", payload.get("bids", [])
    )
    asks = _require_price_levels(
        "replay_case.steps[].book.asks", payload.get("asks", [])
    )
    return OrderBookSnapshot(
        contract=deserialize_contract(payload["contract"]),
        bids=[_deserialize_price_level(level) for level in bids],
        asks=[_deserialize_price_level(level) for level in asks],
        midpoint=_coerce_optional_finite_float(
            "order_book.midpoint", payload.get("midpoint")
        ),
        last_price=_coerce_optional_finite_float(
            "order_book.last_price", payload.get("last_price")
        ),
        observed_at=parse_timestamp(payload.get("observed_at")),
        raw=payload.get("raw"),
    )


def serialize_replay_step(step: ReplayStep) -> dict[str, Any]:
    return {
        "book": serialize_order_book_snapshot(step.book),
        "fair_value": step.fair_value,
        "metadata": step.metadata,
    }


def deserialize_replay_step(payload: dict[str, Any]) -> ReplayStep:
    return ReplayStep(
        book=deserialize_order_book_snapshot(payload["book"]),
        fair_value=_coerce_optional_finite_float(
            "replay_step.fair_value", payload.get("fair_value")
        ),
        metadata=dict(payload.get("metadata", {})),
    )


def serialize_sportsbook_row(row: SportsbookFairValueRow) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "bookmaker": row.bookmaker,
        "outcome": row.outcome,
        "captured_at": row.captured_at.isoformat().replace("+00:00", "Z"),
        "decimal_odds": row.decimal_odds,
    }
    if row.market_key is not None:
        payload["market_key"] = row.market_key
    if row.selection_name is not None:
        payload["selection_name"] = row.selection_name
    if row.home_team is not None:
        payload["home_team"] = row.home_team
    if row.away_team is not None:
        payload["away_team"] = row.away_team
    if row.sport_key is not None:
        payload["sport_key"] = row.sport_key
    if row.condition_id is not None:
        payload["condition_id"] = row.condition_id
    if row.event_key is not None:
        payload["event_key"] = row.event_key
    if row.sport is not None:
        payload["sport"] = row.sport
    if row.series is not None:
        payload["series"] = row.series
    if row.game_id is not None:
        payload["game_id"] = row.game_id
    if row.sports_market_type is not None:
        payload["sports_market_type"] = row.sports_market_type
    return payload


@dataclass(frozen=True)
class FairValueBenchmarkCase:
    rows: list[dict[str, Any]]
    markets: list[dict[str, Any]] = field(default_factory=list)
    devig_method: DevigMethod = "multiplicative"
    book_aggregation: BookAggregation = "independent"
    max_age_seconds: float | None = None
    source: str | None = None
    expected_market_keys: tuple[str, ...] = ()
    outcome_labels: dict[str, int] = field(default_factory=dict)
    model_fair_values: dict[str, float] = field(default_factory=dict)
    model_blend_weight: float | None = None
    calibration_samples: tuple[CalibrationSample, ...] = ()
    calibration_bin_count: int = 5

    def materialize_rows(self) -> list[SportsbookFairValueRow]:
        return parse_sportsbook_rows(self.rows)

    def materialize_markets(self) -> list[MarketSummary]:
        return [deserialize_market_summary(payload) for payload in self.markets]

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "rows": self.rows,
            "markets": self.markets,
            "devig_method": self.devig_method,
            "book_aggregation": self.book_aggregation,
            "expected_market_keys": list(self.expected_market_keys),
            "outcome_labels": self.outcome_labels,
        }
        if self.model_fair_values:
            payload["model_fair_values"] = self.model_fair_values
        if self.model_blend_weight is not None:
            payload["model_blend_weight"] = self.model_blend_weight
        if self.calibration_samples:
            payload["calibration_samples"] = [
                sample.to_payload() for sample in self.calibration_samples
            ]
            payload["calibration_bin_count"] = self.calibration_bin_count
        if self.max_age_seconds is not None:
            payload["max_age_seconds"] = self.max_age_seconds
        if self.source is not None:
            payload["source"] = self.source
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "FairValueBenchmarkCase":
        rows = _require_list_of_dicts("fair_value_case.rows", payload.get("rows", []))
        markets = _require_list_of_dicts(
            "fair_value_case.markets", payload.get("markets", [])
        )
        outcome_labels = payload.get("outcome_labels", {})
        if not isinstance(outcome_labels, dict):
            raise ValueError("fair_value_case.outcome_labels must be an object")
        normalized_labels = {
            str(key): int(value) for key, value in outcome_labels.items()
        }
        invalid_labels = {
            key: value
            for key, value in normalized_labels.items()
            if value not in {0, 1}
        }
        if invalid_labels:
            raise ValueError("fair_value_case.outcome_labels values must be 0 or 1")
        model_fair_values = payload.get("model_fair_values", {})
        if not isinstance(model_fair_values, dict):
            raise ValueError("fair_value_case.model_fair_values must be an object")
        normalized_model_fair_values = {
            str(key): _coerce_unit_probability(
                f"fair_value_case.model_fair_values[{key}]",
                value,
            )
            for key, value in model_fair_values.items()
        }
        raw_calibration_samples = payload.get("calibration_samples", [])
        if not isinstance(raw_calibration_samples, list):
            raise ValueError("fair_value_case.calibration_samples must be a list")
        calibration_samples: list[CalibrationSample] = []
        for index, item in enumerate(raw_calibration_samples):
            if not isinstance(item, dict):
                raise ValueError(
                    f"fair_value_case.calibration_samples[{index}] must be an object"
                )
            prediction = _coerce_unit_probability(
                f"fair_value_case.calibration_samples[{index}].prediction",
                item.get("prediction"),
            )
            raw_outcome = item.get("outcome")
            if raw_outcome is None:
                raise ValueError(
                    f"fair_value_case.calibration_samples[{index}].outcome is required"
                )
            outcome = _coerce_binary_outcome(
                f"fair_value_case.calibration_samples[{index}].outcome",
                raw_outcome,
            )
            calibration_samples.append(
                CalibrationSample(prediction=prediction, outcome=outcome)
            )
        calibration_bin_count = int(payload.get("calibration_bin_count", 5))
        if calibration_bin_count <= 0:
            raise ValueError("fair_value_case.calibration_bin_count must be positive")
        return cls(
            rows=rows,
            markets=markets,
            devig_method=payload.get("devig_method", "multiplicative"),
            book_aggregation=payload.get("book_aggregation", "independent"),
            max_age_seconds=(
                _coerce_finite_float(
                    "fair_value_case.max_age_seconds", payload["max_age_seconds"]
                )
                if payload.get("max_age_seconds") is not None
                else None
            ),
            source=(
                str(payload["source"]) if payload.get("source") is not None else None
            ),
            expected_market_keys=tuple(
                str(item) for item in payload.get("expected_market_keys", [])
            ),
            outcome_labels=normalized_labels,
            model_fair_values=normalized_model_fair_values,
            model_blend_weight=(
                _coerce_unit_probability(
                    "fair_value_case.model_blend_weight",
                    payload["model_blend_weight"],
                )
                if payload.get("model_blend_weight") is not None
                else None
            ),
            calibration_samples=tuple(calibration_samples),
            calibration_bin_count=calibration_bin_count,
        )


@dataclass(frozen=True)
class ReplayStrategyConfig:
    quantity: float = 1.0
    edge_threshold: float = 0.03
    aggressive: bool = True

    def to_payload(self) -> dict[str, Any]:
        return {
            "quantity": self.quantity,
            "edge_threshold": self.edge_threshold,
            "aggressive": self.aggressive,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ReplayStrategyConfig":
        return cls(
            quantity=_coerce_finite_float(
                "replay_case.strategy.quantity", payload.get("quantity", 1.0)
            ),
            edge_threshold=_coerce_finite_float(
                "replay_case.strategy.edge_threshold",
                payload.get("edge_threshold", 0.03),
            ),
            aggressive=bool(payload.get("aggressive", True)),
        )


@dataclass(frozen=True)
class ReplayBrokerConfig:
    cash: float = 1000.0
    max_fill_ratio_per_step: float = 1.0
    slippage_bps: float = 0.0
    resting_max_fill_ratio_per_step: float | None = None
    resting_fill_delay_steps: int = 0
    stale_after_steps: int = 0
    price_move_bps_per_step: float = 0.0

    def to_payload(self) -> dict[str, Any]:
        return {
            "cash": self.cash,
            "max_fill_ratio_per_step": self.max_fill_ratio_per_step,
            "slippage_bps": self.slippage_bps,
            "resting_max_fill_ratio_per_step": self.resting_max_fill_ratio_per_step,
            "resting_fill_delay_steps": self.resting_fill_delay_steps,
            "stale_after_steps": self.stale_after_steps,
            "price_move_bps_per_step": self.price_move_bps_per_step,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ReplayBrokerConfig":
        return cls(
            cash=_coerce_finite_float(
                "replay_case.broker.cash", payload.get("cash", 1000.0)
            ),
            max_fill_ratio_per_step=_coerce_finite_float(
                "replay_case.broker.max_fill_ratio_per_step",
                payload.get("max_fill_ratio_per_step", 1.0),
            ),
            slippage_bps=_coerce_finite_float(
                "replay_case.broker.slippage_bps", payload.get("slippage_bps", 0.0)
            ),
            resting_max_fill_ratio_per_step=(
                _coerce_finite_float(
                    "replay_case.broker.resting_max_fill_ratio_per_step",
                    payload["resting_max_fill_ratio_per_step"],
                )
                if payload.get("resting_max_fill_ratio_per_step") is not None
                else None
            ),
            resting_fill_delay_steps=int(payload.get("resting_fill_delay_steps", 0)),
            stale_after_steps=int(payload.get("stale_after_steps", 0)),
            price_move_bps_per_step=_coerce_finite_float(
                "replay_case.broker.price_move_bps_per_step",
                payload.get("price_move_bps_per_step", 0.0),
            ),
        )


@dataclass(frozen=True)
class ReplayRiskConfig:
    max_global_contracts: int = 20
    max_contracts_per_market: int = 5
    reserve_contracts_buffer: int = 0
    max_order_notional: float | None = None
    min_price: float = 0.01
    max_price: float = 0.99
    max_daily_loss: float | None = None
    daily_realized_pnl: float = 0.0
    enforce_atomic_batches: bool = True

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "max_global_contracts": self.max_global_contracts,
            "max_contracts_per_market": self.max_contracts_per_market,
            "reserve_contracts_buffer": self.reserve_contracts_buffer,
            "min_price": self.min_price,
            "max_price": self.max_price,
        }
        if self.max_order_notional is not None:
            payload["max_order_notional"] = self.max_order_notional
        if self.max_daily_loss is not None:
            payload["max_daily_loss"] = self.max_daily_loss
        if self.daily_realized_pnl != 0.0:
            payload["daily_realized_pnl"] = self.daily_realized_pnl
        if not self.enforce_atomic_batches:
            payload["enforce_atomic_batches"] = self.enforce_atomic_batches
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ReplayRiskConfig":
        return cls(
            max_global_contracts=int(payload.get("max_global_contracts", 20)),
            max_contracts_per_market=int(payload.get("max_contracts_per_market", 5)),
            reserve_contracts_buffer=int(payload.get("reserve_contracts_buffer", 0)),
            max_order_notional=(
                _coerce_finite_float(
                    "replay_case.risk_limits.max_order_notional",
                    payload["max_order_notional"],
                )
                if payload.get("max_order_notional") is not None
                else None
            ),
            min_price=_coerce_finite_float(
                "replay_case.risk_limits.min_price", payload.get("min_price", 0.01)
            ),
            max_price=_coerce_finite_float(
                "replay_case.risk_limits.max_price", payload.get("max_price", 0.99)
            ),
            max_daily_loss=(
                _coerce_finite_float(
                    "replay_case.risk_limits.max_daily_loss",
                    payload["max_daily_loss"],
                )
                if payload.get("max_daily_loss") is not None
                else None
            ),
            daily_realized_pnl=_coerce_finite_float(
                "replay_case.risk_limits.daily_realized_pnl",
                payload.get("daily_realized_pnl", 0.0),
            ),
            enforce_atomic_batches=bool(payload.get("enforce_atomic_batches", True)),
        )


@dataclass(frozen=True)
class ReplayBenchmarkCase:
    steps: list[dict[str, Any]]
    strategy: ReplayStrategyConfig = field(default_factory=ReplayStrategyConfig)
    broker: ReplayBrokerConfig = field(default_factory=ReplayBrokerConfig)
    risk_limits: ReplayRiskConfig = field(default_factory=ReplayRiskConfig)

    def materialize_steps(self) -> list[ReplayStep]:
        return [deserialize_replay_step(payload) for payload in self.steps]

    def to_payload(self) -> dict[str, Any]:
        return {
            "steps": self.steps,
            "strategy": self.strategy.to_payload(),
            "broker": self.broker.to_payload(),
            "risk_limits": self.risk_limits.to_payload(),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ReplayBenchmarkCase":
        steps = _require_list_of_dicts("replay_case.steps", payload.get("steps", []))
        return cls(
            steps=steps,
            strategy=ReplayStrategyConfig.from_payload(
                dict(payload.get("strategy", {}))
            ),
            broker=ReplayBrokerConfig.from_payload(dict(payload.get("broker", {}))),
            risk_limits=ReplayRiskConfig.from_payload(
                dict(payload.get("risk_limits", {}))
            ),
        )


@dataclass(frozen=True)
class SportsBenchmarkCase:
    name: str
    description: str | None = None
    fair_value_case: FairValueBenchmarkCase | None = None
    replay_case: ReplayBenchmarkCase | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name}
        if self.description is not None:
            payload["description"] = self.description
        if self.fair_value_case is not None:
            payload["fair_value_case"] = self.fair_value_case.to_payload()
        if self.replay_case is not None:
            payload["replay_case"] = self.replay_case.to_payload()
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "SportsBenchmarkCase":
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("benchmark case must define a name")
        fair_value_case = payload.get("fair_value_case")
        replay_case = payload.get("replay_case")
        return cls(
            name=name,
            description=(
                str(payload["description"])
                if payload.get("description") is not None
                else None
            ),
            fair_value_case=(
                FairValueBenchmarkCase.from_payload(fair_value_case)
                if isinstance(fair_value_case, dict)
                else None
            ),
            replay_case=(
                ReplayBenchmarkCase.from_payload(replay_case)
                if isinstance(replay_case, dict)
                else None
            ),
        )


def serialize_benchmark_case(case: SportsBenchmarkCase) -> dict[str, Any]:
    return case.to_payload()


def load_benchmark_case(path: str | Path) -> SportsBenchmarkCase:
    return SportsBenchmarkCase.from_payload(_read_json(path))


def load_packaged_benchmark_case(name: str) -> SportsBenchmarkCase:
    allowed_names = packaged_benchmark_fixture_names()
    if name not in allowed_names:
        raise ValueError(f"unknown packaged benchmark fixture: {name}")
    fixture = resources.files("research").joinpath("fixtures").joinpath(name)
    return SportsBenchmarkCase.from_payload(json.loads(fixture.read_text()))


def packaged_benchmark_fixture_names() -> tuple[str, ...]:
    fixtures_dir = resources.files("research").joinpath("fixtures")
    names = sorted(
        item.name
        for item in fixtures_dir.iterdir()
        if item.is_file() and item.name.endswith(".json")
    )
    return tuple(names)

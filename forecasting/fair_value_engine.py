from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol

from adapters import MarketSummary
from forecasting.consensus import ConsensusComponent, consensus_probability, dispersion_score


class FairValueProvider(Protocol):
    def fair_value_for(self, market: MarketSummary) -> float | None: ...


FairValueField = Literal["raw", "calibrated"]


@dataclass(frozen=True)
class FairValueManifestEntry:
    fair_value: float
    calibrated_fair_value: float | None = None
    generated_at: datetime | None = None
    source: str | None = None
    condition_id: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None

    def selected_fair_value(self, fair_value_field: FairValueField = "raw") -> float:
        if fair_value_field == "calibrated" and self.calibrated_fair_value is not None:
            return self.calibrated_fair_value
        return self.fair_value


@dataclass(frozen=True)
class StaticFairValueProvider:
    fair_values: dict[str, float]

    def fair_value_for(self, market: MarketSummary) -> float | None:
        return self.fair_values.get(market.contract.market_key)


@dataclass(frozen=True)
class ManifestFairValueProvider:
    records: dict[str, FairValueManifestEntry]
    generated_at: datetime | None = None
    source: str | None = None
    max_age_seconds: float | None = None
    fair_value_field: FairValueField = "raw"

    def _market_condition_id(self, market: MarketSummary) -> str | None:
        raw = market.raw
        if not isinstance(raw, dict):
            return None

        token = raw.get("token")
        if isinstance(token, dict):
            value = token.get("condition_id") or token.get("conditionId")
            if value not in (None, ""):
                return str(value)

        payload = raw.get("market") if isinstance(raw.get("market"), dict) else raw
        if isinstance(payload, dict):
            value = payload.get("condition_id") or payload.get("conditionId")
            if value not in (None, ""):
                return str(value)

        return None

    def fair_value_for(self, market: MarketSummary) -> float | None:
        record = self.records.get(market.contract.market_key)
        if record is None:
            return None

        if self.max_age_seconds is not None:
            generated_at = record.generated_at or self.generated_at
            if generated_at is None:
                return None
            age_seconds = (datetime.now(timezone.utc) - generated_at).total_seconds()
            if age_seconds > self.max_age_seconds:
                return None

        if record.condition_id is not None:
            market_condition_id = self._market_condition_id(market)
            if (
                market_condition_id is None
                or market_condition_id != record.condition_id
            ):
                return None

        if record.event_key is not None:
            if market.event_key is None or market.event_key != record.event_key:
                return None

        if record.sport is not None:
            if market.sport is None or market.sport.lower() != record.sport.lower():
                return None

        if record.series is not None:
            if market.series is None or market.series.lower() != record.series.lower():
                return None

        if record.game_id is not None:
            if market.game_id is None or market.game_id != record.game_id:
                return None

        if record.sports_market_type is not None:
            if (
                market.sports_market_type is None
                or market.sports_market_type.lower()
                != record.sports_market_type.lower()
            ):
                return None

        return record.selected_fair_value(self.fair_value_field)


@dataclass(frozen=True)
class ConsensusFairValueInput:
    probability: float
    weight: float = 1.0
    freshness_seconds: float | None = None
    source: str | None = None


@dataclass(frozen=True)
class ConsensusFairValueResult:
    fair_value: float
    dispersion: float
    component_count: int


class ConsensusFairValueEngine:
    def __init__(self, *, half_life_seconds: float = 3600.0):
        self.half_life_seconds = half_life_seconds

    def combine(
        self, inputs: list[ConsensusFairValueInput]
    ) -> ConsensusFairValueResult:
        if not inputs:
            raise ValueError("inputs must not be empty")
        components = [
            ConsensusComponent(
                probability=item.probability,
                weight=item.weight,
                freshness_seconds=item.freshness_seconds,
                source=item.source,
            )
            for item in inputs
        ]
        fair_value = consensus_probability(
            components,
            half_life_seconds=self.half_life_seconds,
        )
        return ConsensusFairValueResult(
            fair_value=fair_value,
            dispersion=dispersion_score(components),
            component_count=len(components),
        )

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable


@dataclass(frozen=True)
class ConsensusComponent:
    probability: float
    weight: float = 1.0
    freshness_seconds: float | None = None
    source: str | None = None


def _bounded_probability(probability: float) -> float:
    if not math.isfinite(probability) or probability < 0.0 or probability > 1.0:
        raise ValueError("probability must be finite and between 0 and 1")
    return probability


def freshness_weight(
    freshness_seconds: float | None,
    *,
    half_life_seconds: float = 3600.0,
) -> float:
    if freshness_seconds is None:
        return 1.0
    if freshness_seconds <= 0:
        return 1.0
    if half_life_seconds <= 0:
        return 1.0
    return 0.5 ** (freshness_seconds / half_life_seconds)


def consensus_probability(
    components: Iterable[ConsensusComponent],
    *,
    half_life_seconds: float = 3600.0,
) -> float:
    weighted_total = 0.0
    total_weight = 0.0
    for component in components:
        probability = _bounded_probability(component.probability)
        effective_weight = max(component.weight, 0.0) * freshness_weight(
            component.freshness_seconds,
            half_life_seconds=half_life_seconds,
        )
        if effective_weight <= 0:
            continue
        weighted_total += probability * effective_weight
        total_weight += effective_weight
    if total_weight <= 0:
        raise ValueError("consensus components must contribute positive weight")
    return weighted_total / total_weight


def dispersion_score(
    components: Iterable[ConsensusComponent],
    *,
    half_life_seconds: float = 3600.0,
) -> float:
    resolved = list(components)
    if not resolved:
        raise ValueError("components must not be empty")
    mean = consensus_probability(resolved, half_life_seconds=half_life_seconds)
    weighted_total = 0.0
    total_weight = 0.0
    for component in resolved:
        effective_weight = max(component.weight, 0.0) * freshness_weight(
            component.freshness_seconds,
            half_life_seconds=half_life_seconds,
        )
        if effective_weight <= 0:
            continue
        weighted_total += abs(component.probability - mean) * effective_weight
        total_weight += effective_weight
    if total_weight <= 0:
        raise ValueError("consensus components must contribute positive weight")
    return weighted_total / total_weight


def american_to_prob(odds: int) -> float:
    if odds == 0:
        raise ValueError("american odds must not be zero")
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def decimal_to_prob(odds: float) -> float:
    if odds <= 0:
        raise ValueError("decimal odds must be positive")
    return 1.0 / odds


def remove_overround(probs: dict[str, float]) -> dict[str, float]:
    total = sum(probs.values())
    if total <= 0:
        raise ValueError("probability total must be positive")
    return {key: value / total for key, value in probs.items()}


def weighted_consensus(
    rows: list[dict],
    *,
    half_life_seconds: float = 3600.0,
) -> float:
    components: list[ConsensusComponent] = []
    for row in rows:
        if "implied_prob" in row and row["implied_prob"] is not None:
            probability = float(row["implied_prob"])
        elif "price_decimal" in row and row["price_decimal"] is not None:
            probability = decimal_to_prob(float(row["price_decimal"]))
        elif "decimal_odds" in row and row["decimal_odds"] is not None:
            probability = decimal_to_prob(float(row["decimal_odds"]))
        elif "american_odds" in row and row["american_odds"] is not None:
            probability = american_to_prob(int(row["american_odds"]))
        else:
            continue
        weight = float(row.get("weight", 1.0))
        weight *= float(row.get("reputation_weight", 1.0))
        weight *= float(row.get("agreement_weight", 1.0))
        freshness_seconds = row.get("freshness_seconds")
        if freshness_seconds is None and row.get("source_age_ms") is not None:
            freshness_seconds = float(row["source_age_ms"]) / 1000.0
        components.append(
            ConsensusComponent(
                probability=probability,
                weight=weight,
                freshness_seconds=(
                    float(freshness_seconds) if freshness_seconds is not None else None
                ),
                source=row.get("source"),
            )
        )
    return consensus_probability(components, half_life_seconds=half_life_seconds)

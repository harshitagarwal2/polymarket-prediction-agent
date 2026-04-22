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


def dispersion_score(components: Iterable[ConsensusComponent]) -> float:
    resolved = list(components)
    if not resolved:
        raise ValueError("components must not be empty")
    mean = consensus_probability(resolved)
    return sum(abs(component.probability - mean) for component in resolved) / len(
        resolved
    )

from __future__ import annotations

import math


_PROBABILITY_EPSILON = 1e-12


def _coerce_unit_probability(name: str, value: float) -> float:
    probability = float(value)
    if not math.isfinite(probability):
        raise ValueError(f"{name} must be finite")
    if probability < 0.0 or probability > 1.0:
        raise ValueError(f"{name} must be between 0 and 1")
    return probability


def _bounded_probability(probability: float) -> float:
    return min(max(probability, _PROBABILITY_EPSILON), 1.0 - _PROBABILITY_EPSILON)


def _logit(probability: float) -> float:
    bounded_probability = _bounded_probability(probability)
    return math.log(bounded_probability) - math.log1p(-bounded_probability)


def _sigmoid(logit: float) -> float:
    if logit >= 0.0:
        exp_negated = math.exp(-logit)
        return 1.0 / (1.0 + exp_negated)
    exp_value = math.exp(logit)
    return exp_value / (1.0 + exp_value)


def blend_binary_probabilities(
    sportsbook_probability: float,
    model_probability: float,
    model_weight: float,
) -> float:
    sportsbook = _coerce_unit_probability(
        "sportsbook_probability",
        sportsbook_probability,
    )
    model = _coerce_unit_probability("model_probability", model_probability)
    weight = _coerce_unit_probability("model_weight", model_weight)

    if weight == 0.0:
        return sportsbook
    if weight == 1.0:
        return model

    blended_logit = ((1.0 - weight) * _logit(sportsbook)) + (weight * _logit(model))
    return _sigmoid(blended_logit)

from __future__ import annotations

import math
from dataclasses import dataclass


def _sigmoid(value: float) -> float:
    if value >= 60:
        return 1.0
    if value <= -60:
        return 0.0
    return 1.0 / (1.0 + math.exp(-value))


@dataclass(frozen=True)
class LinearFeatureModelArtifact:
    model_name: str
    model_version: str
    bias: float
    weights: dict[str, float]
    centers: dict[str, float]

    def predict_proba(self, row: dict[str, float]) -> float:
        score = self.bias
        for feature_name, weight in self.weights.items():
            centered_value = float(row.get(feature_name, 0.0)) - float(
                self.centers.get(feature_name, 0.0)
            )
            score += centered_value * weight
        return _sigmoid(score)


def predict_rows(
    artifact: LinearFeatureModelArtifact,
    rows: list[dict[str, float]],
) -> list[float]:
    return [artifact.predict_proba(row) for row in rows]

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from forecasting.calibration import HistogramCalibrator, load_calibration_artifact


@dataclass(frozen=True)
class ForecastCalibrator:
    artifact: HistogramCalibrator

    @classmethod
    def load(cls, source: object) -> "ForecastCalibrator":
        return cls(artifact=load_calibration_artifact(source))

    def apply(self, probability: float) -> float:
        return self.artifact.apply(probability)

    def apply_mapping(self, predictions: Mapping[str, float]) -> dict[str, float]:
        return self.artifact.apply_mapping(predictions)

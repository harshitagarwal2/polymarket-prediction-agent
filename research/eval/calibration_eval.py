from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from forecasting.scoring import CalibrationBin, score_binary_forecasts


@dataclass(frozen=True)
class CalibrationSummary:
    count: int
    bin_count: int
    expected_calibration_error: float
    max_calibration_gap: float
    calibration_bins: tuple[CalibrationBin, ...]


def evaluate_probability_calibration(
    rows: Iterable[Mapping[str, object]],
    *,
    probability_key: str = "fair_value",
    label_key: str = "outcome_label",
    bin_count: int = 5,
) -> CalibrationSummary:
    predictions: dict[str, float] = {}
    outcomes: dict[str, int] = {}
    for index, row in enumerate(rows):
        probability_value = row.get(probability_key)
        label_value = row.get(label_key)
        if probability_value in (None, "") or label_value in (None, ""):
            continue
        if isinstance(probability_value, bool) or not isinstance(
            probability_value, (int, float, str)
        ):
            continue
        if isinstance(label_value, bool) or not isinstance(label_value, (int, str)):
            continue
        key = str(index)
        predictions[key] = float(probability_value)
        outcomes[key] = int(label_value)
    score = score_binary_forecasts(predictions, outcomes, bin_count=bin_count)
    max_gap = max((bin_.gap for bin_ in score.calibration_bins), default=0.0)
    return CalibrationSummary(
        count=score.count,
        bin_count=bin_count,
        expected_calibration_error=score.expected_calibration_error,
        max_calibration_gap=max_gap,
        calibration_bins=score.calibration_bins,
    )

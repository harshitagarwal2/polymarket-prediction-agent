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
        if row.get(probability_key) in (None, "") or row.get(label_key) in (None, ""):
            continue
        key = str(index)
        predictions[key] = float(row[probability_key])
        outcomes[key] = int(row[label_key])
    score = score_binary_forecasts(predictions, outcomes, bin_count=bin_count)
    max_gap = max((bin_.gap for bin_ in score.calibration_bins), default=0.0)
    return CalibrationSummary(
        count=score.count,
        bin_count=bin_count,
        expected_calibration_error=score.expected_calibration_error,
        max_calibration_gap=max_gap,
        calibration_bins=score.calibration_bins,
    )

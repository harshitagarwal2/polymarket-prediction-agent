from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


def _bucket_index(probability: float, *, bin_count: int) -> int:
    bucket_index = min(bin_count - 1, int(probability * bin_count))
    if probability == 1.0:
        return bin_count - 1
    return bucket_index


@dataclass(frozen=True)
class CalibrationSample:
    prediction: float
    outcome: int

    def __post_init__(self) -> None:
        if not math.isfinite(self.prediction):
            raise ValueError("calibration sample prediction must be finite")
        if self.prediction < 0.0 or self.prediction > 1.0:
            raise ValueError("calibration sample prediction must be between 0 and 1")
        if self.outcome not in {0, 1}:
            raise ValueError("calibration sample outcome must be 0 or 1")

    def to_payload(self) -> dict[str, float | int]:
        return {"prediction": self.prediction, "outcome": self.outcome}


@dataclass(frozen=True)
class HistogramCalibrationBin:
    lower_bound: float
    upper_bound: float
    count: int
    mean_prediction: float | None
    empirical_positive_rate: float | None
    calibrated_probability: float

    def to_payload(self) -> dict[str, float | int | None]:
        return {
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "count": self.count,
            "mean_prediction": self.mean_prediction,
            "empirical_positive_rate": self.empirical_positive_rate,
            "calibrated_probability": self.calibrated_probability,
        }


@dataclass(frozen=True)
class HistogramCalibrator:
    bin_count: int
    sample_count: int
    positive_rate: float
    bins: tuple[HistogramCalibrationBin, ...]

    def apply(self, probability: float) -> float:
        if not math.isfinite(probability):
            raise ValueError("calibration probability must be finite")
        if probability < 0.0 or probability > 1.0:
            raise ValueError("calibration probability must be between 0 and 1")
        return self.bins[
            _bucket_index(probability, bin_count=self.bin_count)
        ].calibrated_probability

    def apply_mapping(self, predictions: Mapping[str, float]) -> dict[str, float]:
        return {key: self.apply(value) for key, value in predictions.items()}

    def to_payload(self) -> dict[str, object]:
        return {
            "method": "histogram",
            "bin_count": self.bin_count,
            "sample_count": self.sample_count,
            "positive_rate": self.positive_rate,
            "bins": [bin_.to_payload() for bin_ in self.bins],
        }


def _coerce_finite_float(value: object, *, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{context} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{context} must be finite")
    return parsed


def _coerce_probability(value: object, *, context: str) -> float:
    probability = _coerce_finite_float(value, context=context)
    if probability < 0.0 or probability > 1.0:
        raise ValueError(f"{context} must be between 0 and 1")
    return probability


def _coerce_count(value: object, *, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{context} must be an integer")
    if value < 0:
        raise ValueError(f"{context} must be non-negative")
    return value


def _coerce_binary_outcome(value: object, *, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{context} must be 0 or 1")
    parsed = int(value)
    if parsed not in {0, 1} or float(value) != float(parsed):
        raise ValueError(f"{context} must be 0 or 1")
    return parsed


def histogram_calibrator_from_payload(
    payload: Mapping[str, object],
) -> HistogramCalibrator:
    method = payload.get("method")
    if method != "histogram":
        raise ValueError("unsupported calibration artifact method")

    bin_count = _coerce_count(payload.get("bin_count"), context="artifact.bin_count")
    if bin_count <= 0:
        raise ValueError("artifact.bin_count must be positive")
    sample_count = _coerce_count(
        payload.get("sample_count"), context="artifact.sample_count"
    )
    positive_rate = _coerce_probability(
        payload.get("positive_rate"), context="artifact.positive_rate"
    )
    raw_bins = payload.get("bins")
    if not isinstance(raw_bins, list):
        raise ValueError("artifact.bins must be a list")
    if len(raw_bins) != bin_count:
        raise ValueError("artifact.bins length must equal artifact.bin_count")

    bins: list[HistogramCalibrationBin] = []
    for index, item in enumerate(raw_bins):
        if not isinstance(item, Mapping):
            raise ValueError(f"artifact.bins[{index}] must be an object")
        mean_prediction = item.get("mean_prediction")
        empirical_positive_rate = item.get("empirical_positive_rate")
        bins.append(
            HistogramCalibrationBin(
                lower_bound=_coerce_probability(
                    item.get("lower_bound"),
                    context=f"artifact.bins[{index}].lower_bound",
                ),
                upper_bound=_coerce_probability(
                    item.get("upper_bound"),
                    context=f"artifact.bins[{index}].upper_bound",
                ),
                count=_coerce_count(
                    item.get("count"),
                    context=f"artifact.bins[{index}].count",
                ),
                mean_prediction=(
                    _coerce_probability(
                        mean_prediction,
                        context=f"artifact.bins[{index}].mean_prediction",
                    )
                    if mean_prediction is not None
                    else None
                ),
                empirical_positive_rate=(
                    _coerce_probability(
                        empirical_positive_rate,
                        context=(f"artifact.bins[{index}].empirical_positive_rate"),
                    )
                    if empirical_positive_rate is not None
                    else None
                ),
                calibrated_probability=_coerce_probability(
                    item.get("calibrated_probability"),
                    context=f"artifact.bins[{index}].calibrated_probability",
                ),
            )
        )

    return HistogramCalibrator(
        bin_count=bin_count,
        sample_count=sample_count,
        positive_rate=positive_rate,
        bins=tuple(bins),
    )


def calibration_samples_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    prediction_field: str = "fair_value",
    outcome_field: str = "outcome_label",
) -> tuple[CalibrationSample, ...]:
    samples: list[CalibrationSample] = []
    for index, row in enumerate(rows):
        prediction = _coerce_probability(
            row.get(prediction_field),
            context=f"rows[{index}].{prediction_field}",
        )
        outcome = row.get(outcome_field)
        if outcome is None:
            raise ValueError(f"rows[{index}].{outcome_field} is required")
        samples.append(
            CalibrationSample(
                prediction=prediction,
                outcome=_coerce_binary_outcome(
                    outcome,
                    context=f"rows[{index}].{outcome_field}",
                ),
            )
        )
    return tuple(samples)


def fit_histogram_calibrator_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    bin_count: int = 5,
    prediction_field: str = "fair_value",
    outcome_field: str = "outcome_label",
) -> HistogramCalibrator:
    return fit_histogram_calibrator(
        calibration_samples_from_rows(
            rows,
            prediction_field=prediction_field,
            outcome_field=outcome_field,
        ),
        bin_count=bin_count,
    )


def _extract_histogram_artifact(payload: object) -> Mapping[str, object] | None:
    if not isinstance(payload, Mapping):
        return None
    method = payload.get("method")
    if method == "histogram":
        return payload
    for key in ("artifact", "calibration", "fair_value"):
        resolved = _extract_histogram_artifact(payload.get(key))
        if resolved is not None:
            return resolved
    return None


def _extract_evaluation_rows(
    payload: object,
) -> tuple[Mapping[str, object], ...] | None:
    if isinstance(payload, list):
        if all(isinstance(item, Mapping) for item in payload):
            return tuple(payload)
        raise ValueError("calibration rows payload must contain objects")
    if not isinstance(payload, Mapping):
        return None
    for key in ("rows", "evaluation_rows"):
        raw_rows = payload.get(key)
        if raw_rows is not None:
            return _extract_evaluation_rows(raw_rows)
    for key in ("edge_ledger", "fair_value"):
        resolved = _extract_evaluation_rows(payload.get(key))
        if resolved is not None:
            return resolved
    return None


def _extract_bin_count(payload: object) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    for key in ("bin_count", "calibration_bin_count"):
        raw = payload.get(key)
        if raw is not None:
            count = _coerce_count(raw, context=key)
            if count <= 0:
                raise ValueError(f"{key} must be positive")
            return count
    for key in ("calibration", "fair_value"):
        resolved = _extract_bin_count(payload.get(key))
        if resolved is not None:
            return resolved
    return None


def load_calibration_artifact(
    source: str | Path | Mapping[str, object] | Sequence[Mapping[str, object]],
    *,
    bin_count: int | None = None,
    prediction_field: str = "fair_value",
    outcome_field: str = "outcome_label",
) -> HistogramCalibrator:
    payload: object = source
    if isinstance(source, (str, Path)):
        payload = json.loads(Path(source).read_text())

    artifact_payload = _extract_histogram_artifact(payload)
    if artifact_payload is not None:
        return histogram_calibrator_from_payload(artifact_payload)

    rows = _extract_evaluation_rows(payload)
    if rows is None:
        raise ValueError(
            "calibration artifact must be a histogram artifact or contain evaluation rows"
        )

    resolved_bin_count = bin_count or _extract_bin_count(payload) or 5
    return fit_histogram_calibrator_from_rows(
        rows,
        bin_count=resolved_bin_count,
        prediction_field=prediction_field,
        outcome_field=outcome_field,
    )


def fit_histogram_calibrator(
    samples: Sequence[CalibrationSample],
    *,
    bin_count: int = 5,
) -> HistogramCalibrator:
    if bin_count <= 0:
        raise ValueError("calibration bin_count must be positive")
    if not samples:
        raise ValueError("calibration samples must not be empty")

    counts = [0 for _ in range(bin_count)]
    prediction_sums = [0.0 for _ in range(bin_count)]
    outcome_sums = [0.0 for _ in range(bin_count)]

    for sample in samples:
        bucket_index = _bucket_index(sample.prediction, bin_count=bin_count)
        counts[bucket_index] += 1
        prediction_sums[bucket_index] += sample.prediction
        outcome_sums[bucket_index] += sample.outcome

    non_empty_positive_rates = {
        index: outcome_sums[index] / counts[index]
        for index in range(bin_count)
        if counts[index] > 0
    }
    positive_rate = sum(sample.outcome for sample in samples) / len(samples)

    bins: list[HistogramCalibrationBin] = []
    for index in range(bin_count):
        count = counts[index]
        mean_prediction = prediction_sums[index] / count if count > 0 else None
        empirical_positive_rate = outcome_sums[index] / count if count > 0 else None
        calibrated_probability = empirical_positive_rate
        if calibrated_probability is None:
            nearest_index = min(
                non_empty_positive_rates,
                key=lambda candidate: (abs(candidate - index), candidate),
            )
            calibrated_probability = non_empty_positive_rates[nearest_index]
        bins.append(
            HistogramCalibrationBin(
                lower_bound=index / bin_count,
                upper_bound=(index + 1) / bin_count,
                count=count,
                mean_prediction=mean_prediction,
                empirical_positive_rate=empirical_positive_rate,
                calibrated_probability=calibrated_probability,
            )
        )

    return HistogramCalibrator(
        bin_count=bin_count,
        sample_count=len(samples),
        positive_rate=positive_rate,
        bins=tuple(bins),
    )

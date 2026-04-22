from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Mapping, Sequence


_DEFAULT_BOOTSTRAP_SEED = 1729


def _coerce_finite_series(values: Sequence[float], *, name: str) -> tuple[float, ...]:
    if not values:
        raise ValueError(f"{name} must not be empty")
    series = tuple(float(value) for value in values)
    if any(not math.isfinite(value) for value in series):
        raise ValueError(f"{name} must contain only finite values")
    return series


def _quantile(sorted_values: Sequence[float], probability: float) -> float:
    if not sorted_values:
        raise ValueError("sorted_values must not be empty")
    if probability <= 0.0:
        return float(sorted_values[0])
    if probability >= 1.0:
        return float(sorted_values[-1])
    index = (len(sorted_values) - 1) * probability
    lower_index = math.floor(index)
    upper_index = math.ceil(index)
    if lower_index == upper_index:
        return float(sorted_values[lower_index])
    lower_value = float(sorted_values[lower_index])
    upper_value = float(sorted_values[upper_index])
    weight = index - lower_index
    return lower_value + ((upper_value - lower_value) * weight)


@dataclass(frozen=True)
class BootstrapMeanConfidenceInterval:
    sample_count: int
    sample_mean: float
    confidence_level: float
    lower_bound: float
    upper_bound: float
    statistic: str
    interval_method: str
    resample_count: int
    seed: int

    def to_payload(self) -> dict[str, float | int | str]:
        return {
            "sample_count": self.sample_count,
            "sample_mean": self.sample_mean,
            "confidence_level": self.confidence_level,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "statistic": self.statistic,
            "interval_method": self.interval_method,
            "resample_count": self.resample_count,
            "seed": self.seed,
        }


@dataclass(frozen=True)
class PairedLossComparison:
    sample_count: int
    mean_loss_differential: float
    standard_error: float
    test_statistic: float | None
    p_value_two_sided: float | None
    comparison_method: str
    variance_estimator: str
    null_hypothesis: str
    alternative_hypothesis: str
    assumptions: tuple[str, ...]
    bootstrap_mean_confidence_interval: BootstrapMeanConfidenceInterval
    comparison_note: str | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "sample_count": self.sample_count,
            "mean_loss_differential": self.mean_loss_differential,
            "standard_error": self.standard_error,
            "test_statistic": self.test_statistic,
            "p_value_two_sided": self.p_value_two_sided,
            "comparison_method": self.comparison_method,
            "variance_estimator": self.variance_estimator,
            "null_hypothesis": self.null_hypothesis,
            "alternative_hypothesis": self.alternative_hypothesis,
            "assumptions": list(self.assumptions),
            "bootstrap_mean_confidence_interval": (
                self.bootstrap_mean_confidence_interval.to_payload()
            ),
        }
        if self.comparison_note is not None:
            payload["comparison_note"] = self.comparison_note
        return payload


@dataclass(frozen=True)
class CalibrationBin:
    lower_bound: float
    upper_bound: float
    count: int
    average_prediction: float
    average_outcome: float
    gap: float

    def to_payload(self) -> dict[str, float | int]:
        return {
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "count": self.count,
            "average_prediction": self.average_prediction,
            "average_outcome": self.average_outcome,
            "gap": self.gap,
        }


@dataclass(frozen=True)
class ForecastScore:
    count: int
    brier_score: float
    log_loss: float
    accuracy: float
    mean_prediction: float
    positive_rate: float
    expected_calibration_error: float
    calibration_bins: tuple[CalibrationBin, ...]

    def to_payload(self) -> dict[str, object]:
        return {
            "count": self.count,
            "brier_score": self.brier_score,
            "log_loss": self.log_loss,
            "accuracy": self.accuracy,
            "mean_prediction": self.mean_prediction,
            "positive_rate": self.positive_rate,
            "expected_calibration_error": self.expected_calibration_error,
            "calibration_bins": [bin_.to_payload() for bin_ in self.calibration_bins],
        }


def bootstrap_mean_confidence_interval(
    values: Sequence[float],
    *,
    confidence_level: float = 0.95,
    resample_count: int = 5000,
    seed: int = _DEFAULT_BOOTSTRAP_SEED,
) -> BootstrapMeanConfidenceInterval:
    series = _coerce_finite_series(values, name="values")
    if confidence_level <= 0.0 or confidence_level >= 1.0:
        raise ValueError("confidence_level must be between 0 and 1")
    if resample_count <= 0:
        raise ValueError("resample_count must be positive")

    sample_count = len(series)
    sample_mean = sum(series) / sample_count
    rng = random.Random(seed)
    bootstrap_means = sorted(
        sum(series[rng.randrange(sample_count)] for _ in range(sample_count))
        / sample_count
        for _ in range(resample_count)
    )
    tail_probability = (1.0 - confidence_level) / 2.0
    return BootstrapMeanConfidenceInterval(
        sample_count=sample_count,
        sample_mean=sample_mean,
        confidence_level=confidence_level,
        lower_bound=_quantile(bootstrap_means, tail_probability),
        upper_bound=_quantile(bootstrap_means, 1.0 - tail_probability),
        statistic="mean",
        interval_method="percentile_bootstrap",
        resample_count=resample_count,
        seed=seed,
    )


def compare_paired_loss_differentials(
    loss_differentials: Sequence[float],
    *,
    confidence_level: float = 0.95,
    bootstrap_resample_count: int = 5000,
    seed: int = _DEFAULT_BOOTSTRAP_SEED,
) -> PairedLossComparison:
    series = _coerce_finite_series(
        loss_differentials,
        name="loss_differentials",
    )
    sample_count = len(series)
    mean_loss_differential = sum(series) / sample_count
    if sample_count == 1:
        standard_error = 0.0
    else:
        centered_sum = sum((value - mean_loss_differential) ** 2 for value in series)
        sample_variance = centered_sum / (sample_count - 1)
        standard_error = math.sqrt(sample_variance / sample_count)
    if standard_error == 0.0:
        if mean_loss_differential == 0.0:
            test_statistic = 0.0
            p_value_two_sided = 1.0
            comparison_note = (
                "all paired loss differentials are identical; the normal approximation "
                "is degenerate"
            )
        else:
            test_statistic = None
            p_value_two_sided = None
            comparison_note = (
                "all paired loss differentials are identical; the normal-approximation "
                "test statistic is omitted"
            )
    else:
        test_statistic = mean_loss_differential / standard_error
        p_value_two_sided = math.erfc(abs(test_statistic) / math.sqrt(2.0))
        comparison_note = None
    return PairedLossComparison(
        sample_count=sample_count,
        mean_loss_differential=mean_loss_differential,
        standard_error=standard_error,
        test_statistic=test_statistic,
        p_value_two_sided=p_value_two_sided,
        comparison_method="diebold_mariano_style_two_sided_normal_approximation",
        variance_estimator="sample_variance_of_paired_loss_differentials",
        null_hypothesis="mean_loss_differential_equals_zero",
        alternative_hypothesis="mean_loss_differential_not_equal_zero",
        assumptions=(
            "paired loss differentials are treated as approximately iid",
            "normal approximation is applied to the mean loss differential",
        ),
        bootstrap_mean_confidence_interval=bootstrap_mean_confidence_interval(
            series,
            confidence_level=confidence_level,
            resample_count=bootstrap_resample_count,
            seed=seed,
        ),
        comparison_note=comparison_note,
    )


def score_binary_forecasts(
    predictions: Mapping[str, float],
    outcomes: Mapping[str, int],
    *,
    bin_count: int = 5,
) -> ForecastScore:
    if not predictions:
        raise ValueError("predictions must not be empty")
    if set(predictions) != set(outcomes):
        raise ValueError("predictions and outcomes must have the same keys")
    if bin_count <= 0:
        raise ValueError("bin_count must be positive")

    epsilon = 1e-12
    ordered_keys = sorted(predictions)
    brier_total = 0.0
    log_loss_total = 0.0
    correct = 0
    prediction_sum = 0.0
    outcome_sum = 0.0
    bucket_predictions: list[list[float]] = [[] for _ in range(bin_count)]
    bucket_outcomes: list[list[int]] = [[] for _ in range(bin_count)]

    for key in ordered_keys:
        prediction = float(predictions[key])
        outcome = int(outcomes[key])
        if not math.isfinite(prediction):
            raise ValueError("predictions must be finite")
        if outcome not in {0, 1}:
            raise ValueError("outcomes must be binary (0 or 1)")
        if prediction < 0.0 or prediction > 1.0:
            raise ValueError("predictions must be between 0 and 1")

        bounded_prediction = min(max(prediction, epsilon), 1.0 - epsilon)
        brier_total += (prediction - outcome) ** 2
        log_loss_total += -(
            outcome * math.log(bounded_prediction)
            + (1 - outcome) * math.log(1.0 - bounded_prediction)
        )
        correct += int((prediction >= 0.5) == bool(outcome))
        prediction_sum += prediction
        outcome_sum += outcome

        bucket_index = min(bin_count - 1, int(prediction * bin_count))
        if prediction == 1.0:
            bucket_index = bin_count - 1
        bucket_predictions[bucket_index].append(prediction)
        bucket_outcomes[bucket_index].append(outcome)

    bins: list[CalibrationBin] = []
    ece = 0.0
    total = len(ordered_keys)
    for index in range(bin_count):
        predictions_in_bin = bucket_predictions[index]
        outcomes_in_bin = bucket_outcomes[index]
        if predictions_in_bin:
            average_prediction = sum(predictions_in_bin) / len(predictions_in_bin)
            average_outcome = sum(outcomes_in_bin) / len(outcomes_in_bin)
        else:
            average_prediction = 0.0
            average_outcome = 0.0
        gap = abs(average_prediction - average_outcome)
        ece += (len(predictions_in_bin) / total) * gap
        bins.append(
            CalibrationBin(
                lower_bound=index / bin_count,
                upper_bound=(index + 1) / bin_count,
                count=len(predictions_in_bin),
                average_prediction=average_prediction,
                average_outcome=average_outcome,
                gap=gap,
            )
        )

    return ForecastScore(
        count=total,
        brier_score=brier_total / total,
        log_loss=log_loss_total / total,
        accuracy=correct / total,
        mean_prediction=prediction_sum / total,
        positive_rate=outcome_sum / total,
        expected_calibration_error=ece,
        calibration_bins=tuple(bins),
    )

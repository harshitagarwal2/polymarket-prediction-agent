from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

from research.replay import ReplayResult


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


@dataclass(frozen=True)
class ReplayScore:
    event_count: int
    trade_count: int
    filled_trade_count: int
    buy_trade_count: int
    sell_trade_count: int
    rejection_count: int
    ending_cash: float
    ending_portfolio_value: float
    net_pnl: float
    return_pct: float

    def to_payload(self) -> dict[str, float | int]:
        return {
            "event_count": self.event_count,
            "trade_count": self.trade_count,
            "filled_trade_count": self.filled_trade_count,
            "buy_trade_count": self.buy_trade_count,
            "sell_trade_count": self.sell_trade_count,
            "rejection_count": self.rejection_count,
            "ending_cash": self.ending_cash,
            "ending_portfolio_value": self.ending_portfolio_value,
            "net_pnl": self.net_pnl,
            "return_pct": self.return_pct,
        }


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


def score_replay_result(result: ReplayResult) -> ReplayScore:
    trades = [trade for event in result.events for trade in event.trades]
    filled_trades = [trade for trade in trades if trade.filled]
    initial_cash = result.ending_portfolio_value - result.net_pnl
    return ReplayScore(
        event_count=len(result.events),
        trade_count=len(trades),
        filled_trade_count=len(filled_trades),
        buy_trade_count=sum(1 for trade in trades if trade.action.value == "buy"),
        sell_trade_count=sum(1 for trade in trades if trade.action.value == "sell"),
        rejection_count=sum(len(event.rejected) for event in result.events),
        ending_cash=result.ending_cash,
        ending_portfolio_value=result.ending_portfolio_value,
        net_pnl=result.net_pnl,
        return_pct=(result.net_pnl / initial_cash * 100.0) if initial_cash else 0.0,
    )

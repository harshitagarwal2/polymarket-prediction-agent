from __future__ import annotations

from dataclasses import dataclass

from forecasting.scoring import (
    BootstrapMeanConfidenceInterval,
    CalibrationBin,
    ForecastScore,
    PairedLossComparison,
    bootstrap_mean_confidence_interval,
    compare_paired_loss_differentials,
    score_binary_forecasts,
)
from research.replay import ReplayResult


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
            'event_count': self.event_count,
            'trade_count': self.trade_count,
            'filled_trade_count': self.filled_trade_count,
            'buy_trade_count': self.buy_trade_count,
            'sell_trade_count': self.sell_trade_count,
            'rejection_count': self.rejection_count,
            'ending_cash': self.ending_cash,
            'ending_portfolio_value': self.ending_portfolio_value,
            'net_pnl': self.net_pnl,
            'return_pct': self.return_pct,
        }


def score_replay_result(result: ReplayResult) -> ReplayScore:
    trades = [trade for event in result.events for trade in event.trades]
    filled_trades = [trade for trade in trades if trade.filled]
    initial_cash = result.ending_portfolio_value - result.net_pnl
    return ReplayScore(
        event_count=len(result.events),
        trade_count=len(trades),
        filled_trade_count=len(filled_trades),
        buy_trade_count=sum(1 for trade in trades if trade.action.value == 'buy'),
        sell_trade_count=sum(1 for trade in trades if trade.action.value == 'sell'),
        rejection_count=sum(len(event.rejected) for event in result.events),
        ending_cash=result.ending_cash,
        ending_portfolio_value=result.ending_portfolio_value,
        net_pnl=result.net_pnl,
        return_pct=(result.net_pnl / initial_cash * 100.0) if initial_cash else 0.0,
    )


__all__ = [
    'BootstrapMeanConfidenceInterval',
    'CalibrationBin',
    'ForecastScore',
    'PairedLossComparison',
    'ReplayScore',
    'bootstrap_mean_confidence_interval',
    'compare_paired_loss_differentials',
    'score_binary_forecasts',
    'score_replay_result',
]

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from adapters.types import (
    BalanceSnapshot,
    OrderBookSnapshot,
    OrderIntent,
)
from engine.interfaces import Strategy, StrategyContext
from research.paper import PaperBroker, PaperTrade
from risk.limits import RiskEngine


@dataclass(frozen=True)
class ReplayStep:
    book: OrderBookSnapshot
    fair_value: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayEvent:
    step_index: int
    book: OrderBookSnapshot
    approved: list[OrderIntent]
    rejected: list[str]
    trades: list[PaperTrade]


@dataclass
class ReplayResult:
    events: list[ReplayEvent]
    ending_cash: float
    ending_positions: dict[str, float]
    mark_prices: dict[str, float]
    ending_portfolio_value: float
    net_pnl: float


class ReplayRunner:
    def __init__(
        self,
        strategy: Strategy,
        risk_engine: RiskEngine,
        broker: PaperBroker | None = None,
    ):
        self.strategy = strategy
        self.risk_engine = risk_engine
        self.broker = broker or PaperBroker()

    def run(self, steps: list[ReplayStep]) -> ReplayResult:
        events: list[ReplayEvent] = []
        mark_prices: dict[str, float] = {}
        for index, step in enumerate(steps):
            carry_trades = self.broker.advance(step.book)
            contract = step.book.contract
            mark_price = step.book.midpoint
            if mark_price is None:
                if step.book.best_bid is not None and step.book.best_ask is not None:
                    mark_price = (step.book.best_bid + step.book.best_ask) / 2
                else:
                    mark_price = step.book.best_bid or step.book.best_ask or 0.0
            mark_prices[contract.market_key] = mark_price
            context = StrategyContext(
                contract=contract,
                book=step.book,
                position=self.broker.position_for(contract),
                balance=BalanceSnapshot(
                    venue=contract.venue,
                    available=self.broker.cash,
                    total=self.broker.cash,
                ),
                open_orders=self.broker.open_orders_for(contract),
                fair_value=step.fair_value,
                metadata=step.metadata,
                risk_graph=self.risk_engine.graph_snapshot_for(contract.market_key),
            )
            proposed = self.strategy.generate_intents(context)
            risk = self.risk_engine.evaluate(
                proposed,
                position=context.position,
                positions=self.broker.positions_snapshot(),
                open_orders=self.broker.open_order_snapshots(),
            )
            submitted_trades = self.broker.submit_intents(step.book, risk.approved)
            events.append(
                ReplayEvent(
                    step_index=index,
                    book=step.book,
                    approved=risk.approved,
                    rejected=[rejection.reason for rejection in risk.rejected],
                    trades=[*carry_trades, *submitted_trades],
                )
            )
        ending_portfolio_value = self.broker.portfolio_value(mark_prices)
        return ReplayResult(
            events=events,
            ending_cash=self.broker.cash,
            ending_positions=dict(self.broker.positions),
            mark_prices=mark_prices,
            ending_portfolio_value=ending_portfolio_value,
            net_pnl=ending_portfolio_value - self.broker.initial_cash,
        )


from research.replay.exchange_sim import (  # noqa: E402
    ExchangeSimConfig,
    apply_wait_time_slippage,
    cancel_effective_after_steps,
    simulate_fillable_quantity,
    snapshot_is_stale,
)

__all__ = [
    "ExchangeSimConfig",
    "ReplayEvent",
    "ReplayResult",
    "ReplayRunner",
    "ReplayStep",
    "apply_wait_time_slippage",
    "cancel_effective_after_steps",
    "simulate_fillable_quantity",
    "snapshot_is_stale",
]

from __future__ import annotations

from adapters.types import Contract, OrderBookSnapshot, OutcomeSide, PriceLevel, Venue
from engine.strategies import FairValueBandStrategy
from research.paper import PaperBroker
from research.replay import ReplayRunner, ReplayStep
from risk.limits import RiskEngine, RiskLimits


if __name__ == "__main__":
    contract = Contract(
        venue=Venue.POLYMARKET, symbol="demo-token", outcome=OutcomeSide.YES
    )
    steps = [
        ReplayStep(
            book=OrderBookSnapshot(
                contract=contract,
                bids=[PriceLevel(0.45, 10)],
                asks=[PriceLevel(0.50, 5)],
            ),
            fair_value=0.60,
        ),
        ReplayStep(
            book=OrderBookSnapshot(
                contract=contract,
                bids=[PriceLevel(0.47, 10)],
                asks=[PriceLevel(0.51, 10)],
            ),
            fair_value=0.59,
        ),
    ]

    runner = ReplayRunner(
        strategy=FairValueBandStrategy(quantity=5, edge_threshold=0.03),
        risk_engine=RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
        ),
        broker=PaperBroker(cash=100),
    )
    result = runner.run(steps)

    print("ending cash:", result.ending_cash)
    print("ending positions:", result.ending_positions)
    for event in result.events:
        print(
            f"step={event.step_index} approved={len(event.approved)} rejected={event.rejected} trades={[(t.order_id, t.quantity, t.reason) for t in event.trades]}"
        )

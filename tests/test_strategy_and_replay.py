import unittest

from adapters.types import (
    BalanceSnapshot,
    Contract,
    OrderAction,
    OrderBookSnapshot,
    OutcomeSide,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from engine.interfaces import StrategyContext
from engine.strategies import FairValueBandStrategy
from research.paper import PaperBroker
from research.replay import ReplayRunner, ReplayStep
from risk.limits import RiskEngine, RiskLimits


def make_contract() -> Contract:
    return Contract(venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES)


class StrategyAndReplayTests(unittest.TestCase):
    def test_fair_value_strategy_buys_on_positive_edge(self):
        contract = make_contract()
        strategy = FairValueBandStrategy(quantity=2, edge_threshold=0.03)
        context = StrategyContext(
            contract=contract,
            book=OrderBookSnapshot(
                contract=contract,
                bids=[PriceLevel(price=0.45, quantity=10)],
                asks=[PriceLevel(price=0.50, quantity=10)],
            ),
            position=PositionSnapshot(contract=contract, quantity=0),
            balance=BalanceSnapshot(venue=Venue.POLYMARKET, available=100, total=100),
            fair_value=0.60,
        )

        intents = strategy.generate_intents(context)

        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].action, OrderAction.BUY)
        self.assertEqual(intents[0].price, 0.50)

    def test_replay_runner_generates_trades(self):
        contract = make_contract()
        strategy = FairValueBandStrategy(quantity=5, edge_threshold=0.03)
        runner = ReplayRunner(
            strategy=strategy,
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
            broker=PaperBroker(cash=100),
        )
        steps = [
            ReplayStep(
                book=OrderBookSnapshot(
                    contract=contract,
                    bids=[PriceLevel(price=0.45, quantity=10)],
                    asks=[PriceLevel(price=0.50, quantity=5)],
                ),
                fair_value=0.60,
            )
        ]

        result = runner.run(steps)

        self.assertEqual(len(result.events), 1)
        self.assertTrue(result.events[0].trades)
        self.assertIn(contract.market_key, result.ending_positions)
        self.assertGreater(result.ending_portfolio_value, 0)
        self.assertIn(contract.market_key, result.mark_prices)


if __name__ == "__main__":
    unittest.main()

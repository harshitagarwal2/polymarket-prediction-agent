import unittest

from adapters.types import (
    BalanceSnapshot,
    Contract,
    OrderAction,
    OrderBookSnapshot,
    OrderIntent,
    OutcomeSide,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from engine.interfaces import StrategyContext
from engine.strategies import FairValueBandStrategy
from research.paper import PaperBroker
from research.paper import PaperExecutionConfig
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

    def test_replay_runner_passes_step_fair_value_and_metadata_to_strategy(self):
        contract = make_contract()

        class RecordingStrategy:
            def __init__(self):
                self.seen_contexts = []

            def generate_intents(self, context):
                self.seen_contexts.append(context)
                return []

        strategy = RecordingStrategy()
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
                metadata={"source": "manifest", "step": 1},
            ),
            ReplayStep(
                book=OrderBookSnapshot(
                    contract=contract,
                    bids=[PriceLevel(price=0.55, quantity=10)],
                    asks=[PriceLevel(price=0.57, quantity=5)],
                ),
                fair_value=0.58,
                metadata={"source": "manifest", "step": 2},
            ),
        ]

        result = runner.run(steps)

        self.assertEqual(len(result.events), 2)
        self.assertEqual(len(strategy.seen_contexts), 2)
        self.assertEqual(
            [context.fair_value for context in strategy.seen_contexts],
            [0.60, 0.58],
        )
        self.assertEqual(
            [context.metadata for context in strategy.seen_contexts],
            [
                {"source": "manifest", "step": 1},
                {"source": "manifest", "step": 2},
            ],
        )

    def test_replay_runner_passes_registered_risk_graph_to_strategy(self):
        contract = make_contract()

        class RecordingStrategy:
            def __init__(self):
                self.seen_contexts = []

            def generate_intents(self, context):
                self.seen_contexts.append(context)
                return []

        strategy = RecordingStrategy()
        risk_engine = RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
        )
        risk_engine.register_market_event(
            contract.market_key,
            event_key="event-1",
            mutually_exclusive_group_key="winner",
        )
        runner = ReplayRunner(
            strategy=strategy,
            risk_engine=risk_engine,
            broker=PaperBroker(cash=100),
        )

        runner.run(
            [
                ReplayStep(
                    book=OrderBookSnapshot(
                        contract=contract,
                        bids=[PriceLevel(price=0.45, quantity=10)],
                        asks=[PriceLevel(price=0.50, quantity=5)],
                    ),
                    fair_value=0.60,
                )
            ]
        )

        self.assertEqual(len(strategy.seen_contexts), 1)
        self.assertIsNotNone(strategy.seen_contexts[0].risk_graph)
        self.assertEqual(
            strategy.seen_contexts[0].risk_graph.linked_event_key,
            "event:event-1",
        )

    def test_replay_runner_uses_global_positions_across_contracts_for_risk(self):
        first_contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-1",
            outcome=OutcomeSide.YES,
        )
        second_contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-2",
            outcome=OutcomeSide.YES,
        )

        class ContractAwareStrategy:
            def generate_intents(self, context):
                return [
                    OrderIntent(
                        contract=context.contract,
                        action=OrderAction.BUY,
                        price=context.book.best_ask or 0.0,
                        quantity=1.0,
                    )
                ]

        runner = ReplayRunner(
            strategy=ContractAwareStrategy(),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=1)
            ),
            broker=PaperBroker(cash=100),
        )
        steps = [
            ReplayStep(
                book=OrderBookSnapshot(
                    contract=first_contract,
                    bids=[PriceLevel(price=0.45, quantity=10)],
                    asks=[PriceLevel(price=0.50, quantity=10)],
                ),
                fair_value=0.60,
            ),
            ReplayStep(
                book=OrderBookSnapshot(
                    contract=second_contract,
                    bids=[PriceLevel(price=0.45, quantity=10)],
                    asks=[PriceLevel(price=0.50, quantity=10)],
                ),
                fair_value=0.60,
            ),
        ]

        result = runner.run(steps)

        self.assertEqual(len(result.events[0].approved), 1)
        self.assertEqual(result.events[1].approved, [])
        self.assertEqual(result.events[1].rejected, ["global exposure cap exceeded"])
        self.assertEqual(result.ending_positions, {first_contract.market_key: 1.0})

    def test_paper_broker_applies_resting_fill_delay_and_resting_fill_ratio(self):
        contract = make_contract()
        broker = PaperBroker(
            cash=100,
            config=PaperExecutionConfig(
                max_fill_ratio_per_step=1.0,
                resting_max_fill_ratio_per_step=0.5,
                resting_fill_delay_steps=1,
            ),
        )
        resting_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.60, quantity=10)],
        )
        crossing_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.49, quantity=10)],
        )

        submit_trades = broker.submit_intents(
            resting_book,
            [
                OrderIntent(
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=10.0,
                )
            ],
        )
        first_advance_trades = broker.advance(crossing_book)
        second_advance_trades = broker.advance(crossing_book)
        third_advance_trades = broker.advance(crossing_book)

        self.assertEqual(len(submit_trades), 1)
        self.assertEqual(submit_trades[0].reason, "resting on book")
        self.assertEqual(first_advance_trades, [])
        self.assertEqual(sum(trade.quantity for trade in second_advance_trades), 5.0)
        self.assertEqual(sum(trade.quantity for trade in third_advance_trades), 5.0)
        self.assertEqual(broker.positions[contract.market_key], 10.0)


if __name__ == "__main__":
    unittest.main()

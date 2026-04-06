import unittest

from adapters.types import (
    Contract,
    OrderAction,
    OrderBookSnapshot,
    OrderIntent,
    OutcomeSide,
    PriceLevel,
    Venue,
)
from research.paper import PaperBroker, PaperExecutionConfig


def make_contract() -> Contract:
    return Contract(venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES)


class PaperBrokerTests(unittest.TestCase):
    def test_buy_crosses_best_ask_and_fills(self):
        contract = make_contract()
        book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.46, quantity=10)],
        )
        broker = PaperBroker(cash=100)
        intent = OrderIntent(
            contract=contract, action=OrderAction.BUY, price=0.50, quantity=10
        )

        trades = broker.execute(book, [intent])

        self.assertTrue(trades[0].filled)
        self.assertEqual(broker.cash, 100 - (0.46 * 10))

    def test_partial_fill_leaves_resting_order_then_advances(self):
        contract = make_contract()
        first_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.46, quantity=5)],
        )
        second_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.46, quantity=10)],
        )
        broker = PaperBroker(cash=100)
        intent = OrderIntent(
            contract=contract, action=OrderAction.BUY, price=0.50, quantity=10
        )

        first_trades = broker.execute(first_book, [intent])
        open_orders = broker.open_orders_for(contract)
        remaining_before_advance = open_orders[0].remaining_quantity
        second_trades = broker.advance(second_book)

        self.assertEqual(sum(trade.quantity for trade in first_trades), 5)
        self.assertEqual(len(open_orders), 1)
        self.assertEqual(remaining_before_advance, 5)
        self.assertEqual(sum(trade.quantity for trade in second_trades), 5)
        self.assertFalse(broker.open_orders_for(contract))

    def test_sell_requires_inventory(self):
        contract = make_contract()
        book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.46, quantity=10)],
        )
        broker = PaperBroker(cash=100)
        intent = OrderIntent(
            contract=contract, action=OrderAction.SELL, price=0.40, quantity=5
        )

        trades = broker.execute(book, [intent])

        self.assertFalse(trades[0].filled)
        self.assertEqual(trades[0].reason, "insufficient paper inventory")

    def test_resting_buy_order_reserves_cash(self):
        contract = make_contract()
        resting_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.60, quantity=10)],
        )
        crossing_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=20)],
        )
        broker = PaperBroker(cash=10)

        broker.execute(
            resting_book,
            [
                OrderIntent(
                    contract=contract, action=OrderAction.BUY, price=0.50, quantity=10
                )
            ],
        )
        trades = broker.execute(
            crossing_book,
            [
                OrderIntent(
                    contract=contract, action=OrderAction.BUY, price=0.50, quantity=20
                )
            ],
        )

        self.assertEqual(broker.reserved_cash(), 10.0)
        self.assertEqual(broker.available_cash(), 0.0)
        self.assertEqual(sum(trade.quantity for trade in trades if trade.filled), 10)

    def test_fill_ratio_limits_per_step_liquidity(self):
        contract = make_contract()
        book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=10)],
        )
        broker = PaperBroker(
            cash=100,
            config=PaperExecutionConfig(max_fill_ratio_per_step=0.5),
        )

        trades = broker.execute(
            book,
            [
                OrderIntent(
                    contract=contract, action=OrderAction.BUY, price=0.50, quantity=10
                )
            ],
        )

        self.assertEqual(sum(trade.quantity for trade in trades if trade.filled), 5)
        self.assertEqual(broker.open_orders_for(contract)[0].remaining_quantity, 5)


if __name__ == "__main__":
    unittest.main()

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

    def test_resting_fill_telemetry_tracks_wait_time_staleness_and_price_drift(self):
        contract = make_contract()
        resting_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.60, quantity=10)],
        )
        crossing_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.49, quantity=10)],
        )
        broker = PaperBroker(
            cash=100,
            config=PaperExecutionConfig(
                resting_fill_delay_steps=1,
                stale_after_steps=1,
                price_move_bps_per_step=25.0,
            ),
        )

        broker.submit_intents(
            resting_book,
            [
                OrderIntent(
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=5.0,
                    metadata={
                        "mapping_risk": 0.2,
                        "secret_token": "should-not-persist",
                    },
                )
            ],
        )
        self.assertEqual(broker.advance(crossing_book), [])

        filled_trades = broker.advance(crossing_book)

        self.assertEqual(len(filled_trades), 1)
        self.assertEqual(filled_trades[0].wait_steps, 2)
        self.assertTrue(filled_trades[0].stale_data_flag)
        self.assertAlmostEqual(filled_trades[0].price, 0.49245)
        self.assertEqual(filled_trades[0].decision_best_ask, 0.60)
        self.assertEqual(filled_trades[0].decision_reference_price, 0.60)
        self.assertEqual(filled_trades[0].requested_quantity, 5.0)
        self.assertEqual(filled_trades[0].metadata, {"mapping_risk": 0.2})

    def test_pending_cancel_can_still_fill_before_latency_expires(self):
        contract = make_contract()
        resting_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.60, quantity=10)],
        )
        crossing_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.49, quantity=10)],
        )
        broker = PaperBroker(
            cash=100,
            config=PaperExecutionConfig(
                resting_fill_delay_steps=0,
                cancel_latency_steps=2,
            ),
        )

        broker.submit_intents(
            resting_book,
            [
                OrderIntent(
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=5.0,
                )
            ],
        )
        order = broker.open_orders_for(contract)[0]
        self.assertTrue(broker.request_cancel(order.order_id))

        first_trades = broker.advance(crossing_book)
        self.assertEqual(len(first_trades), 1)
        self.assertTrue(first_trades[0].cancel_race_fill)
        self.assertEqual(first_trades[0].cancel_requested_step, 0)
        self.assertEqual(first_trades[0].cancel_effective_step, 2)

        broker.advance(resting_book)
        broker.advance(resting_book)
        self.assertFalse(broker.open_orders_for(contract))

    def test_effective_cancel_blocks_fill_at_boundary_step(self):
        contract = make_contract()
        resting_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.60, quantity=10)],
        )
        crossing_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.49, quantity=10)],
        )
        broker = PaperBroker(
            cash=100,
            config=PaperExecutionConfig(
                resting_fill_delay_steps=0,
                cancel_latency_steps=0,
            ),
        )

        broker.submit_intents(
            resting_book,
            [
                OrderIntent(
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=5.0,
                )
            ],
        )
        order = broker.open_orders_for(contract)[0]
        self.assertTrue(broker.request_cancel(order.order_id))

        trades = broker.advance(crossing_book)

        self.assertEqual(trades, [])
        self.assertFalse(broker.open_orders_for(contract))

    def test_cancel_requested_resting_row_is_not_tagged_as_cancel_race_fill(self):
        contract = make_contract()
        resting_book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10)],
            asks=[PriceLevel(price=0.60, quantity=10)],
        )
        broker = PaperBroker(
            cash=100,
            config=PaperExecutionConfig(
                resting_fill_delay_steps=0,
                cancel_latency_steps=2,
            ),
        )

        broker.submit_intents(
            resting_book,
            [
                OrderIntent(
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=5.0,
                )
            ],
        )
        order = broker.open_orders_for(contract)[0]
        self.assertTrue(broker.request_cancel(order.order_id))

        trades = broker.advance(resting_book)

        self.assertEqual(len(trades), 1)
        self.assertFalse(trades[0].filled)
        self.assertFalse(trades[0].cancel_race_fill)


if __name__ == "__main__":
    unittest.main()

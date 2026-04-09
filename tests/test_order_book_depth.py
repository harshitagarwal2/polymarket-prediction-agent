from __future__ import annotations

import unittest

from adapters.types import (
    Contract,
    OrderAction,
    OrderBookSnapshot,
    OutcomeSide,
    PriceLevel,
    Venue,
)


def make_contract() -> Contract:
    return Contract(venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES)


class OrderBookDepthTests(unittest.TestCase):
    def test_cumulative_quantity_uses_multiple_levels(self):
        book = OrderBookSnapshot(
            contract=make_contract(),
            bids=[
                PriceLevel(price=0.45, quantity=1.0),
                PriceLevel(price=0.44, quantity=2.0),
            ],
            asks=[
                PriceLevel(price=0.50, quantity=0.5),
                PriceLevel(price=0.51, quantity=1.0),
            ],
        )

        self.assertEqual(book.cumulative_quantity(OrderAction.BUY, max_levels=2), 1.5)
        self.assertEqual(book.cumulative_quantity(OrderAction.SELL, max_levels=2), 3.0)

    def test_estimate_fill_tracks_average_and_worst_price(self):
        book = OrderBookSnapshot(
            contract=make_contract(),
            asks=[
                PriceLevel(price=0.50, quantity=0.5),
                PriceLevel(price=0.51, quantity=1.0),
                PriceLevel(price=0.52, quantity=1.0),
            ],
        )

        estimate = book.estimate_fill(OrderAction.BUY, 1.2, max_levels=3)

        self.assertAlmostEqual(estimate.filled_quantity, 1.2)
        self.assertTrue(estimate.complete)
        self.assertAlmostEqual(
            estimate.average_price or 0.0,
            (0.5 * 0.50 + 0.7 * 0.51) / 1.2,
        )
        self.assertEqual(estimate.worst_price, 0.51)
        self.assertEqual(estimate.levels_consumed, 2)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from adapters.base import AdapterHealth
from adapters.types import Contract, NormalizedOrder, OrderAction, OutcomeSide, Venue
from risk.cleanup import CleanupCoordinator


class CleanupAdapter:
    venue = Venue.POLYMARKET

    def __init__(self, open_order_sequences: list[list[NormalizedOrder]]):
        self.contract = Contract(
            venue=self.venue, symbol="token-1", outcome=OutcomeSide.YES
        )
        self._open_order_sequences = list(open_order_sequences)
        self.cancel_all_calls = 0

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return []

    def get_order_book(self, contract):
        raise NotImplementedError

    def list_open_orders(self, contract=None):
        if len(self._open_order_sequences) > 1:
            return self._open_order_sequences.pop(0)
        return self._open_order_sequences[0]

    def list_positions(self, contract=None):
        return []

    def list_fills(self, contract=None):
        return []

    def get_position(self, contract):
        raise NotImplementedError

    def get_balance(self):
        raise NotImplementedError

    def get_account_snapshot(self, contract=None):
        raise NotImplementedError

    def place_limit_order(self, intent):
        raise NotImplementedError

    def cancel_order(self, order_id: str):
        return True

    def cancel_all(self, contract=None):
        self.cancel_all_calls += 1
        return 1

    def close(self):
        return None


def make_order(order_id: str, contract: Contract) -> NormalizedOrder:
    return NormalizedOrder(
        order_id=order_id,
        contract=contract,
        action=OrderAction.BUY,
        price=0.5,
        quantity=1.0,
        remaining_quantity=1.0,
    )


class CleanupTests(unittest.TestCase):
    def test_cancel_all_and_verify_confirms_after_stable_empty_polls(self):
        adapter = CleanupAdapter(
            [
                [
                    make_order(
                        "order-1",
                        Contract(Venue.POLYMARKET, "token-1", OutcomeSide.YES),
                    )
                ],
                [],
                [],
            ]
        )
        coordinator = CleanupCoordinator(adapter)

        result = coordinator.cancel_all_and_verify(
            adapter.contract,
            stable_polls=2,
            sleep_seconds=0.0,
            max_wait_seconds=1.0,
        )

        self.assertTrue(result.confirmed)
        self.assertEqual(result.remaining_order_ids, [])
        self.assertEqual(adapter.cancel_all_calls, 1)

    def test_cancel_all_and_verify_times_out_with_remaining_orders(self):
        contract = Contract(Venue.POLYMARKET, "token-1", OutcomeSide.YES)
        adapter = CleanupAdapter([[make_order("order-1", contract)]])
        coordinator = CleanupCoordinator(adapter)

        result = coordinator.cancel_all_and_verify(
            adapter.contract,
            stable_polls=2,
            sleep_seconds=0.0,
            max_wait_seconds=0.0,
        )

        self.assertFalse(result.confirmed)
        self.assertEqual(result.remaining_order_ids, ["order-1"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from adapters.base import AdapterHealth
from adapters.types import (
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderStatus,
    OutcomeSide,
    Venue,
)
from engine import OrderLifecycleManager, OrderLifecyclePolicy, summarize_fill_state
from adapters.types import FillSnapshot


class LifecycleAdapter:
    venue = Venue.POLYMARKET

    def __init__(self, orders: list[NormalizedOrder]):
        self._orders = orders
        self.cancelled: list[str] = []

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return []

    def get_order_book(self, contract):
        raise NotImplementedError

    def list_open_orders(self, contract=None):
        return list(self._orders)

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
        self.cancelled.append(order_id)
        return True

    def cancel_all(self, contract=None):
        return 0

    def close(self):
        return None


def make_order(order_id: str, age_seconds: float) -> NormalizedOrder:
    now = datetime.now(timezone.utc)
    contract = Contract(
        venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
    )
    return NormalizedOrder(
        order_id=order_id,
        contract=contract,
        action=OrderAction.BUY,
        price=0.5,
        quantity=1.0,
        remaining_quantity=1.0,
        status=OrderStatus.RESTING,
        created_at=now - timedelta(seconds=age_seconds),
        updated_at=now - timedelta(seconds=age_seconds),
    )


class LifecycleTests(unittest.TestCase):
    def test_policy_marks_stale_orders_for_cancel(self):
        stale = make_order("stale-1", age_seconds=60)
        fresh = make_order("fresh-1", age_seconds=5)
        policy = OrderLifecyclePolicy(max_order_age_seconds=30)

        decisions = policy.evaluate([stale, fresh], now=datetime.now(timezone.utc))

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].order_id, "stale-1")
        self.assertEqual(decisions[0].action, "cancel")

    def test_manager_cancels_only_stale_orders(self):
        stale = make_order("stale-1", age_seconds=60)
        fresh = make_order("fresh-1", age_seconds=5)
        adapter = LifecycleAdapter([stale, fresh])
        manager = OrderLifecycleManager(
            adapter=adapter,
            policy=OrderLifecyclePolicy(max_order_age_seconds=30),
        )

        decisions = manager.cancel_stale_orders(now=datetime.now(timezone.utc))

        self.assertEqual(len(decisions), 1)
        self.assertEqual(adapter.cancelled, ["stale-1"])

    def test_summarize_fill_state_marks_partial_and_filled_orders(self):
        open_order = make_order("order-1", age_seconds=10)
        fill_partial = FillSnapshot(
            order_id="order-1",
            contract=open_order.contract,
            action=OrderAction.BUY,
            price=0.5,
            quantity=0.4,
        )
        fill_full = FillSnapshot(
            order_id="order-2",
            contract=open_order.contract,
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )

        summaries = summarize_fill_state([open_order], [fill_partial, fill_full])

        summary_by_id = {summary.order_id: summary for summary in summaries}
        self.assertEqual(summary_by_id["order-1"].status, "partial")
        self.assertEqual(summary_by_id["order-2"].status, "filled")


if __name__ == "__main__":
    unittest.main()

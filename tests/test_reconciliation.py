from __future__ import annotations

import unittest

from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderAction,
    OrderStatus,
    OutcomeSide,
    PositionSnapshot,
    Venue,
)
from engine.accounting import AccountStateCache
from engine.order_state import OrderState
from engine.reconciliation import ReconciliationEngine


class FakeAdapter:
    venue = Venue.POLYMARKET

    def __init__(
        self,
        observed_orders: list[NormalizedOrder],
        observed_positions: list[PositionSnapshot] | None = None,
        observed_fills: list[FillSnapshot] | None = None,
        balance: BalanceSnapshot | None = None,
    ):
        self._observed_orders = observed_orders
        self._observed_positions = observed_positions or []
        self._observed_fills = observed_fills or []
        self._balance = balance or BalanceSnapshot(
            venue=self.venue, available=100.0, total=100.0
        )

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return []

    def get_order_book(self, contract):
        raise NotImplementedError

    def list_open_orders(self, contract=None):
        return list(self._observed_orders)

    def list_positions(self, contract=None):
        return list(self._observed_positions)

    def list_fills(self, contract=None):
        return list(self._observed_fills)

    def get_position(self, contract):
        for position in self._observed_positions:
            if position.contract.market_key == contract.market_key:
                return position
        return PositionSnapshot(contract=contract, quantity=0.0)

    def get_balance(self):
        return self._balance

    def get_account_snapshot(self, contract=None):
        return AccountSnapshot(
            venue=self.venue,
            balance=self._balance,
            positions=list(self._observed_positions),
            open_orders=list(self._observed_orders),
            fills=list(self._observed_fills),
        )

    def place_limit_order(self, intent):
        raise NotImplementedError

    def cancel_order(self, order_id: str):
        raise NotImplementedError

    def cancel_all(self, contract=None):
        raise NotImplementedError

    def close(self):
        return None


def make_order(
    order_id: str, price: float = 0.5, remaining_quantity: float = 1.0
) -> NormalizedOrder:
    contract = Contract(
        venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
    )
    return NormalizedOrder(
        order_id=order_id,
        contract=contract,
        action=OrderAction.BUY,
        price=price,
        quantity=remaining_quantity,
        remaining_quantity=remaining_quantity,
        status=OrderStatus.RESTING,
    )


class ReconciliationTests(unittest.TestCase):
    def test_detects_missing_and_unexpected_orders(self):
        order_state = OrderState()
        account_state = AccountStateCache()
        local_order = make_order("local-1")
        order_state.sync([local_order])
        adapter = FakeAdapter(observed_orders=[make_order("venue-2")])

        report = ReconciliationEngine(adapter, order_state, account_state).reconcile(
            local_order.contract
        )

        self.assertFalse(report.healthy)
        self.assertEqual(report.missing_on_venue, ["local-1"])
        self.assertEqual(report.unexpected_on_venue, ["venue-2"])

    def test_detects_diverged_order(self):
        order_state = OrderState()
        account_state = AccountStateCache()
        local_order = make_order("same-id", price=0.5, remaining_quantity=2)
        order_state.sync([local_order])
        adapter = FakeAdapter(
            observed_orders=[make_order("same-id", price=0.55, remaining_quantity=2)]
        )

        report = ReconciliationEngine(adapter, order_state, account_state).reconcile(
            local_order.contract
        )

        self.assertIn("same-id", report.diverged_orders)

    def test_detects_balance_position_and_fill_drift(self):
        contract = make_order("seed").contract
        local_fill = FillSnapshot(
            order_id="local-fill",
            contract=contract,
            action=OrderAction.BUY,
            price=0.4,
            quantity=1.0,
        )
        order_state = OrderState()
        account_state = AccountStateCache(
            balance=BalanceSnapshot(
                venue=Venue.POLYMARKET, available=100.0, total=100.0
            ),
            positions={
                contract.market_key: PositionSnapshot(contract=contract, quantity=1.0)
            },
            fills={local_fill.fill_key: local_fill},
        )
        observed_fill = FillSnapshot(
            order_id="venue-fill",
            contract=contract,
            action=OrderAction.BUY,
            price=0.41,
            quantity=2.0,
        )
        adapter = FakeAdapter(
            observed_orders=[],
            observed_positions=[PositionSnapshot(contract=contract, quantity=2.0)],
            observed_fills=[observed_fill],
            balance=BalanceSnapshot(venue=Venue.POLYMARKET, available=90.0, total=90.0),
        )

        report = ReconciliationEngine(adapter, order_state, account_state).reconcile(
            contract
        )

        self.assertAlmostEqual(report.position_drift, 1.0)
        self.assertAlmostEqual(report.balance_drift, -10.0)
        self.assertTrue(report.missing_fills_on_venue)
        self.assertTrue(report.unexpected_fills_on_venue)

    def test_account_state_keeps_distinct_fill_ids(self):
        contract = make_order("seed").contract
        first = FillSnapshot(
            order_id="order-1",
            contract=contract,
            action=OrderAction.BUY,
            price=0.4,
            quantity=1.0,
            fill_id="fill-1",
        )
        second = FillSnapshot(
            order_id="order-1",
            contract=contract,
            action=OrderAction.BUY,
            price=0.4,
            quantity=1.0,
            fill_id="fill-2",
        )
        account_state = AccountStateCache()

        account_state.sync_snapshot(
            AccountSnapshot(
                venue=Venue.POLYMARKET,
                balance=BalanceSnapshot(
                    venue=Venue.POLYMARKET, available=100.0, total=100.0
                ),
                positions=[PositionSnapshot(contract=contract, quantity=0.0)],
                open_orders=[],
                fills=[first, second],
            )
        )

        self.assertEqual(len(account_state.fills), 2)
        self.assertCountEqual(account_state.fills.keys(), ["fill-1", "fill-2"])

    def test_reconcile_treats_missing_pending_cancel_as_acknowledged(self):
        order_state = OrderState()
        account_state = AccountStateCache()
        local_order = make_order("cancel-1")
        order_state.sync([local_order])
        adapter = FakeAdapter(observed_orders=[])

        report = ReconciliationEngine(adapter, order_state, account_state).reconcile(
            local_order.contract,
            pending_cancel_order_ids={"cancel-1"},
        )

        self.assertEqual(report.cancel_acknowledged, ["cancel-1"])
        self.assertFalse(report.missing_on_venue)

    def test_reconcile_treats_post_cancel_fill_as_cancel_race(self):
        contract = make_order("seed").contract
        order_state = OrderState()
        account_state = AccountStateCache()
        local_order = make_order("cancel-1")
        order_state.sync([local_order])
        observed_fill = FillSnapshot(
            order_id="cancel-1",
            contract=contract,
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
            fill_id="fill-race-1",
        )
        adapter = FakeAdapter(
            observed_orders=[],
            observed_positions=[PositionSnapshot(contract=contract, quantity=1.0)],
            observed_fills=[observed_fill],
            balance=BalanceSnapshot(venue=Venue.POLYMARKET, available=99.5, total=99.5),
        )

        report = ReconciliationEngine(adapter, order_state, account_state).reconcile(
            contract,
            pending_cancel_order_ids={"cancel-1"},
        )

        self.assertEqual(report.cancel_acknowledged, ["cancel-1"])
        self.assertEqual(report.cancel_race_fills, ["fill-race-1"])
        self.assertFalse(report.unexpected_fills_on_venue)
        self.assertAlmostEqual(report.position_drift, 0.0)
        self.assertAlmostEqual(report.balance_drift, 0.0)


if __name__ == "__main__":
    unittest.main()

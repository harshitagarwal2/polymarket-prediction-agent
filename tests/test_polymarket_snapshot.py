from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import (
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    OrderAction,
    OrderStatus,
    OutcomeSide,
    PositionSnapshot,
    Venue,
)


class StubPolymarketAdapter(PolymarketAdapter):
    def __init__(self):
        super().__init__(
            PolymarketConfig(
                private_key="pk",
                account_address="0x1234567890123456789012345678901234567890",
            )
        )
        self.contract = Contract(
            venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
        )
        self.raise_balance = False
        self.raise_positions = False
        self.raise_fills = False
        self.raise_open_orders = False
        self.snapshot_order_calls = 0
        self.snapshot_fill_calls = 0
        self.rest_orders = [
            {
                "id": "rest-order-1",
                "asset_id": self.contract.symbol,
                "side": "BUY",
                "price": 0.44,
                "original_size": 2.0,
                "size_matched": 0.0,
            }
        ]

    def get_balance(self) -> BalanceSnapshot:
        if self.raise_balance:
            raise RuntimeError("balance unavailable")
        return BalanceSnapshot(
            venue=Venue.POLYMARKET, available=12.0, total=12.0, currency="USDC"
        )

    def _list_open_orders_rest(self, contract: Contract | None = None):
        self.snapshot_order_calls += 1
        if self.raise_open_orders:
            raise RuntimeError("open orders unavailable")
        orders = [dict(order) for order in self.rest_orders]
        if contract is None:
            return orders
        return [
            order for order in orders if str(order.get("asset_id")) == contract.symbol
        ]

    def list_positions(self, contract: Contract | None = None):
        if self.raise_positions:
            raise RuntimeError("positions unavailable")
        return [PositionSnapshot(contract=self.contract, quantity=2.0)]

    def _list_fills_rest_raw(self, contract: Contract | None = None):
        self.snapshot_fill_calls += 1
        if self.raise_fills:
            raise RuntimeError("fills unavailable")
        return [
            {
                "order_id": "fill-1",
                "asset_id": self.contract.symbol,
                "outcome": "yes",
                "side": "BUY",
                "price": 0.45,
                "size": 2.0,
                "status": "TRADE_STATUS_CONFIRMED",
            }
        ]

    def _snapshot_fills(self, contract: Contract | None = None):
        return super()._snapshot_fills(contract)


class PolymarketSnapshotTests(unittest.TestCase):
    def test_snapshot_complete_when_truth_sources_succeed(self):
        adapter = StubPolymarketAdapter()

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertTrue(snapshot.complete)
        self.assertFalse(snapshot.issues)
        self.assertEqual(snapshot.balance.available, 12.0)
        self.assertEqual(snapshot.positions[0].quantity, 2.0)
        self.assertEqual(snapshot.open_orders[0].order_id, "rest-order-1")
        self.assertEqual(snapshot.fills[0].order_id, "fill-1")
        self.assertEqual(adapter.snapshot_order_calls, 1)
        self.assertEqual(adapter.snapshot_fill_calls, 1)

    def test_snapshot_incomplete_when_fill_recovery_fails(self):
        adapter = StubPolymarketAdapter()
        adapter.raise_fills = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertFalse(snapshot.complete)
        self.assertTrue(snapshot.issues)
        self.assertIn(
            "Polymarket fill truth could not be recovered", snapshot.issues[0]
        )

    def test_snapshot_keeps_rest_fill_backstop_when_live_fill_cache_is_fresh(self):
        adapter = StubPolymarketAdapter()
        adapter._set_live_fills_cache(
            [
                {
                    "trade_id": "live-fill-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.61,
                    "size": 3.0,
                    "status": "TRADE_STATUS_CONFIRMED",
                }
            ]
        )
        adapter._live_state_active = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertEqual(adapter.snapshot_fill_calls, 1)
        self.assertEqual(
            [fill.fill_id for fill in snapshot.fills], ["live-fill-1", None]
        )
        status = adapter.live_state_status()
        self.assertEqual(status.snapshot_fill_overlay_count, 1)
        self.assertEqual(status.snapshot_fill_overlay_source, "rest_plus_live_overlay")

    def test_snapshot_open_order_overlay_adds_fresh_live_order_before_rest(self):
        adapter = StubPolymarketAdapter()
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.61,
                    "original_size": 3.0,
                    "size_matched": 0.0,
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertEqual(
            [order.order_id for order in snapshot.open_orders],
            ["live-order-1", "rest-order-1"],
        )
        status = adapter.live_state_status()
        self.assertEqual(status.snapshot_open_order_overlay_count, 1)
        self.assertEqual(
            status.snapshot_open_order_overlay_source, "rest_plus_live_overlay"
        )
        self.assertIsNone(status.snapshot_open_order_overlay_reason)

    def test_snapshot_open_order_overlay_refreshes_duplicate_without_double_counting(
        self,
    ):
        adapter = StubPolymarketAdapter()
        adapter.rest_orders = [
            {
                "id": "rest-order-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.44,
                "original_size": 3.0,
                "size_matched": 0.0,
            }
        ]
        adapter._set_live_orders_cache(
            [
                {
                    "id": "rest-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.46,
                    "original_size": 3.0,
                    "size_matched": 1.0,
                    "status": "partially_filled",
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertEqual(len(snapshot.open_orders), 1)
        order = snapshot.open_orders[0]
        self.assertEqual(order.order_id, "rest-order-1")
        self.assertEqual(order.price, 0.46)
        self.assertEqual(order.remaining_quantity, 2.0)
        self.assertEqual(order.status, OrderStatus.PARTIALLY_FILLED)
        status = adapter.live_state_status()
        self.assertEqual(status.snapshot_open_order_overlay_count, 1)
        self.assertEqual(
            status.snapshot_open_order_overlay_source, "rest_plus_live_overlay"
        )

    def test_snapshot_open_order_overlay_falls_back_to_rest_when_live_cache_stale(self):
        adapter = StubPolymarketAdapter()
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.61,
                    "original_size": 3.0,
                    "size_matched": 0.0,
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        adapter._live_state_active = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertEqual(
            [order.order_id for order in snapshot.open_orders], ["rest-order-1"]
        )
        status = adapter.live_state_status()
        self.assertEqual(status.snapshot_open_order_overlay_count, 0)
        self.assertEqual(status.snapshot_open_order_overlay_source, "rest_only")
        self.assertEqual(status.snapshot_open_order_overlay_reason, "order_cache_stale")

    def test_snapshot_open_order_overlay_visibility_reports_duplicate_skip_reason(self):
        adapter = StubPolymarketAdapter()
        adapter._set_live_orders_cache(
            [
                {
                    "id": "rest-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.44,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertEqual(
            [order.order_id for order in snapshot.open_orders], ["rest-order-1"]
        )
        status = adapter.live_state_status()
        self.assertEqual(status.snapshot_open_order_overlay_count, 0)
        self.assertEqual(status.snapshot_open_order_overlay_source, "rest_only")
        self.assertEqual(
            status.snapshot_open_order_overlay_reason, "live_order_overlay_duplicate"
        )

    def test_snapshot_overlay_skips_duplicate_live_fill(self):
        adapter = StubPolymarketAdapter()
        adapter._set_live_fills_cache(
            [
                {
                    "id": "fill-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.45,
                    "size": 2.0,
                    "status": "TRADE_STATUS_CONFIRMED",
                }
            ]
        )
        adapter._live_state_active = True

        snapshot = adapter.get_account_snapshot(adapter.contract)

        self.assertEqual(adapter.snapshot_fill_calls, 1)
        self.assertEqual(len(snapshot.fills), 1)
        status = adapter.live_state_status()
        self.assertEqual(status.snapshot_fill_overlay_count, 0)
        self.assertEqual(status.snapshot_fill_overlay_source, "rest_only")
        self.assertEqual(
            status.snapshot_fill_overlay_reason, "live_fill_overlay_duplicate"
        )


if __name__ == "__main__":
    unittest.main()

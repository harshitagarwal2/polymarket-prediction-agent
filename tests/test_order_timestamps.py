from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from adapters.kalshi import KalshiAdapter, KalshiConfig
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import Contract, OutcomeSide, Venue
from engine.order_state import OrderLifecycleManager, OrderLifecyclePolicy


class FakePolymarketClient:
    def __init__(self, orders: list[dict[str, object]]):
        self.orders = orders

    def get_orders(self, _params):
        return list(self.orders)


class StubPolymarketAdapter(PolymarketAdapter):
    def __init__(self, orders: list[dict[str, object]]):
        super().__init__(PolymarketConfig(private_key="pk"))
        self._stub_client = FakePolymarketClient(orders)
        self.cancelled: list[str] = []

    def _ensure_client(self):
        return self._stub_client

    def cancel_order(self, order_id: str) -> bool:
        self.cancelled.append(order_id)
        return True


class FakeKalshiClient:
    def __init__(self, order: object):
        self.order = order
        self.portfolio = self

    def get_orders(self, **_kwargs):
        return [self.order]


class StubKalshiAdapter(KalshiAdapter):
    def __init__(self, order: object):
        super().__init__(KalshiConfig())
        self._stub_client = FakeKalshiClient(order)

    def _ensure_client(self):
        return self._stub_client


class OrderTimestampTests(unittest.TestCase):
    def _import_polymarket_module(self, name: str):
        if name == "py_clob_client.clob_types":
            return SimpleNamespace(OpenOrderParams=lambda: object())
        raise ImportError(name)

    def _import_kalshi_module(self, name: str):
        if name == "pykalshi":
            return SimpleNamespace(OrderStatus=SimpleNamespace(RESTING="resting"))
        raise ImportError(name)

    def test_polymarket_open_order_keeps_first_seen_created_at(self):
        order = {
            "id": "poly-1",
            "asset_id": "token-1",
            "side": "BUY",
            "price": 0.45,
            "original_size": 1.0,
            "size_matched": 0.0,
        }
        adapter = StubPolymarketAdapter([order])

        with patch(
            "adapters.polymarket.importlib.import_module",
            self._import_polymarket_module,
        ):
            first = adapter.list_open_orders()[0]
            second = adapter.list_open_orders()[0]

        self.assertEqual(first.order_id, "poly-1")
        self.assertEqual(second.created_at, first.created_at)
        self.assertGreaterEqual(second.updated_at, first.updated_at)

    def test_polymarket_stale_cancel_uses_first_seen_age(self):
        order = {
            "id": "poly-1",
            "asset_id": "token-1",
            "side": "BUY",
            "price": 0.45,
            "original_size": 1.0,
            "size_matched": 0.0,
        }
        adapter = StubPolymarketAdapter([order])
        with patch(
            "adapters.polymarket.importlib.import_module",
            self._import_polymarket_module,
        ):
            adapter.list_open_orders()
            adapter._open_order_first_seen_at["poly-1"] = datetime.now(
                timezone.utc
            ) - timedelta(seconds=120)
            manager = OrderLifecycleManager(
                adapter=adapter,
                policy=OrderLifecyclePolicy(max_order_age_seconds=30),
            )

            decisions = manager.cancel_stale_orders(now=datetime.now(timezone.utc))

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].order_id, "poly-1")
        self.assertEqual(adapter.cancelled, ["poly-1"])

    def test_kalshi_open_order_uses_venue_created_and_updated_time(self):
        order = SimpleNamespace(
            order_id="kalshi-1",
            ticker="KXTEST",
            side=SimpleNamespace(value="yes"),
            action=SimpleNamespace(value="buy"),
            yes_price_dollars="0.44",
            remaining_count="2",
            count="2",
            created_time="2024-01-01T12:00:00Z",
            last_update_time="2024-01-01T12:00:05Z",
            post_only=True,
            reduce_only=False,
            expiration_ts=None,
            client_order_id="cid-1",
        )
        adapter = StubKalshiAdapter(order)
        contract = Contract(
            venue=Venue.KALSHI, symbol="KXTEST", outcome=OutcomeSide.YES
        )

        with patch(
            "adapters.kalshi.importlib.import_module", self._import_kalshi_module
        ):
            normalized = adapter.list_open_orders(contract)[0]

        self.assertEqual(
            normalized.created_at,
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            normalized.updated_at,
            datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()

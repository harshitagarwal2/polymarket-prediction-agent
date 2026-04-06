from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import (
    Contract,
    OrderAction,
    OrderIntent,
    OrderStatus,
    OutcomeSide,
    Venue,
)


class FakeOrderType:
    GTC = "GTC"
    GTD = "GTD"


class FakeOrderArgs:
    def __init__(self, **kwargs):
        self.token_id = kwargs["token_id"]
        self.price = kwargs["price"]
        self.size = kwargs["size"]
        self.side = kwargs["side"]
        self.expiration = kwargs.get("expiration", 0)


class FakeClient:
    def __init__(self, post_order_effects: list[object] | None = None):
        self.created_orders: list[FakeOrderArgs] = []
        self.posted_orders: list[dict[str, object]] = []
        self.post_order_effects = list(
            post_order_effects or [{"orderID": "poly-1", "status": "live"}]
        )

    def create_order(self, order: FakeOrderArgs):
        self.created_orders.append(order)
        return {"signed_order": order}

    def post_order(self, order, orderType=FakeOrderType.GTC, post_only: bool = False):
        self.posted_orders.append(
            {"order": order, "orderType": orderType, "post_only": post_only}
        )
        effect = self.post_order_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


class FakePolyApiException(Exception):
    def __init__(self, status_code: int | None):
        self.status_code = status_code
        super().__init__(f"status={status_code}")


class StubPlacementPolymarketAdapter(PolymarketAdapter):
    def __init__(self, client: FakeClient):
        super().__init__(PolymarketConfig(private_key="pk"))
        self._stub_client = client

    def _ensure_client(self):
        return self._stub_client


class PolymarketOrderPlacementTests(unittest.TestCase):
    def setUp(self):
        self.contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-1",
            outcome=OutcomeSide.YES,
        )

    def _import_module(self, name: str):
        if name == "py_clob_client.clob_types":
            return SimpleNamespace(OrderArgs=FakeOrderArgs, OrderType=FakeOrderType)
        if name == "py_clob_client.order_builder.constants":
            return SimpleNamespace(BUY="BUY", SELL="SELL")
        raise ImportError(name)

    def test_place_limit_order_maps_post_only_and_gtd_expiration(self):
        client = FakeClient()
        adapter = StubPlacementPolymarketAdapter(client)
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=2.0,
            post_only=True,
            expiration_ts=1_700_000_000,
        )

        with patch("adapters.polymarket.importlib.import_module", self._import_module):
            result = adapter.place_limit_order(intent)

        self.assertTrue(result.accepted)
        self.assertEqual(result.status, OrderStatus.RESTING)
        self.assertEqual(client.created_orders[0].token_id, self.contract.symbol)
        self.assertEqual(client.created_orders[0].expiration, 1_700_000_000)
        self.assertEqual(client.posted_orders[0]["orderType"], FakeOrderType.GTD)
        self.assertTrue(client.posted_orders[0]["post_only"])

    def test_place_limit_order_rejects_unsupported_reduce_only(self):
        client = FakeClient()
        adapter = StubPlacementPolymarketAdapter(client)
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.SELL,
            price=0.55,
            quantity=1.0,
            reduce_only=True,
        )

        result = adapter.place_limit_order(intent)

        self.assertFalse(result.accepted)
        self.assertEqual(result.status, OrderStatus.REJECTED)
        self.assertIn("reduce_only", result.message or "")
        self.assertFalse(client.created_orders)
        self.assertFalse(client.posted_orders)

    def test_place_limit_order_retries_transient_post_order_failure(self):
        client = FakeClient(
            post_order_effects=[
                FakePolyApiException(429),
                {"orderID": "poly-1", "status": "live"},
            ]
        )
        adapter = StubPlacementPolymarketAdapter(client)
        adapter.config.retry_backoff_seconds = 0.01
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=2.0,
        )

        with (
            patch("adapters.polymarket.importlib.import_module", self._import_module),
            patch("adapters.polymarket.time.sleep") as sleep_mock,
        ):
            result = adapter.place_limit_order(intent)

        self.assertTrue(result.accepted)
        self.assertEqual(result.order_id, "poly-1")
        self.assertEqual(len(client.posted_orders), 2)
        sleep_mock.assert_called_once_with(0.01)

    def test_place_limit_order_does_not_retry_non_retryable_failure(self):
        client = FakeClient(post_order_effects=[FakePolyApiException(401)])
        adapter = StubPlacementPolymarketAdapter(client)
        adapter.config.retry_backoff_seconds = 0.01
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=2.0,
        )

        with (
            patch("adapters.polymarket.importlib.import_module", self._import_module),
            patch("adapters.polymarket.time.sleep") as sleep_mock,
            self.assertRaises(FakePolyApiException),
        ):
            adapter.place_limit_order(intent)

        self.assertEqual(len(client.posted_orders), 1)
        sleep_mock.assert_not_called()

    def test_admit_limit_order_denies_non_tradable_market(self):
        adapter = StubPlacementPolymarketAdapter(FakeClient())
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=1.0,
        )
        with adapter._market_state_lock:
            adapter._market_state_active = True
            adapter._market_state_mode = "healthy"
            adapter._market_state_initialized = True
            adapter._market_state_last_update_at = datetime.now(timezone.utc)
            adapter._market_state_tracked_assets.add(self.contract.symbol)
            adapter._market_state_books[self.contract.symbol] = {
                "tradable": False,
                "active": False,
                "bids": {0.44: 1.0},
                "asks": {0.46: 1.0},
                "last_update_at": datetime.now(timezone.utc),
            }

        decision = adapter.admit_limit_order(intent)

        self.assertEqual(decision.action, "deny")
        self.assertIn("non-tradable", decision.reason or "")

    def test_admit_limit_order_requests_refresh_when_market_state_recovering(self):
        adapter = StubPlacementPolymarketAdapter(FakeClient())
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=1.0,
        )
        with adapter._market_state_lock:
            adapter._market_state_active = True
            adapter._market_state_mode = "recovering"
            adapter._market_state_initialized = True
            adapter._market_state_last_update_at = datetime.now(timezone.utc)

        decision = adapter.admit_limit_order(intent)

        self.assertEqual(decision.action, "refresh_then_retry")
        self.assertIn("market_state_recovering", decision.reason or "")

    def test_admit_limit_order_denies_crossing_post_only_buy(self):
        adapter = StubPlacementPolymarketAdapter(FakeClient())
        intent = OrderIntent(
            contract=self.contract,
            action=OrderAction.BUY,
            price=0.50,
            quantity=1.0,
            post_only=True,
        )
        with adapter._market_state_lock:
            adapter._market_state_active = True
            adapter._market_state_mode = "healthy"
            adapter._market_state_initialized = True
            adapter._market_state_last_update_at = datetime.now(timezone.utc)
            adapter._market_state_tracked_assets.add(self.contract.symbol)
            adapter._market_state_books[self.contract.symbol] = {
                "tradable": True,
                "active": True,
                "bids": {0.44: 1.0},
                "asks": {0.49: 1.0},
                "last_update_at": datetime.now(timezone.utc),
            }

        decision = adapter.admit_limit_order(intent)

        self.assertEqual(decision.action, "deny")
        self.assertIn("post-only buy would cross", decision.reason or "")


if __name__ == "__main__":
    unittest.main()

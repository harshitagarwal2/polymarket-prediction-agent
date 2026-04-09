from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
import threading
from types import SimpleNamespace
from unittest.mock import patch

from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import Contract, OutcomeSide, Venue


class StubLiveStatePolymarketAdapter(PolymarketAdapter):
    def __init__(self):
        super().__init__(
            PolymarketConfig(
                private_key="pk",
                live_user_markets=["condition-1"],
                live_state_freshness_seconds=30.0,
            )
        )
        self.contract = Contract(
            venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
        )
        self.rest_orders: list[dict[str, object]] = []
        self.rest_trades: list[dict[str, object]] = []
        self.market_payload: list[dict[str, object]] = []
        self.rest_book = SimpleNamespace(
            bids=[{"price": 0.45, "size": 10.0}],
            asks=[{"price": 0.50, "size": 12.0}],
        )

    def _list_open_orders_rest(self, contract: Contract | None = None):
        return [dict(order) for order in self.rest_orders]

    def _list_fills_rest_raw(self, contract: Contract | None = None):
        trades = [dict(trade) for trade in self.rest_trades]
        if contract is None:
            return trades
        return [
            trade
            for trade in trades
            if str(trade.get("asset_id") or trade.get("token_id") or "")
            == contract.symbol
        ]

    def _call_client(self, operation: str, method_name: str, *args, **kwargs):
        if method_name == "get_simplified_markets":
            return {"data": self.market_payload}
        if method_name == "get_order_book":
            return self.rest_book
        if method_name == "get_midpoint":
            return {"mid": 0.475}
        raise AssertionError(f"unexpected client call: {method_name}")


class PolymarketLiveStateTests(unittest.TestCase):
    def test_list_open_orders_uses_fresh_live_cache_when_active(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_orders = [
            {
                "id": "rest-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.41,
                "original_size": 1.0,
                "size_matched": 0.0,
            }
        ]
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True

        orders = adapter.list_open_orders(adapter.contract)

        self.assertEqual([order.order_id for order in orders], ["live-1"])
        self.assertEqual(orders[0].price, 0.52)

    def test_list_open_orders_falls_back_to_rest_when_cache_stale(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_orders = [
            {
                "id": "rest-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.41,
                "original_size": 1.0,
                "size_matched": 0.0,
            }
        ]
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        adapter._live_state_active = True

        orders = adapter.list_open_orders(adapter.contract)

        self.assertEqual([order.order_id for order in orders], ["rest-1"])
        self.assertEqual(orders[0].price, 0.41)

    def test_list_fills_uses_fresh_live_cache_when_active(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_trades = [
            {
                "id": "rest-fill-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.41,
                "size": 1.0,
                "status": "TRADE_STATUS_CONFIRMED",
            }
        ]
        adapter._set_live_fills_cache(
            [
                {
                    "trade_id": "live-fill-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "size": 2.0,
                    "status": "TRADE_STATUS_CONFIRMED",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True

        fills = adapter.list_fills(adapter.contract)
        status = adapter.live_state_status()

        self.assertEqual([fill.fill_id for fill in fills], ["live-fill-1"])
        self.assertEqual(fills[0].price, 0.52)
        self.assertTrue(status.fills_fresh)
        self.assertEqual(status.cached_fill_count, 1)
        self.assertEqual(status.last_fills_source, "live_cache")
        self.assertIsNone(status.last_fills_fallback_reason)

    def test_list_fills_falls_back_to_rest_when_fill_cache_cold(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_trades = [
            {
                "id": "rest-fill-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.41,
                "size": 1.0,
                "status": "TRADE_STATUS_CONFIRMED",
            }
        ]
        adapter._live_state_active = True

        fills = adapter.list_fills(adapter.contract)
        status = adapter.live_state_status()

        self.assertEqual([fill.fill_id for fill in fills], ["rest-fill-1"])
        self.assertEqual(status.last_fills_source, "rest")
        self.assertEqual(status.last_fills_fallback_reason, "fill_cache_cold")

    def test_list_fills_falls_back_to_rest_when_fill_cache_stale(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_trades = [
            {
                "id": "rest-fill-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.41,
                "size": 1.0,
                "status": "TRADE_STATUS_CONFIRMED",
            }
        ]
        adapter._set_live_fills_cache(
            [
                {
                    "trade_id": "live-fill-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "size": 2.0,
                    "status": "TRADE_STATUS_CONFIRMED",
                }
            ],
            observed_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        adapter._live_state_active = True

        fills = adapter.list_fills(adapter.contract)
        status = adapter.live_state_status()

        self.assertEqual([fill.fill_id for fill in fills], ["rest-fill-1"])
        self.assertEqual(status.last_fills_source, "rest")
        self.assertEqual(status.last_fills_fallback_reason, "fill_cache_stale")

    def test_live_fill_merge_deduplicates_by_stable_id(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter._set_live_fills_cache(
            [
                {
                    "trade_id": "fill-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.45,
                    "size": 1.0,
                    "status": "TRADE_STATUS_CONFIRMED",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True

        adapter._apply_live_state_message(
            {
                "event_type": "trade",
                "trade": {
                    "trade_id": "fill-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.45,
                    "size": 1.0,
                    "status": "TRADE_STATUS_CONFIRMED",
                },
            }
        )
        adapter._apply_live_state_message(
            {
                "event_type": "trade",
                "payload": {
                    "trade": {
                        "match_id": "fill-2",
                        "asset_id": "token-1",
                        "side": "SELL",
                        "price": 0.57,
                        "size": 2.0,
                        "status": "TRADE_STATUS_CONFIRMED",
                    }
                },
            }
        )

        fills = adapter.list_fills(adapter.contract)
        status = adapter.live_state_status()

        self.assertEqual([fill.fill_id for fill in fills], ["fill-2", "fill-1"])
        self.assertEqual(status.cached_fill_count, 2)

    def test_market_discovery_populates_condition_id_mapping_for_live_state(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.market_payload = [
            {
                "question": "Test market",
                "condition_id": "condition-1",
                "tokens": [
                    {
                        "token_id": "token-1",
                        "outcome": "yes",
                        "best_bid": 0.45,
                        "best_ask": 0.5,
                    }
                ],
            }
        ]

        adapter.list_markets(limit=10)

        self.assertEqual(adapter._live_state_subscription_markets(), ("condition-1",))

    def test_start_live_user_state_restarts_when_subscription_markets_expand(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter._live_state_active = True
        adapter._live_state_running = True
        adapter._live_state_markets = ("condition-1",)

        class AliveThread(threading.Thread):
            def is_alive(self):
                return True

            def join(self, timeout=None):
                return None

        class NewThread:
            def __init__(self, *args, **kwargs):
                self.started = False

            def is_alive(self):
                return self.started

            def start(self):
                self.started = True

        adapter._live_state_thread = AliveThread()
        adapter._condition_id_by_token["token-2"] = "condition-2"

        def fake_stop():
            adapter._live_state_thread = None
            adapter._live_state_active = False
            adapter._live_state_running = False
            return adapter.live_state_status()

        def fake_bootstrap():
            with adapter._live_state_lock:
                adapter._live_state_initialized = True
                adapter._live_state_last_update_at = datetime.now(timezone.utc)
                adapter._live_state_markets = adapter._live_state_subscription_markets()
                return adapter._live_state_markets

        with (
            patch.object(
                adapter, "stop_live_user_state", side_effect=fake_stop
            ) as stop,
            patch.object(adapter, "_live_state_bootstrap", side_effect=fake_bootstrap),
            patch("adapters.polymarket.threading.Thread", NewThread),
        ):
            status = adapter.start_live_user_state()

        self.assertTrue(stop.called)
        self.assertEqual(status.subscribed_markets, ("condition-1", "condition-2"))

    def test_terminal_marker_prevents_older_live_order_resurrection(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "updated_at": "2026-01-01T00:00:02+00:00",
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
        )
        adapter._live_state_active = True
        adapter._apply_live_state_message(
            {
                "event_type": "order",
                "order": {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "status": "cancelled",
                    "updated_at": "2026-01-01T00:00:03+00:00",
                },
            }
        )
        adapter._apply_live_state_message(
            {
                "event_type": "order",
                "order": {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.53,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "updated_at": "2026-01-01T00:00:01+00:00",
                },
            }
        )

        delta = adapter.live_user_state_delta(adapter.contract)

        self.assertIsNotNone(delta)
        if delta is None:
            self.fail("expected live delta")
        self.assertFalse(delta.open_orders)
        self.assertEqual(delta.terminal_order_ids, ("live-order-1",))

    def test_exchange_prefixed_terminal_status_marks_live_order_terminal(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "updated_at": "2026-01-01T00:00:02+00:00",
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
        )
        adapter._live_state_active = True

        adapter._apply_live_state_message(
            {
                "event_type": "order",
                "order": {
                    "id": "live-order-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "status": "ORDER_STATUS_CANCELED_MARKET_RESOLVED",
                    "updated_at": "2026-01-01T00:00:03+00:00",
                },
            }
        )

        delta = adapter.live_user_state_delta(adapter.contract)

        self.assertIsNotNone(delta)
        if delta is None:
            self.fail("expected live delta")
        self.assertFalse(delta.open_orders)
        self.assertEqual(delta.terminal_order_ids, ("live-order-1",))

    def test_live_state_recovering_forces_rest_fallback(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_orders = [
            {
                "id": "rest-1",
                "asset_id": "token-1",
                "side": "BUY",
                "price": 0.41,
                "original_size": 1.0,
                "size_matched": 0.0,
            }
        ]
        adapter._set_live_orders_cache(
            [
                {
                    "id": "live-1",
                    "asset_id": "token-1",
                    "side": "BUY",
                    "price": 0.52,
                    "original_size": 2.0,
                    "size_matched": 0.0,
                    "condition_id": "condition-1",
                }
            ],
            observed_at=datetime.now(timezone.utc),
        )
        adapter._live_state_active = True
        adapter._mark_live_state_recovering_locked("reconnecting")

        orders = adapter.list_open_orders(adapter.contract)

        self.assertEqual([order.order_id for order in orders], ["rest-1"])

    def test_confirm_live_state_recovery_marks_mode_healthy(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter._live_state_active = True
        with adapter._live_state_lock:
            adapter._mark_live_state_recovering_locked("reconnecting")

        status = adapter.confirm_live_state_recovery(datetime.now(timezone.utc))

        self.assertEqual(status.mode, "healthy")
        self.assertIsNone(status.degraded_reason)

    def test_market_state_overlay_updates_bbo_when_healthy(self):
        adapter = StubLiveStatePolymarketAdapter()
        with adapter._market_state_lock:
            adapter._market_state_active = True
            adapter._market_state_mode = "healthy"
            adapter._market_state_initialized = True
            adapter._market_state_last_update_at = datetime.now(timezone.utc)
            adapter._market_state_assets = ("token-1",)
            adapter._market_state_tracked_assets.add("token-1")
            adapter._market_state_books["token-1"] = {
                "bids": {0.47: 4.0},
                "asks": {0.49: 5.0},
                "tradable": True,
                "active": True,
                "last_update_at": datetime.now(timezone.utc),
            }

        book = adapter.get_order_book(adapter.contract)
        status = adapter.market_state_status()

        self.assertEqual(book.best_bid, 0.47)
        self.assertEqual(book.best_ask, 0.49)
        self.assertTrue(status.snapshot_book_overlay_applied)
        self.assertEqual(status.snapshot_book_overlay_source, "rest_plus_live_market")

    def test_rest_order_book_caches_tick_and_min_order_size(self):
        adapter = StubLiveStatePolymarketAdapter()
        adapter.rest_book.tick_size = 0.01
        adapter.rest_book.min_order_size = 5.0

        adapter.get_order_book(adapter.contract)

        with adapter._market_state_lock:
            state = adapter._market_state_books[adapter.contract.symbol]

        self.assertEqual(state["tick_size"], 0.01)
        self.assertEqual(state["min_order_size"], 5.0)

    def test_market_state_price_changes_array_updates_book(self):
        adapter = StubLiveStatePolymarketAdapter()

        adapter._apply_market_state_message(
            {
                "event_type": "price_change",
                "price_changes": [
                    {
                        "asset_id": "token-1",
                        "side": "BUY",
                        "price": "0.48",
                        "size": "3.0",
                    },
                    {
                        "asset_id": "token-1",
                        "side": "SELL",
                        "price": "0.49",
                        "size": "4.0",
                    },
                ],
            }
        )

        with adapter._market_state_lock:
            state = adapter._market_state_books["token-1"]

        self.assertEqual(state["bids"][0.48], 3.0)
        self.assertEqual(state["asks"][0.49], 4.0)

    def test_market_state_tick_size_change_updates_cached_tick_size(self):
        adapter = StubLiveStatePolymarketAdapter()

        adapter._apply_market_state_message(
            {
                "event_type": "tick_size_change",
                "asset_id": "token-1",
                "new_tick_size": "0.01",
            }
        )

        with adapter._market_state_lock:
            state = adapter._market_state_books["token-1"]

        self.assertEqual(state["tick_size"], 0.01)

    def test_market_state_tick_size_change_reads_nested_payload(self):
        adapter = StubLiveStatePolymarketAdapter()

        adapter._apply_market_state_message(
            {
                "event_type": "tick_size_change",
                "asset_id": "token-1",
                "payload": {"new_tick_size": "0.001"},
            }
        )

        with adapter._market_state_lock:
            state = adapter._market_state_books["token-1"]

        self.assertEqual(state["tick_size"], 0.001)

    def test_market_state_overlay_blocks_trading_on_non_tradable_market(self):
        adapter = StubLiveStatePolymarketAdapter()
        with adapter._market_state_lock:
            adapter._market_state_active = True
            adapter._market_state_mode = "healthy"
            adapter._market_state_initialized = True
            adapter._market_state_last_update_at = datetime.now(timezone.utc)
            adapter._market_state_assets = ("token-1",)
            adapter._market_state_tracked_assets.add("token-1")
            adapter._market_state_books["token-1"] = {
                "bids": {0.47: 4.0},
                "asks": {0.49: 5.0},
                "tradable": False,
                "active": False,
                "last_update_at": datetime.now(timezone.utc),
            }

        book = adapter.get_order_book(adapter.contract)

        self.assertEqual(book.bids, [])
        self.assertEqual(book.asks, [])
        self.assertIsNone(book.midpoint)

    def test_market_state_recovering_forces_rest_order_book(self):
        adapter = StubLiveStatePolymarketAdapter()
        with adapter._market_state_lock:
            adapter._market_state_active = True
            adapter._market_state_mode = "recovering"
            adapter._market_state_initialized = True
            adapter._market_state_last_update_at = datetime.now(timezone.utc)
            adapter._market_state_assets = ("token-1",)
            adapter._market_state_tracked_assets.add("token-1")
            adapter._market_state_books["token-1"] = {
                "bids": {0.47: 4.0},
                "asks": {0.49: 5.0},
                "tradable": True,
                "active": True,
                "last_update_at": datetime.now(timezone.utc),
            }

        book = adapter.get_order_book(adapter.contract)
        status = adapter.market_state_status()

        self.assertEqual(book.best_bid, 0.45)
        self.assertEqual(book.best_ask, 0.50)
        self.assertFalse(status.snapshot_book_overlay_applied)
        self.assertEqual(status.snapshot_book_overlay_reason, "market_state_recovering")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import io
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

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
from services.capture import polymarket as polymarket_capture
from scripts import run_polymarket_capture
from services.capture.polymarket import (
    PolymarketCaptureStores,
    PolymarketMarketSnapshotRequest,
    hydrate_polymarket_market_snapshot,
    record_polymarket_capture_failure,
)
from services.capture.polymarket_worker import (
    PolymarketMarketCaptureWorker,
    PolymarketMarketCaptureWorkerConfig,
    PolymarketUserCaptureWorker,
    PolymarketUserCaptureWorkerConfig,
)
from storage.raw import RawStore


class _MemoryRepo:
    def __init__(self):
        self.rows: dict[str, dict[str, object]] = {}

    def upsert(self, key: str, row):
        payload = row.__dict__.copy() if hasattr(row, "__dict__") else dict(row)
        self.rows[str(key)] = payload
        return payload

    def read_all(self):
        return dict(self.rows)


class _StaticCatalogClient:
    def __init__(self, markets):
        self._markets = list(markets)

    def fetch_open_markets(self):
        return list(self._markets)


class _FakeWebSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.sent: list[str] = []
        self.closed = False

    def send(self, payload: str):
        self.sent.append(payload)

    def recv(self, timeout: float | None = None):
        if not self.messages:
            raise RuntimeError("socket closed")
        next_item = self.messages.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return json.dumps(next_item)

    def close(self):
        self.closed = True


class _FakeMarketAdapter:
    def __init__(self, sessions):
        self.sessions = list(sessions)
        self.books: dict[str, dict[str, object]] = {}

    def _connect_live_market_websocket(self):
        return self.sessions.pop(0)

    def _market_state_subscription_payload(self, assets):
        return {"type": "market", "assets_ids": list(assets)}

    def _live_state_recv(self, websocket):
        return websocket.recv()

    def _market_message_asset_ids(self, message):
        asset_id = message.get("asset_id")
        return [str(asset_id)] if asset_id not in (None, "") else []

    def _apply_market_state_message(self, payload):
        messages = payload if isinstance(payload, list) else [payload]
        for message in messages:
            if not isinstance(message, dict):
                continue
            asset_id = str(message.get("asset_id") or "")
            if not asset_id:
                continue
            bids = {}
            asks = {}
            if message.get("best_bid") not in (None, ""):
                bids[float(message["best_bid"])] = float(
                    message.get("best_bid_size") or 0.0
                )
            if message.get("best_ask") not in (None, ""):
                asks[float(message["best_ask"])] = float(
                    message.get("best_ask_size") or 0.0
                )
            self.books[asset_id] = {
                "bids": bids,
                "asks": asks,
                "last_update_at": datetime.fromisoformat(
                    str(
                        message.get("timestamp") or "2026-04-21T19:00:00+00:00"
                    ).replace("Z", "+00:00")
                ),
            }

    def _market_state_snapshot_for_asset(self, asset_id):
        return dict(
            self.books.get(asset_id, {"bids": {}, "asks": {}, "last_update_at": None})
        )


class _FakeUserAdapter:
    def __init__(self, sessions):
        self.sessions = list(sessions)

    def _connect_live_user_websocket(self):
        return self.sessions.pop(0)

    def _live_state_subscription_payload(self, markets):
        return {"type": "user", "markets": list(markets)}

    def _live_state_recv(self, websocket):
        return websocket.recv()

    def _iter_live_order_payloads(self, payload):
        return [item for item in payload.get("orders", []) if isinstance(item, dict)]

    def _iter_live_fill_payloads(self, payload):
        return [item for item in payload.get("fills", []) if isinstance(item, dict)]

    def get_account_snapshot(self, contract=None):
        contract = contract or Contract(
            venue=Venue.POLYMARKET, symbol="asset-1", outcome=OutcomeSide.YES
        )
        return AccountSnapshot(
            venue=Venue.POLYMARKET,
            balance=BalanceSnapshot(
                venue=Venue.POLYMARKET,
                available=100.0,
                total=100.0,
            ),
            positions=[PositionSnapshot(contract=contract, quantity=1.0)],
            open_orders=[
                NormalizedOrder(
                    order_id="order-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.45,
                    quantity=2.0,
                    remaining_quantity=2.0,
                    status=OrderStatus.RESTING,
                )
            ],
            fills=[
                FillSnapshot(
                    order_id="order-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.45,
                    quantity=0.5,
                    fill_id="fill-1",
                )
            ],
        )


class PolymarketCaptureWorkerTests(unittest.TestCase):
    def _stores(self, root: Path) -> PolymarketCaptureStores:
        return PolymarketCaptureStores(
            source_health=_MemoryRepo(),
            postgres_root=root / "postgres",
            raw=RawStore(root / "raw"),
        )

    def test_snapshot_hydration_persists_market_rows_checkpoint_and_health(self):
        raw_events: list[dict[str, object]] = []
        checkpoints: list[dict[str, object]] = []
        observed_at = datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = self._stores(root)
            client = _StaticCatalogClient(
                [
                    {
                        "id": "pm-1",
                        "conditionId": "condition-1",
                        "question": "Will Home Team beat Away Team?",
                        "sport": "nba",
                        "sports_market_type": "moneyline",
                        "active": True,
                        "tokenIds": ["yes-token", "no-token"],
                    }
                ]
            )
            with (
                patch.object(
                    polymarket_capture,
                    "append_raw_capture_event",
                    side_effect=lambda **kwargs: raw_events.append(kwargs) or kwargs,
                ),
                patch.object(
                    polymarket_capture,
                    "upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: checkpoints.append(
                        {
                            "checkpoint_name": args[0],
                            "source_name": args[1],
                            "checkpoint_value": args[2],
                        }
                    )
                    or checkpoints[-1],
                ),
            ):
                result = hydrate_polymarket_market_snapshot(
                    PolymarketMarketSnapshotRequest(root=str(root), sport="nba"),
                    client=client,
                    stores=stores,
                    observed_at=observed_at,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["market_count"], 1)
        self.assertEqual(result["rows"][0]["token_id_yes"], "yes-token")
        self.assertEqual(raw_events[0]["entity_type"], "market_catalog_snapshot")
        self.assertEqual(checkpoints[0]["checkpoint_name"], "market_catalog_snapshot")
        self.assertEqual(
            stores.source_health.read_all()["polymarket_market_catalog"]["status"],
            "ok",
        )

    def test_market_worker_rehydrates_after_disconnect_and_persists_bbo_rows(self):
        raw_events: list[dict[str, object]] = []
        checkpoints: list[dict[str, object]] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = self._stores(root)
            worker = PolymarketMarketCaptureWorker(
                adapter=_FakeMarketAdapter(
                    [
                        _FakeWebSocket([RuntimeError("socket dropped")]),
                        _FakeWebSocket(
                            [
                                {
                                    "asset_id": "asset-1",
                                    "best_bid": 0.45,
                                    "best_bid_size": 10,
                                    "best_ask": 0.47,
                                    "best_ask_size": 8,
                                    "timestamp": "2026-04-21T19:00:00Z",
                                }
                            ]
                        ),
                    ]
                ),
                config=PolymarketMarketCaptureWorkerConfig(
                    root=str(root),
                    asset_ids=["asset-1"],
                    max_sessions=2,
                    max_messages_per_session=1,
                ),
                stores=stores,
                sleep_fn=lambda _: None,
            )
            with (
                patch.object(
                    polymarket_capture,
                    "append_raw_capture_event",
                    side_effect=lambda **kwargs: raw_events.append(kwargs) or kwargs,
                ),
                patch.object(
                    polymarket_capture,
                    "upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: checkpoints.append(
                        {
                            "checkpoint_name": args[0],
                            "source_name": args[1],
                            "checkpoint_value": args[2],
                        }
                    )
                    or checkpoints[-1],
                ),
                patch.object(
                    polymarket_capture,
                    "read_capture_checkpoint",
                    return_value={"checkpoint_value": "old-cursor"},
                ),
                patch.object(
                    polymarket_capture,
                    "hydrate_polymarket_market_snapshot",
                    side_effect=lambda *args, **kwargs: {
                        "ok": True,
                        "market_count": 0,
                        "rows": [],
                    },
                ),
            ):
                results = worker.run()

        self.assertGreaterEqual(len(results), 3)
        self.assertEqual(results[-1]["normalized_row_count"], 1)
        self.assertTrue(
            any(
                event["entity_type"] == "market_stream_envelope" for event in raw_events
            )
        )
        self.assertTrue(
            any(item["checkpoint_name"] == "market_stream" for item in checkpoints)
        )
        self.assertEqual(
            stores.source_health.read_all()["polymarket_market_channel"]["status"],
            "ok",
        )

    def test_user_worker_persists_raw_envelopes_orders_and_fills(self):
        raw_events: list[dict[str, object]] = []
        checkpoints: list[dict[str, object]] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = self._stores(root)
            worker = PolymarketUserCaptureWorker(
                adapter=_FakeUserAdapter(
                    [
                        _FakeWebSocket(
                            [
                                {
                                    "orders": [
                                        {"id": "order-1", "asset_id": "asset-1"}
                                    ],
                                    "fills": [
                                        {"trade_id": "fill-1", "asset_id": "asset-1"}
                                    ],
                                }
                            ]
                        )
                    ]
                ),
                config=PolymarketUserCaptureWorkerConfig(
                    root=str(root),
                    market_ids=["condition-1"],
                    max_sessions=1,
                    max_messages_per_session=1,
                ),
                stores=stores,
                sleep_fn=lambda _: None,
            )
            with (
                patch.object(
                    polymarket_capture,
                    "append_raw_capture_event",
                    side_effect=lambda **kwargs: raw_events.append(kwargs) or kwargs,
                ),
                patch.object(
                    polymarket_capture,
                    "upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: checkpoints.append(
                        {
                            "checkpoint_name": args[0],
                            "source_name": args[1],
                            "checkpoint_value": args[2],
                        }
                    )
                    or checkpoints[-1],
                ),
            ):
                results = worker.run()

        self.assertEqual(results[-1]["order_count"], 1)
        self.assertEqual(results[-1]["fill_count"], 1)
        self.assertTrue(results[-1]["account_snapshot"])
        self.assertEqual(
            [event["entity_type"] for event in raw_events],
            [
                "user_stream_envelope",
                "user_order",
                "user_fill",
                "user_account_snapshot",
            ],
        )
        self.assertEqual(checkpoints[0]["checkpoint_name"], "user_stream")
        self.assertEqual(
            stores.source_health.read_all()["polymarket_user_channel"]["status"],
            "ok",
        )

    def test_record_failure_sanitizes_error_and_marks_source_red(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = self._stores(root)
            with patch.object(
                polymarket_capture,
                "read_capture_checkpoint",
                return_value={"checkpoint_value": "cursor-1"},
            ):
                payload = record_polymarket_capture_failure(
                    stores,
                    source_name="polymarket_market_channel",
                    stale_after_ms=4_000,
                    error=RuntimeError("wss://secret.example/socket?token=abc"),
                    observed_at=datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc),
                    checkpoint_name="market_stream",
                )

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error_kind"], "RuntimeError")
        self.assertEqual(
            payload["error_message"],
            "RuntimeError during polymarket capture",
        )
        self.assertEqual(payload["source_health"]["status"], "red")
        self.assertNotIn("secret.example", json.dumps(payload, sort_keys=True))

    def test_snapshot_hydration_writes_raw_fallback_when_postgres_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = self._stores(root)
            with (
                patch.object(
                    polymarket_capture,
                    "append_raw_capture_event",
                    side_effect=RuntimeError("Could not resolve a Postgres DSN"),
                ),
                patch.object(
                    polymarket_capture,
                    "upsert_capture_checkpoint",
                    side_effect=RuntimeError("Could not resolve a Postgres DSN"),
                ),
            ):
                hydrate_polymarket_market_snapshot(
                    PolymarketMarketSnapshotRequest(root=str(root), sport="nba"),
                    client=_StaticCatalogClient(
                        [
                            {
                                "id": "pm-1",
                                "question": "Q",
                                "sport": "nba",
                                "active": True,
                            }
                        ]
                    ),
                    stores=stores,
                    observed_at=datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc),
                )

            raw_files = list(
                (root / "raw" / "polymarket" / "market_catalog").rglob("*.jsonl.gz")
            )

        self.assertEqual(len(raw_files), 1)

    def test_cli_returns_nonzero_on_terminal_worker_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with (
                patch.object(
                    run_polymarket_capture.PolymarketCaptureStores,
                    "from_root",
                    return_value=self._stores(root),
                ),
                patch.object(
                    run_polymarket_capture.PolymarketMarketCaptureWorker,
                    "_run_session",
                    side_effect=RuntimeError("socket dropped"),
                ),
                patch.object(
                    run_polymarket_capture.PolymarketMarketCaptureWorker,
                    "_hydrate",
                    return_value={"ok": True, "market_count": 0, "rows": []},
                ),
            ):
                exit_code = run_polymarket_capture.main(
                    [
                        "market",
                        "--root",
                        str(root),
                        "--asset-id",
                        "asset-1",
                        "--max-sessions",
                        "1",
                        "--quiet",
                    ]
                )

        self.assertEqual(exit_code, 1)

    def test_cli_emits_sanitized_payload_when_postgres_is_unconfigured(self):
        stdout = io.StringIO()
        with (
            patch.object(
                run_polymarket_capture.PolymarketCaptureStores,
                "from_root",
                side_effect=RuntimeError("Could not resolve a Postgres DSN"),
            ),
            patch("sys.stdout", stdout),
        ):
            exit_code = run_polymarket_capture.main(
                [
                    "market",
                    "--asset-id",
                    "asset-1",
                ]
            )

        self.assertEqual(exit_code, 1)
        payload = json.loads(stdout.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(
            payload["error_message"], "Postgres worker storage is not configured"
        )
        self.assertEqual(payload["error_kind"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()

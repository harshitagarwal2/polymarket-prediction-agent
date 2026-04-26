from __future__ import annotations

import argparse
import gzip
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from services.capture import polymarket as polymarket_capture
from services.capture.polymarket import PolymarketCaptureStores
from services.capture.polymarket_worker import (
    PolymarketMarketCaptureWorker,
    PolymarketMarketCaptureWorkerConfig,
)
from storage.raw import RawStore


class _FakeWebSocket:
    def __init__(self, messages):
        self.messages = list(messages)
        self.closed = False

    def send(self, payload: str):
        return None

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


class _MemoryRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}

    def upsert(self, key: str, row):
        payload = row.__dict__.copy() if hasattr(row, "__dict__") else dict(row)
        self.rows[str(key)] = payload
        return payload

    def read_all(self):
        return dict(self.rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic Polymarket raw depth/trade capture verification."
    )
    parser.add_argument("--root", default=None)
    return parser


def _assert_raw_depth_trade_envelope(root: Path) -> None:
    raw_files = list(
        (root / "raw" / "polymarket" / "market_channel").rglob("*.jsonl.gz")
    )
    if not raw_files:
        raise RuntimeError("expected a raw market_channel envelope file")
    with gzip.open(raw_files[0], "rt", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle if line.strip()]
    payload = lines[-1] if lines else None
    if not isinstance(payload, dict):
        raise RuntimeError("expected raw market payload object")
    if not isinstance(payload.get("bids"), list):
        raise RuntimeError("expected bids ladder in raw payload")
    if not isinstance(payload.get("asks"), list):
        raise RuntimeError("expected asks ladder in raw payload")
    if not isinstance(payload.get("trades"), list):
        raise RuntimeError("expected trade list in raw payload")


def _run(root: Path) -> None:
    stores = PolymarketCaptureStores(
        source_health=_MemoryRepo(),
        postgres_root=root / "postgres",
        raw=RawStore(root / "raw"),
    )
    worker = PolymarketMarketCaptureWorker(
        adapter=_FakeMarketAdapter(
            [
                _FakeWebSocket(
                    [
                        {
                            "asset_id": "asset-1",
                            "best_bid": 0.45,
                            "best_bid_size": 10,
                            "best_ask": 0.47,
                            "best_ask_size": 8,
                            "timestamp": "2026-04-21T19:00:00Z",
                            "bids": [[0.45, 10], [0.44, 4]],
                            "asks": [[0.47, 8], [0.48, 5]],
                            "trades": [
                                {"price": 0.46, "size": 2, "side": "buy"},
                                {"price": 0.45, "size": 1, "side": "sell"},
                            ],
                        }
                    ]
                )
            ]
        ),
        config=PolymarketMarketCaptureWorkerConfig(
            root=str(root),
            asset_ids=["asset-1"],
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
            side_effect=RuntimeError("Could not resolve a Postgres DSN"),
        ),
        patch.object(
            polymarket_capture,
            "upsert_capture_checkpoint",
            side_effect=RuntimeError("Could not resolve a Postgres DSN"),
        ),
        patch.object(
            polymarket_capture,
            "read_capture_checkpoint",
            return_value=None,
        ),
    ):
        worker.run()
    _assert_raw_depth_trade_envelope(root)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.root in (None, ""):
        with tempfile.TemporaryDirectory() as temp_dir:
            _run(Path(temp_dir) / "runtime-data")
        return 0
    _run(Path(args.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

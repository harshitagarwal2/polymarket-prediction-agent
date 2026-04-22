from __future__ import annotations

from datetime import datetime, timezone
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from adapters import MarketSummary
from adapters.types import Contract, OrderBookSnapshot, OutcomeSide, PriceLevel, Venue
from storage import (
    EventJournal,
    ParquetStore,
    RawStore,
    build_raw_capture,
    market_row_from_summary,
    order_book_row_from_snapshot,
    write_raw_capture,
)


class StorageLayerTests(unittest.TestCase):
    def test_raw_capture_writes_envelope(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "raw.json"
            envelope = build_raw_capture({"hello": "world"}, source="gamma", layer="raw")
            write_raw_capture(envelope, path)
            payload = json.loads(path.read_text())
        self.assertEqual(payload["source"], "gamma")
        self.assertEqual(payload["payload"]["hello"], "world")

    def test_postgres_rows_normalize_market_and_book(self):
        contract = Contract(venue=Venue.POLYMARKET, symbol="abc", outcome=OutcomeSide.YES)
        market = MarketSummary(
            contract=contract,
            title="Will A happen?",
            category="politics",
            best_bid=0.45,
            best_ask=0.48,
            active=True,
        )
        book = OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.48, quantity=5)],
            observed_at=datetime.now(timezone.utc),
        )
        market_row = market_row_from_summary(market)
        book_row = order_book_row_from_snapshot(book)
        self.assertEqual(market_row.market_key, "abc:yes")
        self.assertEqual(book_row.ask_levels, 1)

    def test_event_journal_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = EventJournal(Path(temp_dir) / "events.jsonl")
            journal.append("scan_cycle", {"market_count": 1})
            lines = (Path(temp_dir) / "events.jsonl").read_text().splitlines()
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["event_type"], "scan_cycle")

    def test_raw_store_writes_gzipped_jsonl_by_partition(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = RawStore(Path(temp_dir))
            path = store.write(
                "polymarket",
                "market_channel",
                datetime(2026, 4, 21, 17, 5, tzinfo=timezone.utc),
                {"market_id": "m1", "best_bid_yes": 0.4},
            )
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle]
        self.assertEqual(rows[0]["market_id"], "m1")
        self.assertTrue(path.endswith("17/05.jsonl.gz"))

    def test_parquet_store_writes_partitioned_dataset(self):
        try:
            import pyarrow  # noqa: F401
        except ModuleNotFoundError:  # pragma: no cover - environment-dependent
            self.skipTest("pyarrow not installed")

        with tempfile.TemporaryDirectory() as temp_dir:
            store = ParquetStore(Path(temp_dir))
            store.append_records(
                "odds_snapshots",
                datetime(2026, 4, 21, 17, 5, tzinfo=timezone.utc),
                [{"market_id": "m1", "prob": 0.52}],
            )
            paths = list(Path(temp_dir).rglob("*.parquet"))
        self.assertEqual(len(paths), 1)
        self.assertIn("odds_snapshots/year=2026", paths[0].as_posix())


if __name__ == "__main__":
    unittest.main()

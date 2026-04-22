from __future__ import annotations

from datetime import datetime, timezone
import json
import tempfile
import unittest
from pathlib import Path

from adapters import MarketSummary
from adapters.types import Contract, OrderBookSnapshot, OutcomeSide, PriceLevel, Venue
from storage import (
    EventJournal,
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


if __name__ == "__main__":
    unittest.main()

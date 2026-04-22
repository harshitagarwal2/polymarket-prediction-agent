from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from adapters.types import Contract, MarketSummary, OutcomeSide, Venue
from risk.freeze_windows import (
    FreezeWindowPolicy,
    freeze_reason_for_market,
    freeze_reasons_for_state,
)


class FreezeWindowTests(unittest.TestCase):
    def test_freeze_reasons_include_resolved_and_stale_source(self):
        now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
        reasons = freeze_reasons_for_state(
            policy=FreezeWindowPolicy(
                freeze_minutes_before_expiry=30,
            ),
            now=now,
            market_end_time=now + timedelta(minutes=10),
            market_resolved=True,
            required_sources=("polymarket_market_channel",),
            source_health={
                "polymarket_market_channel": {
                    "status": "ok",
                    "last_success_at": "2026-04-22T11:59:40+00:00",
                    "stale_after_ms": 10_000,
                }
            },
        )
        self.assertIn("market within pre-expiry freeze window", reasons)
        self.assertIn("market resolved", reasons)
        self.assertIn("source polymarket_market_channel stale", reasons)

    def test_market_wrapper_uses_start_and_expiry_fields(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET,
                symbol="TOKEN-1",
                outcome=OutcomeSide.YES,
            ),
            start_time=datetime.now(timezone.utc) + timedelta(minutes=4),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=60),
            active=True,
        )
        reason = freeze_reason_for_market(
            market,
            policy=FreezeWindowPolicy(freeze_minutes_before_start=5),
        )
        self.assertEqual(reason, "market within pre-start freeze window")

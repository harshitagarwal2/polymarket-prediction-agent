from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from storage import (
    FileBackedCurrentStateStore,
    PolymarketBBORecord,
    ProjectedCurrentStateReadAdapter,
    SportsbookEventRecord,
    SportsbookOddsRecord,
)
from storage.current_state_materializers import (  # pyright: ignore[reportMissingImports]
    materialize_polymarket_bbo_state,
    materialize_source_health_state,
    materialize_sportsbook_event_state,
    materialize_sportsbook_quote_state,
)
from storage.current_state_projectors import (  # pyright: ignore[reportMissingImports]
    SourceHealthUpdate,
    project_polymarket_market_state,
    project_source_health_state,
    project_sportsbook_quote_state,
    sportsbook_quote_current_key,
)
from storage.source_health import SourceHealthStore


@dataclass(frozen=True)
class _StubRepository:
    payload: dict[str, object]

    def read_all(self) -> dict[str, object]:
        return dict(self.payload)


class CurrentStateProjectorTests(unittest.TestCase):
    def test_polymarket_market_projector_is_deterministic_and_latest_wins(self):
        rows = [
            {
                "market_id": "pm-1",
                "title": "Initial title",
                "status": "open",
            },
            {
                "market_id": "pm-2",
                "title": "Other market",
                "status": "open",
            },
            {
                "market_id": "pm-1",
                "title": "Replacement title",
                "status": "closed",
            },
        ]

        first = project_polymarket_market_state(rows)
        second = project_polymarket_market_state(rows)

        self.assertEqual(first, second)
        self.assertEqual(first["pm-1"]["title"], "Replacement title")
        self.assertEqual(first["pm-1"]["status"], "closed")

    def test_polymarket_bbo_materializer_writes_latest_market_snapshot(self):
        rows = [
            PolymarketBBORecord(
                market_id="pm-1",
                best_bid_yes=0.44,
                best_bid_yes_size=12.0,
                best_ask_yes=0.47,
                best_ask_yes_size=10.0,
                midpoint_yes=0.455,
                spread_yes=0.03,
                book_ts="2026-04-21T18:00:00+00:00",
                source_age_ms=150,
                raw_hash=None,
            ),
            PolymarketBBORecord(
                market_id="pm-1",
                best_bid_yes=0.45,
                best_bid_yes_size=8.0,
                best_ask_yes=0.48,
                best_ask_yes_size=9.0,
                midpoint_yes=0.465,
                spread_yes=0.03,
                book_ts="2026-04-21T18:00:01+00:00",
                source_age_ms=120,
                raw_hash=None,
            ),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            store = FileBackedCurrentStateStore(Path(temp_dir))
            projected = materialize_polymarket_bbo_state(store, rows)

            self.assertEqual(store.read_table("polymarket_bbo"), projected)

        self.assertEqual(projected["pm-1"]["best_bid_yes"], 0.45)
        self.assertEqual(projected["pm-1"]["book_ts"], "2026-04-21T18:00:01+00:00")

    def test_sportsbook_event_materializer_prefers_latest_event_payload(self):
        rows = [
            SportsbookEventRecord(
                sportsbook_event_id="sb-1",
                source="theoddsapi",
                sport="basketball_nba",
                league="NBA",
                home_team="Home",
                away_team="Away",
                start_time="2026-04-21T20:00:00+00:00",
                raw_json={
                    "id": "sb-1",
                    "commence_time": "2026-04-21T20:00:00+00:00",
                    "event_key": "event-old",
                },
            ),
            SportsbookEventRecord(
                sportsbook_event_id="sb-1",
                source="theoddsapi",
                sport="basketball_nba",
                league="NBA",
                home_team="Home",
                away_team="Away",
                start_time="2026-04-21T20:05:00+00:00",
                raw_json={
                    "id": "sb-1",
                    "commence_time": "2026-04-21T20:05:00+00:00",
                    "event_key": "event-new",
                },
            ),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            store = FileBackedCurrentStateStore(Path(temp_dir))
            projected = materialize_sportsbook_event_state(store, rows)

            self.assertEqual(store.read_table("sportsbook_events"), projected)

        self.assertEqual(projected["sb-1"]["event_key"], "event-new")
        self.assertEqual(
            projected["sb-1"]["commence_time"], "2026-04-21T20:05:00+00:00"
        )

    def test_sportsbook_quote_projector_uses_composite_key_and_latest_wins(self):
        older_quote = SportsbookOddsRecord(
            sportsbook_event_id="sb-1",
            source="book-a",
            market_type="h2h",
            selection="Home Team",
            price_decimal=1.8,
            implied_prob=0.555556,
            overround=1.04,
            quote_ts="2026-04-21T18:00:00+00:00",
            source_age_ms=100,
            raw_json={"version": 1},
            provider="theoddsapi",
            source_ts="2026-04-21T17:59:59+00:00",
            capture_ts="2026-04-21T18:00:00+00:00",
        )
        newer_quote = SportsbookOddsRecord(
            sportsbook_event_id="sb-1",
            source="book-a",
            market_type="h2h",
            selection="Home Team",
            price_decimal=1.6,
            implied_prob=0.625,
            overround=1.03,
            quote_ts="2026-04-21T18:00:05+00:00",
            source_age_ms=50,
            raw_json={"version": 2},
            provider="theoddsapi",
            source_ts="2026-04-21T18:00:04+00:00",
            capture_ts="2026-04-21T18:00:05+00:00",
        )

        projected = project_sportsbook_quote_state([older_quote, newer_quote])

        with tempfile.TemporaryDirectory() as temp_dir:
            store = FileBackedCurrentStateStore(Path(temp_dir))
            materialized = materialize_sportsbook_quote_state(
                store,
                [older_quote, newer_quote],
            )
            self.assertEqual(store.read_table("sportsbook_odds"), materialized)

        quote_key = sportsbook_quote_current_key(newer_quote)
        self.assertEqual(quote_key, "sb-1|book-a|h2h|Home Team")
        self.assertEqual(projected[quote_key]["price_decimal"], 1.6)
        self.assertEqual(projected[quote_key]["raw_json"]["version"], 2)

    def test_source_health_projector_preserves_last_success_across_failure(self):
        first_seen = datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc)
        second_seen = datetime(2026, 4, 21, 18, 1, tzinfo=timezone.utc)
        updates = [
            SourceHealthUpdate(
                source_name="sportsbook_odds",
                stale_after_ms=60_000,
                status="ok",
                details={"row_count": 2},
                success=True,
                observed_at=first_seen,
            ),
            SourceHealthUpdate(
                source_name="sportsbook_odds",
                stale_after_ms=60_000,
                status="red",
                details={"error": "timeout"},
                success=False,
                observed_at=second_seen,
            ),
        ]

        projected = project_source_health_state(updates)

        with tempfile.TemporaryDirectory() as temp_dir:
            store = SourceHealthStore(Path(temp_dir) / "source_health.json")
            materialized = materialize_source_health_state(store, updates)
            self.assertEqual(cast(Any, store).read_all(), materialized)

        self.assertEqual(
            projected["sportsbook_odds"]["last_seen_at"],
            second_seen.isoformat(),
        )
        self.assertEqual(
            projected["sportsbook_odds"]["last_success_at"],
            first_seen.isoformat(),
        )
        self.assertEqual(projected["sportsbook_odds"]["status"], "red")

    def test_projected_current_read_adapter_projects_lane_tables(self):
        adapter = ProjectedCurrentStateReadAdapter(
            opportunities=_StubRepository({}),
            mappings=_StubRepository({}),
            fair_values=_StubRepository({}),
            bbo_rows=_StubRepository(
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "best_bid_yes": 0.44,
                        "best_ask_yes": 0.47,
                    }
                }
            ),
            sportsbook_events=_StubRepository(
                {
                    "sb-1": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "start_time": "2026-04-21T20:00:00+00:00",
                        "raw_json": {
                            "id": "sb-1",
                            "commence_time": "2026-04-21T20:00:00+00:00",
                            "event_key": "event-1",
                        },
                    }
                }
            ),
            sportsbook_odds=_StubRepository(
                {
                    "0": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.8,
                        "quote_ts": "2026-04-21T20:00:00+00:00",
                    }
                }
            ),
            source_health=_StubRepository(
                {
                    "sportsbook_odds": {
                        "status": "ok",
                        "last_seen_at": "2026-04-21T18:00:00+00:00",
                    }
                }
            ),
            polymarket_markets=_StubRepository(
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "title": "Market 1",
                        "status": "open",
                    }
                }
            ),
        )

        self.assertEqual(
            adapter.read_table("sportsbook_events"),
            {
                "sb-1": {
                    "id": "sb-1",
                    "commence_time": "2026-04-21T20:00:00+00:00",
                    "event_key": "event-1",
                }
            },
        )
        source_health = adapter.read_table("source_health")
        polymarket_markets = adapter.read_table("polymarket_markets")
        sportsbook_odds = adapter.read_table("sportsbook_odds")
        self.assertIsInstance(source_health, dict)
        self.assertIsInstance(polymarket_markets, dict)
        self.assertIsInstance(sportsbook_odds, dict)
        sportsbook_odds_health = cast(dict[str, Any], source_health["sportsbook_odds"])
        polymarket_market = cast(dict[str, Any], polymarket_markets["pm-1"])
        current_quote = cast(
            dict[str, Any], sportsbook_odds["sb-1|book-a|h2h|Home Team"]
        )
        self.assertEqual(sportsbook_odds_health["status"], "ok")
        self.assertEqual(
            polymarket_market["title"],
            "Market 1",
        )
        self.assertEqual(current_quote["price_decimal"], 1.8)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from research.data.schedule_feed import (
    build_event_map_from_schedule_rows,
    fetch_mlb_schedule,
    load_schedule_feed,
)


class _StubHttpClient:
    def __init__(self, payload):
        self.payload = payload

    def get(self, url, params=None, headers=None, timeout=None, follow_redirects=True):
        class _Response:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return _Response(self.payload)


class ScheduleFeedTests(unittest.TestCase):
    def test_build_event_map_from_schedule_rows(self):
        event_map = build_event_map_from_schedule_rows(
            [
                {
                    "gamePk": 123,
                    "homeTeamName": "Home Team",
                    "awayTeamName": "Away Team",
                    "gameDate": "2026-04-21T20:00:00Z",
                    "gameType": "playoffs",
                    "detailedState": "Final",
                }
            ],
            sport="mlb",
            series="regular-season",
        )

        self.assertIn("123", event_map)
        self.assertEqual(event_map["123"]["game_id"], "123")
        self.assertEqual(event_map["123"]["sport"], "mlb")
        self.assertEqual(event_map["123"]["series"], "playoffs")
        self.assertEqual(event_map["123"]["status"], "Final")

    def test_load_schedule_feed_reads_object_payload(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            handle.write('{"games": [{"id": "evt-1"}]}')
            handle.flush()

            rows = load_schedule_feed(Path(handle.name))

        self.assertEqual(rows, [{"id": "evt-1"}])

    def test_load_schedule_feed_rejects_unsupported_object_payload(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            handle.write('{"unexpected": {"nested": "value"}}')
            handle.flush()

            with self.assertRaisesRegex(RuntimeError, "games/events/data"):
                load_schedule_feed(Path(handle.name))

    def test_load_schedule_feed_rejects_non_object_rows(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            handle.write('{"games": [{"id": "evt-1"}, 2]}')
            handle.flush()

            with self.assertRaisesRegex(RuntimeError, "rows must be objects"):
                load_schedule_feed(Path(handle.name))

    def test_fetch_mlb_schedule_flattens_dates_games(self):
        rows = fetch_mlb_schedule(
            date="2026-04-21",
            client=_StubHttpClient(
                {
                    "dates": [
                        {"games": [{"gamePk": 123}, {"gamePk": 456}]},
                    ]
                }
            ),
        )

        self.assertEqual([row["gamePk"] for row in rows], [123, 456])

    def test_fetch_mlb_schedule_rejects_malformed_dates(self):
        with self.assertRaisesRegex(RuntimeError, "malformed dates list"):
            fetch_mlb_schedule(
                date="2026-04-21",
                client=_StubHttpClient({"dates": {"bad": "shape"}}),
            )

    def test_build_event_map_rejects_incomplete_rows(self):
        with self.assertRaisesRegex(
            RuntimeError, "missing required event id or team identity"
        ):
            build_event_map_from_schedule_rows(
                [{"source_event_id": "feed-1", "home_team": "Home Team"}],
                sport="nba",
                series="playoffs",
            )

    def test_build_event_map_from_mlb_statsapi_shape(self):
        event_map = build_event_map_from_schedule_rows(
            [
                {
                    "gamePk": 123,
                    "gameDate": "2026-04-21T20:00:00Z",
                    "gameType": "regular-season",
                    "status": {"detailedState": "In Progress"},
                    "teams": {
                        "home": {"team": {"name": "Home Team"}},
                        "away": {"team": {"name": "Away Team"}},
                    },
                }
            ],
            sport="mlb",
            series="default-series",
        )

        self.assertIn("123", event_map)
        self.assertEqual(event_map["123"]["game_id"], "123")
        self.assertEqual(event_map["123"]["status"], "In Progress")
        self.assertEqual(event_map["123"]["series"], "regular-season")


if __name__ == "__main__":
    unittest.main()

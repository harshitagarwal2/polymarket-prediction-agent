from __future__ import annotations

import io
import json
import tempfile
import unittest
from unittest.mock import patch

from scripts import fetch_the_odds_api_rows


class FetchTheOddsApiRowsTests(unittest.TestCase):
    def test_main_quiet_suppresses_stdout(self):
        fake_payload = [
            {
                "id": "event-1",
                "home_team": "Home",
                "away_team": "Away",
                "bookmakers": [],
            }
        ]
        stdout = io.StringIO()
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle:
            with (
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
                patch.object(
                    fetch_the_odds_api_rows,
                    "_fetch_odds_payload",
                    return_value=fake_payload,
                ),
                patch(
                    "sys.argv",
                    [
                        "fetch_the_odds_api_rows.py",
                        "--sport-key",
                        "basketball_nba",
                        "--output",
                        output_handle.name,
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                fetch_the_odds_api_rows.main()

            payload = json.loads(output_handle.read())

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(payload, [])

    def test_normalize_odds_events_uses_event_map_for_yes_no_outcomes(self):
        events = [
            {
                "id": "event-1",
                "home_team": "Home",
                "away_team": "Away",
                "commence_time": "2026-04-07T19:00:00Z",
                "bookmakers": [
                    {
                        "key": "book-a",
                        "last_update": "2026-04-07T12:00:00Z",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home", "price": 1.7},
                                    {"name": "Away", "price": 2.3},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
        event_map = {
            "event-1": {
                "event_key": "nba-finals-game-1",
                "sport": "nba",
                "series": "nba",
                "sports_market_type": "moneyline",
                "outcome_map": {"Home": "yes", "Away": "no"},
            }
        }

        rows = fetch_the_odds_api_rows.normalize_odds_events(
            events,
            sport_key="basketball_nba",
            event_map=event_map,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["event_key"], "nba-finals-game-1")
        self.assertEqual({row["outcome"] for row in rows}, {"yes", "no"})

    def test_main_fetches_and_writes_rows(self):
        fake_payload = [
            {
                "id": "event-1",
                "home_team": "Home",
                "away_team": "Away",
                "bookmakers": [
                    {
                        "key": "book-a",
                        "last_update": "2026-04-07T12:00:00Z",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home", "price": 1.7},
                                    {"name": "Away", "price": 2.3},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
        event_map = {
            "event-1": {
                "event_key": "nba-finals-game-1",
                "sport": "nba",
                "series": "nba",
                "sports_market_type": "moneyline",
                "outcome_map": {"Home": "yes", "Away": "no"},
            }
        }

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as map_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(event_map, map_handle)
            map_handle.flush()

            with (
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
                patch.object(
                    fetch_the_odds_api_rows,
                    "_fetch_odds_payload",
                    return_value=fake_payload,
                ),
                patch(
                    "sys.argv",
                    [
                        "fetch_the_odds_api_rows.py",
                        "--sport-key",
                        "basketball_nba",
                        "--event-map-file",
                        map_handle.name,
                        "--output",
                        output_handle.name,
                    ],
                ),
            ):
                fetch_the_odds_api_rows.main()

            payload = json.loads(output_handle.read())

        self.assertEqual(len(payload), 2)
        self.assertEqual(payload[0]["event_key"], "nba-finals-game-1")

    def test_normalize_odds_events_keeps_selection_names_without_event_map(self):
        events = [
            {
                "id": "event-1",
                "home_team": "Home",
                "away_team": "Away",
                "commence_time": "2026-04-07T19:00:00Z",
                "bookmakers": [
                    {
                        "key": "book-a",
                        "last_update": "2026-04-07T12:00:00Z",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home", "price": 1.7},
                                    {"name": "Away", "price": 2.3},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        rows = fetch_the_odds_api_rows.normalize_odds_events(
            events,
            sport_key="basketball_nba",
            event_map={},
        )

        self.assertEqual(rows[0]["selection_name"], "Home")
        self.assertNotIn("outcome", rows[0])


if __name__ == "__main__":
    unittest.main()

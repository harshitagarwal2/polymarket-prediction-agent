from __future__ import annotations

import json
from unittest.mock import patch
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from adapters.sportsbooks import SportsbookJsonFeedClient, SportsGameOddsClient
from services.capture import (
    SportsbookCaptureRequest,
    SportsbookCaptureStores,
    SportsGameOddsCaptureSource,
    SportsbookJsonFeedCaptureSource,
    capture_sportsbook_odds_once,
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


class SportsbookJsonFeedProviderTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_client_filters_top_level_event_list(self):
        client = SportsbookJsonFeedClient(
            feed_url="https://example.test/feed.json",
            client=_StubHttpClient(
                {
                    "events": [
                        {
                            "external_id": "keep-me",
                            "sport_key": "basketball_nba",
                            "markets": [{"market_type": "h2h"}],
                        },
                        {
                            "external_id": "drop-me",
                            "sport_key": "americanfootball_nfl",
                            "markets": [{"market_type": "h2h"}],
                        },
                    ]
                }
            ),
        )

        events = client.fetch_upcoming("basketball_nba", "h2h")

        self.assertEqual([event["external_id"] for event in events], ["keep-me"])

    def test_client_rejects_non_list_event_payload(self):
        client = SportsbookJsonFeedClient(
            feed_url="https://example.test/feed.json",
            client=_StubHttpClient({"events": {"bad": "shape"}}),
        )

        with self.assertRaisesRegex(RuntimeError, "non-list event payload"):
            client.fetch_upcoming("basketball_nba", "h2h")

    def test_client_rejects_non_https_feed_url(self):
        with self.assertRaisesRegex(ValueError, "must use https"):
            SportsbookJsonFeedClient(feed_url="http://example.test/feed.json")

    def test_client_rejects_private_network_targets(self):
        with patch(
            "adapters.sportsbooks.base.socket.getaddrinfo",
            return_value=[
                (
                    2,
                    1,
                    6,
                    "",
                    ("127.0.0.1", 443),
                )
            ],
        ):
            with self.assertRaisesRegex(ValueError, "public host"):
                SportsbookJsonFeedClient(feed_url="https://feed.example.test/feed.json")

    def test_capture_source_normalizes_json_feed_rows(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        source_time = "2026-04-21T18:04:30+00:00"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            raw_payloads: list[dict[str, object]] = []
            event_map = Path(temp_dir) / "event_map.json"
            self._write_json(
                event_map,
                {
                    "feed-1": {
                        "event_key": "event-feed-1",
                        "game_id": "game-feed-1",
                        "sport": "nba",
                        "series": "playoffs",
                    }
                },
            )
            source = SportsbookJsonFeedCaptureSource(
                client=SportsbookJsonFeedClient(
                    feed_url="https://example.test/feed.json",
                    client=_StubHttpClient(
                        [
                            {
                                "external_id": "feed-1",
                                "sport_key": "basketball_nba",
                                "league_name": "NBA",
                                "home": "Home Team",
                                "away": "Away Team",
                                "start_time": "2026-04-21T20:00:00+00:00",
                                "provider_updated_at": source_time,
                                "bookmaker": "alt-book",
                                "markets": [
                                    {
                                        "market_type": "h2h",
                                        "outcomes": [
                                            {"selection": "Home Team", "price": "2.0"},
                                            {"selection": "Away Team", "price": 2.1},
                                        ],
                                    }
                                ],
                                "secret_token": "should-not-persist",
                            }
                        ]
                    ),
                )
            )

            with patch.object(
                stores.raw,
                "write",
                side_effect=lambda source_name,
                layer,
                captured_at,
                payload: raw_payloads.append(payload),
            ):
                payload = capture_sportsbook_odds_once(
                    SportsbookCaptureRequest(
                        root=str(root),
                        sport="basketball_nba",
                        market="h2h",
                        event_map_file=str(event_map),
                    ),
                    source=source,
                    stores=stores,
                    observed_at=capture_time,
                )

            postgres_events = json.loads(
                (root / "postgres" / "sportsbook_events.json").read_text(
                    encoding="utf-8"
                )
            )
            postgres_odds = json.loads(
                (root / "postgres" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["provider"], "json_feed")
        self.assertEqual(payload["row_count"], 2)
        self.assertEqual(postgres_events["feed-1"]["source"], "json_feed")
        self.assertEqual(
            postgres_events["feed-1"]["raw_json"]["event_key"], "event-feed-1"
        )
        self.assertEqual(
            postgres_events["feed-1"]["raw_json"]["provider_event_id"], "feed-1"
        )
        self.assertNotIn("provider_payload", postgres_events["feed-1"]["raw_json"])
        self.assertEqual(raw_payloads[0]["provider_event_id"], "feed-1")
        self.assertNotIn("provider_payload", raw_payloads[0])
        self.assertNotIn("secret_token", raw_payloads[0])
        first_quote = postgres_odds["0"]
        self.assertEqual(first_quote["source"], "alt-book")
        self.assertEqual(first_quote["provider"], "json_feed")
        self.assertEqual(first_quote["source_ts"], source_time)
        self.assertEqual(first_quote["capture_ts"], capture_time.isoformat())
        self.assertEqual(first_quote["price_decimal"], 2.0)
        self.assertNotIn("provider_payload", first_quote["raw_json"])

    def test_capture_source_rejects_missing_stable_event_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            source = SportsbookJsonFeedCaptureSource(
                client=SportsbookJsonFeedClient(
                    feed_url="https://example.test/feed.json",
                    client=_StubHttpClient(
                        [
                            {
                                "sport_key": "basketball_nba",
                                "league_name": "NBA",
                                "home": "Home Team",
                                "away": "Away Team",
                                "start_time": "2026-04-21T20:00:00+00:00",
                                "provider_updated_at": "2026-04-21T18:04:30+00:00",
                                "bookmaker": "alt-book",
                                "markets": [
                                    {
                                        "market_type": "h2h",
                                        "outcomes": [
                                            {"selection": "Home Team", "price": 1.9},
                                            {"selection": "Away Team", "price": 2.1},
                                        ],
                                    }
                                ],
                            }
                        ]
                    ),
                )
            )

            with self.assertRaisesRegex(ValueError, "stable event id"):
                capture_sportsbook_odds_once(
                    SportsbookCaptureRequest(
                        root=str(root),
                        sport="basketball_nba",
                        market="h2h",
                    ),
                    source=source,
                    stores=stores,
                    observed_at=datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc),
                )

    def test_capture_source_normalizes_sportsgameodds_rows(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        source_time = "2026-04-21T18:04:30+00:00"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            raw_payloads: list[dict[str, object]] = []
            event_map = Path(temp_dir) / "event_map.json"
            self._write_json(
                event_map,
                {
                    "sgo-1": {
                        "event_key": "event-sgo-1",
                        "game_id": "game-sgo-1",
                        "sport": "nba",
                        "series": "playoffs",
                    }
                },
            )
            source = SportsGameOddsCaptureSource(
                client=SportsGameOddsClient(
                    api_key="test-key",
                    feed_url="https://example.test/events",
                    client=_StubHttpClient(
                        [
                            {
                                "eventID": "sgo-1",
                                "leagueID": "NBA",
                                "status": {"startsAt": "2026-04-21T20:00:00+00:00"},
                                "teams": {
                                    "home": {"team": {"name": "Home Team"}},
                                    "away": {"team": {"name": "Away Team"}},
                                },
                                "odds": {
                                    "moneyline-home": {
                                        "market_type": "h2h",
                                        "byBookmaker": {
                                            "alt-book": {
                                                "updated_at": source_time,
                                                "odds": {"decimal": "2.0"},
                                            }
                                        },
                                    },
                                    "moneyline-away": {
                                        "market_type": "h2h",
                                        "byBookmaker": {
                                            "alt-book": {
                                                "updated_at": source_time,
                                                "odds": {"decimal": 2.1},
                                            }
                                        },
                                    },
                                },
                            }
                        ]
                    ),
                )
            )

            with patch.object(
                stores.raw,
                "write",
                side_effect=lambda source_name,
                layer,
                captured_at,
                payload: raw_payloads.append(payload),
            ):
                payload = capture_sportsbook_odds_once(
                    SportsbookCaptureRequest(
                        root=str(root),
                        sport="basketball_nba",
                        market="h2h",
                        event_map_file=str(event_map),
                    ),
                    source=source,
                    stores=stores,
                    observed_at=capture_time,
                )

            postgres_events = json.loads(
                (root / "postgres" / "sportsbook_events.json").read_text(
                    encoding="utf-8"
                )
            )
            postgres_odds = json.loads(
                (root / "postgres" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["provider"], "sportsgameodds")
        self.assertEqual(payload["row_count"], 2)
        self.assertEqual(postgres_events["sgo-1"]["source"], "sportsgameodds")
        self.assertEqual(
            postgres_events["sgo-1"]["raw_json"]["provider_event_id"], "sgo-1"
        )
        self.assertEqual(raw_payloads[0]["provider_event_id"], "sgo-1")
        first_quote = postgres_odds["0"]
        self.assertEqual(first_quote["source"], "alt-book")
        self.assertEqual(first_quote["provider"], "sportsgameodds")
        self.assertEqual(first_quote["source_ts"], source_time)
        self.assertEqual(first_quote["capture_ts"], capture_time.isoformat())


if __name__ == "__main__":
    unittest.main()

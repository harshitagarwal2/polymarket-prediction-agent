from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_live_data, run_sportsbook_capture
from services.capture import (
    SportsbookCaptureRequest,
    SportsbookCaptureStores,
    SportsbookCaptureWorker,
    SportsbookCaptureWorkerConfig,
    capture_sportsbook_odds_once,
)


class _StaticSource:
    def __init__(self, events_or_errors):
        self.provider_name = "theoddsapi"
        self._events_or_errors = list(events_or_errors)

    def fetch_upcoming(self, sport: str, market_type: str):
        value = self._events_or_errors.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class SportsbookCaptureWorkerTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _sample_event(self, *, last_update: str) -> dict[str, object]:
        return {
            "id": "sb-1",
            "sport_title": "NBA",
            "home_team": "Home Team",
            "away_team": "Away Team",
            "commence_time": "2026-04-21T20:00:00+00:00",
            "bookmakers": [
                {
                    "key": "book-a",
                    "last_update": last_update,
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Home Team", "price": 1.8},
                                {"name": "Away Team", "price": 2.0},
                            ],
                        }
                    ],
                }
            ],
        }

    def test_capture_once_writes_append_only_quotes_and_health(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        source_time = (capture_time - timedelta(seconds=30)).isoformat()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            event_map = Path(temp_dir) / "event_map.json"
            self._write_json(
                event_map,
                {
                    "sb-1": {
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "sport": "nba",
                        "series": "playoffs",
                    }
                },
            )

            payload = capture_sportsbook_odds_once(
                SportsbookCaptureRequest(
                    root=str(root),
                    sport="basketball_nba",
                    market="h2h",
                    event_map_file=str(event_map),
                ),
                source=_StaticSource([[self._sample_event(last_update=source_time)]]),
                stores=SportsbookCaptureStores.from_root(root),
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
            current_odds = json.loads(
                (root / "current" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )
            current_health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )
            postgres_health = json.loads(
                (root / "postgres" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["event_count"], 1)
        self.assertEqual(payload["row_count"], 2)
        self.assertEqual(postgres_events["sb-1"]["source"], "theoddsapi")
        self.assertEqual(postgres_events["sb-1"]["raw_json"]["event_key"], "event-1")
        self.assertEqual(len(postgres_odds), 2)

        first_quote = postgres_odds["0"]
        self.assertEqual(first_quote["source"], "book-a")
        self.assertEqual(first_quote["provider"], "theoddsapi")
        self.assertEqual(first_quote["source_ts"], source_time)
        self.assertEqual(first_quote["capture_ts"], capture_time.isoformat())
        self.assertEqual(first_quote["source_age_ms"], 30000)
        self.assertIn("sb-1|book-a|h2h|Home Team", current_odds)
        self.assertEqual(current_health["sportsbook_odds"]["status"], "ok")
        self.assertEqual(postgres_health["sportsbook_odds"]["status"], "ok")

    def test_capture_once_replaces_current_snapshot(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "sportsbook_events.json",
                {"stale": {"sportsbook_event_id": "stale"}},
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {"stale|book|h2h|Home": {"sportsbook_event_id": "stale"}},
            )

            payload = capture_sportsbook_odds_once(
                SportsbookCaptureRequest(
                    root=str(root),
                    sport="basketball_nba",
                    market="h2h",
                ),
                source=_StaticSource([[]]),
                stores=SportsbookCaptureStores.from_root(root),
                observed_at=capture_time,
            )

            current_events = json.loads(
                (root / "current" / "sportsbook_events.json").read_text(
                    encoding="utf-8"
                )
            )
            current_odds = json.loads(
                (root / "current" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(current_events, {})
        self.assertEqual(current_odds, {})

    def test_worker_records_failure_then_success(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        sleeps: list[float] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            worker = SportsbookCaptureWorker(
                source=_StaticSource(
                    [
                        RuntimeError("fetch failed"),
                        [
                            self._sample_event(
                                last_update=(
                                    capture_time - timedelta(seconds=5)
                                ).isoformat()
                            )
                        ],
                    ]
                ),
                config=SportsbookCaptureWorkerConfig(
                    root=str(root),
                    sport="basketball_nba",
                    market="h2h",
                    refresh_interval_seconds=7.5,
                    max_cycles=2,
                ),
                sleep_fn=sleeps.append,
            )

            results = worker.run()
            health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertEqual(len(results), 2)
        self.assertFalse(results[0]["ok"])
        self.assertTrue(results[1]["ok"])
        self.assertEqual(sleeps, [7.5])
        self.assertEqual(health["sportsbook_odds"]["status"], "ok")
        self.assertIsNotNone(health["sportsbook_odds"]["last_success_at"])

    def test_failure_payload_sanitizes_exception_text(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            worker = SportsbookCaptureWorker(
                source=_StaticSource(
                    [RuntimeError("https://example.com/?apiKey=secret")]
                ),
                config=SportsbookCaptureWorkerConfig(
                    root=str(root),
                    sport="basketball_nba",
                    market="h2h",
                    max_cycles=1,
                ),
                sleep_fn=lambda _: None,
            )

            results = worker.run()
            health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertEqual(results[0]["error_kind"], "RuntimeError")
        self.assertEqual(
            results[0]["error_message"],
            "RuntimeError during sportsbook capture",
        )
        details = health["sportsbook_odds"]["details"]
        self.assertEqual(details["error_kind"], "RuntimeError")
        self.assertEqual(
            details["error_message"],
            "RuntimeError during sportsbook capture",
        )
        self.assertNotIn("apiKey=secret", json.dumps(results[0], sort_keys=True))
        self.assertNotIn("apiKey=secret", json.dumps(health, sort_keys=True))

    def test_ingest_sportsbook_odds_uses_capture_service_contract(self):
        source_time = "2026-04-21T18:04:30+00:00"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            event_map = Path(temp_dir) / "event_map.json"
            self._write_json(
                event_map,
                {
                    "sb-1": {
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "sport": "nba",
                        "series": "playoffs",
                    }
                },
            )
            with (
                patch.object(
                    ingest_live_data.TheOddsApiClient,
                    "fetch_upcoming",
                    return_value=[self._sample_event(last_update=source_time)],
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
            ):
                result = ingest_live_data.main(
                    [
                        "sportsbook-odds",
                        "--sport",
                        "basketball_nba",
                        "--market",
                        "h2h",
                        "--root",
                        str(root),
                        "--event-map-file",
                        str(event_map),
                        "--quiet",
                    ]
                )

            current_odds = json.loads(
                (root / "current" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )
            postgres_health = json.loads(
                (root / "postgres" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertEqual(result, 0)
        record = current_odds["sb-1|book-a|h2h|Home Team"]
        self.assertEqual(record["provider"], "theoddsapi")
        self.assertEqual(record["source_ts"], source_time)
        self.assertIn("capture_ts", record)
        self.assertEqual(postgres_health["sportsbook_odds"]["status"], "ok")

    def test_run_sportsbook_capture_main_executes_worker_cycle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with (
                patch.object(
                    run_sportsbook_capture.TheOddsApiCaptureSource,
                    "fetch_upcoming",
                    return_value=[
                        self._sample_event(last_update="2026-04-21T18:04:30+00:00")
                    ],
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
            ):
                result = run_sportsbook_capture.main(
                    [
                        "--sport",
                        "basketball_nba",
                        "--market",
                        "h2h",
                        "--root",
                        str(root),
                        "--max-cycles",
                        "1",
                        "--quiet",
                    ]
                )

            postgres_events = json.loads(
                (root / "postgres" / "sportsbook_events.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(result, 0)
        self.assertIn("sb-1", postgres_events)


if __name__ == "__main__":
    unittest.main()

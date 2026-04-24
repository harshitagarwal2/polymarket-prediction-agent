from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_live_data, run_sportsbook_capture
from services.capture import (
    SportsbookCaptureRequest,
    SportsbookCaptureStores,
    SportsbookCaptureWorker,
    SportsbookCaptureWorkerConfig,
    SportsbookJsonFeedCaptureSource,
    capture_sportsbook_odds_once,
)
from services.capture import sportsbook as sportsbook_capture


class _StaticSource:
    def __init__(self, events_or_errors):
        self.provider_name = "theoddsapi"
        self._events_or_errors = list(events_or_errors)

    def fetch_upcoming(self, sport: str, market_type: str):
        value = self._events_or_errors.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def event_id(self, event: dict[str, object]) -> str:
        return sportsbook_capture._default_event_id(event)

    def normalize_event(
        self,
        event: dict[str, object],
        *,
        market_type: str,
        captured_at: datetime,
    ):
        return sportsbook_capture._default_normalize_event(
            event,
            provider_name=self.provider_name,
            market_type=market_type,
            captured_at=captured_at,
        )

    def build_raw_capture_payload(
        self,
        event: dict[str, object],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, object],
    ):
        return sportsbook_capture._default_build_raw_capture_payload(
            event,
            sport=sport,
            event_identity=event_identity,
        )

    def build_event_record(
        self,
        event: dict[str, object],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, object],
    ):
        return sportsbook_capture._default_build_event_record(
            event,
            provider_name=self.provider_name,
            sport=sport,
            event_identity=event_identity,
        )

    def build_capture_metadata(
        self,
        event: dict[str, object],
        *,
        sport: str,
        market: str,
        captured_at: datetime,
    ):
        return sportsbook_capture._default_build_capture_metadata(
            event,
            provider_name=self.provider_name,
            sport=sport,
            market=market,
            captured_at=captured_at,
        )


class _HookedProviderSource:
    provider_name = "provider_beta"

    def __init__(self, events_or_errors):
        self._events_or_errors = list(events_or_errors)

    def fetch_upcoming(self, sport: str, market_type: str):
        value = self._events_or_errors.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def event_id(self, event: dict[str, object]) -> str:
        return str(event.get("external_id") or "")

    def normalize_event(
        self,
        event: dict[str, object],
        *,
        market_type: str,
        captured_at: datetime,
    ):
        quote_ts = str(event["provider_updated_at"])
        return [
            {
                "sportsbook_event_id": self.event_id(event),
                "source": "beta-book",
                "market_type": market_type,
                "selection": "Home Team",
                "price_decimal": 1.9,
                "implied_prob": 1 / 1.9,
                "overround": 0.0,
                "quote_ts": quote_ts,
                "source_age_ms": 15000,
                "raw_json": {"provider_quote_id": event["external_id"]},
                "provider": self.provider_name,
                "source_ts": quote_ts,
                "capture_ts": captured_at.isoformat(),
            }
        ]

    def build_raw_capture_payload(
        self,
        event: dict[str, object],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, object],
    ):
        payload = {
            "id": self.event_id(event),
            "sport_key": sport,
            "sport_title": event["league_name"],
            "home_team": event["home"],
            "away_team": event["away"],
            "commence_time": event["start_time"],
            "bookmakers": [
                {
                    "key": "beta-book",
                    "last_update": event["provider_updated_at"],
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": event["home"], "price": 1.9},
                                {"name": event["away"], "price": 2.1},
                            ],
                        }
                    ],
                }
            ],
        }
        for field in ("event_key", "game_id", "sport", "series"):
            if event_identity.get(field) not in (None, ""):
                payload[field] = event_identity[field]
        return payload

    def build_event_record(
        self,
        event: dict[str, object],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, object],
    ):
        raw_json = self.build_raw_capture_payload(
            event,
            sport=sport,
            market_type=market_type,
            event_identity=event_identity,
        )
        return sportsbook_capture.SportsbookEventRecord(
            sportsbook_event_id=self.event_id(event),
            source=self.provider_name,
            sport=sport,
            league=str(event["league_name"]),
            home_team=str(event["home"]),
            away_team=str(event["away"]),
            start_time=str(event["start_time"]),
            raw_json=raw_json,
        )

    def build_capture_metadata(
        self,
        event: dict[str, object],
        *,
        sport: str,
        market: str,
        captured_at: datetime,
    ):
        return {
            "provider": self.provider_name,
            "provider_event_id": event["external_id"],
            "sport": sport,
            "market": market,
            "capture_ts": captured_at.isoformat(),
            "source_ts_min": str(event["provider_updated_at"]),
            "source_ts_max": str(event["provider_updated_at"]),
        }


class SportsbookCaptureWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._postgres_env_patch = patch.dict(
            "os.environ",
            {
                "PREDICTION_MARKET_POSTGRES_DSN": "",
                "POSTGRES_DSN": "",
                "DATABASE_URL": "",
            },
        )
        self._postgres_env_patch.start()
        self.addCleanup(self._postgres_env_patch.stop)

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
            stores = SportsbookCaptureStores.from_root(root)
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

    def test_capture_once_appends_raw_capture_event_and_checkpoint(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        source_time = (capture_time - timedelta(seconds=30)).isoformat()
        raw_events: list[dict[str, object]] = []
        checkpoints: list[dict[str, object]] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
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
                    sportsbook_capture,
                    "append_raw_capture_event",
                    side_effect=lambda **kwargs: raw_events.append(kwargs) or kwargs,
                ),
                patch.object(
                    sportsbook_capture,
                    "upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: checkpoints.append(
                        {
                            "checkpoint_name": args[0],
                            "source_name": args[1],
                            "checkpoint_value": args[2],
                            "checkpoint_ts": kwargs.get("checkpoint_ts"),
                        }
                    )
                    or checkpoints[-1],
                ),
            ):
                payload = capture_sportsbook_odds_once(
                    SportsbookCaptureRequest(
                        root=str(root),
                        sport="basketball_nba",
                        market="h2h",
                        event_map_file=str(event_map),
                    ),
                    source=_StaticSource(
                        [[self._sample_event(last_update=source_time)]]
                    ),
                    stores=stores,
                    observed_at=capture_time,
                )

        self.assertTrue(payload["ok"])
        self.assertEqual(len(raw_events), 1)
        self.assertEqual(raw_events[0]["source"], "sportsbook")
        self.assertEqual(raw_events[0]["layer"], "odds_api")
        self.assertEqual(raw_events[0]["entity_type"], "sportsbook_odds_envelope")
        raw_payload = raw_events[0]["payload"]
        if not isinstance(raw_payload, dict):
            self.fail("expected raw sportsbook payload dict")
        self.assertEqual(raw_payload["event_key"], "event-1")
        self.assertEqual(raw_events[0]["captured_at"], capture_time)
        raw_metadata = raw_events[0]["metadata"]
        if not isinstance(raw_metadata, dict):
            self.fail("expected raw sportsbook metadata dict")
        self.assertEqual(raw_metadata["provider"], "theoddsapi")
        self.assertEqual(raw_metadata["capture_ts"], capture_time.isoformat())
        self.assertEqual(raw_metadata["source_ts_min"], source_time)
        self.assertEqual(raw_metadata["source_ts_max"], source_time)
        self.assertEqual(checkpoints[0]["checkpoint_name"], "sportsbook_odds")
        self.assertEqual(checkpoints[0]["source_name"], "theoddsapi")
        self.assertEqual(checkpoints[0]["checkpoint_value"], source_time)

    def test_capture_once_supports_provider_specific_source_hooks(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        source_time = (capture_time - timedelta(seconds=30)).isoformat()
        raw_events: list[dict[str, object]] = []
        checkpoints: list[dict[str, object]] = []

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            event_map = Path(temp_dir) / "event_map.json"
            self._write_json(
                event_map,
                {
                    "beta-1": {
                        "event_key": "event-beta-1",
                        "game_id": "game-beta-1",
                        "sport": "nba",
                        "series": "playoffs",
                    }
                },
            )
            source = _HookedProviderSource(
                [
                    [
                        {
                            "external_id": "beta-1",
                            "league_name": "NBA",
                            "home": "Home Team",
                            "away": "Away Team",
                            "start_time": "2026-04-21T20:00:00+00:00",
                            "provider_updated_at": source_time,
                        }
                    ]
                ]
            )

            with (
                patch.object(
                    sportsbook_capture,
                    "append_raw_capture_event",
                    side_effect=lambda **kwargs: raw_events.append(kwargs) or kwargs,
                ),
                patch.object(
                    sportsbook_capture,
                    "upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: checkpoints.append(
                        {
                            "checkpoint_name": args[0],
                            "source_name": args[1],
                            "checkpoint_value": args[2],
                            "checkpoint_ts": kwargs.get("checkpoint_ts"),
                        }
                    )
                    or checkpoints[-1],
                ),
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

        self.assertTrue(payload["ok"])
        self.assertEqual(raw_events[0]["source"], "sportsbook")
        self.assertEqual(raw_events[0]["layer"], "odds_api")
        self.assertEqual(raw_events[0]["entity_type"], "sportsbook_odds_envelope")
        raw_metadata = raw_events[0]["metadata"]
        if not isinstance(raw_metadata, dict):
            self.fail("expected custom provider metadata dict")
        self.assertEqual(raw_metadata["provider"], "provider_beta")
        self.assertEqual(raw_metadata["provider_event_id"], "beta-1")
        raw_payload = raw_events[0]["payload"]
        if not isinstance(raw_payload, dict):
            self.fail("expected projector-compatible raw payload dict")
        self.assertEqual(raw_payload["id"], "beta-1")
        self.assertEqual(raw_payload["home_team"], "Home Team")
        bookmakers = raw_payload.get("bookmakers")
        if not isinstance(bookmakers, list) or not bookmakers:
            self.fail("expected projector-compatible bookmaker list")
        bookmaker = bookmakers[0]
        if not isinstance(bookmaker, dict):
            self.fail("expected projector-compatible bookmaker dict")
        self.assertEqual(bookmaker["last_update"], source_time)
        self.assertEqual(checkpoints[0]["checkpoint_name"], "sportsbook_odds")
        self.assertEqual(checkpoints[0]["source_name"], "provider_beta")
        self.assertIn("beta-1", postgres_events)
        self.assertEqual(postgres_events["beta-1"]["source"], "provider_beta")
        self.assertEqual(
            postgres_events["beta-1"]["raw_json"]["event_key"], "event-beta-1"
        )

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

    def test_capture_once_skips_current_exports_when_postgres_authoritative(self):
        capture_time = datetime(2026, 4, 21, 18, 5, tzinfo=timezone.utc)
        source_time = (capture_time - timedelta(seconds=30)).isoformat()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
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

            with patch(
                "storage.current_state_materializers.resolve_postgres_dsn",
                return_value="postgresql://user:pass@localhost:5432/db",
            ):
                payload = capture_sportsbook_odds_once(
                    SportsbookCaptureRequest(
                        root=str(root),
                        sport="basketball_nba",
                        market="h2h",
                        event_map_file=str(event_map),
                    ),
                    source=_StaticSource(
                        [[self._sample_event(last_update=source_time)]]
                    ),
                    stores=stores,
                    observed_at=capture_time,
                )

            postgres_health = json.loads(
                (root / "postgres" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(postgres_health["sportsbook_odds"]["status"], "ok")
        self.assertFalse((root / "current" / "sportsbook_events.json").exists())
        self.assertFalse((root / "current" / "sportsbook_odds.json").exists())
        self.assertFalse((root / "current" / "source_health.json").exists())

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
                (root / "postgres" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertEqual(len(results), 2)
        self.assertFalse(results[0]["ok"])
        self.assertTrue(results[1]["ok"])
        self.assertEqual(sleeps, [7.5])
        self.assertEqual(health["sportsbook_odds"]["status"], "ok")
        self.assertIsNotNone(health["sportsbook_odds"]["last_success_at"])
        self.assertFalse((root / "postgres" / "sportsbook_events.json").exists())
        self.assertFalse((root / "postgres" / "sportsbook_odds.json").exists())

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
                (root / "postgres" / "source_health.json").read_text(encoding="utf-8")
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

    def test_ingest_sportsbook_odds_command_is_retired(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                result = ingest_live_data.main(
                    [
                        "sportsbook-odds",
                        "--sport",
                        "basketball_nba",
                        "--market",
                        "h2h",
                        "--root",
                        str(root),
                    ]
                )
            payload = json.loads(stdout.getvalue())

        self.assertEqual(result, 1)
        self.assertEqual(payload["error_kind"], "RuntimeError")
        self.assertEqual(
            payload["error_message"],
            "ingest-live-data sportsbook-odds is retired; use run-sportsbook-capture and run-current-projection for the sanctioned sportsbook capture path",
        )
        self.assertEqual(payload["root"], str(root))

    def test_run_sportsbook_capture_main_executes_worker_cycle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            with (
                patch.object(
                    run_sportsbook_capture.TheOddsApiCaptureSource,
                    "fetch_upcoming",
                    return_value=[
                        self._sample_event(last_update="2026-04-21T18:04:30+00:00")
                    ],
                ),
                patch(
                    "scripts.run_sportsbook_capture.SportsbookCaptureStores.from_root",
                    return_value=stores,
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

            raw_files = list((root / "raw").rglob("*.jsonl.gz"))

        self.assertEqual(result, 0)
        self.assertTrue(raw_files)
        self.assertFalse((root / "postgres" / "sportsbook_events.json").exists())
        self.assertFalse((root / "postgres" / "sportsbook_odds.json").exists())
        self.assertFalse((root / "current" / "sportsbook_events.json").exists())
        self.assertFalse((root / "current" / "sportsbook_odds.json").exists())
        self.assertFalse((root / "current" / "source_health.json").exists())

    def test_run_sportsbook_capture_main_supports_json_feed_provider(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            with (
                patch.object(
                    SportsbookJsonFeedCaptureSource,
                    "fetch_upcoming",
                    return_value=[
                        {
                            "external_id": "feed-1",
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
                    ],
                ),
                patch(
                    "scripts.run_sportsbook_capture.SportsbookCaptureStores.from_root",
                    return_value=stores,
                ),
            ):
                result = run_sportsbook_capture.main(
                    [
                        "--provider",
                        "json_feed",
                        "--provider-url",
                        "https://93.184.216.34/feed.json",
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

            raw_files = list((root / "raw").rglob("*.jsonl.gz"))

        self.assertEqual(result, 0)
        self.assertTrue(raw_files)
        self.assertFalse((root / "postgres" / "sportsbook_events.json").exists())
        self.assertFalse((root / "postgres" / "sportsbook_odds.json").exists())
        self.assertFalse((root / "current" / "sportsbook_events.json").exists())
        self.assertFalse((root / "current" / "sportsbook_odds.json").exists())

    def test_run_sportsbook_capture_main_requires_provider_url_for_json_feed(self):
        with self.assertRaisesRegex(RuntimeError, "requires --provider-url"):
            run_sportsbook_capture.main(
                [
                    "--provider",
                    "json_feed",
                    "--sport",
                    "basketball_nba",
                    "--market",
                    "h2h",
                    "--root",
                    "runtime/data",
                    "--max-cycles",
                    "1",
                ]
            )

    def test_worker_does_not_materialize_selector_facing_current_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            worker = SportsbookCaptureWorker(
                source=_StaticSource(
                    [[self._sample_event(last_update="2026-04-21T18:04:30+00:00")]]
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

        self.assertTrue(results[-1]["ok"])
        self.assertFalse((root / "postgres" / "sportsbook_events.json").exists())
        self.assertFalse((root / "postgres" / "sportsbook_odds.json").exists())
        self.assertFalse((root / "current" / "sportsbook_events.json").exists())
        self.assertFalse((root / "current" / "sportsbook_odds.json").exists())

    def test_run_sportsbook_capture_main_requires_postgres_setup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            with patch.dict(
                "os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
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
                        ]
                    )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 1)
        self.assertEqual(payload["error_kind"], "PostgresDsnNotConfiguredError")
        self.assertEqual(
            payload["error_message"],
            "Postgres worker storage is not configured",
        )

    def test_run_sportsbook_capture_main_handles_runtime_storage_failures_cleanly(self):
        class _BrokenHealthRepo:
            def read_all(self):
                return {}

            def upsert(self, key, row):
                raise RuntimeError("psycopg is required for Postgres storage")

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = SportsbookCaptureStores.from_root(root)
            broken_stores = SportsbookCaptureStores(
                raw=stores.raw,
                parquet=stores.parquet,
                current=stores.current,
                sportsbook_events=stores.sportsbook_events,
                sportsbook_odds=stores.sportsbook_odds,
                current_health=stores.current_health,
                postgres_health=_BrokenHealthRepo(),
            )
            with (
                patch.object(
                    run_sportsbook_capture.TheOddsApiCaptureSource,
                    "fetch_upcoming",
                    return_value=[
                        self._sample_event(last_update="2026-04-21T18:04:30+00:00")
                    ],
                ),
                patch(
                    "scripts.run_sportsbook_capture.SportsbookCaptureStores.from_root",
                    return_value=broken_stores,
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
            ):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
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
                        ]
                    )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 1)
        self.assertEqual(payload["error_kind"], "RuntimeError")
        self.assertEqual(
            payload["error_message"],
            "RuntimeError during sportsbook capture",
        )
        self.assertEqual(payload["health_error_kind"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()

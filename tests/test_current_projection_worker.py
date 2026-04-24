from __future__ import annotations

import importlib
import json
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.projection import CurrentProjectionStores, project_current_state_once
from storage import FileBackedCurrentStateStore, SourceHealthStore


class _MemoryKeyedRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}

    def upsert(self, key: str, row) -> dict[str, object]:
        payload = row if isinstance(row, dict) else row.__dict__.copy()
        self.rows[str(key)] = dict(payload)
        return dict(payload)

    def read_all(self) -> dict[str, object]:
        return dict(self.rows)

    def read_current(self) -> dict[str, object]:
        return self.read_all()


class _MemoryAppendRepo:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def append(self, row) -> dict[str, object]:
        payload = row if isinstance(row, dict) else row.__dict__.copy()
        self.rows.append(dict(payload))
        return dict(payload)

    def read_all(self) -> dict[str, object]:
        return {str(index): dict(payload) for index, payload in enumerate(self.rows)}

    def read_current(self) -> dict[str, object]:
        current: dict[str, object] = {}
        for payload in self.rows:
            key = "|".join(
                [
                    str(payload.get("sportsbook_event_id") or ""),
                    str(payload.get("source") or ""),
                    str(payload.get("market_type") or ""),
                    str(payload.get("selection") or ""),
                ]
            )
            current[key] = dict(payload)
        return current


class CurrentProjectionWorkerTests(unittest.TestCase):
    @staticmethod
    def _cli_module():
        return importlib.import_module("scripts.run_current_projection")

    def test_project_current_state_once_projects_raw_capture_lanes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = CurrentProjectionStores(
                root=root,
                current=FileBackedCurrentStateStore(root / "current"),
                current_health=SourceHealthStore(
                    root / "current" / "source_health.json"
                ),
                markets=_MemoryKeyedRepo(),
                bbo=_MemoryKeyedRepo(),
                sportsbook_events=_MemoryKeyedRepo(),
                sportsbook_odds=_MemoryAppendRepo(),
                source_health=_MemoryKeyedRepo(),
            )

            def _raw_events(*, source=None, layer=None, **_kwargs):
                if source == "sportsbook" and layer == "odds_api":
                    return [
                        {
                            "capture_id": 1,
                            "source": source,
                            "layer": layer,
                            "entity_type": "sportsbook_odds_envelope",
                            "entity_key": "sb-1",
                            "operation": "append",
                            "captured_at": "2026-04-21T18:00:00+00:00",
                            "payload": {
                                "id": "sb-1",
                                "sport_key": "basketball_nba",
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "commence_time": "2026-04-21T20:00:00+00:00",
                                "event_key": "event-1",
                                "game_id": "game-1",
                                "bookmakers": [
                                    {
                                        "key": "book-a",
                                        "last_update": "2026-04-21T17:59:30+00:00",
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
                            },
                            "metadata": {
                                "provider": "theoddsapi",
                                "sport": "basketball_nba",
                                "market": "h2h",
                            },
                        }
                    ]
                if source == "polymarket" and layer == "market_catalog":
                    return [
                        {
                            "capture_id": 2,
                            "source": source,
                            "layer": layer,
                            "entity_type": "market_catalog_snapshot",
                            "entity_key": "nba",
                            "operation": "snapshot",
                            "captured_at": "2026-04-21T18:00:01+00:00",
                            "payload": {
                                "markets": [
                                    {
                                        "id": "pm-1",
                                        "conditionId": "pm-1",
                                        "question": "Will Home Team beat Away Team?",
                                        "active": True,
                                        "tokenIds": ["yes-token", "no-token"],
                                    }
                                ]
                            },
                            "metadata": {},
                        }
                    ]
                if source == "polymarket" and layer == "market_channel":
                    return [
                        {
                            "capture_id": 3,
                            "source": source,
                            "layer": layer,
                            "entity_type": "market_stream_envelope",
                            "entity_key": None,
                            "operation": "append",
                            "captured_at": "2026-04-21T18:00:02+00:00",
                            "payload": {
                                "asset_id": "pm-1",
                                "best_bid": 0.45,
                                "best_bid_size": 10,
                                "best_ask": 0.47,
                                "best_ask_size": 8,
                                "timestamp": "2026-04-21T18:00:02Z",
                            },
                            "metadata": {},
                        }
                    ]
                return []

            with (
                patch(
                    "services.projection.current_state.CurrentProjectionStores.from_root",
                    return_value=stores,
                ),
                patch(
                    "services.projection.current_state.list_raw_capture_events",
                    side_effect=_raw_events,
                ),
                patch(
                    "services.projection.current_state.read_capture_checkpoint",
                    return_value=None,
                ),
                patch(
                    "services.projection.current_state.upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: {
                        "checkpoint_name": args[0],
                        "source_name": args[1],
                        "checkpoint_value": args[2],
                    },
                ),
            ):
                result = project_current_state_once(root)

            sportsbook_events = json.loads(
                (root / "current" / "sportsbook_events.json").read_text(
                    encoding="utf-8"
                )
            )
            sportsbook_odds = json.loads(
                (root / "current" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )
            polymarket_markets = json.loads(
                (root / "current" / "polymarket_markets.json").read_text(
                    encoding="utf-8"
                )
            )
            polymarket_bbo = json.loads(
                (root / "current" / "polymarket_bbo.json").read_text(encoding="utf-8")
            )
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["lanes"]["sportsbook"]["event_count"], 1)
        self.assertEqual(result["lanes"]["polymarket_markets"]["row_count"], 1)
        self.assertEqual(result["lanes"]["polymarket_bbo"]["row_count"], 1)
        self.assertIn("sb-1", sportsbook_events)
        self.assertIn("sb-1|book-a|h2h|Home Team", sportsbook_odds)
        self.assertIn("pm-1", polymarket_markets)
        self.assertIn("pm-1", polymarket_bbo)
        self.assertIn("projection_sportsbook_odds", source_health)
        self.assertEqual(
            source_health["projection_polymarket_market_catalog"]["status"], "ok"
        )

    def test_project_current_state_once_replays_canonical_json_feed_sportsbook_rows(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = CurrentProjectionStores(
                root=root,
                current=FileBackedCurrentStateStore(root / "current"),
                current_health=SourceHealthStore(
                    root / "current" / "source_health.json"
                ),
                markets=_MemoryKeyedRepo(),
                bbo=_MemoryKeyedRepo(),
                sportsbook_events=_MemoryKeyedRepo(),
                sportsbook_odds=_MemoryAppendRepo(),
                source_health=_MemoryKeyedRepo(),
            )

            with (
                patch(
                    "services.projection.current_state.CurrentProjectionStores.from_root",
                    return_value=stores,
                ),
                patch(
                    "services.projection.current_state.list_raw_capture_events",
                    return_value=[
                        {
                            "capture_id": 1,
                            "source": "sportsbook",
                            "layer": "odds_api",
                            "entity_type": "sportsbook_odds_envelope",
                            "entity_key": "feed-1",
                            "operation": "append",
                            "captured_at": "2026-04-21T18:05:00+00:00",
                            "payload": {
                                "id": "feed-1",
                                "provider_event_id": "feed-1",
                                "sport_key": "basketball_nba",
                                "sport_title": "NBA",
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "commence_time": "2026-04-21T20:00:00+00:00",
                                "event_key": "event-feed-1",
                                "game_id": "game-feed-1",
                                "bookmakers": [
                                    {
                                        "key": "alt-book",
                                        "title": "alt-book",
                                        "last_update": "2026-04-21T18:04:30+00:00",
                                        "markets": [
                                            {
                                                "key": "h2h",
                                                "outcomes": [
                                                    {"name": "Home Team", "price": 2.0},
                                                    {"name": "Away Team", "price": 2.1},
                                                ],
                                            }
                                        ],
                                    }
                                ],
                            },
                            "metadata": {
                                "provider": "json_feed",
                                "sport": "basketball_nba",
                                "market": "h2h",
                                "capture_ts": "2026-04-21T18:05:00+00:00",
                            },
                        }
                    ],
                ),
                patch(
                    "services.projection.current_state.read_capture_checkpoint",
                    return_value=None,
                ),
                patch(
                    "services.projection.current_state.upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: {
                        "checkpoint_name": args[0],
                        "source_name": args[1],
                        "checkpoint_value": args[2],
                    },
                ),
            ):
                result = project_current_state_once(root)

            sportsbook_events = json.loads(
                (root / "current" / "sportsbook_events.json").read_text(
                    encoding="utf-8"
                )
            )
            sportsbook_odds = json.loads(
                (root / "current" / "sportsbook_odds.json").read_text(encoding="utf-8")
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["lanes"]["sportsbook"]["event_count"], 1)
        self.assertEqual(sportsbook_events["feed-1"]["provider_event_id"], "feed-1")
        self.assertEqual(sportsbook_events["feed-1"]["event_key"], "event-feed-1")
        projected_quote = sportsbook_odds["feed-1|alt-book|h2h|Home Team"]
        self.assertEqual(projected_quote["provider"], "json_feed")
        self.assertEqual(projected_quote["source_ts"], "2026-04-21T18:04:30+00:00")
        self.assertEqual(projected_quote["capture_ts"], "2026-04-21T18:05:00+00:00")
        self.assertEqual(projected_quote["price_decimal"], 2.0)

    def test_run_current_projection_main_returns_latest_worker_payload(self):
        with patch(
            "scripts.run_current_projection.CurrentProjectionWorker.run",
            return_value=[{"ok": True, "lanes": {}}],
        ):
            result = self._cli_module().main(["--quiet"])

        self.assertEqual(result, 0)

    def test_project_current_state_once_preserves_builder_owned_source_health_rows(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = CurrentProjectionStores(
                root=root,
                current=FileBackedCurrentStateStore(root / "current"),
                current_health=SourceHealthStore(
                    root / "current" / "source_health.json"
                ),
                markets=_MemoryKeyedRepo(),
                bbo=_MemoryKeyedRepo(),
                sportsbook_events=_MemoryKeyedRepo(),
                sportsbook_odds=_MemoryAppendRepo(),
                source_health=_MemoryKeyedRepo(),
            )
            stores.current_health.write_all(
                {
                    "fair_values": {
                        "source_name": "fair_values",
                        "status": "ok",
                        "last_seen_at": "2026-04-21T18:00:00+00:00",
                        "last_success_at": "2026-04-21T18:00:00+00:00",
                        "stale_after_ms": 60_000,
                        "details": {"row_count": 1},
                    }
                }
            )
            stores.source_health.upsert(
                "sportsbook_odds",
                {
                    "source_name": "sportsbook_odds",
                    "status": "ok",
                    "last_seen_at": "2026-04-21T18:00:00+00:00",
                    "last_success_at": "2026-04-21T18:00:00+00:00",
                    "stale_after_ms": 60_000,
                    "details": {"row_count": 2},
                },
            )

            with (
                patch(
                    "services.projection.current_state.CurrentProjectionStores.from_root",
                    return_value=stores,
                ),
                patch(
                    "services.projection.current_state.list_raw_capture_events",
                    return_value=[],
                ),
                patch(
                    "services.projection.current_state.read_capture_checkpoint",
                    return_value=None,
                ),
            ):
                result = project_current_state_once(root)

            source_health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertTrue(result["ok"])
        self.assertIn("fair_values", source_health)
        self.assertEqual(source_health["fair_values"]["details"]["row_count"], 1)
        self.assertIn("sportsbook_odds", source_health)
        self.assertIn("projection_sportsbook_odds", source_health)

    def test_project_current_state_ignores_repository_audit_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stores = CurrentProjectionStores(
                root=root,
                current=FileBackedCurrentStateStore(root / "current"),
                current_health=SourceHealthStore(
                    root / "current" / "source_health.json"
                ),
                markets=_MemoryKeyedRepo(),
                bbo=_MemoryKeyedRepo(),
                sportsbook_events=_MemoryKeyedRepo(),
                sportsbook_odds=_MemoryAppendRepo(),
                source_health=_MemoryKeyedRepo(),
            )

            with (
                patch(
                    "services.projection.current_state.CurrentProjectionStores.from_root",
                    return_value=stores,
                ),
                patch(
                    "services.projection.current_state.list_raw_capture_events",
                    return_value=[
                        {
                            "capture_id": 1,
                            "source": "polymarket",
                            "layer": "market_channel",
                            "entity_type": "market_stream_envelope",
                            "entity_key": None,
                            "operation": "append",
                            "captured_at": "2026-04-21T18:00:02+00:00",
                            "payload": {
                                "asset_id": "pm-1",
                                "best_bid": 0.45,
                                "best_bid_size": 10,
                                "best_ask": 0.47,
                                "best_ask_size": 8,
                                "timestamp": "2026-04-21T18:00:02Z",
                            },
                            "metadata": {},
                        },
                        {
                            "capture_id": 2,
                            "source": "polymarket",
                            "layer": "market_channel",
                            "entity_type": "polymarket_bbo",
                            "entity_key": "pm-1",
                            "operation": "upsert",
                            "captured_at": "2026-04-21T18:00:03+00:00",
                            "payload": {
                                "market_id": "pm-1",
                                "best_bid_yes": None,
                                "best_ask_yes": None,
                                "book_ts": "2026-04-21T18:00:03+00:00",
                                "source_age_ms": 0,
                            },
                            "metadata": {},
                        },
                    ],
                ),
                patch(
                    "services.projection.current_state.read_capture_checkpoint",
                    return_value=None,
                ),
                patch(
                    "services.projection.current_state.upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: {
                        "checkpoint_name": args[0],
                        "source_name": args[1],
                        "checkpoint_value": args[2],
                    },
                ),
            ):
                result = project_current_state_once(root, max_events_per_lane=10)

        self.assertEqual(result["lanes"]["polymarket_bbo"]["event_count"], 1)
        bbo_row = stores.bbo.read_all()["pm-1"]
        if not isinstance(bbo_row, dict):
            self.fail("expected projected polymarket bbo row dict")
        self.assertEqual(bbo_row["best_bid_yes"], 0.45)

    def test_run_current_projection_main_returns_nonzero_on_failed_payload(self):
        stdout = io.StringIO()
        with (
            patch(
                "scripts.run_current_projection.CurrentProjectionWorker.run",
                return_value=[{"ok": False, "root": "runtime/data"}],
            ),
            patch("sys.stdout", stdout),
        ):
            result = self._cli_module().main([])

        self.assertEqual(result, 1)
        self.assertIn('"ok": false', stdout.getvalue().lower())

    def test_run_current_projection_main_reports_runtime_error(self):
        stdout = io.StringIO()
        with (
            patch(
                "scripts.run_current_projection.CurrentProjectionWorker.run",
                side_effect=RuntimeError("Postgres DSN not configured"),
            ),
            patch("sys.stdout", stdout),
        ):
            result = self._cli_module().main([])

        self.assertEqual(result, 1)
        self.assertIn('"error_kind": "RuntimeError"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()

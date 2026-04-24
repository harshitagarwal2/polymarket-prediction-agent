from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from services.projection.current_state import (
    CurrentProjectionStores,
    project_current_state_once,
)
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


class ProjectionAtomicityTests(unittest.TestCase):
    def test_projection_does_not_advance_checkpoint_when_materialization_fails(self):
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
            checkpoint_calls: list[tuple[str, str, str]] = []

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
                            "entity_key": "sb-1",
                            "operation": "append",
                            "captured_at": "2026-04-21T18:00:00+00:00",
                            "payload": {
                                "id": "sb-1",
                                "sport_key": "basketball_nba",
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "commence_time": "2026-04-21T20:00:00+00:00",
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
                    ],
                ),
                patch(
                    "services.projection.current_state.read_capture_checkpoint",
                    return_value=None,
                ),
                patch(
                    "services.projection.current_state.upsert_capture_checkpoint",
                    side_effect=lambda *args, **kwargs: checkpoint_calls.append(
                        (args[0], args[1], args[2])
                    )
                    or {
                        "checkpoint_name": args[0],
                        "source_name": args[1],
                        "checkpoint_value": args[2],
                    },
                ),
                patch(
                    "services.projection.current_state.materialize_current_compatibility_tables",
                    side_effect=RuntimeError("materialization failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "materialization failed"):
                    project_current_state_once(root)

        self.assertEqual(checkpoint_calls, [])
        health_row = cast(
            dict[str, object],
            stores.source_health.read_current()["projection_sportsbook_odds"],
        )
        self.assertEqual(health_row["status"], "red")
        self.assertEqual(
            cast(dict[str, object], health_row["details"])["error_kind"],
            "RuntimeError",
        )


if __name__ == "__main__":
    unittest.main()

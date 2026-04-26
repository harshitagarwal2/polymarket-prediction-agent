from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import cast
from unittest.mock import patch

from services.projection.current_state import (
    CurrentProjectionStores,
    SPORTSBOOK_PROJECTION_CHECKPOINT,
    _project_lane,
    _project_polymarket_account_snapshot_events,
    _project_sportsbook_capture_events,
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


class _MemoryReplaceRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}

    def replace_all(self, rows: dict[str, object]) -> None:
        self.rows = {
            str(key): dict(value)
            for key, value in rows.items()
            if isinstance(value, dict)
        }

    def read_all(self) -> dict[str, object]:
        return dict(self.rows)

    def read_current(self) -> dict[str, object]:
        return self.read_all()


class _RecordingConnection:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class _ConnectionAwareKeyedRepo(_MemoryKeyedRepo):
    def __init__(self, connection: _RecordingConnection | None = None) -> None:
        super().__init__()
        self._connection = connection
        self.connections: list[object | None] = []

    def _connect(self) -> _RecordingConnection:
        if self._connection is None:
            raise RuntimeError("connection not configured")
        return self._connection

    def upsert(self, key: str, row, *, connection=None) -> dict[str, object]:
        self.connections.append(connection)
        return super().upsert(key, row)

    def read_all(self, *, connection=None) -> dict[str, object]:
        self.connections.append(connection)
        return super().read_all()

    def read_current(self, *, connection=None) -> dict[str, object]:
        self.connections.append(connection)
        return super().read_current()


class _ConnectionAwareAppendRepo(_MemoryAppendRepo):
    def __init__(self) -> None:
        super().__init__()
        self.connections: list[object | None] = []

    def append(self, row, *, connection=None) -> dict[str, object]:
        self.connections.append(connection)
        return super().append(row)

    def read_all(self, *, connection=None) -> dict[str, object]:
        self.connections.append(connection)
        return super().read_all()

    def read_current(self, *, connection=None) -> dict[str, object]:
        self.connections.append(connection)
        return super().read_current()


class ProjectionAtomicityTests(unittest.TestCase):
    def test_projection_lane_commits_shared_connection_after_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            connection = _RecordingConnection()
            source_health = _ConnectionAwareKeyedRepo(connection)
            sportsbook_events = _ConnectionAwareKeyedRepo()
            sportsbook_odds = _ConnectionAwareAppendRepo()
            stores = CurrentProjectionStores(
                root=root,
                current=FileBackedCurrentStateStore(root / "current"),
                current_health=SourceHealthStore(root / "current" / "source_health.json"),
                markets=_MemoryKeyedRepo(),
                bbo=_MemoryKeyedRepo(),
                sportsbook_events=sportsbook_events,
                sportsbook_odds=sportsbook_odds,
                source_health=source_health,
            )
            checkpoint_connections: list[object | None] = []

            with (
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
                    side_effect=lambda *args, **kwargs: checkpoint_connections.append(
                        kwargs.get("connection")
                    )
                    or {
                        "checkpoint_name": args[0],
                        "source_name": args[1],
                        "checkpoint_value": args[2],
                    },
                ),
            ):
                result = _project_lane(
                    stores,
                    checkpoint_name=SPORTSBOOK_PROJECTION_CHECKPOINT,
                    source="sportsbook",
                    layer="odds_api",
                    entity_types=("sportsbook_odds_envelope",),
                    processor=_project_sportsbook_capture_events,
                    materialize_tables=("sportsbook_events", "sportsbook_odds", "source_health"),
                    max_events=10,
                )

        self.assertEqual(result["event_count"], 1)
        self.assertEqual(connection.commit_calls, 1)
        self.assertEqual(connection.rollback_calls, 0)
        self.assertEqual(connection.close_calls, 1)
        self.assertEqual(checkpoint_connections, [connection])
        self.assertIn(connection, sportsbook_events.connections)
        self.assertIn(connection, sportsbook_odds.connections)
        self.assertIn(connection, source_health.connections)

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

    def test_projection_lane_rolls_back_shared_connection_when_materialization_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            connection = _RecordingConnection()
            source_health = _ConnectionAwareKeyedRepo(connection)
            stores = CurrentProjectionStores(
                root=root,
                current=FileBackedCurrentStateStore(root / "current"),
                current_health=SourceHealthStore(root / "current" / "source_health.json"),
                markets=_MemoryKeyedRepo(),
                bbo=_MemoryKeyedRepo(),
                sportsbook_events=_ConnectionAwareKeyedRepo(),
                sportsbook_odds=_ConnectionAwareAppendRepo(),
                source_health=source_health,
            )

            with (
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
                    side_effect=AssertionError("checkpoint should not be written"),
                ),
                patch(
                    "services.projection.current_state.materialize_current_compatibility_tables",
                    side_effect=RuntimeError("materialization failed"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "materialization failed"):
                    _project_lane(
                        stores,
                        checkpoint_name=SPORTSBOOK_PROJECTION_CHECKPOINT,
                        source="sportsbook",
                        layer="odds_api",
                        entity_types=("sportsbook_odds_envelope",),
                        processor=_project_sportsbook_capture_events,
                        materialize_tables=(
                            "sportsbook_events",
                            "sportsbook_odds",
                            "source_health",
                        ),
                        max_events=10,
                    )

        self.assertEqual(connection.commit_calls, 0)
        self.assertEqual(connection.rollback_calls, 1)
        self.assertEqual(connection.close_calls, 1)
        self.assertIn(None, source_health.connections)

    def test_account_projection_replaces_tables_with_single_snapshot_cohort(self):
        order_repo = _MemoryReplaceRepo()
        fill_repo = _MemoryReplaceRepo()
        position_repo = _MemoryReplaceRepo()
        balance_repo = _MemoryReplaceRepo()
        stores = CurrentProjectionStores(
            root=Path("runtime/data"),
            current=FileBackedCurrentStateStore(Path("runtime/data/current")),
            current_health=SourceHealthStore(
                Path("runtime/data/current/source_health.json")
            ),
            markets=_MemoryKeyedRepo(),
            bbo=_MemoryKeyedRepo(),
            sportsbook_events=_MemoryKeyedRepo(),
            sportsbook_odds=_MemoryAppendRepo(),
            source_health=_MemoryKeyedRepo(),
            polymarket_orders=order_repo,
            polymarket_fills=fill_repo,
            polymarket_positions=position_repo,
            polymarket_balance=balance_repo,
        )
        events = [
            {
                "capture_id": 7,
                "payload": {
                    "observed_at": "2026-04-21T18:00:00+00:00",
                    "complete": True,
                    "issues": [],
                    "balance": {
                        "venue": "polymarket",
                        "available": 100.0,
                        "total": 100.0,
                        "currency": "USDC",
                    },
                    "open_orders": [
                        {
                            "order_id": "order-1",
                            "contract": {
                                "venue": "polymarket",
                                "symbol": "asset-1",
                                "outcome": "yes",
                                "title": None,
                            },
                            "action": "buy",
                            "price": 0.45,
                            "quantity": 2.0,
                            "remaining_quantity": 2.0,
                            "status": "resting",
                        }
                    ],
                    "positions": [
                        {
                            "contract": {
                                "venue": "polymarket",
                                "symbol": "asset-1",
                                "outcome": "yes",
                                "title": None,
                            },
                            "quantity": 1.0,
                            "average_price": 0.44,
                            "mark_price": 0.46,
                        }
                    ],
                    "fills": [
                        {
                            "order_id": "order-1",
                            "contract": {
                                "venue": "polymarket",
                                "symbol": "asset-1",
                                "outcome": "yes",
                                "title": None,
                            },
                            "action": "buy",
                            "price": 0.45,
                            "quantity": 0.5,
                            "fee": 0.0,
                            "fill_id": "fill-1",
                        }
                    ],
                },
            }
        ]

        event_count, row_count = _project_polymarket_account_snapshot_events(
            events, stores
        )

        self.assertEqual(event_count, 1)
        self.assertEqual(row_count, 4)
        cohort_ids = {
            str(payload["snapshot_cohort_id"])
            for table in (
                balance_repo.read_all(),
                order_repo.read_all(),
                position_repo.read_all(),
                fill_repo.read_all(),
            )
            for payload in table.values()
            if isinstance(payload, dict)
        }
        self.assertEqual(len(cohort_ids), 1)


if __name__ == "__main__":
    unittest.main()

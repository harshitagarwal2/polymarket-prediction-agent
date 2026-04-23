from __future__ import annotations

import io
import json
import tempfile
import unittest
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from research.datasets import DatasetRegistry
from services.capture import sportsbook as sportsbook_capture
from scripts import ingest_live_data


def _fake_repo_payload(row: Any) -> dict[str, Any]:
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    if isinstance(row, dict):
        return dict(row)
    raise TypeError("row must be a dataclass instance or dict")


class _FakeJsonRepository:
    table_name = "table"

    def __init__(self, root: str | Path = "runtime/data/postgres") -> None:
        self.root = Path(root)

    @property
    def path(self) -> Path:
        return self.root / f"{self.table_name}.json"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _fake_repo_payload(row)
        existing = self.read_all()
        existing[str(key)] = payload
        self.write_all(existing)
        return payload

    def write_all(self, rows: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8"
        )

    def read_all(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))


class _FakeMarketRepository(_FakeJsonRepository):
    table_name = "polymarket_markets"


class _FakeBBORepository(_FakeJsonRepository):
    table_name = "polymarket_bbo"


class _FakeSportsbookEventRepository(_FakeJsonRepository):
    table_name = "sportsbook_events"


class _FakeSportsbookOddsRepository(_FakeJsonRepository):
    table_name = "sportsbook_odds"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _fake_repo_payload(row)
        existing = self.read_all()
        existing[str(len(existing))] = payload
        self.write_all(existing)
        return payload


class _FakeMappingRepository(_FakeJsonRepository):
    table_name = "market_mappings"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _fake_repo_payload(row)
        existing = self.read_all()
        existing[str(len(existing))] = payload
        self.write_all(existing)
        return payload


class _FakeFairValueRepository(_FakeJsonRepository):
    table_name = "fair_values"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _fake_repo_payload(row)
        key = "|".join(
            [
                str(payload["market_id"]),
                str(payload["as_of"]),
                str(payload["model_name"]),
                str(payload["model_version"]),
            ]
        )
        return self.upsert(key, payload)


class _FakeOpportunityRepository(_FakeJsonRepository):
    table_name = "opportunities"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _fake_repo_payload(row)
        key = "|".join(
            [
                str(payload["market_id"]),
                str(payload["as_of"]),
                str(payload["side"]),
            ]
        )
        return self.upsert(key, payload)


class _FakeSourceHealthRepository(_FakeJsonRepository):
    table_name = "source_health"


class IngestLiveDataTests(unittest.TestCase):
    def setUp(self) -> None:
        patchers = [
            patch.object(ingest_live_data, "MarketRepository", _FakeMarketRepository),
            patch.object(ingest_live_data, "BBORepository", _FakeBBORepository),
            patch.object(
                ingest_live_data,
                "SportsbookEventRepository",
                _FakeSportsbookEventRepository,
            ),
            patch.object(
                ingest_live_data,
                "SportsbookOddsRepository",
                _FakeSportsbookOddsRepository,
            ),
            patch.object(ingest_live_data, "MappingRepository", _FakeMappingRepository),
            patch.object(
                ingest_live_data,
                "FairValueRepository",
                _FakeFairValueRepository,
            ),
            patch.object(
                ingest_live_data,
                "OpportunityRepository",
                _FakeOpportunityRepository,
            ),
            patch.object(
                ingest_live_data,
                "SourceHealthRepository",
                _FakeSourceHealthRepository,
            ),
            patch.object(
                sportsbook_capture,
                "SportsbookEventRepository",
                _FakeSportsbookEventRepository,
            ),
            patch.object(
                sportsbook_capture,
                "SportsbookOddsRepository",
                _FakeSportsbookOddsRepository,
            ),
            patch.object(
                sportsbook_capture,
                "SourceHealthRepository",
                _FakeSourceHealthRepository,
            ),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _seed_mapping_build_inputs(
        self,
        root: Path,
        *,
        sportsbook_event_key: str = "event-1",
    ) -> None:
        start_time = "2026-04-21T19:00:00Z"
        self._write_json(
            root / "postgres" / "polymarket_markets.json",
            {
                "pm-1": {
                    "market_id": "pm-1",
                    "condition_id": "condition-1",
                    "token_id_yes": None,
                    "token_id_no": None,
                    "title": "Will Home Team beat Away Team?",
                    "description": None,
                    "event_slug": None,
                    "market_slug": None,
                    "category": "sports",
                    "end_time": "2026-04-21T22:00:00Z",
                    "status": "open",
                    "raw_json": {
                        "id": "pm-1",
                        "conditionId": "condition-1",
                        "eventKey": "event-1",
                        "gameId": "game-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "sportsMarketType": "moneyline",
                        "question": "Will Home Team beat Away Team?",
                        "gameStartTime": start_time,
                    },
                }
            },
        )
        self._write_json(
            root / "postgres" / "sportsbook_events.json",
            {
                "sb-1": {
                    "sportsbook_event_id": "sb-1",
                    "source": "theoddsapi",
                    "sport": "basketball_nba",
                    "league": "playoffs",
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "start_time": start_time,
                    "raw_json": {
                        "id": "sb-1",
                        "event_key": sportsbook_event_key,
                        "game_id": "game-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "start_time": start_time,
                    },
                }
            },
        )

    def _seed_opportunity_build_inputs(
        self,
        root: Path,
        *,
        event_start_time: datetime,
        market_end_time: datetime | None = None,
        market_status: str = "open",
        source_health_overrides: dict[str, dict[str, object]] | None = None,
    ) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        market_end = market_end_time or (event_start_time + timedelta(hours=3))
        source_health = {
            "polymarket_market_channel": {
                "source_name": "polymarket_market_channel",
                "last_seen_at": now_iso,
                "last_success_at": now_iso,
                "stale_after_ms": 60_000,
                "status": "ok",
                "details": {},
            },
            "sportsbook_odds": {
                "source_name": "sportsbook_odds",
                "last_seen_at": now_iso,
                "last_success_at": now_iso,
                "stale_after_ms": 60_000,
                "status": "ok",
                "details": {},
            },
            "market_mappings": {
                "source_name": "market_mappings",
                "last_seen_at": now_iso,
                "last_success_at": now_iso,
                "stale_after_ms": 60_000,
                "status": "ok",
                "details": {},
            },
            "fair_values": {
                "source_name": "fair_values",
                "last_seen_at": now_iso,
                "last_success_at": now_iso,
                "stale_after_ms": 60_000,
                "status": "ok",
                "details": {},
            },
        }
        for source_name, override in (source_health_overrides or {}).items():
            if source_name in source_health:
                source_health[source_name] = {
                    **source_health[source_name],
                    **override,
                }

        self._write_json(
            root / "postgres" / "market_mappings.json",
            {
                "0": {
                    "polymarket_market_id": "pm-1",
                    "sportsbook_event_id": "sb-1",
                    "sportsbook_market_type": "h2h",
                    "normalized_market_type": "moneyline_full_game",
                    "match_confidence": 0.98,
                    "resolution_risk": 0.05,
                    "mismatch_reason": None,
                    "is_active": True,
                }
            },
        )
        self._write_json(
            root / "current" / "market_mappings.json",
            {
                "pm-1|sb-1": {
                    "polymarket_market_id": "pm-1",
                    "sportsbook_event_id": "sb-1",
                    "sportsbook_market_type": "h2h",
                    "normalized_market_type": "moneyline_full_game",
                    "match_confidence": 0.98,
                    "resolution_risk": 0.05,
                    "mismatch_reason": None,
                    "is_active": True,
                }
            },
        )
        self._write_json(
            root / "postgres" / "fair_values.json",
            {
                "pm-1|2026-04-21T18:00:00+00:00|deterministic_consensus|v1": {
                    "market_id": "pm-1",
                    "as_of": "2026-04-21T18:00:00+00:00",
                    "fair_yes_prob": 0.61,
                    "lower_prob": 0.58,
                    "upper_prob": 0.64,
                    "book_dispersion": 0.01,
                    "data_age_ms": 250,
                    "source_count": 2,
                    "model_name": "deterministic_consensus",
                    "model_version": "v1",
                }
            },
        )
        self._write_json(
            root / "current" / "fair_values.json",
            {
                "pm-1": {
                    "market_id": "pm-1",
                    "as_of": "2026-04-21T18:00:00+00:00",
                    "fair_yes_prob": 0.61,
                    "lower_prob": 0.58,
                    "upper_prob": 0.64,
                    "book_dispersion": 0.01,
                    "data_age_ms": 250,
                    "source_count": 2,
                    "model_name": "deterministic_consensus",
                    "model_version": "v1",
                }
            },
        )
        self._write_json(
            root / "postgres" / "polymarket_bbo.json",
            {
                "pm-1": {
                    "market_id": "pm-1",
                    "best_bid_yes": 0.50,
                    "best_bid_yes_size": 10.0,
                    "best_ask_yes": 0.52,
                    "best_ask_yes_size": 8.0,
                    "midpoint_yes": 0.51,
                    "spread_yes": 0.02,
                    "book_ts": now_iso,
                    "source_age_ms": 100,
                    "raw_hash": None,
                }
            },
        )
        self._write_json(
            root / "current" / "source_health.json",
            source_health,
        )
        self._write_json(
            root / "current" / "sportsbook_events.json",
            {
                "sb-1": {
                    "id": "sb-1",
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "commence_time": event_start_time.isoformat(),
                }
            },
        )
        self._write_json(
            root / "current" / "polymarket_markets.json",
            {
                "pm-1": {
                    "market_id": "pm-1",
                    "title": "Will Home Team beat Away Team?",
                    "end_time": market_end.isoformat(),
                    "status": market_status,
                }
            },
        )

    def test_gamma_ingest_quiet_suppresses_stdout(self):
        gamma_payload = [
            {
                "conditionId": "condition-1",
                "eventKey": "event-1",
                "question": "Will Home Team win?",
                "tokens": [
                    {"token_id": "token-yes", "outcome": "Yes", "midpoint": 0.55},
                    {"token_id": "token-no", "outcome": "No", "midpoint": 0.45},
                ],
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "gamma.json"
            stdout = io.StringIO()
            with (
                patch.object(
                    ingest_live_data, "fetch_markets", return_value=gamma_payload
                ),
                patch(
                    "sys.argv",
                    [
                        "ingest_live_data.py",
                        "--layer",
                        "gamma",
                        "--config-file",
                        "configs/sports_nba.yaml",
                        "--output",
                        str(output_path),
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                ingest_live_data.main()

            payload = json.loads(output_path.read_text())

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(payload["layer"], "gamma")

    def test_gamma_ingest_writes_typed_market_capture(self):
        gamma_payload = [
            {
                "conditionId": "condition-1",
                "eventKey": "event-1",
                "gameId": "game-1",
                "sportsMarketType": "moneyline",
                "question": "Will Home Team win?",
                "tokens": [
                    {"token_id": "token-yes", "outcome": "Yes", "midpoint": 0.55},
                    {"token_id": "token-no", "outcome": "No", "midpoint": 0.45},
                ],
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "gamma.json"
            with (
                patch.object(
                    ingest_live_data, "fetch_markets", return_value=gamma_payload
                ),
                patch(
                    "sys.argv",
                    [
                        "ingest_live_data.py",
                        "--layer",
                        "gamma",
                        "--config-file",
                        "configs/sports_nba.yaml",
                        "--output",
                        str(output_path),
                    ],
                ),
            ):
                ingest_live_data.main()

            payload = json.loads(output_path.read_text())

        self.assertEqual(payload["layer"], "gamma")
        self.assertIn("markets", payload)
        self.assertEqual(len(payload["markets"]), 2)
        self.assertEqual(payload["markets"][0]["sports_market_type"], "moneyline")
        self.assertIsNotNone(payload["markets"][0]["contract"])

    def test_polymarket_ingest_preserves_raw_fallback_without_postgres_dsn(self):
        event_start = datetime(2026, 4, 21, 19, 0, tzinfo=timezone.utc)
        market_payload = [
            {
                "id": "pm-1",
                "conditionId": "condition-1",
                "question": "Will Home Team beat Away Team?",
                "sport": "nba",
                "eventKey": "event-1",
                "gameId": "game-1",
                "sports_market_type": "moneyline",
                "active": True,
                "gameStartTime": event_start.isoformat(),
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            bbo_path = Path(temp_dir) / "bbo.json"
            bbo_path.write_text(
                json.dumps(
                    [
                        {
                            "market_id": "pm-1",
                            "best_bid": 0.5,
                            "best_bid_size": 10,
                            "best_ask": 0.52,
                            "best_ask_size": 8,
                            "timestamp": int(event_start.timestamp() * 1000),
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with patch.object(
                ingest_live_data.PolymarketMarketCatalogClient,
                "fetch_open_markets",
                return_value=market_payload,
            ):
                ingest_live_data.main(
                    [
                        "polymarket-markets",
                        "--sport",
                        "nba",
                        "--root",
                        str(root),
                        "--quiet",
                    ]
                )

            ingest_live_data.main(
                [
                    "polymarket-bbo",
                    "--input",
                    str(bbo_path),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )

            raw_market_files = list(
                (root / "raw" / "polymarket" / "market_catalog").rglob("*.jsonl.gz")
            )
            raw_bbo_files = list(
                (root / "raw" / "polymarket" / "market_channel").rglob("*.jsonl.gz")
            )

        self.assertEqual(len(raw_market_files), 1)
        self.assertEqual(len(raw_bbo_files), 1)

    def test_live_pipeline_subcommands_build_fair_values_and_opportunities(self):
        event_start = datetime.now(timezone.utc) + timedelta(hours=2)
        market_payload = [
            {
                "id": "pm-1",
                "question": "Will Home Team beat Away Team?",
                "sports_market_type": "moneyline",
                "sport": "nba",
                "eventKey": "event-1",
                "gameId": "game-1",
                "active": True,
                "gameStartTime": event_start.isoformat(),
            }
        ]
        odds_payload = [
            {
                "id": "sb-1",
                "sport_title": "NBA",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "commence_time": event_start.isoformat(),
                "bookmakers": [
                    {
                        "key": "book-a",
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
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            bbo_path = Path(temp_dir) / "bbo.json"
            event_map_path = Path(temp_dir) / "event_map.json"
            event_map_path.write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "event_key": "event-1",
                            "game_id": "game-1",
                            "sport": "nba",
                            "series": "playoffs",
                        }
                    }
                ),
                encoding="utf-8",
            )
            bbo_path.write_text(
                json.dumps(
                    [
                        {
                            "market_id": "pm-1",
                            "best_bid": 0.50,
                            "best_bid_size": 10,
                            "best_ask": 0.52,
                            "best_ask_size": 8,
                            "timestamp": int(
                                datetime.now(timezone.utc).timestamp() * 1000
                            ),
                        }
                    ]
                )
            )

            with patch.object(
                ingest_live_data.PolymarketMarketCatalogClient,
                "fetch_open_markets",
                return_value=market_payload,
            ):
                ingest_live_data.main(
                    [
                        "polymarket-markets",
                        "--sport",
                        "nba",
                        "--root",
                        str(root),
                        "--quiet",
                    ]
                )

            with (
                patch.object(
                    ingest_live_data.TheOddsApiClient,
                    "fetch_upcoming",
                    return_value=odds_payload,
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
            ):
                ingest_live_data.main(
                    [
                        "sportsbook-odds",
                        "--sport",
                        "basketball_nba",
                        "--market",
                        "h2h",
                        "--root",
                        str(root),
                        "--event-map-file",
                        str(event_map_path),
                        "--quiet",
                    ]
                )

            ingest_live_data.main(
                [
                    "polymarket-bbo",
                    "--input",
                    str(bbo_path),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )
            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )
            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])
            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        self.assertIn("pm-1", fair_values)
        self.assertTrue(any(key.startswith("pm-1|") for key in opportunities))

    def test_live_pipeline_subcommands_can_use_config_defaults(self):
        event_start = datetime.now(timezone.utc) + timedelta(hours=2)
        market_payload = [
            {
                "id": "pm-1",
                "question": "Will Home Team beat Away Team?",
                "sports_market_type": "moneyline",
                "sport": "nba",
                "eventKey": "event-1",
                "gameId": "game-1",
                "active": True,
                "gameStartTime": event_start.isoformat(),
            }
        ]
        odds_payload = [
            {
                "id": "sb-1",
                "sport_title": "NBA",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "commence_time": event_start.isoformat(),
                "bookmakers": [
                    {
                        "key": "book-a",
                        "last_update": event_start.isoformat(),
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home Team", "price": 5.0},
                                    {"name": "Away Team", "price": 1.25},
                                ],
                            }
                        ],
                    },
                    {
                        "key": "book-b",
                        "last_update": (event_start - timedelta(hours=1)).isoformat(),
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home Team", "price": 1.25},
                                    {"name": "Away Team", "price": 5.0},
                                ],
                            }
                        ],
                    },
                ],
            }
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            bbo_path = Path(temp_dir) / "bbo.json"
            event_map_path = Path(temp_dir) / "event_map.json"
            consensus_artifact_path = Path(temp_dir) / "consensus.json"
            calibration_artifact_path = Path(temp_dir) / "calibration.json"
            config_path = Path(temp_dir) / "sports.json"

            event_map_path.write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "event_key": "event-1",
                            "game_id": "game-1",
                            "sport": "nba",
                            "series": "playoffs",
                        }
                    }
                ),
                encoding="utf-8",
            )
            consensus_artifact_path.write_text(
                json.dumps(
                    {
                        "model": "consensus",
                        "model_version": "v1",
                        "half_life_seconds": 60.0,
                        "bookmaker_count": 2,
                        "row_count": 2,
                    }
                ),
                encoding="utf-8",
            )
            calibration_artifact_path.write_text(
                json.dumps(
                    {
                        "row_count": 4,
                        "rows": [
                            {"fair_value": 0.42, "outcome_label": 0},
                            {"fair_value": 0.45, "outcome_label": 0},
                            {"fair_value": 0.55, "outcome_label": 1},
                            {"fair_value": 0.58, "outcome_label": 1},
                        ],
                        "bin_count": 2,
                    }
                ),
                encoding="utf-8",
            )
            config_path.write_text(
                json.dumps(
                    {
                        "league": "nba",
                        "capture": {"sport_key": "basketball_nba"},
                        "runtime": {
                            "sportsbook_market": "h2h",
                            "event_map_file": str(event_map_path),
                            "consensus_artifact": str(consensus_artifact_path),
                            "calibration_artifact": str(calibration_artifact_path),
                        },
                    }
                ),
                encoding="utf-8",
            )
            bbo_path.write_text(
                json.dumps(
                    [
                        {
                            "market_id": "pm-1",
                            "best_bid": 0.50,
                            "best_bid_size": 10,
                            "best_ask": 0.52,
                            "best_ask_size": 8,
                            "timestamp": int(
                                datetime.now(timezone.utc).timestamp() * 1000
                            ),
                        }
                    ]
                )
            )

            with patch.object(
                ingest_live_data.PolymarketMarketCatalogClient,
                "fetch_open_markets",
                return_value=market_payload,
            ):
                ingest_live_data.main(
                    [
                        "polymarket-markets",
                        "--sport",
                        "nba",
                        "--root",
                        str(root),
                        "--quiet",
                    ]
                )

            with (
                patch.object(
                    ingest_live_data.TheOddsApiClient,
                    "fetch_upcoming",
                    return_value=odds_payload,
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
            ):
                ingest_live_data.main(
                    [
                        "sportsbook-odds",
                        "--config-file",
                        str(config_path),
                        "--root",
                        str(root),
                        "--quiet",
                    ]
                )

            ingest_live_data.main(
                [
                    "polymarket-bbo",
                    "--input",
                    str(bbo_path),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )
            ingest_live_data.main(
                [
                    "build-mappings",
                    "--config-file",
                    str(config_path),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )
            ingest_live_data.main(
                [
                    "build-fair-values",
                    "--config-file",
                    str(config_path),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )

            fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            mappings = json.loads(
                (root / "current" / "market_mappings.json").read_text()
            )
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text()
            )

        self.assertEqual(list(mappings.keys()), ["pm-1|sb-1"])
        self.assertEqual(fair_values["pm-1"]["model_name"], "consensus")
        self.assertAlmostEqual(
            fair_values["pm-1"]["calibrated_fair_yes_prob"],
            1.0,
        )
        self.assertTrue(
            source_health["fair_values"]["details"]["consensus_artifact_configured"]
        )
        self.assertTrue(
            source_health["fair_values"]["details"]["calibration_artifact_configured"]
        )

    def test_sportsbook_odds_failure_returns_sanitized_json_without_traceback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stdout = io.StringIO()
            with (
                patch.object(
                    ingest_live_data.TheOddsApiClient,
                    "fetch_upcoming",
                    side_effect=RuntimeError("https://example.com/?apiKey=secret"),
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
                patch("sys.stdout", stdout),
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
                    ]
                )

            payload = json.loads(stdout.getvalue())

        self.assertEqual(result, 1)
        self.assertEqual(payload["error_kind"], "RuntimeError")
        self.assertEqual(
            payload["error_message"],
            "RuntimeError during sportsbook capture",
        )
        self.assertNotIn("apiKey=secret", json.dumps(payload, sort_keys=True))

    def test_sportsbook_odds_failure_survives_failure_recording_errors(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            stdout = io.StringIO()
            with (
                patch.object(
                    ingest_live_data.TheOddsApiClient,
                    "fetch_upcoming",
                    side_effect=RuntimeError("https://example.com/?apiKey=secret"),
                ),
                patch.object(
                    ingest_live_data,
                    "record_sportsbook_capture_failure",
                    side_effect=RuntimeError(
                        "psycopg is required for Postgres storage"
                    ),
                ),
                patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False),
                patch("sys.stdout", stdout),
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
        self.assertEqual(
            payload["health_error_message"],
            "RuntimeError during sportsbook capture",
        )
        self.assertNotIn("apiKey=secret", json.dumps(payload, sort_keys=True))

    def test_build_mappings_persists_research_identity_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root)

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )

            mappings = json.loads(
                (root / "current" / "market_mappings.json").read_text()
            )

        persisted = next(iter(mappings.values()))
        self.assertEqual(persisted["event_key"], "event-1")
        self.assertEqual(persisted["sport"], "nba")
        self.assertEqual(persisted["series"], "playoffs")
        self.assertEqual(persisted["game_id"], "game-1")
        self.assertIsNone(persisted["blocked_reason"])
        self.assertIsNone(persisted["mismatch_reason"])
        self.assertTrue(persisted["is_active"])

    def test_build_mappings_uses_research_block_reason_for_event_mismatch(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root, sportsbook_event_key="event-2")

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )

            mappings = json.loads(
                (root / "current" / "market_mappings.json").read_text()
            )

        persisted = next(iter(mappings.values()))
        self.assertEqual(persisted["event_key"], "event-1")
        self.assertEqual(persisted["blocked_reason"], "event key mismatch")
        self.assertEqual(persisted["mismatch_reason"], "event key mismatch")
        self.assertEqual(persisted["match_confidence"], 0.0)
        self.assertFalse(persisted["is_active"])

    def test_build_opportunities_persists_pre_start_freeze_reason(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(minutes=4),
            )

            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertEqual(
            persisted["blocked_reason"],
            "market within pre-start freeze window",
        )
        self.assertEqual(
            persisted["blocked_reasons"],
            ["market within pre-start freeze window"],
        )
        self.assertIn("edge_buy_after_costs_bps", persisted)
        self.assertIn("edge_sell_after_costs_bps", persisted)

    def test_build_opportunities_persists_all_known_blocked_reasons_in_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(minutes=4),
                source_health_overrides={
                    "sportsbook_odds": {"status": "red"},
                },
            )
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": "event key mismatch",
                        "blocked_reason": "event key mismatch",
                        "is_active": False,
                    }
                },
            )
            self._write_json(root / "current" / "fair_values.json", {})
            self._write_json(root / "postgres" / "polymarket_bbo.json", {})

            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertEqual(persisted["blocked_reason"], "event key mismatch")
        self.assertEqual(
            persisted["blocked_reasons"],
            [
                "event key mismatch",
                "market within pre-start freeze window",
                "source sportsbook_odds unhealthy",
                "missing fair value",
                "missing executable bbo",
            ],
        )

    def test_build_opportunities_persists_unhealthy_source_reason(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(hours=2),
                source_health_overrides={
                    "sportsbook_odds": {"status": "red"},
                },
            )

            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertEqual(
            persisted["blocked_reason"],
            "source sportsbook_odds unhealthy",
        )

    def test_build_opportunities_uses_policy_file_for_pre_expiry_freeze(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            policy_path = Path(temp_dir) / "runtime-policy.json"
            policy_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "proposal_planner": {
                            "freeze_minutes_before_expiry": 30,
                        },
                    }
                ),
                encoding="utf-8",
            )
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(hours=2),
                market_end_time=datetime.now(timezone.utc) + timedelta(minutes=10),
            )

            ingest_live_data.main(
                [
                    "build-opportunities",
                    "--root",
                    str(root),
                    "--policy-file",
                    str(policy_path),
                    "--quiet",
                ]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertEqual(
            persisted["blocked_reason"],
            "market within pre-expiry freeze window",
        )

    def test_build_opportunities_can_disable_source_health_block_via_policy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            policy_path = Path(temp_dir) / "runtime-policy.json"
            policy_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "proposal_planner": {
                            "block_on_unhealthy_source": False,
                        },
                    }
                ),
                encoding="utf-8",
            )
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(hours=2),
                source_health_overrides={
                    "sportsbook_odds": {"status": "red"},
                },
            )

            ingest_live_data.main(
                [
                    "build-opportunities",
                    "--root",
                    str(root),
                    "--policy-file",
                    str(policy_path),
                    "--quiet",
                ]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertIsNone(persisted["blocked_reason"])

    def test_build_opportunities_replaces_stale_current_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            self._write_json(
                root / "current" / "fair_values.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                        "fair_yes_prob": 0.40,
                        "lower_prob": 0.38,
                        "upper_prob": 0.42,
                        "book_dispersion": 0.01,
                        "data_age_ms": 250,
                        "source_count": 2,
                        "model_name": "deterministic_consensus",
                        "model_version": "v1",
                    }
                },
            )
            self._write_json(
                root / "postgres" / "polymarket_bbo.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "best_bid_yes": 0.54,
                        "best_bid_yes_size": 9.0,
                        "best_ask_yes": 0.60,
                        "best_ask_yes_size": 2.0,
                        "midpoint_yes": 0.57,
                        "spread_yes": 0.06,
                        "book_ts": datetime.now(timezone.utc).isoformat(),
                        "source_age_ms": 100,
                        "raw_hash": None,
                    }
                },
            )
            self._write_json(
                root / "current" / "opportunities.json",
                {
                    "pm-1|buy_yes": {
                        "market_id": "pm-1",
                        "side": "buy_yes",
                    },
                    "stale-market|buy_yes": {
                        "market_id": "stale-market",
                        "side": "buy_yes",
                    },
                },
            )

            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        self.assertEqual(list(opportunities.keys()), ["pm-1|sell_yes"])

    def test_build_opportunities_uses_executable_side_visible_depth(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            self._write_json(
                root / "current" / "fair_values.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                        "fair_yes_prob": 0.40,
                        "lower_prob": 0.38,
                        "upper_prob": 0.42,
                        "book_dispersion": 0.01,
                        "data_age_ms": 250,
                        "source_count": 2,
                        "model_name": "deterministic_consensus",
                        "model_version": "v1",
                    }
                },
            )
            self._write_json(
                root / "postgres" / "polymarket_bbo.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "best_bid_yes": 0.54,
                        "best_bid_yes_size": 9.0,
                        "best_ask_yes": 0.60,
                        "best_ask_yes_size": 2.0,
                        "midpoint_yes": 0.57,
                        "spread_yes": 0.06,
                        "book_ts": datetime.now(timezone.utc).isoformat(),
                        "source_age_ms": 100,
                        "raw_hash": None,
                    }
                },
            )

            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertEqual(persisted["side"], "sell_yes")
        self.assertEqual(persisted["fillable_size"], 9.0)

    def test_build_fair_values_prefers_best_mapping_per_market(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-low": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-low",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.45,
                        "resolution_risk": 0.30,
                        "mismatch_reason": None,
                        "event_key": "event-low",
                        "is_active": True,
                    },
                    "pm-1|sb-high": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-high",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-high",
                        "is_active": True,
                    },
                },
            )
            self._write_json(
                root / "postgres" / "sportsbook_odds.json",
                {
                    "0": {
                        "sportsbook_event_id": "sb-low",
                        "source": "theoddsapi",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 4.0,
                        "implied_prob": 0.25,
                        "overround": 0.25,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                    "1": {
                        "sportsbook_event_id": "sb-high",
                        "source": "theoddsapi",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                },
            )

            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])

            fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )

        self.assertAlmostEqual(fair_values["pm-1"]["fair_yes_prob"], 0.666667, places=5)

    def test_build_fair_values_prefers_current_sportsbook_odds_over_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "postgres" / "sportsbook_odds.json",
                {
                    "0": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 5.0,
                        "implied_prob": 0.20,
                        "overround": 0.20,
                        "quote_ts": "2026-04-21T17:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                    "1": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.111111,
                        "implied_prob": 0.90,
                        "overround": 0.90,
                        "quote_ts": "2026-04-21T17:05:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|theoddsapi|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )

            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])

            fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )

        self.assertAlmostEqual(fair_values["pm-1"]["fair_yes_prob"], 0.666667, places=5)

    def test_build_fair_values_uses_consensus_artifact(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            artifact_path = Path(temp_dir) / "consensus.json"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 5.0,
                        "implied_prob": 0.2,
                        "overround": 0.20,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                    "sb-1|book-b|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-b",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.25,
                        "implied_prob": 0.8,
                        "overround": 0.80,
                        "quote_ts": "2026-04-21T17:00:00+00:00",
                        "source_age_ms": 3600000,
                        "raw_json": {},
                    },
                },
            )
            artifact_path.write_text(
                json.dumps(
                    {
                        "model": "consensus",
                        "model_version": "v1",
                        "half_life_seconds": 60.0,
                        "bookmaker_count": 2,
                        "row_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            ingest_live_data.main(
                [
                    "build-fair-values",
                    "--root",
                    str(root),
                    "--consensus-artifact",
                    str(artifact_path),
                    "--quiet",
                ]
            )

            fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text()
            )

        self.assertLess(fair_values["pm-1"]["fair_yes_prob"], 0.25)
        self.assertEqual(fair_values["pm-1"]["model_name"], "consensus")
        self.assertEqual(source_health["fair_values"]["status"], "ok")

    def test_build_fair_values_replaces_current_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )
            self._write_json(
                root / "current" / "fair_values.json",
                {
                    "stale-market": {
                        "market_id": "stale-market",
                        "as_of": "2026-04-21T17:00:00+00:00",
                        "fair_yes_prob": 0.42,
                        "lower_prob": 0.40,
                        "upper_prob": 0.44,
                        "book_dispersion": 0.02,
                        "data_age_ms": 50,
                        "source_count": 1,
                        "model_name": "baseline",
                        "model_version": "v1",
                    }
                },
            )

            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])

            fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )

        self.assertEqual(list(fair_values.keys()), ["pm-1"])

    def test_build_fair_values_writes_runtime_manifest_projection(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "polymarket_markets.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "condition_id": "condition-1",
                        "raw_json": {
                            "id": "pm-1",
                            "conditionId": "condition-1",
                        },
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )

            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])

            manifest_payload = json.loads(
                (root / "current" / "fair_value_manifest.json").read_text()
            )

        self.assertEqual(manifest_payload["schema_version"], 1)
        self.assertIn("pm-1", manifest_payload["values"])
        self.assertEqual(
            manifest_payload["values"]["pm-1"]["condition_id"], "condition-1"
        )
        self.assertEqual(manifest_payload["values"]["pm-1"]["event_key"], "event-1")
        self.assertEqual(manifest_payload["source"], "live-current-state")

    def test_build_fair_values_projects_calibrated_runtime_manifest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            calibration_artifact_path = Path(temp_dir) / "calibration.json"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "polymarket_markets.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "condition_id": "condition-1",
                        "raw_json": {
                            "id": "pm-1",
                            "conditionId": "condition-1",
                        },
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )
            calibration_artifact_path.write_text(
                json.dumps(
                    {
                        "row_count": 4,
                        "rows": [
                            {"fair_value": 0.42, "outcome_label": 0},
                            {"fair_value": 0.45, "outcome_label": 0},
                            {"fair_value": 0.55, "outcome_label": 1},
                            {"fair_value": 0.58, "outcome_label": 1},
                        ],
                        "bin_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            ingest_live_data.main(
                [
                    "build-fair-values",
                    "--root",
                    str(root),
                    "--calibration-artifact",
                    str(calibration_artifact_path),
                    "--quiet",
                ]
            )

            fair_values_payload = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            manifest_payload = json.loads(
                (root / "current" / "fair_value_manifest.json").read_text()
            )

        self.assertAlmostEqual(
            fair_values_payload["pm-1"]["calibrated_fair_yes_prob"],
            1.0,
        )
        self.assertAlmostEqual(
            manifest_payload["values"]["pm-1"]["calibrated_fair_value"],
            1.0,
        )
        metadata = manifest_payload.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata")
        calibration_metadata = metadata.get("calibration")
        self.assertIsInstance(calibration_metadata, dict)
        if not isinstance(calibration_metadata, dict):
            self.fail("expected manifest calibration metadata")
        self.assertEqual(calibration_metadata["method"], "histogram")
        self.assertEqual(calibration_metadata["bin_count"], 2)
        self.assertEqual(calibration_metadata["sample_count"], 4)

    def test_build_mappings_blocks_missing_upstream_event_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root)
            self._write_json(
                root / "postgres" / "sportsbook_events.json",
                {
                    "sb-1": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "sport": "basketball_nba",
                        "league": "playoffs",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "start_time": "2026-04-21T19:00:00Z",
                        "raw_json": {
                            "id": "sb-1",
                            "sport": "nba",
                            "series": "playoffs",
                            "home_team": "Home Team",
                            "away_team": "Away Team",
                            "start_time": "2026-04-21T19:00:00Z",
                        },
                    }
                },
            )

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )

            mappings = json.loads(
                (root / "current" / "market_mappings.json").read_text()
            )

        persisted = next(iter(mappings.values()))
        self.assertFalse(persisted["is_active"])
        self.assertEqual(persisted["blocked_reason"], "missing upstream event identity")
        self.assertEqual(
            persisted["resolution_risk"],
            round(1.0 - persisted["match_confidence"], 4),
        )

    def test_build_mappings_writes_mapping_manifest_projection(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root)

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )

            manifest_payload = json.loads(
                (root / "current" / "market_mapping_manifest.json").read_text()
            )

        self.assertEqual(manifest_payload["schema_version"], 1)
        self.assertIn("pm-1", manifest_payload["values"])
        record = manifest_payload["values"]["pm-1"]
        self.assertEqual(record["mapping_status"], "exact_match")
        self.assertEqual(record["target"]["sportsbook_event_id"], "sb-1")
        self.assertEqual(record["identity"]["event_key"], "event-1")
        self.assertEqual(record["mapping_confidence"]["band"], "high")
        self.assertIsNone(record["blocked_reason"])

    def test_build_mappings_manifest_captures_blocked_reason_structure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root)
            self._write_json(
                root / "postgres" / "sportsbook_events.json",
                {
                    "sb-1": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "sport": "basketball_nba",
                        "league": "playoffs",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "start_time": "2026-04-21T19:00:00Z",
                        "raw_json": {
                            "id": "sb-1",
                            "sport": "nba",
                            "series": "playoffs",
                            "home_team": "Home Team",
                            "away_team": "Away Team",
                            "start_time": "2026-04-21T19:00:00Z",
                        },
                    }
                },
            )

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )

            manifest_payload = json.loads(
                (root / "current" / "market_mapping_manifest.json").read_text()
            )

        record = manifest_payload["values"]["pm-1"]
        self.assertEqual(record["mapping_status"], "blocked")
        self.assertEqual(
            record["blocked_reason"]["code"],
            "missing_upstream_event_identity",
        )
        self.assertEqual(
            record["blocked_reason"]["message"],
            "missing upstream event identity",
        )
        self.assertEqual(record["mapping_confidence"]["score"], 0.59)

    def test_build_mappings_replaces_current_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root)
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "stale-market": {
                        "polymarket_market_id": "stale-market",
                        "sportsbook_event_id": "stale-sb",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.25,
                        "resolution_risk": 0.50,
                        "mismatch_reason": "stale",
                        "is_active": False,
                    }
                },
            )

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )

            mappings = json.loads(
                (root / "current" / "market_mappings.json").read_text()
            )

        self.assertEqual(list(mappings.keys()), ["pm-1|sb-1"])

    def test_build_fair_values_does_not_partially_write_on_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "is_active": True,
                    },
                    "pm-2|sb-2": {
                        "polymarket_market_id": "pm-2",
                        "sportsbook_event_id": "sb-2",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.97,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-2",
                        "game_id": "game-2",
                        "is_active": True,
                    },
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                    "sb-2|book-b|h2h|Home Team": {
                        "sportsbook_event_id": "sb-2",
                        "source": "book-b",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 0.0,
                        "implied_prob": None,
                        "overround": None,
                        "quote_ts": "2026-04-21T18:05:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    },
                },
            )
            self._write_json(
                root / "current" / "fair_values.json",
                {
                    "existing-market": {
                        "market_id": "existing-market",
                        "as_of": "2026-04-21T17:00:00+00:00",
                        "fair_yes_prob": 0.42,
                        "lower_prob": 0.40,
                        "upper_prob": 0.44,
                        "book_dispersion": 0.02,
                        "data_age_ms": 50,
                        "source_count": 1,
                        "model_name": "baseline",
                        "model_version": "v1",
                    }
                },
            )
            self._write_json(
                root / "postgres" / "fair_values.json",
                {
                    "existing-market|2026-04-21T17:00:00+00:00|baseline|v1": {
                        "market_id": "existing-market",
                        "as_of": "2026-04-21T17:00:00+00:00",
                        "fair_yes_prob": 0.42,
                        "lower_prob": 0.40,
                        "upper_prob": 0.44,
                        "book_dispersion": 0.02,
                        "data_age_ms": 50,
                        "source_count": 1,
                        "model_name": "baseline",
                        "model_version": "v1",
                    }
                },
            )

            with self.assertRaises(ValueError):
                ingest_live_data.main(
                    ["build-fair-values", "--root", str(root), "--quiet"]
                )

            current_fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            history_fair_values = json.loads(
                (root / "postgres" / "fair_values.json").read_text()
            )
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text()
            )

        self.assertEqual(list(current_fair_values.keys()), ["existing-market"])
        self.assertEqual(
            list(history_fair_values.keys()),
            ["existing-market|2026-04-21T17:00:00+00:00|baseline|v1"],
        )
        self.assertEqual(source_health["fair_values"]["status"], "red")
        self.assertEqual(
            source_health["fair_values"]["details"]["error_kind"], "ValueError"
        )

    def test_build_fair_values_does_not_commit_json_when_parquet_write_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )
            self._write_json(
                root / "current" / "fair_values.json",
                {
                    "existing-market": {
                        "market_id": "existing-market",
                        "as_of": "2026-04-21T17:00:00+00:00",
                        "fair_yes_prob": 0.42,
                        "lower_prob": 0.40,
                        "upper_prob": 0.44,
                        "book_dispersion": 0.02,
                        "data_age_ms": 50,
                        "source_count": 1,
                        "model_name": "baseline",
                        "model_version": "v1",
                    }
                },
            )
            self._write_json(
                root / "postgres" / "fair_values.json",
                {
                    "existing-market|2026-04-21T17:00:00+00:00|baseline|v1": {
                        "market_id": "existing-market",
                        "as_of": "2026-04-21T17:00:00+00:00",
                        "fair_yes_prob": 0.42,
                        "lower_prob": 0.40,
                        "upper_prob": 0.44,
                        "book_dispersion": 0.02,
                        "data_age_ms": 50,
                        "source_count": 1,
                        "model_name": "baseline",
                        "model_version": "v1",
                    }
                },
            )

            with patch.object(
                ingest_live_data.ParquetStore,
                "append_records",
                side_effect=RuntimeError("parquet failed"),
            ):
                with self.assertRaises(RuntimeError):
                    ingest_live_data.main(
                        ["build-fair-values", "--root", str(root), "--quiet"]
                    )

            current_fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            history_fair_values = json.loads(
                (root / "postgres" / "fair_values.json").read_text()
            )
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text()
            )

        self.assertEqual(list(current_fair_values.keys()), ["existing-market"])
        self.assertEqual(
            list(history_fair_values.keys()),
            ["existing-market|2026-04-21T17:00:00+00:00|baseline|v1"],
        )
        self.assertEqual(source_health["fair_values"]["status"], "red")
        self.assertEqual(
            source_health["fair_values"]["details"]["error_kind"],
            "RuntimeError",
        )

    def test_build_fair_values_succeeds_when_health_write_fails_after_commit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "event_key": "event-1",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )

            with patch.object(
                ingest_live_data,
                "materialize_source_health_state",
                side_effect=RuntimeError("health failed"),
            ):
                result = ingest_live_data.main(
                    ["build-fair-values", "--root", str(root), "--quiet"]
                )

            current_fair_values = json.loads(
                (root / "current" / "fair_values.json").read_text()
            )
            history_fair_values = json.loads(
                (root / "postgres" / "fair_values.json").read_text()
            )

        self.assertEqual(result, 0)
        self.assertIn("pm-1", current_fair_values)
        self.assertEqual(len(history_fair_values), 1)

    def test_build_inference_dataset_writes_processed_rows_and_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            now = datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc)
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=now + timedelta(hours=2),
            )
            self._write_json(
                root / "current" / "market_mappings.json",
                {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "sportsbook_market_type": "h2h",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "mismatch_reason": None,
                        "blocked_reason": None,
                        "event_key": "event-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "game_id": "game-1",
                        "is_active": True,
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.50,
                        "implied_prob": 0.6666666667,
                        "overround": 0.02,
                        "quote_ts": now.isoformat(),
                        "source_age_ms": 100,
                        "raw_json": {},
                    },
                    "sb-1|book-b|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-b",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.55,
                        "implied_prob": 0.6451612903,
                        "overround": 0.03,
                        "quote_ts": now.isoformat(),
                        "source_age_ms": 200,
                        "raw_json": {},
                    },
                },
            )
            self._write_json(
                root / "current" / "polymarket_markets.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "condition_id": "condition-1",
                        "title": "Will Home Team beat Away Team?",
                        "status": "open",
                        "end_time": (now + timedelta(hours=4)).isoformat(),
                        "raw_json": {
                            "id": "pm-1",
                            "conditionId": "condition-1",
                        },
                    }
                },
            )

            result = ingest_live_data.main(
                ["build-inference-dataset", "--root", str(root), "--quiet"]
            )

            latest_rows_path = (
                root / "processed" / "inference" / "joined_inference_dataset.jsonl"
            )
            latest_rows = [
                json.loads(line)
                for line in latest_rows_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            registry = DatasetRegistry(root / "datasets")
            manifest = registry.load_snapshot("joined-inference-dataset")
            snapshot_rows = registry.read_rows("joined-inference-dataset")

        self.assertEqual(result, 0)
        self.assertEqual(manifest.record_count, 1)
        self.assertEqual(len(latest_rows), 1)
        self.assertEqual(snapshot_rows, latest_rows)
        self.assertEqual(latest_rows[0]["market_id"], "pm-1")
        self.assertEqual(latest_rows[0]["sportsbook_event_id"], "sb-1")
        self.assertEqual(latest_rows[0]["bookmaker_count"], 2)
        self.assertTrue(latest_rows[0]["has_polymarket_book"])
        self.assertTrue(latest_rows[0]["inference_allowed"])
        self.assertEqual(latest_rows[0]["blocked_reasons"], [])
        self.assertTrue(latest_rows[0]["record_id"].startswith("pm-1|sb-1|"))
        self.assertIsNotNone(latest_rows[0]["recorded_at"])

    def test_build_training_dataset_persists_rows_and_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            training_input = Path(temp_dir) / "sports-inputs.json"
            polymarket_input = Path(temp_dir) / "polymarket-markets.json"
            captured_at = "2026-04-21T18:00:00Z"
            training_input.write_text(
                json.dumps(
                    {
                        "source": "sports-inputs",
                        "captured_at": captured_at,
                        "rows": [
                            {
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "label": 1,
                                "event_key": "event-1",
                                "game_id": "game-1",
                                "sport": "nba",
                                "series": "playoffs",
                                "sports_market_type": "moneyline",
                                "selection_name": "Home Team",
                                "decimal_odds": 1.8,
                                "start_time": "2026-04-21T20:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            polymarket_input.write_text(
                json.dumps(
                    {
                        "layer": "gamma",
                        "captured_at": captured_at,
                        "markets": [
                            {
                                "market_key": "token-home:yes",
                                "condition_id": "condition-1",
                                "event_key": "event-1",
                                "game_id": "game-1",
                                "sport": "nba",
                                "series": "playoffs",
                                "sports_market_type": "moneyline",
                                "best_bid": 0.42,
                                "best_ask": 0.48,
                                "best_bid_size": 20,
                                "best_ask_size": 10,
                                "volume": 100,
                                "start_time": "2026-04-21T20:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = ingest_live_data.main(
                [
                    "build-training-dataset",
                    "--input",
                    str(training_input),
                    "--polymarket-input",
                    str(polymarket_input),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )

            latest_rows_path = (
                root / "processed" / "training" / "historical_training_dataset.jsonl"
            )
            latest_rows = [
                json.loads(line)
                for line in latest_rows_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            registry = DatasetRegistry(root / "datasets")
            manifest = registry.load_snapshot("historical-training-dataset")
            snapshot_rows = registry.read_rows("historical-training-dataset")

        self.assertEqual(result, 0)
        self.assertEqual(manifest.record_count, 1)
        self.assertEqual(snapshot_rows, latest_rows)
        self.assertEqual(latest_rows[0]["home_team"], "Home Team")
        self.assertEqual(latest_rows[0]["away_team"], "Away Team")
        self.assertEqual(latest_rows[0]["label"], 1)
        self.assertEqual(latest_rows[0]["event_key"], "event-1")
        self.assertEqual(latest_rows[0]["source"], "sports-inputs")
        self.assertEqual(latest_rows[0]["recorded_at"], captured_at)
        self.assertTrue(
            latest_rows[0]["record_id"].startswith("sports-inputs|event-1|")
        )
        self.assertEqual(latest_rows[0]["metadata"]["market_market_count"], 1.0)

    def test_build_inference_dataset_supports_empty_runtime_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"

            result = ingest_live_data.main(
                ["build-inference-dataset", "--root", str(root), "--quiet"]
            )

            latest_rows_path = (
                root / "processed" / "inference" / "joined_inference_dataset.jsonl"
            )
            latest_rows = latest_rows_path.read_text(encoding="utf-8")
            registry = DatasetRegistry(root / "datasets")
            manifest = registry.load_snapshot("joined-inference-dataset")
            snapshot_rows = registry.read_rows("joined-inference-dataset")
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertEqual(result, 0)
        self.assertEqual(latest_rows, "")
        self.assertEqual(manifest.record_count, 0)
        self.assertEqual(snapshot_rows, [])
        self.assertEqual(source_health["joined_inference_dataset"]["status"], "ok")
        self.assertEqual(
            source_health["joined_inference_dataset"]["details"]["row_count"], 0
        )

    def test_build_training_dataset_supports_empty_rows_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            training_input = Path(temp_dir) / "sports-inputs.json"
            training_input.write_text(
                json.dumps(
                    {
                        "source": "sports-inputs",
                        "captured_at": "2026-04-21T18:00:00Z",
                        "rows": [],
                    }
                ),
                encoding="utf-8",
            )

            result = ingest_live_data.main(
                [
                    "build-training-dataset",
                    "--input",
                    str(training_input),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )

            latest_rows_path = (
                root / "processed" / "training" / "historical_training_dataset.jsonl"
            )
            latest_rows = latest_rows_path.read_text(encoding="utf-8")
            registry = DatasetRegistry(root / "datasets")
            manifest = registry.load_snapshot("historical-training-dataset")
            snapshot_rows = registry.read_rows("historical-training-dataset")
            source_health = json.loads(
                (root / "current" / "source_health.json").read_text(encoding="utf-8")
            )

        self.assertEqual(result, 0)
        self.assertEqual(latest_rows, "")
        self.assertEqual(manifest.record_count, 0)
        self.assertEqual(snapshot_rows, [])
        self.assertEqual(source_health["historical_training_dataset"]["status"], "ok")
        self.assertEqual(
            source_health["historical_training_dataset"]["details"]["row_count"],
            0,
        )

    def test_build_training_dataset_keeps_fixed_schema_and_unique_record_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            training_input = Path(temp_dir) / "sports-inputs.json"
            captured_at = "2026-04-21T18:00:00Z"
            training_input.write_text(
                json.dumps(
                    {
                        "source": "sports-inputs",
                        "captured_at": captured_at,
                        "rows": [
                            {
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "label": 0,
                                "event_key": "event-1",
                                "game_id": "game-1",
                                "sport": "nba",
                                "series": "playoffs",
                                "sports_market_type": "moneyline",
                                "selection_name": "Home Team",
                                "bookmaker": "book-a",
                                "decimal_odds": 1.8,
                                "start_time": "2026-04-21T20:00:00Z",
                            },
                            {
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "label": 1,
                                "event_key": "event-1",
                                "game_id": "game-1",
                                "sport": "nba",
                                "series": "playoffs",
                                "sports_market_type": "moneyline",
                                "selection_name": "Home Team",
                                "bookmaker": "book-b",
                                "decimal_odds": 1.9,
                                "start_time": "2026-04-21T20:00:00Z",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = ingest_live_data.main(
                [
                    "build-training-dataset",
                    "--input",
                    str(training_input),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )

            latest_rows_path = (
                root / "processed" / "training" / "historical_training_dataset.jsonl"
            )
            latest_rows = [
                json.loads(line)
                for line in latest_rows_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(result, 0)
        self.assertEqual([row["label"] for row in latest_rows], [0, 1])
        self.assertEqual(len({row["record_id"] for row in latest_rows}), 2)
        expected_keys = {
            "home_team",
            "away_team",
            "label",
            "record_id",
            "recorded_at",
            "event_key",
            "sport",
            "series",
            "game_id",
            "sports_market_type",
            "source",
            "market_key",
            "condition_id",
            "metadata",
        }
        for row in latest_rows:
            self.assertEqual(set(row.keys()), expected_keys)
            self.assertIsNone(row["market_key"])
            self.assertIsNone(row["condition_id"])

    def test_build_training_dataset_skips_ambiguous_polymarket_market_links(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            training_input = Path(temp_dir) / "sports-inputs.json"
            polymarket_input = Path(temp_dir) / "polymarket-markets.json"
            captured_at = "2026-04-21T18:00:00Z"
            training_input.write_text(
                json.dumps(
                    {
                        "source": "sports-inputs",
                        "captured_at": captured_at,
                        "rows": [
                            {
                                "home_team": "Home Team",
                                "away_team": "Away Team",
                                "label": 1,
                                "event_key": "event-1",
                                "game_id": "game-1",
                                "sport": "nba",
                                "series": "playoffs",
                                "sports_market_type": "moneyline",
                                "selection_name": "Home Team",
                                "decimal_odds": 1.8,
                                "start_time": "2026-04-21T20:00:00Z",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            polymarket_input.write_text(
                json.dumps(
                    {
                        "layer": "gamma",
                        "captured_at": captured_at,
                        "markets": [
                            {
                                "conditionId": "condition-1",
                                "eventKey": "event-1",
                                "sportsMarketType": "moneyline",
                                "question": "Will Home Team beat Away Team?",
                                "tokens": [
                                    {
                                        "token_id": "token-home",
                                        "outcome": "Yes",
                                        "midpoint": 0.6,
                                    },
                                    {
                                        "token_id": "token-away",
                                        "outcome": "No",
                                        "midpoint": 0.4,
                                    },
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = ingest_live_data.main(
                [
                    "build-training-dataset",
                    "--input",
                    str(training_input),
                    "--polymarket-input",
                    str(polymarket_input),
                    "--root",
                    str(root),
                    "--quiet",
                ]
            )

            latest_rows_path = (
                root / "processed" / "training" / "historical_training_dataset.jsonl"
            )
            latest_rows = [
                json.loads(line)
                for line in latest_rows_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

        self.assertEqual(result, 0)
        self.assertEqual(len(latest_rows), 1)
        self.assertIsNone(latest_rows[0]["market_key"])
        self.assertIsNone(latest_rows[0]["condition_id"])
        self.assertNotIn("market_market_count", latest_rows[0]["metadata"])

    def test_read_current_table_prefers_projected_state_when_postgres_authoritative(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            current_store = ingest_live_data.FileBackedCurrentStateStore(
                root / "current"
            )
            current_store.write_table(
                "fair_values",
                {"stale-market": {"market_id": "stale-market", "fair_yes_prob": 0.12}},
            )

            class _Projected:
                def read_table(self, table: str):
                    self.table = table
                    return {"pm-1": {"market_id": "pm-1", "fair_yes_prob": 0.61}}

            projected = _Projected()
            rows = ingest_live_data._read_current_table(
                {
                    "projected_authoritative": True,
                    "current": current_store,
                    "current_read": projected,
                },
                "fair_values",
            )

        self.assertEqual(projected.table, "fair_values")
        self.assertEqual(list(rows.keys()), ["pm-1"])

    def test_build_opportunities_uses_current_fair_values_over_history_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_opportunity_build_inputs(
                root,
                event_start_time=datetime.now(timezone.utc) + timedelta(hours=2),
            )
            self._write_json(
                root / "postgres" / "fair_values.json",
                {
                    "pm-1|2026-04-21T18:00:00+00:00|aaa|v1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                        "fair_yes_prob": 0.20,
                        "lower_prob": 0.18,
                        "upper_prob": 0.22,
                        "book_dispersion": 0.01,
                        "data_age_ms": 250,
                        "source_count": 2,
                        "model_name": "aaa",
                        "model_version": "v1",
                    },
                    "pm-1|2026-04-21T18:00:00+00:00|zzz|v1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                        "fair_yes_prob": 0.90,
                        "lower_prob": 0.88,
                        "upper_prob": 0.92,
                        "book_dispersion": 0.01,
                        "data_age_ms": 250,
                        "source_count": 2,
                        "model_name": "zzz",
                        "model_version": "v1",
                    },
                },
            )
            self._write_json(
                root / "current" / "fair_values.json",
                {
                    "pm-1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                        "fair_yes_prob": 0.61,
                        "lower_prob": 0.58,
                        "upper_prob": 0.64,
                        "book_dispersion": 0.01,
                        "data_age_ms": 250,
                        "source_count": 2,
                        "model_name": "deterministic_consensus",
                        "model_version": "v1",
                    }
                },
            )

            ingest_live_data.main(
                ["build-opportunities", "--root", str(root), "--quiet"]
            )

            opportunities = json.loads(
                (root / "current" / "opportunities.json").read_text()
            )

        persisted = next(iter(opportunities.values()))
        self.assertAlmostEqual(persisted["edge_after_costs_bps"], 900.0, places=4)
        self.assertAlmostEqual(persisted["edge_buy_after_costs_bps"], 900.0, places=4)
        self.assertAlmostEqual(
            persisted["edge_sell_after_costs_bps"], -1100.0, places=4
        )
        self.assertEqual(persisted["blocked_reasons"], [])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib.util
import os
import stat
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_live_data
from services.capture import (
    PolymarketMarketSnapshotRequest,
    SportsbookCaptureRequest,
    SportsbookCaptureStores,
    capture_sportsbook_odds_once,
    hydrate_polymarket_market_snapshot,
    persist_polymarket_bbo_input_events,
)
from services.capture import sportsbook as sportsbook_capture
from services.projection import project_current_state_once
from storage.current_projection import build_preview_runtime_context
from storage.postgres import (
    BBORepository,
    FairValueRepository,
    MappingRepository,
    MarketRepository,
    ModelRegistryRepository,
    OpportunityRepository,
    SourceHealthRepository,
    SportsbookEventRepository,
    SportsbookOddsRepository,
    TradeAttributionRepository,
)
from storage.postgres.bootstrap import (
    bootstrap_postgres,
    connect_postgres,
    resolve_postgres_dsn,
    write_dsn_marker,
)
from storage.postgres.repositories import upsert_capture_checkpoint
from storage.current_read_adapter import ProjectedCurrentStateReadAdapter


TEST_DSN_ENV = "PREDICTION_MARKET_POSTGRES_TEST_DSN"


def _postgres_test_dsn() -> str | None:
    return os.getenv(TEST_DSN_ENV)


class PostgresBootstrapTests(unittest.TestCase):
    def test_write_dsn_marker_supports_runtime_root_resolution(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            marker_path = write_dsn_marker(root, "postgresql://example/test")

            self.assertEqual(marker_path, root / "postgres" / "postgres.dsn")
            self.assertEqual(
                resolve_postgres_dsn(root / "postgres"),
                "postgresql://example/test",
            )
            self.assertEqual(stat.S_IMODE(marker_path.stat().st_mode), 0o600)

    def test_resolve_postgres_dsn_supports_direct_marker_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / "postgres.dsn"
            marker_path.write_text("postgresql://file/test", encoding="utf-8")

            self.assertEqual(
                resolve_postgres_dsn(marker_path),
                "postgresql://file/test",
            )

    def test_live_ingest_stores_fail_closed_when_postgres_is_required(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "PREDICTION_MARKET_POSTGRES_DSN": "",
                    "POSTGRES_DSN": "",
                    "DATABASE_URL": "",
                },
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "ingest-live-data sportsbook-odds requires Postgres authority",
                ):
                    ingest_live_data._stores(
                        temp_dir,
                        require_postgres=True,
                        authority_context="ingest-live-data sportsbook-odds",
                    )

    def test_non_live_ingest_stores_can_still_use_json_backed_fallback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "PREDICTION_MARKET_POSTGRES_DSN": "",
                    "POSTGRES_DSN": "",
                    "DATABASE_URL": "",
                },
            ):
                stores = ingest_live_data._stores(temp_dir)
            payload = {
                "market_id": "pm-1",
                "as_of": "2026-04-22T18:03:00+00:00",
                "fair_yes_prob": 0.61,
                "lower_prob": 0.58,
                "upper_prob": 0.64,
                "book_dispersion": 0.01,
                "data_age_ms": 100,
                "source_count": 2,
                "model_name": "consensus",
                "model_version": "v1",
            }

            stores["fair_values"].append(payload)

            self.assertFalse(stores["projected_authoritative"])
            self.assertEqual(len(stores["fair_values"].read_all()), 1)


class PostgresStorageIntegrationTests(unittest.TestCase):
    dsn: str

    @classmethod
    def setUpClass(cls) -> None:
        if importlib.util.find_spec("psycopg") is None:
            raise unittest.SkipTest(
                "psycopg not installed; skipping Postgres integration tests"
            )
        dsn = _postgres_test_dsn()
        if not dsn:
            raise unittest.SkipTest(
                f"Set {TEST_DSN_ENV} to run Postgres integration tests"
            )
        cls.dsn = dsn
        bootstrap_postgres(cls.dsn)

    def setUp(self) -> None:
        with connect_postgres(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE
                      raw_capture_events,
                      source_health_events,
                      capture_checkpoints,
                      opportunities_current,
                      fair_values_current,
                      market_mappings_current,
                      sportsbook_odds_current,
                      polymarket_book_snapshots,
                      model_registry,
                      trade_attribution,
                      opportunities,
                      fair_values,
                      market_mappings,
                      sportsbook_odds,
                      source_health,
                      sportsbook_events,
                      polymarket_bbo,
                      polymarket_markets
                    RESTART IDENTITY CASCADE
                    """
                )
            connection.commit()

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

    def test_bootstrap_records_all_migrations(self):
        with connect_postgres(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT filename FROM schema_migrations ORDER BY filename"
                )
                filenames = [row[0] for row in cursor.fetchall()]

        self.assertEqual(
            filenames,
            [
                "001_initial_schema.sql",
                "002_projection_substrate.sql",
                "003_capture_substrate.sql",
                "004_sportsbook_capture_timestamps.sql",
            ],
        )

    def test_repositories_round_trip_and_update_current_state_tables(self):
        market_repo = MarketRepository(dsn=self.dsn)
        bbo_repo = BBORepository(dsn=self.dsn)
        event_repo = SportsbookEventRepository(dsn=self.dsn)
        odds_repo = SportsbookOddsRepository(dsn=self.dsn)
        mapping_repo = MappingRepository(dsn=self.dsn)
        fair_value_repo = FairValueRepository(dsn=self.dsn)
        opportunity_repo = OpportunityRepository(dsn=self.dsn)
        trade_repo = TradeAttributionRepository(dsn=self.dsn)
        model_repo = ModelRegistryRepository(dsn=self.dsn)
        health_repo = SourceHealthRepository(dsn=self.dsn)

        market_repo.upsert(
            "pm-1",
            {
                "market_id": "pm-1",
                "condition_id": "condition-1",
                "token_id_yes": "yes-token",
                "token_id_no": "no-token",
                "title": "Will Team A win?",
                "description": "test market",
                "event_slug": "team-a-vs-team-b",
                "market_slug": "win",
                "category": "sports",
                "end_time": "2026-04-22T22:00:00+00:00",
                "status": "open",
                "raw_json": {"id": "pm-1"},
            },
        )
        bbo_repo.upsert(
            "pm-1",
            {
                "market_id": "pm-1",
                "best_bid_yes": 0.45,
                "best_bid_yes_size": 12.0,
                "best_ask_yes": 0.47,
                "best_ask_yes_size": 8.0,
                "midpoint_yes": 0.46,
                "spread_yes": 0.02,
                "book_ts": "2026-04-22T18:05:00+00:00",
                "source_age_ms": 250,
                "raw_hash": "hash-1",
            },
        )
        event_repo.upsert(
            "sb-1",
            {
                "sportsbook_event_id": "sb-1",
                "source": "theoddsapi",
                "sport": "basketball_nba",
                "league": "NBA",
                "home_team": "Team A",
                "away_team": "Team B",
                "start_time": "2026-04-22T19:00:00+00:00",
                "raw_json": {"id": "sb-1", "event_key": "event-1"},
            },
        )
        odds_repo.append(
            {
                "sportsbook_event_id": "sb-1",
                "source": "theoddsapi",
                "market_type": "h2h",
                "selection": "Team A",
                "price_decimal": 1.55,
                "implied_prob": 0.645161,
                "overround": 1.01,
                "provider": "theoddsapi",
                "source_ts": "2026-04-22T17:59:30+00:00",
                "capture_ts": "2026-04-22T18:00:00+00:00",
                "quote_ts": "2026-04-22T18:00:00+00:00",
                "source_age_ms": 0,
                "raw_json": {"book": "a"},
            },
        )
        odds_repo.append(
            {
                "sportsbook_event_id": "sb-1",
                "source": "theoddsapi",
                "market_type": "h2h",
                "selection": "Team A",
                "price_decimal": 1.50,
                "implied_prob": 0.666667,
                "overround": 1.02,
                "provider": "theoddsapi",
                "source_ts": "2026-04-22T18:01:30+00:00",
                "capture_ts": "2026-04-22T18:02:00+00:00",
                "quote_ts": "2026-04-22T18:02:00+00:00",
                "source_age_ms": 0,
                "raw_json": {"book": "b"},
            },
        )
        mapping_repo.append(
            {
                "polymarket_market_id": "pm-1",
                "sportsbook_event_id": "sb-1",
                "sportsbook_market_type": "h2h",
                "normalized_market_type": "moneyline_full_game",
                "match_confidence": 0.98,
                "resolution_risk": 0.02,
                "mismatch_reason": None,
                "event_key": "event-1",
                "sport": "nba",
                "series": "playoffs",
                "game_id": "game-1",
                "blocked_reason": None,
                "is_active": True,
            },
        )
        fair_value_repo.append(
            {
                "market_id": "pm-1",
                "as_of": "2026-04-22T18:03:00+00:00",
                "fair_yes_prob": 0.61,
                "calibrated_fair_yes_prob": 0.60,
                "lower_prob": 0.58,
                "upper_prob": 0.64,
                "book_dispersion": 0.01,
                "data_age_ms": 100,
                "source_count": 2,
                "model_name": "consensus",
                "model_version": "v1",
            },
        )
        opportunity_repo.append(
            {
                "market_id": "pm-1",
                "as_of": "2026-04-22T18:04:00+00:00",
                "side": "buy_yes",
                "fair_yes_prob": 0.61,
                "best_bid_yes": 0.45,
                "best_ask_yes": 0.47,
                "edge_buy_bps": 140.0,
                "edge_sell_bps": -160.0,
                "edge_buy_after_costs_bps": 120.0,
                "edge_sell_after_costs_bps": -180.0,
                "edge_after_costs_bps": 120.0,
                "fillable_size": 8.0,
                "confidence": 0.98,
                "blocked_reason": None,
                "blocked_reasons": [],
                "fair_value_ref": "2026-04-22T18:03:00+00:00",
            },
        )
        trade_repo.upsert(
            "trade-1",
            {
                "trade_id": "trade-1",
                "market_id": "pm-1",
                "expected_edge_bps": 120.0,
                "realized_edge_bps": 95.0,
                "slippage_bps": -25.0,
                "pnl": 10.5,
                "model_error": 5.0,
                "stale_data_flag": False,
                "mapping_risk": 0.02,
                "notes": {"strategy": "test"},
            },
        )
        model_repo.append(
            {
                "model_name": "consensus",
                "model_version": "v1",
                "created_at": "2026-04-22T18:00:00+00:00",
                "feature_spec": {"features": ["fair_yes_prob"]},
                "metrics": {"brier": 0.12},
                "artifact_uri": "runtime/models/consensus-v1.json",
            },
        )
        health_repo.upsert(
            "fair_values",
            {
                "source_name": "fair_values",
                "last_seen_at": "2026-04-22T18:05:00+00:00",
                "last_success_at": "2026-04-22T18:05:00+00:00",
                "stale_after_ms": 60000,
                "status": "ok",
                "details": {"rows": 1},
            },
        )
        upsert_capture_checkpoint(
            "sportsbook_odds",
            "theoddsapi",
            "cursor-1",
            checkpoint_ts="2026-04-22T18:05:00+00:00",
            metadata={"league": "nba"},
            dsn=self.dsn,
        )

        self.assertEqual(market_repo.read_all()["pm-1"]["title"], "Will Team A win?")
        self.assertEqual(bbo_repo.read_all()["pm-1"]["best_ask_yes"], 0.47)
        self.assertEqual(event_repo.read_all()["sb-1"]["home_team"], "Team A")
        self.assertEqual(len(odds_repo.read_all()), 2)
        self.assertEqual(len(odds_repo.read_current()), 1)
        self.assertEqual(len(mapping_repo.read_all()), 1)
        self.assertEqual(len(mapping_repo.read_current()), 1)
        fair_value_history = fair_value_repo.read_all()
        self.assertEqual(len(fair_value_history), 1)
        self.assertEqual(next(iter(fair_value_history.values()))["fair_yes_prob"], 0.61)
        self.assertEqual(fair_value_repo.read_current()["pm-1"]["fair_yes_prob"], 0.61)
        self.assertEqual(len(opportunity_repo.read_all()), 1)
        self.assertEqual(
            opportunity_repo.read_current()["pm-1|buy_yes"]["confidence"], 0.98
        )
        self.assertEqual(
            model_repo.read_all()["consensus|v1"]["artifact_uri"],
            "runtime/models/consensus-v1.json",
        )
        self.assertEqual(health_repo.read_all()["fair_values"]["status"], "ok")

        with connect_postgres(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM raw_capture_events")
                raw_event_count = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT payload->>'quote_ts' FROM sportsbook_odds_current WHERE sportsbook_event_id = 'sb-1'"
                )
                latest_quote_ts = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT provider, source_ts::text, capture_ts::text FROM sportsbook_odds ORDER BY quote_ts DESC LIMIT 1"
                )
                latest_quote_metadata = cursor.fetchone()
                cursor.execute(
                    "SELECT checkpoint_value FROM capture_checkpoints WHERE checkpoint_name = 'sportsbook_odds' AND source_name = 'theoddsapi'"
                )
                checkpoint_value = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM source_health_events")
                source_health_events = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM polymarket_book_snapshots")
                book_snapshot_count = cursor.fetchone()[0]

        self.assertGreaterEqual(raw_event_count, 10)
        self.assertEqual(latest_quote_ts, "2026-04-22T18:02:00+00:00")
        self.assertEqual(latest_quote_metadata[0], "theoddsapi")
        self.assertEqual(latest_quote_metadata[1], "2026-04-22 18:01:30+00")
        self.assertEqual(latest_quote_metadata[2], "2026-04-22 18:02:00+00")
        self.assertEqual(checkpoint_value, "cursor-1")
        self.assertEqual(source_health_events, 1)
        self.assertEqual(book_snapshot_count, 1)

    def test_capture_projection_runtime_read_chain_works_with_postgres(self):
        class _StaticSource:
            def __init__(self, event_payload):
                self.event_payload = event_payload

            provider_name = "theoddsapi"

            def fetch_upcoming(self, sport: str, market_type: str):
                return [self.event_payload]

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

        class _StaticCatalogClient:
            def fetch_open_markets(self):
                return [
                    {
                        "id": "pm-1",
                        "conditionId": "pm-1",
                        "question": "Will Home Team beat Away Team?",
                        "active": True,
                        "tokenIds": ["yes-token", "no-token"],
                    }
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            write_dsn_marker(root, self.dsn)
            event_map = Path(temp_dir) / "event_map.json"
            event_map.write_text(
                '{"sb-1": {"event_key": "event-1", "game_id": "game-1", "sport": "nba", "series": "playoffs"}}',
                encoding="utf-8",
            )

            capture_sportsbook_odds_once(
                SportsbookCaptureRequest(
                    root=str(root),
                    sport="basketball_nba",
                    market="h2h",
                    event_map_file=str(event_map),
                ),
                source=_StaticSource(
                    {
                        "id": "sb-1",
                        "sport_title": "NBA",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "commence_time": "2026-05-21T20:00:00+00:00",
                        "bookmakers": [
                            {
                                "key": "book-a",
                                "last_update": "2026-05-21T17:59:30+00:00",
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
                ),
                stores=SportsbookCaptureStores.from_root(root, require_postgres=True),
                observed_at=datetime(2026, 5, 21, 18, 0, tzinfo=timezone.utc),
            )
            hydrate_polymarket_market_snapshot(
                request=PolymarketMarketSnapshotRequest(
                    root=str(root),
                    sport=None,
                    market_type=None,
                    limit=500,
                    stale_after_ms=60_000,
                ),
                client=_StaticCatalogClient(),
                observed_at=datetime(2026, 5, 21, 18, 1, tzinfo=timezone.utc),
            )
            persist_polymarket_bbo_input_events(
                [
                    {
                        "asset_id": "pm-1",
                        "best_bid": 0.45,
                        "best_bid_size": 10,
                        "best_ask": 0.47,
                        "best_ask_size": 8,
                        "timestamp": "2026-05-21T18:02:00Z",
                    }
                ],
                root=str(root),
                observed_at=datetime(2026, 5, 21, 18, 2, tzinfo=timezone.utc),
            )

            projection_result = project_current_state_once(root)

            mapping_repo = MappingRepository(root / "postgres")
            fair_value_repo = FairValueRepository(root / "postgres")
            opportunity_repo = OpportunityRepository(root / "postgres")
            mapping_repo.append(
                {
                    "polymarket_market_id": "pm-1",
                    "sportsbook_event_id": "sb-1",
                    "sportsbook_market_type": "h2h",
                    "normalized_market_type": "moneyline_full_game",
                    "match_confidence": 0.98,
                    "resolution_risk": 0.02,
                    "mismatch_reason": None,
                    "event_key": "event-1",
                    "sport": "nba",
                    "series": "playoffs",
                    "game_id": "game-1",
                    "blocked_reason": None,
                    "is_active": True,
                }
            )
            fair_value_repo.append(
                {
                    "market_id": "pm-1",
                    "as_of": "2026-05-21T18:03:00+00:00",
                    "fair_yes_prob": 0.61,
                    "calibrated_fair_yes_prob": 0.60,
                    "lower_prob": 0.58,
                    "upper_prob": 0.64,
                    "book_dispersion": 0.01,
                    "data_age_ms": 100,
                    "source_count": 2,
                    "model_name": "consensus",
                    "model_version": "v1",
                }
            )
            opportunity_repo.append(
                {
                    "market_id": "pm-1",
                    "as_of": "2026-05-21T18:04:00+00:00",
                    "side": "buy_yes",
                    "fair_yes_prob": 0.61,
                    "best_bid_yes": 0.45,
                    "best_ask_yes": 0.47,
                    "edge_buy_bps": 190.0,
                    "edge_sell_bps": -160.0,
                    "edge_buy_after_costs_bps": 170.0,
                    "edge_sell_after_costs_bps": -180.0,
                    "edge_after_costs_bps": 170.0,
                    "fillable_size": 8.0,
                    "confidence": 0.98,
                    "blocked_reason": None,
                    "blocked_reasons": [],
                    "fair_value_ref": "2026-05-21T18:03:00+00:00",
                }
            )
            adapter = ProjectedCurrentStateReadAdapter.from_root(root)
            sportsbook_events = adapter.read_table("sportsbook_events")
            sportsbook_odds = adapter.read_table("sportsbook_odds")
            polymarket_markets = adapter.read_table("polymarket_markets")
            polymarket_bbo = adapter.read_table("polymarket_bbo")
            preview_context = build_preview_runtime_context(None, read_adapter=adapter)

        self.assertTrue(projection_result["ok"])
        self.assertIn("sb-1", sportsbook_events)
        self.assertIn("sb-1|book-a|h2h|Home Team", sportsbook_odds)
        self.assertIn("pm-1", polymarket_markets)
        self.assertIn("pm-1", polymarket_bbo)
        self.assertGreaterEqual(len(preview_context.preview_order_proposals), 1)

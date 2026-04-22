from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_live_data


class IngestLiveDataTests(unittest.TestCase):
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
            config_path.write_text(
                json.dumps(
                    {
                        "league": "nba",
                        "capture": {"sport_key": "basketball_nba"},
                        "runtime": {
                            "sportsbook_market": "h2h",
                            "event_map_file": str(event_map_path),
                            "consensus_artifact": str(consensus_artifact_path),
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
        self.assertTrue(
            source_health["fair_values"]["details"]["consensus_artifact_configured"]
        )

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
                ingest_live_data.SourceHealthStore,
                "upsert",
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


if __name__ == "__main__":
    unittest.main()

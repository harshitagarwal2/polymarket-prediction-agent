from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_live_data


class IngestLiveDataTests(unittest.TestCase):
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
        market_payload = [
            {
                "id": "pm-1",
                "question": "Will Home Team beat Away Team?",
                "sports_market_type": "moneyline",
                "sport": "nba",
                "active": True,
                "gameStartTime": "2026-04-21T19:00:00Z",
            }
        ]
        odds_payload = [
            {
                "id": "sb-1",
                "sport_title": "NBA",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "commence_time": "2026-04-21T19:00:00Z",
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
                                datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc).timestamp()
                                * 1000
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
                    ["polymarket-markets", "--sport", "nba", "--root", str(root), "--quiet"]
                )

            with patch.object(
                ingest_live_data.TheOddsApiClient,
                "fetch_upcoming",
                return_value=odds_payload,
            ), patch.dict("os.environ", {"THE_ODDS_API_KEY": "test-key"}, clear=False):
                ingest_live_data.main(
                    [
                        "sportsbook-odds",
                        "--sport",
                        "basketball_nba",
                        "--market",
                        "h2h",
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
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )
            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])
            ingest_live_data.main(["build-opportunities", "--root", str(root), "--quiet"])

            fair_values = json.loads((root / "current" / "fair_values.json").read_text())
            opportunities = json.loads((root / "current" / "opportunities.json").read_text())

        self.assertIn("pm-1", fair_values)
        self.assertTrue(any(key.startswith("pm-1|") for key in opportunities))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from adapters import MarketSummary
from adapters.types import Contract, OutcomeSide, Venue
from scripts import refresh_sports_fair_values


class RefreshSportsFairValuesTests(unittest.TestCase):
    def _args(self, output: str, status_file: str):
        return SimpleNamespace(
            sport_key="basketball_nba",
            output=output,
            status_file=status_file,
            event_map_file=None,
            regions="us",
            markets="h2h",
            odds_format="decimal",
            bookmakers=None,
            book_aggregation="best-line",
            devig_method="multiplicative",
            max_age_seconds=900.0,
            markets_limit=200,
            refresh_interval_seconds=0.0,
            max_cycles=1,
            api_key_env="THE_ODDS_API_KEY",
        )

    def test_run_refresh_cycle_writes_manifest_and_status(self):
        markets = [
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET, symbol="token-yes", outcome=OutcomeSide.YES
                ),
                title="Will Home Team win?",
                sport="nba",
                sports_market_type="moneyline",
                active=True,
                raw={"market": {"condition_id": "condition-1"}},
            ),
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET, symbol="token-no", outcome=OutcomeSide.NO
                ),
                title="Will Home Team win?",
                sport="nba",
                sports_market_type="moneyline",
                active=True,
                raw={"market": {"condition_id": "condition-1"}},
            ),
        ]
        payload = [
            {
                "id": "event-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "bookmakers": [
                    {
                        "key": "book-a",
                        "last_update": "2026-04-07T12:00:00Z",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home Team", "price": 1.7},
                                    {"name": "Away Team", "price": 2.3},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as status_handle,
            patch.object(
                refresh_sports_fair_values, "build_adapter"
            ) as build_adapter_mock,
            patch.object(
                refresh_sports_fair_values, "fetch_odds_payload", return_value=payload
            ),
        ):
            build_adapter_mock.return_value = SimpleNamespace(
                list_markets=lambda limit=200: markets
            )
            args = self._args(output_handle.name, status_handle.name)

            status = refresh_sports_fair_values._run_refresh_cycle_impl(args)

            manifest = json.loads(open(output_handle.name, encoding="utf-8").read())
            status_payload = json.loads(
                open(status_handle.name, encoding="utf-8").read()
            )

        self.assertTrue(status["ok"])
        self.assertEqual(
            sorted(manifest["values"].keys()), ["token-no:no", "token-yes:yes"]
        )
        self.assertTrue(status_payload["ok"])

    def test_main_keeps_last_good_manifest_on_failure(self):
        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as status_handle,
            patch.object(
                refresh_sports_fair_values, "_run_refresh_cycle_impl"
            ) as cycle_mock,
            patch("sys.argv") as argv,
        ):
            json.dump({"values": {"token-1:yes": {"fair_value": 0.6}}}, output_handle)
            output_handle.flush()
            cycle_mock.side_effect = [
                {
                    "ok": True,
                    "last_success_at": "2026-04-07T12:00:00+00:00",
                    "output": output_handle.name,
                },
                RuntimeError("fetch failed"),
            ]
            argv.return_value = None
            argv.configure_mock(
                **{
                    "__iter__.return_value": iter(
                        [
                            "refresh_sports_fair_values.py",
                            "--sport-key",
                            "basketball_nba",
                            "--output",
                            output_handle.name,
                            "--status-file",
                            status_handle.name,
                            "--max-cycles",
                            "2",
                            "--refresh-interval-seconds",
                            "0",
                        ]
                    )
                }
            )

            with patch.object(
                refresh_sports_fair_values, "build_parser"
            ) as parser_mock:
                parser_mock.return_value.parse_args.return_value = self._args(
                    output_handle.name, status_handle.name
                )
                parser_mock.return_value.parse_args.return_value.max_cycles = 2
                refresh_sports_fair_values.main()

            manifest = json.loads(open(output_handle.name, encoding="utf-8").read())
            status_payload = json.loads(
                open(status_handle.name, encoding="utf-8").read()
            )

        self.assertEqual(manifest["values"]["token-1:yes"]["fair_value"], 0.6)
        self.assertFalse(status_payload["ok"])
        self.assertEqual(status_payload["last_success_at"], "2026-04-07T12:00:00+00:00")

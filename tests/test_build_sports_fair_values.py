from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adapters.types import (
    Contract,
    MarketSummary,
    OutcomeSide,
    Venue,
    serialize_market_summary,
)
from scripts import build_sports_fair_values, fetch_the_odds_api_rows, run_agent_loop


class BuildSportsFairValuesScriptTests(unittest.TestCase):
    def test_script_builds_manifest_consumable_by_runtime(self):
        input_payload = [
            {
                "market_key": "token-yes:yes",
                "bookmaker": "book-a",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 1.7,
                "condition_id": "condition-1",
                "event_key": "event-1",
                "sport": "nba",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-no:no",
                "bookmaker": "book-a",
                "outcome": "no",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 2.3,
                "condition_id": "condition-1",
                "event_key": "event-1",
                "sport": "nba",
                "sports_market_type": "moneyline",
            },
        ]

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as input_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(input_payload, input_handle)
            input_handle.flush()

            with patch(
                "sys.argv",
                [
                    "build_sports_fair_values.py",
                    "--input",
                    input_handle.name,
                    "--output",
                    output_handle.name,
                    "--devig-method",
                    "multiplicative",
                    "--max-age-seconds",
                    "900",
                ],
            ):
                build_sports_fair_values.main()

            provider = run_agent_loop.build_fair_value_provider(output_handle.name)
            output_payload = json.loads(Path(output_handle.name).read_text())

        self.assertIsInstance(provider, run_agent_loop.ManifestFairValueProvider)
        if not isinstance(provider, run_agent_loop.ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")
        self.assertEqual(provider.max_age_seconds, 900.0)
        self.assertIn("token-yes:yes", provider.records)
        self.assertIn("token-no:no", provider.records)
        metadata = output_payload.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata")
        self.assertEqual(metadata["provenance"]["book_aggregation"], "independent")
        self.assertEqual(metadata["coverage"]["value_count"], 2)

    def test_script_can_apply_optional_calibration_artifact(self):
        input_payload = [
            {
                "market_key": "token-yes:yes",
                "bookmaker": "book-a",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 1.7,
                "event_key": "event-1",
            },
            {
                "market_key": "token-no:no",
                "bookmaker": "book-a",
                "outcome": "no",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 2.3,
                "event_key": "event-1",
            },
        ]
        calibration_payload = {
            "row_count": 4,
            "rows": [
                {"market_key": "a", "fair_value": 0.42, "outcome_label": 0},
                {"market_key": "b", "fair_value": 0.45, "outcome_label": 0},
                {"market_key": "c", "fair_value": 0.55, "outcome_label": 1},
                {"market_key": "d", "fair_value": 0.58, "outcome_label": 1},
            ],
            "bin_count": 2,
        }

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as input_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as artifact_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(input_payload, input_handle)
            input_handle.flush()
            json.dump(calibration_payload, artifact_handle)
            artifact_handle.flush()

            with patch(
                "sys.argv",
                [
                    "build_sports_fair_values.py",
                    "--input",
                    input_handle.name,
                    "--output",
                    output_handle.name,
                    "--calibration-artifact",
                    artifact_handle.name,
                ],
            ):
                build_sports_fair_values.main()

            output_payload = json.loads(Path(output_handle.name).read_text())

        self.assertAlmostEqual(
            output_payload["values"]["token-yes:yes"]["fair_value"],
            0.575,
        )
        self.assertEqual(
            output_payload["values"]["token-yes:yes"]["calibrated_fair_value"],
            1.0,
        )
        self.assertEqual(
            output_payload["values"]["token-no:no"]["calibrated_fair_value"],
            0.0,
        )
        metadata = output_payload.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata")
        self.assertEqual(metadata["calibration"]["bin_count"], 2)
        self.assertEqual(metadata["calibration"]["sample_count"], 4)

    def test_script_can_match_rows_against_market_snapshot(self):
        input_payload = [
            {
                "bookmaker": "book-a",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 1.7,
                "event_key": "event-1",
                "sport": "nba",
                "sports_market_type": "moneyline",
            },
            {
                "bookmaker": "book-a",
                "outcome": "no",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 2.3,
                "event_key": "event-1",
                "sport": "nba",
                "sports_market_type": "moneyline",
            },
        ]
        markets = [
            serialize_market_summary(
                MarketSummary(
                    contract=Contract(
                        venue=Venue.POLYMARKET,
                        symbol="token-yes",
                        outcome=OutcomeSide.YES,
                    ),
                    event_key="event-1",
                    sport="nba",
                    sports_market_type="moneyline",
                    active=True,
                )
            ),
            serialize_market_summary(
                MarketSummary(
                    contract=Contract(
                        venue=Venue.POLYMARKET,
                        symbol="token-no",
                        outcome=OutcomeSide.NO,
                    ),
                    event_key="event-1",
                    sport="nba",
                    sports_market_type="moneyline",
                    active=True,
                )
            ),
        ]

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as input_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as markets_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(input_payload, input_handle)
            input_handle.flush()
            json.dump(markets, markets_handle)
            markets_handle.flush()

            with patch(
                "sys.argv",
                [
                    "build_sports_fair_values.py",
                    "--input",
                    input_handle.name,
                    "--markets-file",
                    markets_handle.name,
                    "--output",
                    output_handle.name,
                ],
            ):
                build_sports_fair_values.main()

            payload = json.loads(output_handle.read())

        self.assertEqual(
            sorted(payload["values"].keys()), ["token-no:no", "token-yes:yes"]
        )

    def test_script_supports_best_line_aggregation(self):
        input_payload = [
            {
                "market_key": "token-yes:yes",
                "bookmaker": "book-a",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 1.7,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-no:no",
                "bookmaker": "book-a",
                "outcome": "no",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 2.0,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-yes:yes",
                "bookmaker": "book-b",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 1.8,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-no:no",
                "bookmaker": "book-b",
                "outcome": "no",
                "captured_at": "2026-04-07T12:03:00Z",
                "decimal_odds": 2.1,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
        ]

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as input_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(input_payload, input_handle)
            input_handle.flush()

            with patch(
                "sys.argv",
                [
                    "build_sports_fair_values.py",
                    "--input",
                    input_handle.name,
                    "--output",
                    output_handle.name,
                    "--book-aggregation",
                    "best-line",
                ],
            ):
                build_sports_fair_values.main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(
            sorted(payload["values"].keys()), ["token-no:no", "token-yes:yes"]
        )
        self.assertEqual(payload["source"], "sportsbook-devig:multiplicative:best-line")
        self.assertEqual(
            payload["values"]["token-yes:yes"]["generated_at"],
            "2026-04-07T12:03:00Z",
        )
        self.assertEqual(
            payload["values"]["token-no:no"]["generated_at"],
            "2026-04-07T12:03:00Z",
        )
        self.assertEqual(payload["values"]["token-yes:yes"]["bookmaker"], "best-line")
        self.assertEqual(
            payload["values"]["token-yes:yes"]["source_bookmaker"], "book-b"
        )

    def test_script_can_read_devig_and_aggregation_from_config_file(self):
        input_payload = [
            {
                "market_key": "token-yes:yes",
                "bookmaker": "book-a",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 1.7,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-no:no",
                "bookmaker": "book-a",
                "outcome": "no",
                "captured_at": "2026-04-07T12:00:00Z",
                "decimal_odds": 2.0,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-yes:yes",
                "bookmaker": "book-b",
                "outcome": "yes",
                "captured_at": "2026-04-07T12:01:00Z",
                "decimal_odds": 1.8,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
            {
                "market_key": "token-no:no",
                "bookmaker": "book-b",
                "outcome": "no",
                "captured_at": "2026-04-07T12:03:00Z",
                "decimal_odds": 2.1,
                "event_key": "event-1",
                "sports_market_type": "moneyline",
            },
        ]

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as input_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(input_payload, input_handle)
            input_handle.flush()

            with patch(
                "sys.argv",
                [
                    "build_sports_fair_values.py",
                    "--input",
                    input_handle.name,
                    "--output",
                    output_handle.name,
                    "--config-file",
                    "configs/sports_nba.yaml",
                ],
            ):
                build_sports_fair_values.main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(payload["source"], "sportsbook-devig:multiplicative:best-line")
        metadata = payload.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata")
        self.assertEqual(metadata["provenance"]["book_aggregation"], "best-line")

    def test_collector_rows_can_build_without_event_map_for_moneyline_titles(self):
        events = [
            {
                "id": "event-1",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "commence_time": "2026-04-07T19:00:00Z",
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
        rows = fetch_the_odds_api_rows.normalize_odds_events(
            events,
            sport_key="basketball_nba",
            event_map={},
        )
        markets = [
            serialize_market_summary(
                MarketSummary(
                    contract=Contract(
                        venue=Venue.POLYMARKET,
                        symbol="token-yes",
                        outcome=OutcomeSide.YES,
                    ),
                    title="Will Home Team win?",
                    sport="nba",
                    sports_market_type="moneyline",
                    active=True,
                    raw={"market": {"condition_id": "condition-1"}},
                )
            ),
            serialize_market_summary(
                MarketSummary(
                    contract=Contract(
                        venue=Venue.POLYMARKET,
                        symbol="token-no",
                        outcome=OutcomeSide.NO,
                    ),
                    title="Will Home Team win?",
                    sport="nba",
                    sports_market_type="moneyline",
                    active=True,
                    raw={"market": {"condition_id": "condition-1"}},
                )
            ),
        ]

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as input_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as markets_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
        ):
            json.dump(rows, input_handle)
            input_handle.flush()
            json.dump(markets, markets_handle)
            markets_handle.flush()

            with patch(
                "sys.argv",
                [
                    "build_sports_fair_values.py",
                    "--input",
                    input_handle.name,
                    "--markets-file",
                    markets_handle.name,
                    "--output",
                    output_handle.name,
                ],
            ):
                build_sports_fair_values.main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(
            sorted(payload["values"].keys()), ["token-no:no", "token-yes:yes"]
        )


if __name__ == "__main__":
    unittest.main()

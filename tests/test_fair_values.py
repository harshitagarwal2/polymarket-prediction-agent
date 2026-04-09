from __future__ import annotations

import unittest

from research.calibration import fit_histogram_calibrator_from_rows
from research.fair_values import (
    SportsbookFairValueRow,
    american_to_decimal,
    build_fair_value_manifest,
    devig_probabilities,
    implied_probability_from_odds,
    parse_timestamp,
    resolve_rows_to_markets,
)
from adapters.types import Contract, MarketSummary, OutcomeSide, Venue


class FairValueResearchTests(unittest.TestCase):
    def test_american_to_decimal_positive_and_negative(self):
        self.assertAlmostEqual(american_to_decimal(150), 2.5)
        self.assertAlmostEqual(american_to_decimal(-120), 1.8333333333)

    def test_implied_probability_accepts_decimal_and_american(self):
        self.assertAlmostEqual(implied_probability_from_odds(decimal_odds=2.0), 0.5)
        self.assertAlmostEqual(
            implied_probability_from_odds(american_odds=150),
            0.4,
        )

    def test_devig_probabilities_multiplicative(self):
        fair = devig_probabilities([0.6, 0.5], method="multiplicative")

        self.assertAlmostEqual(sum(fair), 1.0)
        self.assertAlmostEqual(fair[0], 0.5454545454)
        self.assertAlmostEqual(fair[1], 0.4545454545)

    def test_devig_probabilities_power(self):
        fair = devig_probabilities([0.6, 0.5], method="power")

        self.assertAlmostEqual(sum(fair), 1.0)
        self.assertGreater(fair[0], fair[1])

    def test_build_fair_value_manifest_emits_binary_records(self):
        rows = [
            SportsbookFairValueRow(
                market_key="token-yes:yes",
                bookmaker="book-a",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                condition_id="condition-1",
                event_key="event-1",
                sport="nba",
                sports_market_type="moneyline",
            ),
            SportsbookFairValueRow(
                market_key="token-no:no",
                bookmaker="book-a",
                outcome="no",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=2.3,
                condition_id="condition-1",
                event_key="event-1",
                sport="nba",
                sports_market_type="moneyline",
            ),
        ]

        manifest = build_fair_value_manifest(
            rows,
            method="multiplicative",
            max_age_seconds=900,
        )

        yes_fair_value = manifest.values["token-yes:yes"]["fair_value"]
        no_fair_value = manifest.values["token-no:no"]["fair_value"]
        self.assertIsInstance(yes_fair_value, float)
        self.assertIsInstance(no_fair_value, float)
        if not isinstance(yes_fair_value, float) or not isinstance(
            no_fair_value, float
        ):
            self.fail("expected float fair values in manifest")

        self.assertEqual(manifest.max_age_seconds, 900)
        self.assertEqual(set(manifest.values), {"token-yes:yes", "token-no:no"})
        self.assertAlmostEqual(
            yes_fair_value + no_fair_value,
            1.0,
            places=6,
        )
        self.assertEqual(
            manifest.values["token-yes:yes"]["condition_id"], "condition-1"
        )
        self.assertEqual(manifest.values["token-yes:yes"]["event_key"], "event-1")
        self.assertEqual(manifest.values["token-yes:yes"]["bookmaker"], "book-a")
        self.assertEqual(manifest.values["token-yes:yes"]["source_bookmaker"], "book-a")
        self.assertEqual(
            manifest.values["token-yes:yes"]["source_captured_at"],
            "2026-04-07T12:00:00Z",
        )
        self.assertEqual(
            manifest.values["token-yes:yes"]["match_strategy"], "input_market_key"
        )

        payload = manifest.to_payload()
        metadata = payload.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata")
        self.assertEqual(metadata["provenance"]["devig_method"], "multiplicative")
        self.assertEqual(metadata["provenance"]["book_aggregation"], "independent")
        self.assertEqual(metadata["provenance"]["bookmakers"], ["book-a"])
        self.assertEqual(
            metadata["freshness"]["captured_at_min"], "2026-04-07T12:00:00Z"
        )
        self.assertEqual(
            metadata["freshness"]["captured_at_max"], "2026-04-07T12:00:00Z"
        )
        self.assertEqual(metadata["freshness"]["max_age_seconds"], 900)
        self.assertEqual(metadata["coverage"]["input_row_count"], 2)
        self.assertEqual(metadata["coverage"]["value_count"], 2)
        self.assertEqual(metadata["coverage"]["skipped_group_count"], 0)
        self.assertEqual(
            metadata["match_quality"]["match_strategy_counts"],
            {"input_market_key": 2},
        )

    def test_build_fair_value_manifest_preserves_raw_and_adds_calibrated_field(self):
        rows = [
            SportsbookFairValueRow(
                market_key="token-yes:yes",
                bookmaker="book-a",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                event_key="event-1",
            ),
            SportsbookFairValueRow(
                market_key="token-no:no",
                bookmaker="book-a",
                outcome="no",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=2.3,
                event_key="event-1",
            ),
        ]
        calibration_artifact = fit_histogram_calibrator_from_rows(
            [
                {"fair_value": 0.42, "outcome_label": 0},
                {"fair_value": 0.45, "outcome_label": 0},
                {"fair_value": 0.55, "outcome_label": 1},
                {"fair_value": 0.58, "outcome_label": 1},
            ],
            bin_count=2,
        )

        manifest = build_fair_value_manifest(
            rows, calibration_artifact=calibration_artifact
        )
        yes_record = manifest.values["token-yes:yes"]
        no_record = manifest.values["token-no:no"]
        yes_fair_value = yes_record["fair_value"]
        yes_calibrated_fair_value = yes_record["calibrated_fair_value"]
        no_fair_value = no_record["fair_value"]
        no_calibrated_fair_value = no_record["calibrated_fair_value"]

        self.assertIsInstance(yes_fair_value, float)
        self.assertIsInstance(yes_calibrated_fair_value, float)
        self.assertIsInstance(no_fair_value, float)
        self.assertIsInstance(no_calibrated_fair_value, float)
        if (
            not isinstance(
                yes_fair_value,
                float,
            )
            or not isinstance(
                yes_calibrated_fair_value,
                float,
            )
            or not isinstance(
                no_fair_value,
                float,
            )
            or not isinstance(
                no_calibrated_fair_value,
                float,
            )
        ):
            self.fail("expected manifest fair values to be floats")

        self.assertAlmostEqual(yes_fair_value, 0.575)
        self.assertAlmostEqual(
            yes_calibrated_fair_value,
            1.0,
        )
        self.assertAlmostEqual(no_fair_value, 0.425)
        self.assertAlmostEqual(
            no_calibrated_fair_value,
            0.0,
        )
        metadata = manifest.to_payload()["metadata"]
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

    def test_build_fair_value_manifest_skips_non_binary_groups(self):
        rows = [
            SportsbookFairValueRow(
                market_key="token-yes:yes",
                bookmaker="book-a",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                condition_id="condition-1",
                event_key="event-1",
            )
        ]

        manifest = build_fair_value_manifest(rows)

        self.assertEqual(manifest.values, {})
        self.assertEqual(len(manifest.skipped_groups), 1)

    def test_build_fair_value_manifest_best_line_aggregates_across_books(self):
        rows = [
            SportsbookFairValueRow(
                market_key="token-yes:yes",
                bookmaker="book-a",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                event_key="event-1",
                sports_market_type="moneyline",
            ),
            SportsbookFairValueRow(
                market_key="token-no:no",
                bookmaker="book-a",
                outcome="no",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=2.0,
                event_key="event-1",
                sports_market_type="moneyline",
            ),
            SportsbookFairValueRow(
                market_key="token-yes:yes",
                bookmaker="book-b",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.8,
                event_key="event-1",
                sports_market_type="moneyline",
            ),
            SportsbookFairValueRow(
                market_key="token-no:no",
                bookmaker="book-b",
                outcome="no",
                captured_at=parse_timestamp("2026-04-07T12:03:00Z"),
                decimal_odds=2.1,
                event_key="event-1",
                sports_market_type="moneyline",
            ),
        ]

        manifest = build_fair_value_manifest(rows, aggregation="best-line")

        self.assertEqual(set(manifest.values), {"token-yes:yes", "token-no:no"})
        self.assertEqual(manifest.source, "sportsbook-devig:multiplicative:best-line")
        self.assertEqual(manifest.skipped_groups, [])
        self.assertEqual(
            manifest.values["token-yes:yes"]["generated_at"],
            "2026-04-07T12:03:00Z",
        )
        self.assertEqual(
            manifest.values["token-no:no"]["generated_at"],
            "2026-04-07T12:03:00Z",
        )
        self.assertEqual(manifest.values["token-yes:yes"]["bookmaker"], "best-line")
        self.assertEqual(manifest.values["token-yes:yes"]["source_bookmaker"], "book-b")
        self.assertEqual(
            manifest.values["token-yes:yes"]["source_captured_at"],
            "2026-04-07T12:00:00Z",
        )

        payload = manifest.to_payload()
        metadata = payload.get("metadata")
        self.assertIsInstance(metadata, dict)
        if not isinstance(metadata, dict):
            self.fail("expected manifest metadata")
        self.assertEqual(metadata["provenance"]["book_aggregation"], "best-line")
        self.assertEqual(metadata["provenance"]["bookmakers"], ["book-a", "book-b"])
        self.assertEqual(
            metadata["freshness"]["captured_at_max"], "2026-04-07T12:03:00Z"
        )
        self.assertEqual(metadata["coverage"]["input_row_count"], 4)

    def test_resolve_rows_to_markets_matches_by_normalized_sports_fields(self):
        rows = [
            SportsbookFairValueRow(
                market_key=None,
                bookmaker="book-a",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                event_key="nba-finals-game-1",
                sport="nba",
                sports_market_type="moneyline",
            )
        ]
        markets = [
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET,
                    symbol="token-yes",
                    outcome=OutcomeSide.YES,
                ),
                event_key="nba-finals-game-1",
                sport="nba",
                sports_market_type="moneyline",
                active=True,
            )
        ]

        resolved, skipped = resolve_rows_to_markets(rows, markets)

        self.assertEqual(len(skipped), 0)
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].market_key, "token-yes:yes")
        self.assertEqual(resolved[0].market_match_strategy, "market_snapshot")

    def test_resolve_rows_to_markets_skips_ambiguous_match(self):
        rows = [
            SportsbookFairValueRow(
                market_key=None,
                bookmaker="book-a",
                outcome="yes",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                event_key="nba-finals-game-1",
                sport="nba",
                sports_market_type="moneyline",
            )
        ]
        markets = [
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET,
                    symbol="token-a",
                    outcome=OutcomeSide.YES,
                ),
                event_key="nba-finals-game-1",
                sport="nba",
                sports_market_type="moneyline",
                active=True,
            ),
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET,
                    symbol="token-b",
                    outcome=OutcomeSide.YES,
                ),
                event_key="nba-finals-game-1",
                sport="nba",
                sports_market_type="moneyline",
                active=True,
            ),
        ]

        resolved, skipped = resolve_rows_to_markets(rows, markets)

        self.assertEqual(resolved, [])
        self.assertEqual(len(skipped), 1)

    def test_resolve_rows_to_markets_derives_yes_no_from_moneyline_titles(self):
        rows = [
            SportsbookFairValueRow(
                market_key=None,
                bookmaker="book-a",
                outcome="Home Team",
                selection_name="Home Team",
                home_team="Home Team",
                away_team="Away Team",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=1.7,
                sport="nba",
                sports_market_type="moneyline",
            ),
            SportsbookFairValueRow(
                market_key=None,
                bookmaker="book-a",
                outcome="Away Team",
                selection_name="Away Team",
                home_team="Home Team",
                away_team="Away Team",
                captured_at=parse_timestamp("2026-04-07T12:00:00Z"),
                decimal_odds=2.3,
                sport="nba",
                sports_market_type="moneyline",
            ),
        ]
        markets = [
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
            ),
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
            ),
        ]

        resolved, skipped = resolve_rows_to_markets(rows, markets)

        self.assertEqual(len(skipped), 0)
        self.assertEqual(
            [row.market_key for row in resolved], ["token-yes:yes", "token-no:no"]
        )
        self.assertEqual([row.outcome for row in resolved], ["yes", "no"])
        self.assertTrue(all(row.condition_id == "condition-1" for row in resolved))
        self.assertTrue(
            all(row.market_match_strategy == "market_snapshot" for row in resolved)
        )


if __name__ == "__main__":
    unittest.main()

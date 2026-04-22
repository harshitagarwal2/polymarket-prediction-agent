from __future__ import annotations

import json
import tempfile
import unittest

from forecasting import ForecastCalibrator
from contracts.models import ContractMatch
from forecasting.consensus import (
    ConsensusComponent,
    consensus_probability,
    dispersion_score,
    remove_overround,
)
from forecasting.fair_value_engine import (
    ConsensusFairValueEngine,
    ConsensusFairValueInput,
    FairValueEngine,
)
from research.models.book_consensus import (
    consensus_probability_from_rows,
    fit_book_consensus_artifact,
    load_book_consensus_artifact,
)
from research.train.train_consensus import write_consensus_artifact


class ForecastingLayerTests(unittest.TestCase):
    def test_consensus_probability_respects_weights(self):
        probability = consensus_probability(
            [
                ConsensusComponent(probability=0.2, weight=1),
                ConsensusComponent(probability=0.8, weight=3),
            ]
        )
        self.assertAlmostEqual(probability, 0.65)

    def test_consensus_engine_returns_dispersion(self):
        result = ConsensusFairValueEngine().combine(
            [
                ConsensusFairValueInput(probability=0.4, weight=1),
                ConsensusFairValueInput(probability=0.6, weight=1),
            ]
        )
        self.assertAlmostEqual(result.fair_value, 0.5)
        self.assertGreater(result.dispersion, 0.0)

    def test_forecast_calibrator_loads_from_rows(self):
        calibrator = ForecastCalibrator.load(
            [
                {"fair_value": 0.2, "outcome_label": 0},
                {"fair_value": 0.8, "outcome_label": 1},
            ]
        )
        calibrated = calibrator.apply(0.75)
        self.assertGreaterEqual(calibrated, 0.0)
        self.assertLessEqual(calibrated, 1.0)
        self.assertGreater(
            dispersion_score(
                [
                    ConsensusComponent(probability=0.3),
                    ConsensusComponent(probability=0.7),
                ]
            ),
            0.0,
        )

    def test_remove_overround_normalizes_two_book_probs(self):
        normalized = remove_overround({"yes": 0.55, "no": 0.50})
        self.assertAlmostEqual(normalized["yes"] + normalized["no"], 1.0)
        self.assertGreater(normalized["yes"], normalized["no"])

    def test_fair_value_engine_builds_snapshot_from_two_books(self):
        snapshot = FairValueEngine().build(
            ContractMatch(
                polymarket_market_id="pm-1",
                sportsbook_event_id="sb-1",
                sportsbook_market_type="moneyline",
                normalized_market_type="moneyline_full_game",
                match_confidence=0.97,
                resolution_risk=0.03,
            ),
            [
                {"source": "book-a", "price_decimal": 1.80, "source_age_ms": 1000},
                {"source": "book-b", "price_decimal": 1.90, "source_age_ms": 2000},
            ],
        )
        self.assertEqual(snapshot.market_id, "pm-1")
        self.assertGreater(snapshot.fair_yes_prob, 0.0)
        self.assertEqual(snapshot.source_count, 2)

    def test_book_consensus_artifact_tracks_sources(self):
        rows = [
            {"source": "book-a", "price_decimal": 1.80, "source_age_ms": 1000},
            {"source": "book-b", "price_decimal": 1.95, "source_age_ms": 3000},
        ]

        probability = consensus_probability_from_rows(rows, half_life_seconds=1800.0)
        artifact = fit_book_consensus_artifact(rows, half_life_seconds=1800.0)

        self.assertGreater(probability, 0.0)
        self.assertEqual(artifact.bookmaker_count, 2)
        self.assertEqual(artifact.row_count, 2)
        self.assertEqual(artifact.half_life_seconds, 1800.0)

    def test_consensus_artifact_half_life_changes_fair_value_snapshot(self):
        odds_rows = [
            {"source": "book-a", "implied_prob": 0.2, "source_age_ms": 0},
            {"source": "book-b", "implied_prob": 0.8, "source_age_ms": 3_600_000},
        ]

        slow_decay = FairValueEngine(half_life_seconds=3600.0).build(
            ContractMatch(
                polymarket_market_id="pm-1",
                sportsbook_event_id="sb-1",
                sportsbook_market_type="moneyline",
                normalized_market_type="moneyline_full_game",
                match_confidence=0.97,
                resolution_risk=0.03,
            ),
            odds_rows,
        )
        fast_decay = FairValueEngine(half_life_seconds=60.0).build(
            ContractMatch(
                polymarket_market_id="pm-1",
                sportsbook_event_id="sb-1",
                sportsbook_market_type="moneyline",
                normalized_market_type="moneyline_full_game",
                match_confidence=0.97,
                resolution_risk=0.03,
            ),
            odds_rows,
        )

        self.assertLess(fast_decay.fair_yes_prob, slow_decay.fair_yes_prob)
        self.assertLess(fast_decay.fair_yes_prob, 0.25)
        self.assertGreater(slow_decay.fair_yes_prob, 0.35)
        self.assertLess(fast_decay.book_dispersion, slow_decay.book_dispersion)
        self.assertLess(
            fast_decay.upper_prob - fast_decay.lower_prob,
            slow_decay.upper_prob - slow_decay.lower_prob,
        )

    def test_write_consensus_artifact_serializes_expected_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = write_consensus_artifact(
                output_path=f"{temp_dir}/consensus.json",
                half_life_seconds=900.0,
            )
            payload = json.loads(path.read_text())

        self.assertEqual(payload["model"], "consensus")
        self.assertEqual(payload["half_life_seconds"], 900.0)
        self.assertEqual(payload["model_version"], "v1")

    def test_load_book_consensus_artifact_rejects_invalid_payloads(self):
        with self.assertRaises(ValueError):
            load_book_consensus_artifact({"half_life_seconds": "NaN"})
        with self.assertRaises(ValueError):
            load_book_consensus_artifact({"bookmaker_count": 1.5})
        with self.assertRaises(ValueError):
            load_book_consensus_artifact({"model": ["consensus"]})


if __name__ == "__main__":
    unittest.main()

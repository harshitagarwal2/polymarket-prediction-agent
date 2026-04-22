from __future__ import annotations

import unittest

from forecasting import ForecastCalibrator
from forecasting.consensus import ConsensusComponent, consensus_probability, dispersion_score
from forecasting.fair_value_engine import ConsensusFairValueEngine, ConsensusFairValueInput


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
        self.assertGreater(dispersion_score([ConsensusComponent(probability=0.3), ConsensusComponent(probability=0.7)]), 0.0)


if __name__ == "__main__":
    unittest.main()

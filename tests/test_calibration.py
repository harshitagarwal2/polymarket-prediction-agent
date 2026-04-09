from __future__ import annotations

import unittest

from research.calibration import (
    CalibrationSample,
    fit_histogram_calibrator,
    fit_histogram_calibrator_from_rows,
    load_calibration_artifact,
)


class CalibrationTests(unittest.TestCase):
    def test_fit_histogram_calibrator_fills_empty_bins_from_nearest_sampled_bin(self):
        calibrator = fit_histogram_calibrator(
            [
                CalibrationSample(prediction=0.1, outcome=0),
                CalibrationSample(prediction=0.9, outcome=1),
            ],
            bin_count=3,
        )

        self.assertEqual(calibrator.sample_count, 2)
        self.assertAlmostEqual(calibrator.positive_rate, 0.5)
        self.assertEqual(len(calibrator.bins), 3)
        self.assertAlmostEqual(calibrator.apply(0.5), 0.0)
        self.assertAlmostEqual(calibrator.apply(0.95), 1.0)
        self.assertIsNone(calibrator.bins[1].mean_prediction)
        self.assertEqual(calibrator.bins[1].calibrated_probability, 0.0)

    def test_fit_histogram_calibrator_rejects_empty_samples(self):
        with self.assertRaisesRegex(
            ValueError,
            "calibration samples must not be empty",
        ):
            fit_histogram_calibrator([], bin_count=2)

    def test_histogram_calibrator_round_trips_serialized_artifact(self):
        calibrator = fit_histogram_calibrator(
            [
                CalibrationSample(prediction=0.2, outcome=0),
                CalibrationSample(prediction=0.3, outcome=0),
                CalibrationSample(prediction=0.7, outcome=1),
                CalibrationSample(prediction=0.8, outcome=1),
            ],
            bin_count=2,
        )

        restored = load_calibration_artifact(calibrator.to_payload())

        self.assertEqual(restored.bin_count, 2)
        self.assertEqual(restored.sample_count, 4)
        self.assertAlmostEqual(restored.apply(0.25), calibrator.apply(0.25))
        self.assertAlmostEqual(restored.apply(0.75), calibrator.apply(0.75))

    def test_fit_histogram_calibrator_from_evaluation_rows(self):
        rows = [
            {"market_key": "a", "fair_value": 0.2, "outcome_label": 0},
            {"market_key": "b", "fair_value": 0.3, "outcome_label": 0},
            {"market_key": "c", "fair_value": 0.7, "outcome_label": 1},
            {"market_key": "d", "fair_value": 0.8, "outcome_label": 1},
        ]

        calibrator = fit_histogram_calibrator_from_rows(rows, bin_count=2)

        self.assertEqual(calibrator.sample_count, 4)
        self.assertAlmostEqual(calibrator.apply(0.25), 0.0)
        self.assertAlmostEqual(calibrator.apply(0.75), 1.0)

    def test_load_calibration_artifact_fits_from_suite_edge_ledger_payload(self):
        edge_ledger_payload = {
            "row_count": 4,
            "rows": [
                {"market_key": "a", "fair_value": 0.2, "outcome_label": 0},
                {"market_key": "b", "fair_value": 0.3, "outcome_label": 0},
                {"market_key": "c", "fair_value": 0.7, "outcome_label": 1},
                {"market_key": "d", "fair_value": 0.8, "outcome_label": 1},
            ],
        }

        calibrator = load_calibration_artifact(edge_ledger_payload, bin_count=2)

        self.assertEqual(calibrator.sample_count, 4)
        self.assertAlmostEqual(calibrator.apply(0.25), 0.0)
        self.assertAlmostEqual(calibrator.apply(0.75), 1.0)


if __name__ == "__main__":
    unittest.main()

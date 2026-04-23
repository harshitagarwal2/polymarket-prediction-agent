from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from research.schemas import load_benchmark_case, load_packaged_benchmark_case


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "research"
    / "fixtures"
    / "sports_benchmark_tiny.json"
)


class BenchmarkSchemaTests(unittest.TestCase):
    def test_load_benchmark_case_from_path(self):
        case = load_benchmark_case(FIXTURE_PATH)

        self.assertEqual(case.name, "sports-benchmark-tiny")
        self.assertIsNotNone(case.fair_value_case)
        self.assertIsNotNone(case.replay_case)
        if case.fair_value_case is None or case.replay_case is None:
            self.fail("expected both fair-value and replay benchmark cases")
        self.assertEqual(
            case.fair_value_case.expected_market_keys,
            ("token-home:yes", "token-home:no"),
        )
        self.assertEqual(case.fair_value_case.calibration_samples, ())
        self.assertEqual(case.fair_value_case.calibration_bin_count, 5)
        self.assertEqual(len(case.replay_case.materialize_steps()), 2)

    def test_load_packaged_benchmark_case(self):
        case = load_packaged_benchmark_case("sports_benchmark_tiny.json")

        self.assertEqual(case.name, "sports-benchmark-tiny")

    def test_load_packaged_benchmark_case_rejects_unknown_name(self):
        with self.assertRaisesRegex(
            ValueError, r"unknown packaged benchmark fixture: ../../not-a-fixture.json"
        ):
            load_packaged_benchmark_case("../../not-a-fixture.json")

    def test_invalid_row_entry_fails_closed(self):
        payload = {
            "name": "invalid-case",
            "fair_value_case": {
                "rows": ["not-an-object"],
                "markets": [],
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError, r"fair_value_case.rows\[0\] must be an object"
            ):
                load_benchmark_case(handle.name)

    def test_invalid_replay_step_entry_fails_closed(self):
        payload = {
            "name": "invalid-case",
            "replay_case": {
                "steps": [123],
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError, r"replay_case.steps\[0\] must be an object"
            ):
                load_benchmark_case(handle.name)

    def test_non_finite_replay_numeric_value_fails_closed(self):
        payload = {
            "name": "invalid-case",
            "replay_case": {
                "broker": {"cash": "NaN"},
                "steps": [],
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError, r"replay_case.broker.cash must be finite"
            ):
                load_benchmark_case(handle.name)

    def test_calibration_samples_are_loaded_when_present(self):
        payload = {
            "name": "calibrated-case",
            "fair_value_case": {
                "rows": [],
                "calibration_samples": [
                    {"prediction": 0.2, "outcome": 0},
                    {"prediction": 0.8, "outcome": 1},
                ],
                "calibration_bin_count": 2,
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            case = load_benchmark_case(handle.name)

        if case.fair_value_case is None:
            self.fail("expected fair-value case")
        self.assertEqual(len(case.fair_value_case.calibration_samples), 2)
        self.assertEqual(case.fair_value_case.calibration_bin_count, 2)

    def test_replay_broker_realism_knobs_are_loaded_when_present(self):
        payload = {
            "name": "replay-realism-case",
            "replay_case": {
                "broker": {
                    "cash": 1000.0,
                    "stale_after_steps": 2,
                    "price_move_bps_per_step": 12.5,
                },
                "steps": [],
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            case = load_benchmark_case(handle.name)

        if case.replay_case is None:
            self.fail("expected replay case")
        self.assertEqual(case.replay_case.broker.stale_after_steps, 2)
        self.assertEqual(case.replay_case.broker.price_move_bps_per_step, 12.5)

    def test_model_fair_values_and_blend_weight_are_loaded_when_present(self):
        payload = {
            "name": "modeled-case",
            "fair_value_case": {
                "rows": [],
                "model_fair_values": {
                    "token-home:yes": 0.63,
                    "token-home:no": 0.37,
                },
                "model_blend_weight": 0.4,
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            case = load_benchmark_case(handle.name)

        if case.fair_value_case is None:
            self.fail("expected fair-value case")
        self.assertEqual(
            case.fair_value_case.model_fair_values,
            {"token-home:yes": 0.63, "token-home:no": 0.37},
        )
        self.assertEqual(case.fair_value_case.model_blend_weight, 0.4)

    def test_invalid_model_fair_value_fails_closed(self):
        payload = {
            "name": "invalid-case",
            "fair_value_case": {
                "rows": [],
                "model_fair_values": {"token-home:yes": 1.1},
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                r"fair_value_case.model_fair_values\[token-home:yes\] must be between 0 and 1",
            ):
                load_benchmark_case(handle.name)

    def test_invalid_model_blend_weight_fails_closed(self):
        payload = {
            "name": "invalid-case",
            "fair_value_case": {
                "rows": [],
                "model_blend_weight": -0.01,
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                r"fair_value_case.model_blend_weight must be between 0 and 1",
            ):
                load_benchmark_case(handle.name)

    def test_invalid_calibration_bin_count_fails_closed(self):
        payload = {
            "name": "invalid-case",
            "fair_value_case": {
                "rows": [],
                "calibration_samples": [{"prediction": 0.2, "outcome": 0}],
                "calibration_bin_count": 0,
            },
        }

        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(payload, handle)
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                r"fair_value_case.calibration_bin_count must be positive",
            ):
                load_benchmark_case(handle.name)


if __name__ == "__main__":
    unittest.main()

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


if __name__ == "__main__":
    unittest.main()

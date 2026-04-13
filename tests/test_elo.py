from __future__ import annotations

import json
import unittest
from pathlib import Path

from research.models.elo import fit_elo_model, generate_model_fair_values
from research.schemas import SportsBenchmarkCase


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"


class EloModelTests(unittest.TestCase):
    def test_elo_model_generates_home_favored_probability_after_home_win(self):
        training_case = SportsBenchmarkCase.from_payload(
            json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        )
        eval_payload = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_payload["name"] = "sports-benchmark-elo-eval"
        eval_case = SportsBenchmarkCase.from_payload(eval_payload)

        artifact = fit_elo_model([training_case])
        self.assertEqual(artifact.training_match_count, 1)

        if eval_case.fair_value_case is None:
            self.fail("expected fair-value case")
        probabilities = generate_model_fair_values(eval_case.fair_value_case, artifact)

        self.assertEqual(set(probabilities), {"token-home:yes", "token-home:no"})
        self.assertGreater(probabilities["token-home:yes"], 0.5)
        self.assertLess(probabilities["token-home:no"], 0.5)
        self.assertAlmostEqual(
            probabilities["token-home:yes"] + probabilities["token-home:no"],
            1.0,
        )


if __name__ == "__main__":
    unittest.main()

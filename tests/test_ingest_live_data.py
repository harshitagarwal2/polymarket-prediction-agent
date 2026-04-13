from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import ingest_live_data


class IngestLiveDataTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

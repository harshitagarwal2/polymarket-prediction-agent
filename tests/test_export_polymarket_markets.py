from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adapters.types import Contract, MarketSummary, OutcomeSide, Venue
from scripts import export_polymarket_markets


class _FakeAdapter:
    def list_markets(self, limit: int = 100):
        return [
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET,
                    symbol="token-1",
                    outcome=OutcomeSide.YES,
                ),
                raw={"token": {"condition_id": "condition-1"}},
            )
        ]


class ExportPolymarketMarketsTests(unittest.TestCase):
    def test_export_quiet_suppresses_stdout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "markets.json"
            stdout = io.StringIO()
            with (
                patch.object(
                    export_polymarket_markets,
                    "build_adapter",
                    return_value=_FakeAdapter(),
                ),
                patch(
                    "sys.argv",
                    [
                        "export_polymarket_markets.py",
                        "--output",
                        str(output_path),
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                export_polymarket_markets.main()

            payload = json.loads(output_path.read_text())

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["contract"]["symbol"], "token-1")


if __name__ == "__main__":
    unittest.main()

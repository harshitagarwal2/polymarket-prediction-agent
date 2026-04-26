from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.run_polymarket_depth_trades_smoke import main


class RunPolymarketDepthTradesSmokeTests(unittest.TestCase):
    def test_main_invokes_runner(self):
        with patch("scripts.run_polymarket_depth_trades_smoke._run") as run:
            self.assertEqual(main([]), 0)
        run.assert_called_once()


if __name__ == "__main__":
    unittest.main()

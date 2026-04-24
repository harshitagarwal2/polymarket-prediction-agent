from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_service_stack_smoke import _build_readiness_summary


class RunServiceStackSmokeTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_build_readiness_summary_requires_ready_sources_and_nonempty_tables(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "source_health.json",
                {
                    "projection_sportsbook_odds": {"status": "ok"},
                    "projection_polymarket_market_catalog": {"status": "ok"},
                    "projection_polymarket_market_channel": {"status": "ok"},
                    "market_mappings": {"status": "ok"},
                    "fair_values": {"status": "red"},
                },
            )
            self._write_json(
                root / "current" / "market_mappings.json", {"pm-1|sb-1": {}}
            )
            self._write_json(root / "current" / "fair_values.json", {"pm-1": {}})
            self._write_json(
                root / "current" / "opportunities.json", {"pm-1|buy_yes": {}}
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "source health not ready after smoke: fair_values=red",
            ):
                _build_readiness_summary(root)

    def test_build_readiness_summary_reports_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._write_json(
                root / "current" / "source_health.json",
                {
                    "projection_sportsbook_odds": {"status": "ok"},
                    "projection_polymarket_market_catalog": {"status": "ok"},
                    "projection_polymarket_market_channel": {"status": "ok"},
                    "market_mappings": {"status": "ok"},
                    "fair_values": {"status": "ok"},
                },
            )
            self._write_json(
                root / "current" / "market_mappings.json", {"pm-1|sb-1": {}}
            )
            self._write_json(root / "current" / "fair_values.json", {"pm-1": {}})
            self._write_json(
                root / "current" / "opportunities.json",
                {"pm-1|buy_yes": {}, "pm-1|sell_yes": {}},
            )

            summary = _build_readiness_summary(root)

        self.assertEqual(summary["source_health"]["fair_values"], "ok")
        self.assertEqual(summary["counts"]["market_mappings"], 1)
        self.assertEqual(summary["counts"]["fair_values"], 1)
        self.assertEqual(summary["counts"]["opportunities"], 2)


if __name__ == "__main__":
    unittest.main()

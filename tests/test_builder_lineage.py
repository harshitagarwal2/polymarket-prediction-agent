from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts import ingest_live_data


class BuilderLineageTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _seed_mapping_build_inputs(self, root: Path) -> None:
        self._write_json(
            root / "current" / "polymarket_markets.json",
            {
                "pm-1": {
                    "market_id": "pm-1",
                    "condition_id": "condition-1",
                    "event_key": "event-1",
                    "sport": "basketball_nba",
                    "series": "playoffs",
                    "game_id": "game-1",
                    "status": "open",
                    "raw_json": {
                        "id": "pm-1",
                        "conditionId": "condition-1",
                    },
                }
            },
        )

    def test_mapping_and_fair_value_manifests_include_lineage_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            self._seed_mapping_build_inputs(root)
            self._write_json(
                root / "postgres" / "sportsbook_events.json",
                {
                    "sb-1": {
                        "sportsbook_event_id": "sb-1",
                        "source": "theoddsapi",
                        "sport": "basketball_nba",
                        "league": "playoffs",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "start_time": "2026-04-21T19:00:00Z",
                        "raw_json": {
                            "id": "sb-1",
                            "sport": "nba",
                            "commence_time": "2026-04-21T19:00:00Z",
                        },
                    }
                },
            )
            self._write_json(
                root / "current" / "sportsbook_odds.json",
                {
                    "sb-1|book-a|h2h|Home Team": {
                        "sportsbook_event_id": "sb-1",
                        "source": "book-a",
                        "market_type": "h2h",
                        "selection": "Home Team",
                        "price_decimal": 1.5,
                        "implied_prob": 0.6666666667,
                        "overround": 0.6666666667,
                        "quote_ts": "2026-04-21T18:00:00+00:00",
                        "source_age_ms": 0,
                        "raw_json": {},
                    }
                },
            )

            ingest_live_data.main(
                ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
            )
            ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])

            mapping_manifest = json.loads(
                (root / "current" / "market_mapping_manifest.json").read_text()
            )
            fair_value_manifest = json.loads(
                (root / "current" / "fair_value_manifest.json").read_text()
            )

        self.assertIn("generated_at", mapping_manifest)
        self.assertIn("metadata", mapping_manifest)
        self.assertIsInstance(mapping_manifest["metadata"].get("provenance"), dict)
        self.assertIn("generated_at", fair_value_manifest)
        self.assertIn("metadata", fair_value_manifest)
        self.assertIn("source", fair_value_manifest)

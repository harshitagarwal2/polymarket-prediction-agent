from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from engine.config_loader import load_config_file, nested_config_value
from engine.runtime_policy import load_runtime_policy
from research.data.build_training_set import build_training_set_rows
from research.data.build_training_set import build_training_set_rows_from_sports_inputs
from research.data.build_training_set import load_training_set_rows
from research.data.capture_polymarket import (
    build_polymarket_capture,
    load_polymarket_capture,
    write_polymarket_capture,
)
from research.data.capture_sports_inputs import (
    build_sports_input_capture,
    load_sports_input_capture,
    write_sports_input_capture,
)
from research.eval.dm_test import compare_loss_differentials
from research.eval.metrics import score_forecasts
from research.features.joiners import merge_feature_sets
from research.features.market_features import build_market_microstructure_features
from research.features.sports_features import build_team_strength_features
from research.fair_values import load_market_snapshot
from research.datasets import DatasetRegistry
from research.models.bradley_terry import fit_bradley_terry_from_rows
from research.models.blend import blend_probability
from research.schemas import SportsBenchmarkCase
from scripts import train_models


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"
REPO_ROOT = Path(__file__).resolve().parents[1]


class ResearchArchitectureScaffoldingTests(unittest.TestCase):
    def test_runtime_policy_preview_config_loads(self):
        policy = load_runtime_policy(
            REPO_ROOT / "configs" / "runtime_policy.preview.json"
        )
        self.assertEqual(policy.schema_version, 1)

    def test_yaml_config_loader_reads_nested_values(self):
        config = load_config_file(REPO_ROOT / "configs" / "sports_nba.yaml")
        self.assertEqual(nested_config_value(config, "league"), "nba")
        self.assertEqual(
            nested_config_value(config, "research", "model_generator"),
            "elo",
        )

    def test_capture_envelopes_write_json(self):
        captured_at = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temp_dir:
            poly_path = write_polymarket_capture(
                build_polymarket_capture(
                    [
                        {
                            "market_key": "token-a:yes",
                            "condition_id": "abc",
                            "event_key": "event-1",
                            "best_bid_size": 12,
                            "best_ask_size": 8,
                            "gameStartTime": "2026-04-07T15:00:00Z",
                        }
                    ],
                    layer="gamma",
                    captured_at=captured_at,
                ),
                Path(temp_dir) / "gamma.json",
            )
            sports_path = write_sports_input_capture(
                build_sports_input_capture(
                    [
                        {
                            "sport": "nba",
                            "home_team": "Home Team",
                            "away_team": "Away Team",
                            "selection_name": "Home Team",
                            "decimal_odds": 1.80,
                            "start_time": "2026-04-07T15:00:00Z",
                            "label": 1,
                        }
                    ],
                    source="sports-inputs",
                    captured_at=captured_at,
                ),
                Path(temp_dir) / "sports.json",
            )
            poly_payload = json.loads(poly_path.read_text())
            sports_payload = json.loads(sports_path.read_text())
            self.assertEqual(poly_payload["layer"], "gamma")
            self.assertIn("markets", poly_payload)
            self.assertEqual(poly_payload["markets"][0]["condition_id"], "abc")
            self.assertEqual(sports_payload["source"], "sports-inputs")
            self.assertIn("rows", sports_payload)
            self.assertEqual(sports_payload["rows"][0]["label"], 1)

            loaded_poly = load_polymarket_capture(poly_path)
            loaded_sports = load_sports_input_capture(sports_path)
            self.assertEqual(loaded_poly.captured_at, captured_at)
            self.assertEqual(loaded_sports.captured_at, captured_at)
            self.assertEqual(loaded_poly.markets[0].best_bid_size, 12.0)
            self.assertEqual(loaded_poly.markets[0].best_ask_size, 8.0)
            self.assertEqual(
                loaded_poly.markets[0].start_time,
                datetime(2026, 4, 7, 15, 0, tzinfo=timezone.utc),
            )
            implied_probability = loaded_sports.rows[0].implied_probability
            if implied_probability is None:
                self.fail("expected implied_probability to be populated")
            self.assertAlmostEqual(float(implied_probability), 1 / 1.80)
            self.assertEqual(
                loaded_sports.rows[0].start_time,
                datetime(2026, 4, 7, 15, 0, tzinfo=timezone.utc),
            )

            markets = load_market_snapshot(poly_path)
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0].contract.symbol, "token-a")

    def test_feature_helpers_and_research_exports_work(self):
        market_features = build_market_microstructure_features(
            best_bid=0.42,
            best_ask=0.48,
            volume=100,
            best_bid_size=20,
            best_ask_size=10,
            captured_at=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            start_time=datetime(2026, 4, 7, 14, 0, tzinfo=timezone.utc),
        )
        sports_features = build_team_strength_features(
            home_team="A",
            away_team="B",
            home_rating=1520,
            away_rating=1480,
            selection_name="A",
            decimal_odds=1.8,
            captured_at=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            start_time=datetime(2026, 4, 7, 14, 0, tzinfo=timezone.utc),
        )
        merged = merge_feature_sets(market_features, sports_features)
        self.assertAlmostEqual(market_features["midpoint"], 0.45)
        self.assertEqual(market_features["quoted_liquidity"], 30.0)
        self.assertAlmostEqual(market_features["book_imbalance"], 1 / 3)
        self.assertEqual(market_features["time_to_start_minutes"], 120.0)
        self.assertEqual(merged["home_team"], "A")
        self.assertEqual(sports_features["selection_is_home"], 1.0)
        selection_implied_probability = sports_features["selection_implied_probability"]
        if not isinstance(selection_implied_probability, float):
            self.fail("expected selection_implied_probability to be a float")
        self.assertAlmostEqual(float(selection_implied_probability), 1 / 1.8)
        self.assertEqual(sports_features["time_to_start_minutes"], 120.0)
        self.assertGreater(blend_probability(0.6, 0.7, model_weight=0.5), 0.6)

    def test_training_rows_from_inputs_can_join_polymarket_market_features(self):
        captured_at = datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc)
        start_time = datetime(2026, 4, 7, 15, 0, tzinfo=timezone.utc)
        sports_capture = build_sports_input_capture(
            [
                {
                    "event_key": "event-1",
                    "sport": "nba",
                    "sports_market_type": "moneyline",
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "selection_name": "Home Team",
                    "decimal_odds": 1.80,
                    "start_time": start_time.isoformat(),
                    "label": 1,
                }
            ],
            source="sports-inputs",
            captured_at=captured_at,
        )
        market_capture = build_polymarket_capture(
            [
                {
                    "market_key": "token-home:yes",
                    "event_key": "event-1",
                    "sports_market_type": "moneyline",
                    "best_bid": 0.42,
                    "best_ask": 0.48,
                    "best_bid_size": 20,
                    "best_ask_size": 10,
                    "volume": 100,
                    "start_time": start_time.isoformat(),
                }
            ],
            layer="gamma",
            captured_at=captured_at,
        )

        rows = build_training_set_rows_from_sports_inputs(
            sports_capture.rows,
            polymarket_markets=market_capture.markets,
        )

        self.assertEqual(len(rows), 1)
        metadata = rows[0].metadata
        selection_implied_probability = metadata["selection_implied_probability"]
        if not isinstance(selection_implied_probability, float):
            self.fail("expected selection_implied_probability metadata to be a float")
        self.assertAlmostEqual(float(selection_implied_probability), 1 / 1.8)
        self.assertEqual(metadata["market_quoted_liquidity"], 30.0)
        self.assertEqual(metadata["market_market_count"], 1.0)
        self.assertEqual(metadata["market_time_to_start_minutes"], 180.0)

    def test_training_and_eval_scaffolding_works(self):
        case = SportsBenchmarkCase.from_payload(
            json.loads((FIXTURES_DIR / "sports_benchmark_tiny.json").read_text())
        )
        rows = build_training_set_rows([case])
        artifact = fit_bradley_terry_from_rows([row.to_payload() for row in rows])
        score = score_forecasts({"a": 0.7, "b": 0.3}, {"a": 1, "b": 0})
        dm = compare_loss_differentials([0.1, 0.2])
        self.assertEqual(len(rows), 1)
        self.assertTrue(artifact.skill_by_team)
        self.assertGreater(score.accuracy, 0.0)
        self.assertEqual(dm.sample_count, 2)

    def test_train_models_can_use_training_data_capture(self):
        training_payload = {
            "source": "sports-inputs",
            "rows": [
                {
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "label": 1,
                    "event_key": "event-1",
                    "sport": "nba",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            training_path = Path(temp_dir) / "training.json"
            market_path = Path(temp_dir) / "market.json"
            output_path = Path(temp_dir) / "elo.json"
            registry_root = Path(temp_dir) / "registry"
            training_path.write_text(json.dumps(training_payload))
            market_path.write_text(
                json.dumps(
                    {
                        "layer": "gamma",
                        "markets": [
                            {
                                "market_key": "token-home:yes",
                                "event_key": "event-1",
                                "sports_market_type": "moneyline",
                                "best_bid": 0.42,
                                "best_ask": 0.48,
                                "best_bid_size": 20,
                                "best_ask_size": 10,
                                "volume": 100,
                            }
                        ],
                    }
                )
            )
            rows = load_training_set_rows(
                str(training_path),
                polymarket_capture_path=str(market_path),
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].metadata["market_quoted_liquidity"], 30.0)

            with patch(
                "sys.argv",
                [
                    "train_models.py",
                    "--model",
                    "elo",
                    "--training-data",
                    str(training_path),
                    "--output",
                    str(output_path),
                    "--registry-root",
                    str(registry_root),
                ],
            ):
                train_models.main()

            artifact_payload = json.loads(output_path.read_text())
            registry_payload = json.loads(
                (registry_root / "model_registry.json").read_text()
            )

        self.assertEqual(artifact_payload["model_generator"], "elo")
        self.assertEqual(artifact_payload["training_match_count"], 1)
        self.assertIn("elo|v1", registry_payload)

    def test_train_models_can_use_training_dataset_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "runtime-data" / "datasets"
            output_path = Path(temp_dir) / "elo.json"
            registry_root = Path(temp_dir) / "registry"
            dataset_registry = DatasetRegistry(dataset_root)
            dataset_registry.write_rows_snapshot(
                "historical-training-dataset",
                [
                    {
                        "record_id": "sports-inputs|event-1|2026-04-21T18:00:00Z|abc123",
                        "recorded_at": "2026-04-21T18:00:00Z",
                        "home_team": "Home Team",
                        "away_team": "Away Team",
                        "label": 1,
                        "event_key": "event-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "game_id": "game-1",
                        "sports_market_type": "moneyline",
                        "source": "sports-inputs",
                        "market_key": "token-home:yes",
                        "condition_id": "condition-1",
                        "metadata": {},
                    }
                ],
                version="v1",
                record_id_field="record_id",
                timestamp_field="recorded_at",
            )

            with patch(
                "sys.argv",
                [
                    "train_models.py",
                    "--model",
                    "elo",
                    "--training-dataset",
                    "historical-training-dataset",
                    "--training-dataset-version",
                    "v1",
                    "--dataset-root",
                    str(dataset_root),
                    "--output",
                    str(output_path),
                    "--registry-root",
                    str(registry_root),
                ],
            ):
                train_models.main()

            artifact_payload = json.loads(output_path.read_text())
            registry_payload = json.loads(
                (registry_root / "model_registry.json").read_text()
            )

        self.assertEqual(artifact_payload["model_generator"], "elo")
        self.assertEqual(artifact_payload["training_match_count"], 1)
        self.assertEqual(
            registry_payload["elo|v1"]["feature_spec"]["input"], "training-dataset"
        )

    def test_train_models_can_read_model_from_config_file(self):
        training_payload = {
            "source": "sports-inputs",
            "rows": [
                {
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "label": 1,
                    "event_key": "event-1",
                    "sport": "nba",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            training_path = Path(temp_dir) / "training.json"
            output_path = Path(temp_dir) / "elo.json"
            training_path.write_text(json.dumps(training_payload))

            with patch(
                "sys.argv",
                [
                    "train_models.py",
                    "--config-file",
                    "configs/sports_nba.yaml",
                    "--training-data",
                    str(training_path),
                    "--output",
                    str(output_path),
                ],
            ):
                train_models.main()

            artifact_payload = json.loads(output_path.read_text())

        self.assertEqual(artifact_payload["model_generator"], "elo")

    def test_train_models_can_train_bt_from_training_data_capture(self):
        training_payload = {
            "source": "sports-inputs",
            "rows": [
                {
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "label": 1,
                    "event_key": "event-1",
                    "sport": "nba",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            training_path = Path(temp_dir) / "training.json"
            output_path = Path(temp_dir) / "bt.json"
            training_path.write_text(json.dumps(training_payload))

            with patch(
                "sys.argv",
                [
                    "train_models.py",
                    "--model",
                    "bt",
                    "--training-data",
                    str(training_path),
                    "--output",
                    str(output_path),
                ],
            ):
                train_models.main()

            artifact_payload = json.loads(output_path.read_text())

        self.assertIn("skill_by_team", artifact_payload)

    def test_train_models_quiet_suppresses_stdout(self):
        training_payload = {
            "source": "sports-inputs",
            "rows": [
                {
                    "home_team": "Home Team",
                    "away_team": "Away Team",
                    "label": 1,
                    "event_key": "event-1",
                    "sport": "nba",
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            training_path = Path(temp_dir) / "training.json"
            output_path = Path(temp_dir) / "elo.json"
            training_path.write_text(json.dumps(training_payload))
            stdout = io.StringIO()

            with (
                patch(
                    "sys.argv",
                    [
                        "train_models.py",
                        "--model",
                        "elo",
                        "--training-data",
                        str(training_path),
                        "--output",
                        str(output_path),
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                train_models.main()

            artifact_payload = json.loads(output_path.read_text())

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(artifact_payload["model_generator"], "elo")


if __name__ == "__main__":
    unittest.main()

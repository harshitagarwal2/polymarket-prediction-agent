from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research.datasets import DatasetRegistry
from scripts import run_sports_benchmark_suite


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"


class BenchmarkSuiteCliTests(unittest.TestCase):
    def test_suite_cli_quiet_suppresses_stdout(self):
        with tempfile.TemporaryDirectory() as output_dir:
            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "run_sports_benchmark_suite.py",
                        "--fixtures-dir",
                        str(FIXTURES_DIR),
                        "--output-dir",
                        output_dir,
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                run_sports_benchmark_suite.main()

            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_summary.json").exists()
            )

        self.assertEqual(stdout.getvalue(), "")

    def test_suite_cli_writes_json_and_markdown_summaries(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark_suite.py",
                    "--fixtures-dir",
                    str(FIXTURES_DIR),
                    "--output-dir",
                    output_dir,
                ],
            ):
                run_sports_benchmark_suite.main()

            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_summary.json").exists()
            )
            self.assertTrue((Path(output_dir) / "benchmark_suite_summary.md").exists())
            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_attribution_ledger.json").exists()
            )

    def test_suite_cli_runs_benchmark_case_dataset_snapshot(self):
        case_payload = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        case_payload["recorded_at"] = "2026-04-01T12:00:00Z"

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            output_dir = Path(temp_dir) / "output"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "benchmark-case-dataset",
                [case_payload],
                version="v1",
                timestamp_field="recorded_at",
            )

            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark_suite.py",
                    "--dataset-root",
                    str(dataset_root),
                    "--dataset-name",
                    "benchmark-case-dataset",
                    "--dataset-version",
                    "v1",
                    "--output-dir",
                    str(output_dir),
                ],
            ):
                run_sports_benchmark_suite.main()

            self.assertTrue((output_dir / "benchmark_suite_summary.json").exists())
            self.assertTrue((output_dir / "benchmark_suite_summary.md").exists())

    def test_suite_cli_runs_walk_forward_dataset_snapshot(self):
        training_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        training_case["recorded_at"] = "2026-04-01T12:00:00Z"
        eval_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case["name"] = "sports-benchmark-cli-eval"
        eval_case["recorded_at"] = "2026-04-02T12:00:00Z"
        eval_case["fair_value_case"]["calibration_samples"] = [
            {"prediction": 0.42, "outcome": 1},
            {"prediction": 0.45, "outcome": 1},
            {"prediction": 0.55, "outcome": 0},
            {"prediction": 0.58, "outcome": 0},
        ]
        eval_case["fair_value_case"]["calibration_bin_count"] = 2

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            output_dir = Path(temp_dir) / "output"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "walk-forward-cli-dataset",
                [training_case, eval_case],
                version="v1",
                timestamp_field="recorded_at",
            )

            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark_suite.py",
                    "--dataset-root",
                    str(dataset_root),
                    "--dataset-name",
                    "walk-forward-cli-dataset",
                    "--dataset-version",
                    "v1",
                    "--walk-forward",
                    "--min-train-size",
                    "1",
                    "--test-size",
                    "1",
                    "--calibration-bin-count",
                    "2",
                    "--output-dir",
                    str(output_dir),
                ],
            ):
                run_sports_benchmark_suite.main()

            summary_path = output_dir / "walk_forward_benchmark_summary.json"
            self.assertTrue(summary_path.exists())
            payload = json.loads(summary_path.read_text())
            self.assertIn("aggregate", payload)
            self.assertEqual(payload["aggregate"]["successful_cases"], 1)
            root_attribution_ledger = (
                output_dir / payload["report_artifacts"]["attribution_ledger_json"]
            )
            split_summary = (
                output_dir / payload["splits"][0]["report_artifacts"]["summary_json"]
            )
            split_attribution_ledger = (
                output_dir
                / payload["splits"][0]["report_artifacts"]["attribution_ledger_json"]
            )
            self.assertTrue(root_attribution_ledger.exists())
            self.assertTrue(split_summary.exists())
            self.assertTrue(split_attribution_ledger.exists())
            split_payload = json.loads(split_summary.read_text())
            fair_value_payload = split_payload["case_results"][0]["report"][
                "fair_value"
            ]
            self.assertEqual(fair_value_payload["calibration"]["source"], "prefit")

    def test_suite_cli_runs_walk_forward_with_elo_model_generator(self):
        training_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        training_case["recorded_at"] = "2026-04-01T12:00:00Z"
        eval_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case["name"] = "sports-benchmark-cli-elo-eval"
        eval_case["recorded_at"] = "2026-04-02T12:00:00Z"

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            output_dir = Path(temp_dir) / "output"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "walk-forward-cli-elo-dataset",
                [training_case, eval_case],
                version="v1",
                timestamp_field="recorded_at",
            )

            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark_suite.py",
                    "--dataset-root",
                    str(dataset_root),
                    "--dataset-name",
                    "walk-forward-cli-elo-dataset",
                    "--dataset-version",
                    "v1",
                    "--walk-forward",
                    "--min-train-size",
                    "1",
                    "--test-size",
                    "1",
                    "--model-generator",
                    "elo",
                    "--output-dir",
                    str(output_dir),
                ],
            ):
                run_sports_benchmark_suite.main()

            summary_path = output_dir / "walk_forward_benchmark_summary.json"
            payload = json.loads(summary_path.read_text())
            self.assertEqual(payload["walk_forward"]["model_generator"], "elo")
            self.assertEqual(payload["splits"][0]["model"]["training_match_count"], 1)
            split_summary = (
                output_dir / payload["splits"][0]["report_artifacts"]["summary_json"]
            )
            split_payload = json.loads(split_summary.read_text())
            fair_value_payload = split_payload["case_results"][0]["report"][
                "fair_value"
            ]
            self.assertGreater(
                fair_value_payload["evaluation_rows"][1]["model_fair_value"],
                0.5,
            )

    def test_suite_cli_runs_walk_forward_with_bt_model_generator(self):
        training_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        training_case["recorded_at"] = "2026-04-01T12:00:00Z"
        eval_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        eval_case["name"] = "sports-benchmark-cli-bt-eval"
        eval_case["recorded_at"] = "2026-04-02T12:00:00Z"

        with tempfile.TemporaryDirectory() as temp_dir:
            dataset_root = Path(temp_dir) / "datasets"
            output_dir = Path(temp_dir) / "output"
            registry = DatasetRegistry(dataset_root)
            registry.write_benchmark_case_snapshot(
                "walk-forward-cli-bt-dataset",
                [training_case, eval_case],
                version="v1",
                timestamp_field="recorded_at",
            )

            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark_suite.py",
                    "--dataset-root",
                    str(dataset_root),
                    "--dataset-name",
                    "walk-forward-cli-bt-dataset",
                    "--dataset-version",
                    "v1",
                    "--walk-forward",
                    "--min-train-size",
                    "1",
                    "--test-size",
                    "1",
                    "--model-generator",
                    "bt",
                    "--output-dir",
                    str(output_dir),
                ],
            ):
                run_sports_benchmark_suite.main()

            summary_path = output_dir / "walk_forward_benchmark_summary.json"
            payload = json.loads(summary_path.read_text())
            self.assertEqual(payload["walk_forward"]["model_generator"], "bt")
            self.assertIn("skill_by_team", payload["splits"][0]["model"])
            split_summary = (
                output_dir / payload["splits"][0]["report_artifacts"]["summary_json"]
            )
            split_payload = json.loads(split_summary.read_text())
            fair_value_payload = split_payload["case_results"][0]["report"][
                "fair_value"
            ]
            self.assertGreater(
                fair_value_payload["evaluation_rows"][1]["model_fair_value"],
                0.5,
            )


if __name__ == "__main__":
    unittest.main()

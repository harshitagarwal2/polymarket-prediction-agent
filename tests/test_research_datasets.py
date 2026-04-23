from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from pathlib import Path

from research.benchmark_suite import run_benchmark_suite
from research.data.storage_paths import build_research_storage_paths
from research.features.quality_checks import evaluate_inference_quality


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"


def _datasets_module():
    return importlib.import_module("research.datasets")


class ResearchDatasetTests(unittest.TestCase):
    def test_research_storage_paths_build_expected_tree(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            paths = build_research_storage_paths(Path(temp_dir) / "runtime" / "data")
            paths.create_dirs()

            expected_paths = (
                paths.raw_polymarket_root,
                paths.raw_sportsbook_root,
                paths.processed_training_root,
                paths.processed_inference_root,
                paths.model_artifacts_root,
                paths.calibration_artifacts_root,
            )

            self.assertEqual(paths.root.name, "data")
            for path in expected_paths:
                self.assertTrue(
                    path.exists(), msg=f"expected directory to exist: {path}"
                )

    def test_inference_quality_checks_collect_blocked_reasons(self):
        result = evaluate_inference_quality(
            source_age_ms=12_000,
            max_source_age_ms=4_000,
            bookmaker_count=1,
            min_bookmaker_count=2,
            has_polymarket_book=False,
            match_confidence=0.5,
            min_match_confidence=0.9,
            book_dispersion=0.08,
            max_book_dispersion=0.03,
        )

        self.assertFalse(result.allowed)
        self.assertIn("source data stale", result.blocked_reasons)
        self.assertIn("insufficient book coverage", result.blocked_reasons)
        self.assertIn("missing Polymarket book", result.blocked_reasons)
        self.assertIn("low match confidence", result.blocked_reasons)
        self.assertIn("book dispersion exceeds threshold", result.blocked_reasons)

    def test_rows_snapshot_round_trips_and_registry_tracks_latest_version(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = _datasets_module().DatasetRegistry(Path(temp_dir) / "datasets")
            first_rows = [
                {
                    "record_id": "row-1",
                    "market_key": "token-home:yes",
                    "event_key": "event-1",
                    "captured_at": "2026-04-01T12:00:00Z",
                    "decimal_odds": 1.80,
                },
                {
                    "record_id": "row-2",
                    "market_key": "token-away:yes",
                    "event_key": "event-2",
                    "captured_at": "2026-04-02T12:00:00Z",
                    "decimal_odds": 1.95,
                },
            ]
            second_rows = [
                {
                    "record_id": "row-3",
                    "market_key": "token-draw:yes",
                    "event_key": "event-3",
                    "captured_at": "2026-04-03T12:00:00Z",
                    "decimal_odds": 2.10,
                }
            ]

            first_manifest = registry.write_rows_snapshot(
                "sports-rows",
                first_rows,
                version="v1",
                record_id_field="record_id",
                metadata={"source": "test"},
            )
            registry.write_rows_snapshot(
                "sports-rows",
                second_rows,
                version="v2",
                record_id_field="record_id",
            )

            latest_manifest = registry.load_snapshot("sports-rows")
            v1_rows = registry.read_rows("sports-rows", version="v1")
            registry_payload = json.loads(registry.registry_path.read_text())

        self.assertEqual(first_manifest.record_count, 2)
        self.assertEqual(first_manifest.earliest_recorded_at, "2026-04-01T12:00:00Z")
        self.assertEqual(first_manifest.latest_recorded_at, "2026-04-02T12:00:00Z")
        self.assertEqual(latest_manifest.version, "v2")
        self.assertEqual(v1_rows, first_rows)
        self.assertEqual(
            registry_payload["datasets"]["sports-rows"]["latest_version"],
            "v2",
        )

    def test_rows_snapshot_can_round_trip_empty_dataset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = _datasets_module().DatasetRegistry(Path(temp_dir) / "datasets")
            manifest = registry.write_rows_snapshot(
                "empty-rows",
                [],
                version="v1",
                metadata={"source": "test"},
            )
            rows = registry.read_rows("empty-rows", version="v1")
            registry_payload = json.loads(registry.registry_path.read_text())

        self.assertEqual(manifest.record_count, 0)
        self.assertEqual(manifest.records, ())
        self.assertEqual(rows, [])
        self.assertEqual(
            registry_payload["datasets"]["empty-rows"]["versions"]["v1"][
                "record_count"
            ],
            0,
        )

    def test_rows_snapshot_rejects_dataset_dir_path_escape(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "datasets"
            registry = _datasets_module().DatasetRegistry(root)
            registry.registry_path.write_text(
                json.dumps(
                    {
                        "datasets": {
                            "sports-rows": {
                                "dataset_name": "sports-rows",
                                "dataset_dir": "../../escape",
                                "latest_version": "v1",
                                "versions": {},
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                r"dataset_dir must stay within the dataset registry root",
            ):
                registry.write_rows_snapshot(
                    "sports-rows",
                    [{"record_id": "row-1", "recorded_at": "2026-04-01T12:00:00Z"}],
                    version="v1",
                    record_id_field="record_id",
                )

    def test_generate_walk_forward_splits_over_dated_row_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = _datasets_module().DatasetRegistry(Path(temp_dir) / "datasets")
            rows = [
                {
                    "record_id": f"row-{index}",
                    "market_key": f"token-{index}:yes",
                    "event_key": f"event-{index}",
                    "captured_at": f"2026-04-0{index}T12:00:00Z",
                    "decimal_odds": 1.50 + (index * 0.1),
                }
                for index in range(1, 7)
            ]
            manifest = registry.write_rows_snapshot(
                "walk-forward-rows",
                rows,
                version="v1",
                record_id_field="record_id",
            )

            splits = _datasets_module().generate_walk_forward_splits(
                manifest,
                min_train_size=2,
                test_size=2,
                step_size=2,
            )
            second_test_rows = registry.read_rows_by_record_ids(
                "walk-forward-rows",
                splits[1].test_record_ids,
                version="v1",
            )

        self.assertEqual(len(splits), 2)
        self.assertEqual(splits[0].train_record_ids, ("row-1", "row-2"))
        self.assertEqual(splits[0].test_record_ids, ("row-3", "row-4"))
        self.assertEqual(
            splits[1].train_record_ids, ("row-1", "row-2", "row-3", "row-4")
        )
        self.assertEqual(splits[1].test_record_ids, ("row-5", "row-6"))
        self.assertEqual(splits[1].test_start_at, "2026-04-05T12:00:00Z")
        self.assertEqual(
            [row["record_id"] for row in second_test_rows],
            ["row-5", "row-6"],
        )

    def test_benchmark_case_snapshots_feed_existing_benchmark_suite_and_splits(self):
        older_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_tiny.json").read_text()
        )
        newer_case = json.loads(
            (FIXTURES_DIR / "sports_benchmark_round_trip.json").read_text()
        )
        older_case["recorded_at"] = "2026-04-01T12:00:00Z"
        newer_case["recorded_at"] = "2026-04-02T12:00:00Z"

        with tempfile.TemporaryDirectory() as temp_dir:
            registry = _datasets_module().DatasetRegistry(Path(temp_dir) / "datasets")
            manifest = registry.write_benchmark_case_snapshot(
                "benchmark-cases",
                [older_case, newer_case],
                version="v1",
                timestamp_field="recorded_at",
            )

            full_report = run_benchmark_suite(
                registry.benchmark_case_paths("benchmark-cases")
            )
            splits = _datasets_module().generate_walk_forward_splits(
                manifest,
                min_train_size=1,
                test_size=1,
                step_size=1,
            )
            split_paths = registry.benchmark_case_paths_by_record_ids(
                "benchmark-cases",
                splits[0].test_record_ids,
                version="v1",
            )
            split_report = run_benchmark_suite(split_paths)

        self.assertEqual(full_report.aggregate.total_cases, 2)
        self.assertEqual(full_report.aggregate.successful_cases, 2)
        self.assertEqual(len(splits), 1)
        self.assertEqual(len(split_paths), 1)
        self.assertEqual(split_report.aggregate.total_cases, 1)
        self.assertEqual(split_report.aggregate.successful_cases, 1)
        self.assertEqual(
            split_report.case_results[0].report.case_name, newer_case["name"]
        )

    def test_walk_forward_splits_fail_when_snapshot_records_are_undated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = _datasets_module().DatasetRegistry(Path(temp_dir) / "datasets")
            manifest = registry.write_rows_snapshot(
                "undated-rows",
                [{"record_id": "row-1", "market_key": "token-home:yes"}],
                version="v1",
                record_id_field="record_id",
            )

            with self.assertRaisesRegex(
                ValueError,
                r"walk-forward splits require recorded_at for every record",
            ):
                _datasets_module().generate_walk_forward_splits(
                    manifest,
                    min_train_size=1,
                    test_size=1,
                )


if __name__ == "__main__":
    unittest.main()

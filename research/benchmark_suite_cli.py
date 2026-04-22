from __future__ import annotations

import argparse
from pathlib import Path

from engine.cli_output import add_quiet_flag, emit_lines
from research.benchmark_suite import (
    packaged_benchmark_case_paths,
    run_benchmark_suite,
    run_walk_forward_benchmark_suite,
    write_walk_forward_suite_report,
    write_suite_report,
)
from research.datasets import DatasetRegistry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the packaged sports benchmark suite and write aggregate artifacts."
    )
    parser.add_argument(
        "--fixtures-dir",
        default=None,
        help="Optional directory of benchmark case JSON files. Defaults to packaged fixtures.",
    )
    parser.add_argument(
        "--dataset-root",
        default="research/datasets",
        help="Dataset registry root for snapshot-backed benchmark suites.",
    )
    parser.add_argument(
        "--dataset-name",
        default=None,
        help="Dataset snapshot name to use instead of fixture files.",
    )
    parser.add_argument(
        "--dataset-version",
        default=None,
        help="Optional dataset snapshot version. Defaults to the latest registered version.",
    )
    parser.add_argument(
        "--walk-forward",
        action="store_true",
        help="Run chronological walk-forward evaluation for a benchmark-case dataset snapshot.",
    )
    parser.add_argument(
        "--min-train-size",
        type=int,
        default=None,
        help="Minimum number of dated records in each walk-forward training window.",
    )
    parser.add_argument(
        "--test-size",
        type=int,
        default=None,
        help="Number of records in each walk-forward test window.",
    )
    parser.add_argument(
        "--step-size",
        type=int,
        default=None,
        help="Optional walk-forward step size. Defaults to test-size.",
    )
    parser.add_argument(
        "--max-splits",
        type=int,
        default=None,
        help="Optional maximum number of walk-forward splits to evaluate.",
    )
    parser.add_argument(
        "--calibration-bin-count",
        type=int,
        default=None,
        help="Optional histogram bin count for walk-forward prefit calibration.",
    )
    parser.add_argument(
        "--model-generator",
        choices=("elo", "bt"),
        default=None,
        help="Optional in-repo model generator for walk-forward evaluation.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where suite summary artifacts should be written.",
    )
    add_quiet_flag(parser)
    return parser


def _resolve_case_paths(
    *,
    fixtures_dir: str | None,
    dataset_root: str,
    dataset_name: str | None,
    dataset_version: str | None,
) -> list[Path]:
    if dataset_name is not None:
        registry = DatasetRegistry(dataset_root)
        return list(registry.benchmark_case_paths(dataset_name, dataset_version))
    if fixtures_dir is None:
        return list(packaged_benchmark_case_paths())
    path = Path(fixtures_dir)
    return sorted(case for case in path.glob("*.json") if case.is_file())


def _validate_args(args: argparse.Namespace) -> None:
    if args.dataset_name is not None and args.fixtures_dir is not None:
        raise ValueError("choose either --fixtures-dir or --dataset-name")
    if args.walk_forward and args.dataset_name is None:
        raise ValueError("--walk-forward requires --dataset-name")
    if args.walk_forward and args.fixtures_dir is not None:
        raise ValueError("--walk-forward does not support --fixtures-dir")
    if args.walk_forward and args.min_train_size is None:
        raise ValueError("--walk-forward requires --min-train-size")
    if args.walk_forward and args.test_size is None:
        raise ValueError("--walk-forward requires --test-size")
    if args.model_generator is not None and not args.walk_forward:
        raise ValueError("--model-generator requires --walk-forward")


def main() -> None:
    args = build_parser().parse_args()
    _validate_args(args)
    if args.walk_forward:
        report = run_walk_forward_benchmark_suite(
            dataset_name=args.dataset_name,
            dataset_root=args.dataset_root,
            version=args.dataset_version,
            min_train_size=args.min_train_size,
            test_size=args.test_size,
            step_size=args.step_size,
            max_splits=args.max_splits,
            calibration_bin_count=args.calibration_bin_count,
            model_generator=args.model_generator,
        )
        summary_path = write_walk_forward_suite_report(report, args.output_dir)
        emit_lines(summary_path, quiet=args.quiet)
        return
    case_paths = _resolve_case_paths(
        fixtures_dir=args.fixtures_dir,
        dataset_root=args.dataset_root,
        dataset_name=args.dataset_name,
        dataset_version=args.dataset_version,
    )
    report = run_benchmark_suite(case_paths)
    summary_path, markdown_path = write_suite_report(report, args.output_dir)
    emit_lines(summary_path, markdown_path, quiet=args.quiet)


if __name__ == "__main__":
    main()

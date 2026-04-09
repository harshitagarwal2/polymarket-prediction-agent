from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.benchmark_runner import run_benchmark_case, write_benchmark_report
from research.schemas import (
    load_benchmark_case,
    load_packaged_benchmark_case,
    packaged_benchmark_fixture_names,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the offline sports fair-value and replay benchmark toolkit."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--case", help="Path to a benchmark case JSON file")
    source_group.add_argument(
        "--fixture",
        choices=packaged_benchmark_fixture_names(),
        help="Name of a packaged benchmark fixture, e.g. sports_benchmark_tiny.json",
    )
    parser.add_argument(
        "--output", default=None, help="Optional output path for report JSON"
    )
    parser.add_argument(
        "--write-manifest",
        default=None,
        help="Optional output path for the benchmark manifest payload",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    case = (
        load_benchmark_case(args.case)
        if args.case is not None
        else load_packaged_benchmark_case(args.fixture)
    )
    report = run_benchmark_case(case)
    if args.output is not None:
        write_benchmark_report(report, args.output)
    if args.write_manifest is not None:
        if report.fair_value_report is None:
            raise RuntimeError("benchmark case did not produce a fair-value manifest")
        manifest_path = Path(args.write_manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                report.fair_value_report.manifest,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
        )
    rendered = json.dumps(
        report.to_payload(), indent=2, sort_keys=True, allow_nan=False
    )
    print(rendered)


if __name__ == "__main__":
    main()

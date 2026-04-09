from __future__ import annotations

import argparse
from pathlib import Path
from research.benchmark_suite import (
    packaged_benchmark_case_paths,
    run_benchmark_suite,
    write_suite_report,
)


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
        "--output-dir",
        required=True,
        help="Directory where suite summary artifacts should be written.",
    )
    return parser


def _resolve_case_paths(fixtures_dir: str | None) -> list[Path]:
    if fixtures_dir is None:
        return list(packaged_benchmark_case_paths())
    path = Path(fixtures_dir)
    return sorted(case for case in path.glob("*.json") if case.is_file())


def main() -> None:
    args = build_parser().parse_args()
    case_paths = _resolve_case_paths(args.fixtures_dir)
    report = run_benchmark_suite(case_paths)
    summary_path, markdown_path = write_suite_report(report, args.output_dir)
    print(summary_path)
    print(markdown_path)


if __name__ == "__main__":
    main()

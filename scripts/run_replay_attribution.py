from __future__ import annotations

import argparse
import json
from pathlib import Path

from engine.cli_output import add_quiet_flag, emit_json
from research.benchmark_runner import run_benchmark_case
from research.schemas import (
    load_benchmark_case,
    load_packaged_benchmark_case,
    packaged_benchmark_fixture_names,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run replay attribution for a benchmark case and emit attribution artifacts."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--case", help="Path to a benchmark case JSON file")
    source_group.add_argument(
        "--fixture",
        choices=packaged_benchmark_fixture_names(),
        help="Name of a packaged benchmark fixture, e.g. sports_benchmark_tiny.json",
    )
    parser.add_argument(
        "--output", default=None, help="Optional output path for attribution JSON"
    )
    add_quiet_flag(parser)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    case = (
        load_benchmark_case(args.case)
        if args.case is not None
        else load_packaged_benchmark_case(args.fixture)
    )
    report = run_benchmark_case(case)
    if report.replay_report is None:
        raise RuntimeError("benchmark case did not produce replay attribution")
    payload: dict[str, object] = {
        "case_name": report.case_name,
        "trade_attributions": [
            attribution.to_payload()
            for attribution in report.replay_report.trade_attributions
        ],
    }
    if report.description is not None:
        payload["description"] = report.description
    if report.replay_report.attribution_summary is not None:
        payload["attribution_summary"] = (
            report.replay_report.attribution_summary.to_payload()
        )
    if args.output is not None:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
            encoding="utf-8",
        )
    emit_json(payload, quiet=args.quiet, allow_nan=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

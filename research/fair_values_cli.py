from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import cast

from engine.cli_output import add_quiet_flag, emit_json
from engine.config_loader import load_config_file, nested_config_value
from research.calibration import load_calibration_artifact
from research.fair_values import (
    BookAggregation,
    DevigMethod,
    build_fair_value_manifest,
    load_market_snapshot,
    load_sportsbook_rows,
    resolve_rows_to_markets,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a fair-value manifest from normalized sportsbook odds rows."
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markets-file", default=None)
    parser.add_argument("--config-file", default=None)
    parser.add_argument(
        "--devig-method",
        choices=("multiplicative", "power"),
        default=None,
    )
    parser.add_argument(
        "--book-aggregation",
        choices=("independent", "best-line"),
        default=None,
    )
    parser.add_argument("--max-age-seconds", type=float, default=None)
    parser.add_argument("--source", default=None)
    parser.add_argument("--calibration-artifact", default=None)
    add_quiet_flag(parser)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_config_file(args.config_file) if args.config_file else {}
    devig_method = args.devig_method or nested_config_value(
        config, "research", "devig_method"
    )
    if devig_method not in {"multiplicative", "power"}:
        devig_method = "multiplicative"
    resolved_devig_method = cast(DevigMethod, devig_method)
    book_aggregation = args.book_aggregation or nested_config_value(
        config, "research", "book_aggregation"
    )
    if book_aggregation not in {"independent", "best-line"}:
        book_aggregation = "independent"
    resolved_book_aggregation = cast(BookAggregation, book_aggregation)
    rows = load_sportsbook_rows(args.input)
    skipped_rows: list[dict[str, object]] = []
    calibration_artifact = None
    if args.calibration_artifact is not None:
        try:
            calibration_artifact = load_calibration_artifact(args.calibration_artifact)
        except ValueError as exc:
            print(
                (
                    "warning: calibration artifact could not be loaded; "
                    f"continuing with raw fair values ({exc})"
                ),
                file=sys.stderr,
            )
    if args.markets_file is not None:
        markets = load_market_snapshot(args.markets_file)
        rows, skipped_rows = resolve_rows_to_markets(rows, markets)
    manifest = build_fair_value_manifest(
        rows,
        method=resolved_devig_method,
        source=args.source,
        max_age_seconds=args.max_age_seconds,
        aggregation=resolved_book_aggregation,
        calibration_artifact=calibration_artifact,
    )
    manifest_skipped_groups = list(manifest.skipped_groups or [])
    if skipped_rows:
        manifest_skipped_groups.extend(skipped_rows)
        manifest = replace(manifest, skipped_groups=manifest_skipped_groups)
    generated_at = manifest.generated_at
    if generated_at is None:
        raise ValueError("generated_at is required")
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest.to_payload(), indent=2, sort_keys=True))
    emit_json(
        {
            "output": str(output_path),
            "value_count": len(manifest.values or {}),
            "skipped_group_count": len(manifest.skipped_groups or []),
            "matched_row_count": len(rows),
            "source": manifest.source,
            "generated_at": generated_at.isoformat(),
            "book_aggregation": resolved_book_aggregation,
            "devig_method": resolved_devig_method,
            "calibration_applied": calibration_artifact is not None,
        },
        quiet=args.quiet,
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.calibration import load_calibration_artifact
from research.fair_values import (
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
    parser.add_argument(
        "--devig-method",
        choices=("multiplicative", "power"),
        default="multiplicative",
    )
    parser.add_argument(
        "--book-aggregation",
        choices=("independent", "best-line"),
        default="independent",
    )
    parser.add_argument("--max-age-seconds", type=float, default=None)
    parser.add_argument("--source", default=None)
    parser.add_argument("--calibration-artifact", default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
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
        method=args.devig_method,
        source=args.source,
        max_age_seconds=args.max_age_seconds,
        aggregation=args.book_aggregation,
        calibration_artifact=calibration_artifact,
    )
    if skipped_rows:
        manifest.skipped_groups.extend(skipped_rows)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest.to_payload(), indent=2, sort_keys=True))
    print(
        json.dumps(
            {
                "output": str(output_path),
                "value_count": len(manifest.values),
                "skipped_group_count": len(manifest.skipped_groups),
                "matched_row_count": len(rows),
                "source": manifest.source,
                "generated_at": manifest.generated_at.isoformat(),
                "book_aggregation": args.book_aggregation,
                "calibration_applied": calibration_artifact is not None,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

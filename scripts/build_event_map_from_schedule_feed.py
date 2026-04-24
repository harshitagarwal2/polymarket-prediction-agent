from __future__ import annotations

import argparse
import json
from pathlib import Path

from engine.cli_output import add_quiet_flag, emit_json
from research.data.schedule_feed import (
    build_event_map_from_schedule_rows,
    fetch_mlb_schedule,
    load_schedule_feed,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build an event_map JSON from a schedule/status feed."
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--provider", choices=("mlb-statsapi", "file"), default="file")
    parser.add_argument("--schedule-file", default=None)
    parser.add_argument("--date", default=None)
    parser.add_argument("--sport", default="mlb")
    parser.add_argument("--series", default="regular-season")
    add_quiet_flag(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.provider == "mlb-statsapi":
        if args.date in (None, ""):
            raise RuntimeError("--date is required for provider mlb-statsapi")
        rows = fetch_mlb_schedule(date=args.date)
    else:
        if args.schedule_file in (None, ""):
            raise RuntimeError("--schedule-file is required for provider file")
        rows = load_schedule_feed(args.schedule_file)

    event_map = build_event_map_from_schedule_rows(
        rows,
        sport=args.sport,
        series=args.series,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(event_map, indent=2, sort_keys=True), encoding="utf-8"
    )
    emit_json(
        {
            "output": str(output_path),
            "event_count": len(rows),
            "mapped_event_count": len(event_map),
            "provider": args.provider,
        },
        quiet=args.quiet,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

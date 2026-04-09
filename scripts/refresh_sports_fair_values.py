from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from adapters.types import serialize_market_summary
from research.fair_values import (
    build_fair_value_manifest,
    parse_sportsbook_rows,
    resolve_rows_to_markets,
)
from scripts.fetch_the_odds_api_rows import (
    fetch_odds_payload,
    load_event_map,
    normalize_odds_events,
)
from scripts.run_agent_loop import build_adapter


def _atomic_write_json(path: str | Path, payload: object) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        suffix=".json",
        dir=target.parent,
        delete=False,
        encoding="utf-8",
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        temp_path = Path(handle.name)
    temp_path.replace(target)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Continuously refresh sportsbook-derived fair values for Polymarket sports trading."
    )
    parser.add_argument("--sport-key", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--status-file", default="runtime/fair_values_refresh_status.json"
    )
    parser.add_argument("--event-map-file", default=None)
    parser.add_argument("--regions", default="us")
    parser.add_argument("--markets", default="h2h")
    parser.add_argument(
        "--odds-format", choices=("decimal", "american"), default="decimal"
    )
    parser.add_argument("--bookmakers", default=None)
    parser.add_argument(
        "--book-aggregation", choices=("independent", "best-line"), default="best-line"
    )
    parser.add_argument(
        "--devig-method", choices=("multiplicative", "power"), default="multiplicative"
    )
    parser.add_argument("--max-age-seconds", type=float, default=900)
    parser.add_argument("--markets-limit", type=int, default=200)
    parser.add_argument("--refresh-interval-seconds", type=float, default=60.0)
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    return parser


def _run_refresh_cycle_impl(args) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    event_map = load_event_map(args.event_map_file)
    adapter = build_adapter("polymarket")
    markets = adapter.list_markets(limit=args.markets_limit)
    payload = fetch_odds_payload(
        sport_key=args.sport_key,
        api_key=os.getenv(args.api_key_env, ""),
        regions=args.regions,
        markets=args.markets,
        odds_format=args.odds_format,
        bookmakers=args.bookmakers,
    )
    rows = parse_sportsbook_rows(
        normalize_odds_events(payload, sport_key=args.sport_key, event_map=event_map)
    )
    resolved_rows, skipped_rows = resolve_rows_to_markets(rows, markets)
    manifest = build_fair_value_manifest(
        resolved_rows,
        method=args.devig_method,
        max_age_seconds=args.max_age_seconds,
        aggregation=args.book_aggregation,
    )
    if skipped_rows:
        manifest.skipped_groups.extend(skipped_rows)
    _atomic_write_json(args.output, manifest.to_payload())
    status_payload = {
        "ok": True,
        "last_success_at": started_at.isoformat(),
        "sport_key": args.sport_key,
        "event_count": len(payload),
        "market_count": len(markets),
        "row_count": len(rows),
        "resolved_row_count": len(resolved_rows),
        "skipped_group_count": len(manifest.skipped_groups),
        "output": str(Path(args.output)),
        "book_aggregation": args.book_aggregation,
        "devig_method": args.devig_method,
    }
    _atomic_write_json(args.status_file, status_payload)
    return status_payload


def main() -> None:
    args = build_parser().parse_args()
    last_success: dict[str, object] | None = None
    cycle = 0
    while cycle < args.max_cycles:
        try:
            status_payload = _run_refresh_cycle_impl(args)
            last_success = status_payload
            print(json.dumps(status_payload, indent=2, sort_keys=True))
        except Exception as exc:
            failure_payload = {
                "ok": False,
                "error": str(exc),
                "last_success_at": (
                    last_success.get("last_success_at")
                    if last_success is not None
                    else None
                ),
                "output": str(Path(args.output)),
            }
            _atomic_write_json(args.status_file, failure_payload)
            print(json.dumps(failure_payload, indent=2, sort_keys=True))
        cycle += 1
        if cycle < args.max_cycles:
            time.sleep(max(0.0, args.refresh_interval_seconds))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from engine.cli_output import add_quiet_flag, emit_json
from research.data.odds_api import fetch_odds_payload as _fetch_odds_payload
from research.data.odds_api import load_event_map as _load_event_map
from research.data.odds_api import normalize_odds_events as _normalize_odds_events


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch live sportsbook odds from The Odds API and normalize them for fair-value building."
    )
    parser.add_argument("--sport-key", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--regions", default="us")
    parser.add_argument("--markets", default="h2h")
    parser.add_argument(
        "--odds-format", choices=("decimal", "american"), default="decimal"
    )
    parser.add_argument("--bookmakers", default=None)
    parser.add_argument("--event-map-file", default=None)
    parser.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    add_quiet_flag(parser)
    return parser


def load_event_map(path: str | None) -> dict[str, dict[str, Any]]:
    return _load_event_map(path)


def normalize_odds_events(
    events: list[dict[str, Any]],
    *,
    sport_key: str,
    event_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    return _normalize_odds_events(events, sport_key=sport_key, event_map=event_map)


def fetch_odds_payload(
    *,
    sport_key: str,
    api_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    bookmakers: str | None,
) -> list[dict[str, Any]]:
    return _fetch_odds_payload(
        sport_key=sport_key,
        api_key=api_key,
        regions=regions,
        markets=markets,
        odds_format=odds_format,
        bookmakers=bookmakers,
        urlopen_fn=urlopen,
    )


def main() -> None:
    args = build_parser().parse_args()
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"missing required environment variable: {args.api_key_env}")
    event_map = load_event_map(args.event_map_file)
    payload = fetch_odds_payload(
        sport_key=args.sport_key,
        api_key=api_key,
        regions=args.regions,
        markets=args.markets,
        odds_format=args.odds_format,
        bookmakers=args.bookmakers,
    )
    rows = normalize_odds_events(payload, sport_key=args.sport_key, event_map=event_map)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2, sort_keys=True))
    emit_json(
        {
            "output": str(output_path),
            "event_count": len(payload),
            "row_count": len(rows),
            "mapped_event_count": len(event_map),
        },
        quiet=args.quiet,
    )


if __name__ == "__main__":
    raise SystemExit(main())

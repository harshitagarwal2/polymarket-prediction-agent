from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from adapters.polymarket import PolymarketAdapter
from adapters.polymarket.gamma_client import fetch_markets
from adapters.types import serialize_market_summary
from research.data.capture_polymarket import (
    build_polymarket_capture,
    write_polymarket_capture,
)
from research.data.capture_sports_inputs import (
    build_sports_input_capture,
    write_sports_input_capture,
)
from scripts.config_loader import load_config_file, nested_config_value
from scripts.fetch_the_odds_api_rows import (
    fetch_odds_payload,
    load_event_map,
    normalize_odds_events,
)
from scripts.run_agent_loop import build_adapter


SPORT_KEY_BY_LEAGUE = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture normalized offline research inputs."
    )
    parser.add_argument(
        "--layer",
        choices=("gamma", "clob", "data-api", "sports-inputs"),
        required=True,
    )
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--sport-key", default=None)
    parser.add_argument("--event-map-file", default=None)
    parser.add_argument("--regions", default="us")
    parser.add_argument("--markets", default="h2h")
    parser.add_argument(
        "--odds-format", choices=("decimal", "american"), default="decimal"
    )
    parser.add_argument("--bookmakers", default=None)
    parser.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    parser.add_argument("--data-api-path", default="/trades")
    return parser


def _load_live_payload(args) -> object:
    config = load_config_file(args.config_file) if args.config_file else {}
    if args.layer == "sports-inputs":
        sport_key = args.sport_key or nested_config_value(
            config, "capture", "sport_key"
        )
        if sport_key in (None, ""):
            league = nested_config_value(config, "league")
            if isinstance(league, str):
                sport_key = SPORT_KEY_BY_LEAGUE.get(league.strip().lower())
        if not isinstance(sport_key, str) or not sport_key:
            raise RuntimeError(
                "sports-inputs live capture requires --sport-key or a config with league/capture.sport_key"
            )
        event_map = load_event_map(args.event_map_file)
        api_key = os.getenv(args.api_key_env)
        if not api_key:
            raise RuntimeError(
                f"missing required environment variable: {args.api_key_env}"
            )
        payload = fetch_odds_payload(
            sport_key=sport_key,
            api_key=api_key,
            regions=args.regions,
            markets=args.markets,
            odds_format=args.odds_format,
            bookmakers=args.bookmakers,
        )
        return normalize_odds_events(payload, sport_key=sport_key, event_map=event_map)
    adapter = build_adapter("polymarket")
    if args.layer == "gamma":
        return fetch_markets(limit=args.limit)
    if not isinstance(adapter, PolymarketAdapter):
        raise RuntimeError("live capture requires the Polymarket adapter")
    if args.layer == "clob":
        markets = adapter.list_markets(limit=args.limit)
        return [
            {
                "market": serialize_market_summary(market),
                "order_book": adapter.get_order_book(market.contract).raw,
            }
            for market in markets
        ]
    return adapter._fetch_data_api(args.data_api_path, {"limit": args.limit})


def main() -> None:
    args = build_parser().parse_args()
    if args.input is not None:
        payload = json.loads(Path(args.input).read_text())
    else:
        payload = _load_live_payload(args)
    if args.layer == "sports-inputs":
        envelope = build_sports_input_capture(payload, source=args.layer)
        path = write_sports_input_capture(envelope, args.output)
    else:
        envelope = build_polymarket_capture(payload, layer=args.layer)
        path = write_polymarket_capture(envelope, args.output)
    print(
        json.dumps({"output": str(path), "layer": args.layer}, indent=2, sort_keys=True)
    )


if __name__ == "__main__":
    main()

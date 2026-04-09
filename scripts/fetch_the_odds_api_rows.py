from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


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
    return parser


def load_event_map(path: str | None) -> dict[str, dict[str, Any]]:
    if path in (None, ""):
        return {}
    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("event map must be a JSON object keyed by source event id")
    return {
        str(key): value for key, value in payload.items() if isinstance(value, dict)
    }


def normalize_odds_events(
    events: list[dict[str, Any]],
    *,
    sport_key: str,
    event_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    normalized_market_types = {
        "h2h": "moneyline",
        "spreads": "spreads",
        "totals": "totals",
    }
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    for event in events:
        if not isinstance(event, dict):
            continue
        event_id = str(event.get("id") or "").strip()
        mapping = event_map.get(event_id, {})
        home_team = event.get("home_team")
        away_team = event.get("away_team")
        for bookmaker in event.get("bookmakers") or []:
            if not isinstance(bookmaker, dict):
                continue
            bookmaker_key = str(
                bookmaker.get("key") or bookmaker.get("title") or ""
            ).strip()
            captured_at = str(
                bookmaker.get("last_update") or event.get("commence_time") or now
            )
            for market in bookmaker.get("markets") or []:
                if not isinstance(market, dict):
                    continue
                market_key = str(market.get("key") or "").strip()
                normalized_market_type = str(
                    mapping.get("sports_market_type")
                    or normalized_market_types.get(market_key, market_key)
                )
                outcome_map = mapping.get("outcome_map")
                for outcome in market.get("outcomes") or []:
                    if not isinstance(outcome, dict):
                        continue
                    selection_name = str(outcome.get("name") or "").strip()
                    price = outcome.get("price")
                    if price in (None, ""):
                        continue
                    normalized_outcome = None
                    if isinstance(outcome_map, dict) and selection_name in outcome_map:
                        normalized_outcome = (
                            str(outcome_map[selection_name]).strip().lower()
                        )
                    elif selection_name.lower() in {"yes", "no"}:
                        normalized_outcome = selection_name.lower()
                    row = {
                        "source_event_id": event_id,
                        "sport_key": sport_key,
                        "bookmaker": bookmaker_key,
                        "captured_at": captured_at,
                        "decimal_odds": float(price),
                        "event_key": mapping.get("event_key"),
                        "sport": mapping.get("sport"),
                        "series": mapping.get("series"),
                        "game_id": mapping.get("game_id"),
                        "sports_market_type": normalized_market_type,
                        "selection_name": selection_name,
                        "home_team": home_team,
                        "away_team": away_team,
                    }
                    if normalized_outcome is not None:
                        row["outcome"] = normalized_outcome
                    rows.append(row)

    return rows


def fetch_odds_payload(
    *,
    sport_key: str,
    api_key: str,
    regions: str,
    markets: str,
    odds_format: str,
    bookmakers: str | None,
) -> list[dict[str, Any]]:
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
        "dateFormat": "iso",
    }
    if bookmakers not in (None, ""):
        params["bookmakers"] = bookmakers
    url = (
        f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/?{urlencode(params)}"
    )
    with urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError("The Odds API returned a non-list payload")
    return [event for event in payload if isinstance(event, dict)]


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
    print(
        json.dumps(
            {
                "output": str(output_path),
                "event_count": len(payload),
                "row_count": len(rows),
                "mapped_event_count": len(event_map),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

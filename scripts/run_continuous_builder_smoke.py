from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

from scripts import ingest_live_data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic continuous-builder baseline verification."
    )
    parser.add_argument("--root", default=None)
    return parser


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"expected JSON object at {path}")
    return payload


def _assert_builder_outputs(root: Path) -> None:
    current = root / "current"
    mappings = _read_json(current / "market_mappings.json")
    opportunities = _read_json(current / "opportunities.json")
    fair_value_manifest = _read_json(current / "fair_value_manifest.json")
    mapping_manifest = _read_json(current / "market_mapping_manifest.json")
    if not mappings:
        raise RuntimeError("continuous builder smoke expected nonempty market_mappings")
    if not opportunities:
        raise RuntimeError("continuous builder smoke expected nonempty opportunities")
    if not fair_value_manifest.get("generated_at"):
        raise RuntimeError(
            "continuous builder smoke expected fair value manifest generated_at"
        )
    metadata = fair_value_manifest.get("metadata")
    if not isinstance(metadata, dict):
        raise RuntimeError(
            "continuous builder smoke expected fair value manifest metadata"
        )
    if not mapping_manifest.get("generated_at"):
        raise RuntimeError(
            "continuous builder smoke expected mapping manifest generated_at"
        )
    mapping_metadata = mapping_manifest.get("metadata")
    if not isinstance(mapping_metadata, dict):
        raise RuntimeError(
            "continuous builder smoke expected mapping manifest metadata"
        )
    if not isinstance(mapping_metadata.get("provenance"), dict):
        raise RuntimeError(
            "continuous builder smoke expected mapping manifest provenance"
        )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_builder_inputs(root: Path) -> None:
    _write_json(
        root / "current" / "polymarket_markets.json",
        {
            "pm-1": {
                "market_id": "pm-1",
                "condition_id": "condition-1",
                "event_key": "event-1",
                "sport": "basketball_nba",
                "series": "playoffs",
                "game_id": "game-1",
                "status": "open",
                "raw_json": {
                    "id": "pm-1",
                    "conditionId": "condition-1",
                },
            }
        },
    )
    _write_json(
        root / "postgres" / "sportsbook_events.json",
        {
            "sb-1": {
                "sportsbook_event_id": "sb-1",
                "source": "theoddsapi",
                "sport": "basketball_nba",
                "league": "playoffs",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
                "raw_json": {
                    "id": "sb-1",
                    "sport": "nba",
                    "commence_time": "2026-04-21T19:00:00Z",
                },
            }
        },
    )
    _write_json(
        root / "current" / "sportsbook_events.json",
        {
            "sb-1": {
                "sportsbook_event_id": "sb-1",
                "source": "theoddsapi",
                "sport": "basketball_nba",
                "league": "playoffs",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "start_time": "2026-04-21T19:00:00Z",
                "raw_json": {
                    "id": "sb-1",
                    "sport": "nba",
                    "commence_time": "2026-04-21T19:00:00Z",
                },
            }
        },
    )
    _write_json(
        root / "current" / "sportsbook_odds.json",
        {
            "sb-1|book-a|h2h|Home Team": {
                "sportsbook_event_id": "sb-1",
                "source": "book-a",
                "market_type": "h2h",
                "selection": "Home Team",
                "price_decimal": 1.5,
                "implied_prob": 0.6666666667,
                "overround": 0.6666666667,
                "quote_ts": "2026-04-21T18:00:00+00:00",
                "source_age_ms": 0,
                "raw_json": {},
            }
        },
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.root in (None, ""):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            _seed_builder_inputs(root)
            previous_env = {
                key: os.environ.get(key)
                for key in (
                    "PREDICTION_MARKET_POSTGRES_DSN",
                    "POSTGRES_DSN",
                    "DATABASE_URL",
                )
            }
            try:
                for key in previous_env:
                    os.environ.pop(key, None)
                ingest_live_data.main(
                    [
                        "build-mappings",
                        "--market",
                        "h2h",
                        "--root",
                        str(root),
                        "--quiet",
                    ]
                )
                ingest_live_data.main(
                    ["build-fair-values", "--root", str(root), "--quiet"]
                )
                ingest_live_data.main(
                    ["build-opportunities", "--root", str(root), "--quiet"]
                )
                ingest_live_data.main(
                    ["build-fair-values", "--root", str(root), "--quiet"]
                )
                ingest_live_data.main(
                    ["build-opportunities", "--root", str(root), "--quiet"]
                )
            finally:
                for key, value in previous_env.items():
                    if value in (None, ""):
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
            _assert_builder_outputs(root)
        return 0

    root = Path(args.root)
    _seed_builder_inputs(root)
    previous_env = {
        key: os.environ.get(key)
        for key in (
            "PREDICTION_MARKET_POSTGRES_DSN",
            "POSTGRES_DSN",
            "DATABASE_URL",
        )
    }
    try:
        for key in previous_env:
            os.environ.pop(key, None)
        ingest_live_data.main(
            ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
        )
        ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])
        ingest_live_data.main(["build-opportunities", "--root", str(root), "--quiet"])
        ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])
        ingest_live_data.main(["build-opportunities", "--root", str(root), "--quiet"])
    finally:
        for key, value in previous_env.items():
            if value in (None, ""):
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    _assert_builder_outputs(root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, cast

from engine.cli_output import add_quiet_flag, emit_json
from scripts import ingest_live_data
from services.capture import (
    PolymarketMarketSnapshotRequest,
    SportsbookCaptureRequest,
    SportsbookCaptureStores,
    TheOddsApiCaptureSource,
    capture_sportsbook_odds_once,
    hydrate_polymarket_market_snapshot,
    persist_polymarket_bbo_input_events,
)
from services.projection import project_current_state_once
from storage.postgres.bootstrap import write_dsn_marker


_DUMMY_POLYMARKET_PRIVATE_KEY = "0x" + "1" * 64


class _StaticOddsClient:
    def fetch_upcoming(self, sport: str, market_type: str):
        return [
            {
                "id": "sb-1",
                "sport_title": "NBA",
                "home_team": "Home Team",
                "away_team": "Away Team",
                "commence_time": "2026-05-21T20:00:00+00:00",
                "bookmakers": [
                    {
                        "key": "book-a",
                        "last_update": "2026-05-21T17:59:30+00:00",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Home Team", "price": 1.8},
                                    {"name": "Away Team", "price": 2.0},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]


class _StaticCatalogClient:
    def fetch_open_markets(self):
        return [
            {
                "id": "pm-1",
                "conditionId": "pm-1",
                "question": "Will Home Team beat Away Team?",
                "active": True,
                "tokenIds": ["yes-token", "no-token"],
            }
        ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic Postgres-backed capture -> projection -> runtime-read smoke verification."
    )
    parser.add_argument("--root", default="runtime/data")
    parser.add_argument("--dsn", default=None)
    add_quiet_flag(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    if args.dsn not in (None, ""):
        write_dsn_marker(root, args.dsn)
    event_map = root / "odds_event_map.json"
    event_map.write_text(
        json.dumps(
            {
                "sb-1": {
                    "event_key": "event-1",
                    "game_id": "game-1",
                    "sport": "nba",
                    "series": "playoffs",
                }
            }
        ),
        encoding="utf-8",
    )

    capture_payload = capture_sportsbook_odds_once(
        SportsbookCaptureRequest(
            root=str(root),
            sport="basketball_nba",
            market="h2h",
            event_map_file=str(event_map),
        ),
        source=TheOddsApiCaptureSource(client=cast(Any, _StaticOddsClient())),
        stores=SportsbookCaptureStores.from_root(root, require_postgres=True),
        observed_at=datetime(2026, 5, 21, 18, 0, tzinfo=timezone.utc),
    )
    hydrate_polymarket_market_snapshot(
        request=PolymarketMarketSnapshotRequest(
            root=str(root),
            sport=None,
            market_type=None,
            limit=500,
            stale_after_ms=60_000,
        ),
        client=_StaticCatalogClient(),
        observed_at=datetime(2026, 5, 21, 18, 1, tzinfo=timezone.utc),
    )
    persist_polymarket_bbo_input_events(
        [
            {
                "asset_id": "pm-1",
                "best_bid": 0.45,
                "best_bid_size": 10,
                "best_ask": 0.47,
                "best_ask_size": 8,
                "timestamp": "2026-05-21T18:02:00Z",
            }
        ],
        root=str(root),
        observed_at=datetime(2026, 5, 21, 18, 2, tzinfo=timezone.utc),
    )
    projection_payload = project_current_state_once(root)
    ingest_live_data.main(
        ["build-mappings", "--market", "h2h", "--root", str(root), "--quiet"]
    )
    ingest_live_data.main(["build-fair-values", "--root", str(root), "--quiet"])
    ingest_live_data.main(["build-opportunities", "--root", str(root), "--quiet"])
    runtime_process = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.run_agent_loop",
            "--venue",
            "polymarket",
            "--mode",
            "preview",
            "--fair-values-file",
            str(root / "current" / "fair_value_manifest.json"),
            "--opportunity-root",
            str(root),
            "--max-cycles",
            "1",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "POLYMARKET_PRIVATE_KEY": os.environ.get(
                "POLYMARKET_PRIVATE_KEY",
                _DUMMY_POLYMARKET_PRIVATE_KEY,
            ),
        },
    )
    runtime_payload = json.loads(runtime_process.stdout)

    emit_json(
        {
            "ok": True,
            "capture": capture_payload,
            "projection": projection_payload,
            "runtime": runtime_payload,
            "root": str(root),
        },
        quiet=args.quiet,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

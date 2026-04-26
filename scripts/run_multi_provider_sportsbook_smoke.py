from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from adapters.sportsbooks import SportsbookJsonFeedClient
from adapters.sportsbooks.sportsgameodds import SportsGameOddsClient
from services.capture import (
    SportsbookCaptureRequest,
    SportsbookCaptureStores,
    SportsGameOddsCaptureSource,
    SportsbookJsonFeedCaptureSource,
    capture_sportsbook_odds_once,
)


class _StubHttpClient:
    def __init__(self, payload):
        self.payload = payload

    def get(self, url, params=None, headers=None, timeout=None, follow_redirects=True):
        class _Response:
            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return _Response(self.payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic multi-provider sportsbook staging verification."
    )
    parser.add_argument("--root", default=None)
    return parser


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"expected JSON object at {path}")
    return payload


def _run(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    stores = SportsbookCaptureStores.from_root(root)
    observed_at = datetime(2026, 5, 22, 18, 0, tzinfo=timezone.utc)
    event_map = root / "odds_event_map.json"
    event_map.write_text(
        json.dumps(
            {
                "feed-1": {
                    "event_key": "event-feed-1",
                    "game_id": "game-feed-1",
                    "sport": "nba",
                    "series": "playoffs",
                },
                "sgo-1": {
                    "event_key": "event-sgo-1",
                    "game_id": "game-sgo-1",
                    "sport": "nba",
                    "series": "playoffs",
                },
            }
        ),
        encoding="utf-8",
    )
    json_feed_source = SportsbookJsonFeedCaptureSource(
        client=SportsbookJsonFeedClient(
            feed_url="https://example.test/feed.json",
            client=_StubHttpClient(
                [
                    {
                        "external_id": "feed-1",
                        "sport_key": "basketball_nba",
                        "league_name": "NBA",
                        "home": "Home Team",
                        "away": "Away Team",
                        "start_time": "2026-05-22T20:00:00+00:00",
                        "provider_updated_at": "2026-05-22T17:59:00+00:00",
                        "bookmaker": "alt-book",
                        "markets": [
                            {
                                "market_type": "h2h",
                                "outcomes": [
                                    {"selection": "Home Team", "price": 2.0},
                                    {"selection": "Away Team", "price": 2.1},
                                ],
                            }
                        ],
                    }
                ]
            ),
        )
    )
    sportsgameodds_source = SportsGameOddsCaptureSource(
        client=SportsGameOddsClient(
            api_key="smoke-key",
            feed_url="https://example.test/events",
            client=_StubHttpClient(
                [
                    {
                        "eventID": "sgo-1",
                        "leagueID": "NBA",
                        "status": {"startsAt": "2026-05-22T21:00:00+00:00"},
                        "teams": {
                            "home": {"team": {"name": "Home Team 2"}},
                            "away": {"team": {"name": "Away Team 2"}},
                        },
                        "odds": {
                            "moneyline-home": {
                                "market_type": "h2h",
                                "byBookmaker": {
                                    "beta-book": {
                                        "updated_at": "2026-05-22T17:58:00+00:00",
                                        "odds": {"decimal": "1.9"},
                                    }
                                },
                            },
                            "moneyline-away": {
                                "market_type": "h2h",
                                "byBookmaker": {
                                    "beta-book": {
                                        "updated_at": "2026-05-22T17:58:00+00:00",
                                        "odds": {"decimal": 2.2},
                                    }
                                },
                            },
                        },
                    }
                ]
            ),
        )
    )

    first_payload = capture_sportsbook_odds_once(
        SportsbookCaptureRequest(
            root=str(root),
            sport="basketball_nba",
            market="h2h",
            event_map_file=str(event_map),
        ),
        source=json_feed_source,
        stores=stores,
        observed_at=observed_at,
    )
    second_payload = capture_sportsbook_odds_once(
        SportsbookCaptureRequest(
            root=str(root),
            sport="basketball_nba",
            market="h2h",
            event_map_file=str(event_map),
        ),
        source=sportsgameodds_source,
        stores=stores,
        observed_at=observed_at,
    )
    sportsbook_events = _read_json(root / "postgres" / "sportsbook_events.json")
    sportsbook_odds = _read_json(root / "postgres" / "sportsbook_odds.json")
    providers = {
        str(row.get("source") or "")
        for row in sportsbook_events.values()
        if isinstance(row, dict)
    }
    quote_providers = {
        str(row.get("provider") or "")
        for row in sportsbook_odds.values()
        if isinstance(row, dict)
    }
    if providers != {"json_feed", "sportsgameodds"}:
        raise RuntimeError(f"expected both providers in events, got {providers}")
    if quote_providers != {"json_feed", "sportsgameodds"}:
        raise RuntimeError(f"expected both providers in odds, got {quote_providers}")
    for payload, provider_name in (
        (first_payload, "json_feed"),
        (second_payload, "sportsgameodds"),
    ):
        capture_health = payload.get("source_health")
        if (
            not isinstance(capture_health, dict)
            or capture_health.get("status") != "ok"
            or payload.get("provider") != provider_name
        ):
            raise RuntimeError(
                f"{provider_name} capture payload not healthy after multi-provider smoke"
            )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.root in (None, ""):
        with tempfile.TemporaryDirectory() as temp_dir:
            _run(Path(temp_dir) / "runtime-data")
        return 0
    _run(Path(args.root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engine.http_client import get_json


def load_schedule_feed(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        if "games" in payload:
            rows = payload["games"]
        elif "events" in payload:
            rows = payload["events"]
        elif "data" in payload:
            rows = payload["data"]
        else:
            raise RuntimeError(
                "schedule feed object payload must contain a games/events/data list"
            )
    else:
        raise RuntimeError("schedule feed must contain a list or object payload")
    if not isinstance(rows, list):
        raise RuntimeError("schedule feed rows must be a list")
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("schedule feed rows must be objects")
    return [row for row in rows if isinstance(row, dict)]


def fetch_mlb_schedule(*, date: str, client=None) -> list[dict[str, Any]]:
    payload = get_json(
        "https://statsapi.mlb.com/api/v1/schedule",
        params={"sportId": 1, "date": date},
        timeout_seconds=30.0,
        client=client,
    )
    if not isinstance(payload, dict):
        raise RuntimeError("MLB schedule feed returned an unsupported payload")
    dates = payload.get("dates")
    if not isinstance(dates, list):
        raise RuntimeError("MLB schedule feed returned a malformed dates list")
    rows: list[dict[str, Any]] = []
    for date_row in dates:
        if not isinstance(date_row, dict):
            raise RuntimeError("MLB schedule feed returned a malformed date row")
        games = date_row.get("games")
        if not isinstance(games, list):
            raise RuntimeError("MLB schedule feed returned a malformed games list")
        for game in games:
            if not isinstance(game, dict):
                raise RuntimeError("MLB schedule feed returned a malformed game row")
            rows.append(game)
    return rows


def _slug(value: str) -> str:
    return "-".join(part for part in value.lower().replace("/", "-").split() if part)


def _schedule_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        current: Any = row
        found = True
        for part in key.split("."):
            if not isinstance(current, dict) or part not in current:
                found = False
                break
            current = current[part]
        if found and current not in (None, ""):
            return current
    return None


def build_event_map_from_schedule_rows(
    rows: list[dict[str, Any]],
    *,
    sport: str,
    series: str,
) -> dict[str, dict[str, Any]]:
    event_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_event_id = _schedule_value(row, "source_event_id", "gamePk", "id")
        home_team = _schedule_value(
            row,
            "home_team",
            "home",
            "homeTeamName",
            "teams.home.team.name",
        )
        away_team = _schedule_value(
            row,
            "away_team",
            "away",
            "awayTeamName",
            "teams.away.team.name",
        )
        start_time = _schedule_value(row, "start_time", "gameDate", "startTime")
        if (
            source_event_id in (None, "")
            or home_team in (None, "")
            or away_team in (None, "")
        ):
            raise RuntimeError(
                "schedule feed row is missing required event id or team identity fields"
            )
        event_id = str(source_event_id)
        game_id = str(_schedule_value(row, "game_id") or source_event_id)
        normalized_sport = str(_schedule_value(row, "sport") or sport)
        normalized_series = str(_schedule_value(row, "series", "gameType") or series)
        event_key = (
            str(_schedule_value(row, "event_key"))
            if _schedule_value(row, "event_key") not in (None, "")
            else f"{_slug(normalized_sport)}-{_slug(str(away_team))}-at-{_slug(str(home_team))}-{game_id}"
        )
        event_map[event_id] = {
            "event_key": event_key,
            "game_id": game_id,
            "sport": normalized_sport,
            "series": normalized_series,
            "scheduled_start_time": start_time,
            "status": _schedule_value(
                row, "status.detailedState", "detailedState", "status"
            ),
        }
    return event_map


__all__ = [
    "build_event_map_from_schedule_rows",
    "fetch_mlb_schedule",
    "load_schedule_feed",
]

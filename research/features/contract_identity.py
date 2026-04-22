from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Mapping


def _optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip() or None


def _parse_optional_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tokens(*values: str | None) -> tuple[str, ...]:
    parts: list[str] = []
    for value in values:
        if value in (None, ""):
            continue
        parts.extend(
            token
            for token in re.split(r"[^a-z0-9]+", str(value).strip().lower())
            if token
        )
    return tuple(sorted(set(parts)))


@dataclass(frozen=True)
class ContractIdentity:
    market_id: str | None = None
    sportsbook_event_id: str | None = None
    condition_id: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    title: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    start_time: datetime | None = None
    participant_tokens: tuple[str, ...] = ()


def polymarket_contract_identity(payload: Mapping[str, object]) -> ContractIdentity:
    title = _optional_text(payload.get("question") or payload.get("title"))
    home_team = _optional_text(payload.get("home_team") or payload.get("homeTeam"))
    away_team = _optional_text(payload.get("away_team") or payload.get("awayTeam"))
    return ContractIdentity(
        market_id=_optional_text(
            payload.get("market_id")
            or payload.get("marketId")
            or payload.get("id")
        ),
        condition_id=_optional_text(
            payload.get("condition_id") or payload.get("conditionId")
        ),
        event_key=_optional_text(payload.get("event_key") or payload.get("eventKey")),
        sport=_optional_text(payload.get("sport")),
        series=_optional_text(payload.get("series")),
        game_id=_optional_text(payload.get("game_id") or payload.get("gameId")),
        sports_market_type=_optional_text(
            payload.get("sports_market_type") or payload.get("sportsMarketType")
        ),
        title=title,
        home_team=home_team,
        away_team=away_team,
        start_time=_parse_optional_datetime(
            payload.get("start_time") or payload.get("gameStartTime") or payload.get("endDate")
        ),
        participant_tokens=_tokens(title, home_team, away_team),
    )


def sportsbook_contract_identity(
    payload: Mapping[str, object],
    *,
    sportsbook_market_type: str,
) -> ContractIdentity:
    home_team = _optional_text(payload.get("home_team"))
    away_team = _optional_text(payload.get("away_team"))
    title = _optional_text(payload.get("title") or payload.get("name"))
    return ContractIdentity(
        sportsbook_event_id=_optional_text(
            payload.get("sportsbook_event_id") or payload.get("id")
        ),
        event_key=_optional_text(payload.get("event_key")),
        sport=_optional_text(payload.get("sport")),
        series=_optional_text(payload.get("series") or payload.get("league")),
        game_id=_optional_text(payload.get("game_id")),
        sports_market_type=_optional_text(sportsbook_market_type),
        title=title,
        home_team=home_team,
        away_team=away_team,
        start_time=_parse_optional_datetime(
            payload.get("start_time") or payload.get("commence_time")
        ),
        participant_tokens=_tokens(
            title,
            home_team,
            away_team,
            _optional_text(payload.get("selection_name")),
            _optional_text(payload.get("outcome")),
        ),
    )


def team_overlap_score(
    left: ContractIdentity,
    right: ContractIdentity,
) -> float:
    left_tokens = set(left.participant_tokens)
    right_tokens = set(right.participant_tokens)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens.intersection(right_tokens)) / max(1, len(right_tokens))


def start_time_alignment_score(
    left: ContractIdentity,
    right: ContractIdentity,
) -> float:
    if left.start_time is None or right.start_time is None:
        return 0.0
    diff_minutes = abs((left.start_time - right.start_time).total_seconds()) / 60.0
    if diff_minutes <= 10:
        return 1.0
    if diff_minutes <= 60:
        return 0.5
    return 0.0

from __future__ import annotations

from datetime import datetime


def _selection_flag(selection_name: str | None, team_name: str) -> float:
    if selection_name in (None, "") or team_name == "":
        return 0.0
    return (
        1.0
        if str(selection_name).strip().casefold() == str(team_name).strip().casefold()
        else 0.0
    )


def _resolve_implied_probability(
    *, implied_probability: float | None, decimal_odds: float | None
) -> float:
    if implied_probability is not None:
        return float(implied_probability)
    if decimal_odds is not None and decimal_odds > 1.0:
        return 1.0 / float(decimal_odds)
    return 0.0


def _minutes_until(
    *, captured_at: datetime | None, start_time: datetime | None
) -> float:
    if captured_at is None or start_time is None:
        return 0.0
    return (start_time - captured_at).total_seconds() / 60.0


def build_team_strength_features(
    *,
    home_team: str,
    away_team: str,
    home_rating: float | None = None,
    away_rating: float | None = None,
    selection_name: str | None = None,
    decimal_odds: float | None = None,
    implied_probability: float | None = None,
    captured_at: datetime | None = None,
    start_time: datetime | None = None,
) -> dict[str, object]:
    home = float(home_rating or 1500.0)
    away = float(away_rating or 1500.0)
    rating_delta = home - away
    return {
        "home_team": home_team,
        "away_team": away_team,
        "home_rating": home,
        "away_rating": away,
        "rating_delta": rating_delta,
        "home_is_rating_favorite": 1.0 if rating_delta >= 0.0 else 0.0,
        "selection_is_home": _selection_flag(selection_name, home_team),
        "selection_is_away": _selection_flag(selection_name, away_team),
        "selection_implied_probability": _resolve_implied_probability(
            implied_probability=implied_probability,
            decimal_odds=decimal_odds,
        ),
        "time_to_start_minutes": _minutes_until(
            captured_at=captured_at,
            start_time=start_time,
        ),
    }

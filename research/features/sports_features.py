from __future__ import annotations


def build_team_strength_features(
    *,
    home_team: str,
    away_team: str,
    home_rating: float | None = None,
    away_rating: float | None = None,
) -> dict[str, object]:
    home = float(home_rating or 1500.0)
    away = float(away_rating or 1500.0)
    return {
        "home_team": home_team,
        "away_team": away_team,
        "home_rating": home,
        "away_rating": away,
        "rating_delta": home - away,
    }

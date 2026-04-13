from __future__ import annotations

from dataclasses import dataclass, field

from adapters.types import OutcomeSide
from research.schemas import FairValueBenchmarkCase, SportsBenchmarkCase


@dataclass(frozen=True)
class EloModelArtifact:
    initial_rating: float = 1500.0
    k_factor: float = 32.0
    team_ratings: dict[str, float] = field(default_factory=dict)
    training_match_count: int = 0

    def rating_for(self, team: str) -> float:
        return float(self.team_ratings.get(team, self.initial_rating))

    def expected_home_win_probability(self, home_team: str, away_team: str) -> float:
        home_rating = self.rating_for(home_team)
        away_rating = self.rating_for(away_team)
        return 1.0 / (1.0 + 10 ** ((away_rating - home_rating) / 400.0))

    def to_payload(self) -> dict[str, object]:
        return {
            "model_generator": "elo",
            "initial_rating": float(self.initial_rating),
            "k_factor": float(self.k_factor),
            "training_match_count": int(self.training_match_count),
            "team_ratings": {
                team: float(rating)
                for team, rating in sorted(self.team_ratings.items())
            },
        }


@dataclass(frozen=True)
class EloMatchExample:
    home_team: str
    away_team: str
    home_win: int


@dataclass(frozen=True)
class EloCasePrediction:
    home_team: str
    away_team: str
    yes_market_key: str
    no_market_key: str

    def to_probability_map(self, home_win_probability: float) -> dict[str, float]:
        return {
            self.yes_market_key: float(home_win_probability),
            self.no_market_key: float(1.0 - home_win_probability),
        }


def _unique_non_empty(values: list[str | None]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _extract_case_prediction(
    case: FairValueBenchmarkCase,
) -> EloCasePrediction | None:
    if not case.outcome_labels or not case.rows or not case.markets:
        return None
    rows = case.materialize_rows()
    home_teams = _unique_non_empty([row.home_team for row in rows])
    away_teams = _unique_non_empty([row.away_team for row in rows])
    if len(home_teams) != 1 or len(away_teams) != 1:
        return None
    if len(case.outcome_labels) != 2:
        return None
    yes_market_key = None
    no_market_key = None
    for market in case.materialize_markets():
        market_key = market.contract.market_key
        if market_key not in case.outcome_labels:
            continue
        if market.contract.outcome is OutcomeSide.YES:
            yes_market_key = market_key
        elif market.contract.outcome is OutcomeSide.NO:
            no_market_key = market_key
    if yes_market_key is None or no_market_key is None:
        return None
    if case.outcome_labels[yes_market_key] + case.outcome_labels[no_market_key] != 1:
        return None
    return EloCasePrediction(
        home_team=home_teams[0],
        away_team=away_teams[0],
        yes_market_key=yes_market_key,
        no_market_key=no_market_key,
    )


def extract_elo_training_example(
    case: SportsBenchmarkCase,
) -> EloMatchExample | None:
    if case.fair_value_case is None:
        return None
    prediction = _extract_case_prediction(case.fair_value_case)
    if prediction is None:
        return None
    home_win = case.fair_value_case.outcome_labels[prediction.yes_market_key]
    return EloMatchExample(
        home_team=prediction.home_team,
        away_team=prediction.away_team,
        home_win=int(home_win),
    )


def fit_elo_model(
    cases: list[SportsBenchmarkCase],
    *,
    initial_rating: float = 1500.0,
    k_factor: float = 32.0,
) -> EloModelArtifact:
    rows = [
        {
            "home_team": example.home_team,
            "away_team": example.away_team,
            "label": example.home_win,
        }
        for case in cases
        if (example := extract_elo_training_example(case)) is not None
    ]
    return fit_elo_model_from_rows(
        rows,
        initial_rating=initial_rating,
        k_factor=k_factor,
    )


def fit_elo_model_from_rows(
    rows: list[dict[str, object]],
    *,
    initial_rating: float = 1500.0,
    k_factor: float = 32.0,
) -> EloModelArtifact:
    ratings: dict[str, float] = {}
    training_match_count = 0
    for row in rows:
        home_team = str(row.get("home_team", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        raw_label = row.get("label", 0)
        if not home_team or not away_team:
            continue
        if isinstance(raw_label, bool) or not isinstance(raw_label, (int, float, str)):
            continue
        label = int(raw_label)
        home_rating = float(ratings.get(home_team, initial_rating))
        away_rating = float(ratings.get(away_team, initial_rating))
        expected_home = 1.0 / (1.0 + 10 ** ((away_rating - home_rating) / 400.0))
        rating_delta = float(k_factor) * (float(label) - expected_home)
        ratings[home_team] = home_rating + rating_delta
        ratings[away_team] = away_rating - rating_delta
        training_match_count += 1
    return EloModelArtifact(
        initial_rating=float(initial_rating),
        k_factor=float(k_factor),
        team_ratings=ratings,
        training_match_count=training_match_count,
    )


def generate_model_fair_values(
    case: FairValueBenchmarkCase,
    artifact: EloModelArtifact,
) -> dict[str, float]:
    prediction = _extract_case_prediction(case)
    if prediction is None:
        return {}
    home_win_probability = artifact.expected_home_win_probability(
        prediction.home_team,
        prediction.away_team,
    )
    return prediction.to_probability_map(home_win_probability)

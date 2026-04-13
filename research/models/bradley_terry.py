from __future__ import annotations

from dataclasses import dataclass, field
import math

from adapters.types import OutcomeSide
from research.schemas import FairValueBenchmarkCase, SportsBenchmarkCase


@dataclass(frozen=True)
class BradleyTerryArtifact:
    skill_by_team: dict[str, float] = field(default_factory=dict)

    def probability(self, home_team: str, away_team: str) -> float:
        home_skill = float(self.skill_by_team.get(home_team, 0.0))
        away_skill = float(self.skill_by_team.get(away_team, 0.0))
        return 1.0 / (1.0 + math.exp(-(home_skill - away_skill)))


def _extract_case_prediction(
    case: FairValueBenchmarkCase,
) -> tuple[str, str, str, str] | None:
    if not case.outcome_labels or not case.rows or not case.markets:
        return None
    rows = case.materialize_rows()
    home_teams = sorted({str(row.home_team).strip() for row in rows if row.home_team})
    away_teams = sorted({str(row.away_team).strip() for row in rows if row.away_team})
    if len(home_teams) != 1 or len(away_teams) != 1:
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
    return home_teams[0], away_teams[0], yes_market_key, no_market_key


def fit_bradley_terry_from_rows(rows: list[dict[str, object]]) -> BradleyTerryArtifact:
    wins: dict[str, float] = {}
    losses: dict[str, float] = {}
    for row in rows:
        home_team = str(row.get("home_team", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        raw_label = row.get("label", 0)
        if isinstance(raw_label, bool) or not isinstance(raw_label, (int, float, str)):
            continue
        label = int(raw_label)
        if not home_team or not away_team:
            continue
        wins.setdefault(home_team, 0.0)
        wins.setdefault(away_team, 0.0)
        losses.setdefault(home_team, 0.0)
        losses.setdefault(away_team, 0.0)
        if label == 1:
            wins[home_team] += 1.0
            losses[away_team] += 1.0
        else:
            wins[away_team] += 1.0
            losses[home_team] += 1.0
    skill_by_team = {
        team: math.log((wins.get(team, 0.0) + 1.0) / (losses.get(team, 0.0) + 1.0))
        for team in set(wins) | set(losses)
    }
    return BradleyTerryArtifact(skill_by_team=skill_by_team)


def fit_bradley_terry_from_cases(
    cases: list[SportsBenchmarkCase],
) -> BradleyTerryArtifact:
    rows: list[dict[str, object]] = []
    for case in cases:
        if case.fair_value_case is None:
            continue
        prediction = _extract_case_prediction(case.fair_value_case)
        if prediction is None:
            continue
        home_team, away_team, yes_market_key, _ = prediction
        rows.append(
            {
                "home_team": home_team,
                "away_team": away_team,
                "label": case.fair_value_case.outcome_labels[yes_market_key],
            }
        )
    return fit_bradley_terry_from_rows(rows)


def generate_model_fair_values(
    case: FairValueBenchmarkCase,
    artifact: BradleyTerryArtifact,
) -> dict[str, float]:
    prediction = _extract_case_prediction(case)
    if prediction is None:
        return {}
    home_team, away_team, yes_market_key, no_market_key = prediction
    home_win_probability = artifact.probability(home_team, away_team)
    return {
        yes_market_key: float(home_win_probability),
        no_market_key: float(1.0 - home_win_probability),
    }

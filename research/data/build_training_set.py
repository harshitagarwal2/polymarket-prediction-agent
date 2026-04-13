from __future__ import annotations

from collections.abc import Iterable

from research.data.capture_sports_inputs import load_sports_input_capture
from research.data.schemas import SportsInputRow
from research.data.schemas import TrainingSetRow
from research.features.sports_features import build_team_strength_features
from research.models.elo import extract_elo_training_example
from research.schemas import SportsBenchmarkCase


def build_training_set_rows(cases: list[SportsBenchmarkCase]) -> list[TrainingSetRow]:
    rows: list[TrainingSetRow] = []
    for case in cases:
        example = extract_elo_training_example(case)
        if example is None:
            continue
        rows.append(
            TrainingSetRow(
                home_team=example.home_team,
                away_team=example.away_team,
                label=int(example.home_win),
                metadata={"source_case": case.name},
            )
        )
    return rows


def build_training_set_rows_from_sports_inputs(
    rows: Iterable[SportsInputRow],
) -> list[TrainingSetRow]:
    training_rows: list[TrainingSetRow] = []
    for row in rows:
        if row.home_team in (None, "") or row.away_team in (None, ""):
            continue
        if row.label not in {0, 1}:
            continue
        feature_metadata = build_team_strength_features(
            home_team=str(row.home_team),
            away_team=str(row.away_team),
        )
        training_rows.append(
            TrainingSetRow(
                home_team=str(row.home_team),
                away_team=str(row.away_team),
                label=int(row.label),
                metadata={
                    **feature_metadata,
                    "event_key": row.event_key,
                    "sport": row.sport,
                    "series": row.series,
                    "game_id": row.game_id,
                    "sports_market_type": row.sports_market_type,
                    "source": row.source,
                },
            )
        )
    return training_rows


def load_training_set_rows(path: str) -> list[TrainingSetRow]:
    capture = load_sports_input_capture(path)
    return build_training_set_rows_from_sports_inputs(capture.rows)

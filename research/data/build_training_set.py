from __future__ import annotations

from research.data.schemas import TrainingSetRow
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

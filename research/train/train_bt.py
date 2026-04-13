from __future__ import annotations

import json
from pathlib import Path

from research.data.schemas import TrainingSetRow
from research.data.build_training_set import build_training_set_rows
from research.models.bradley_terry import fit_bradley_terry_from_rows
from research.schemas import SportsBenchmarkCase


def write_bt_artifact(
    cases: list[SportsBenchmarkCase], output_path: str | Path
) -> Path:
    rows = [row.to_payload() for row in build_training_set_rows(cases)]
    return write_bt_artifact_from_rows(rows, output_path)


def write_bt_artifact_from_rows(
    rows: list[TrainingSetRow] | list[dict[str, object]], output_path: str | Path
) -> Path:
    payload_rows = [
        row.to_payload() if isinstance(row, TrainingSetRow) else row for row in rows
    ]
    artifact = fit_bradley_terry_from_rows(payload_rows)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"skill_by_team": artifact.skill_by_team}, indent=2, sort_keys=True)
    )
    return path

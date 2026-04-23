from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from research.data.schemas import TrainingSetRow
from research.models.elo import fit_elo_model, fit_elo_model_from_rows
from research.schemas import SportsBenchmarkCase


def write_elo_artifact(
    cases: list[SportsBenchmarkCase], output_path: str | Path
) -> Path:
    artifact = fit_elo_model(cases)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact.to_payload(), indent=2, sort_keys=True))
    return path


def write_elo_artifact_from_rows(
    rows: list[TrainingSetRow] | list[dict[str, Any]], output_path: str | Path
) -> Path:
    payload_rows = [
        row.to_payload() if isinstance(row, TrainingSetRow) else row for row in rows
    ]
    artifact = fit_elo_model_from_rows(payload_rows)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact.to_payload(), indent=2, sort_keys=True))
    return path

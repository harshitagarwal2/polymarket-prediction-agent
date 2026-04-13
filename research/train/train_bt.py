from __future__ import annotations

import json
from pathlib import Path

from research.data.build_training_set import build_training_set_rows
from research.models.bradley_terry import fit_bradley_terry_from_rows
from research.schemas import SportsBenchmarkCase


def write_bt_artifact(
    cases: list[SportsBenchmarkCase], output_path: str | Path
) -> Path:
    rows = [row.to_payload() for row in build_training_set_rows(cases)]
    artifact = fit_bradley_terry_from_rows(rows)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"skill_by_team": artifact.skill_by_team}, indent=2, sort_keys=True)
    )
    return path

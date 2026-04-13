from __future__ import annotations

import json
from pathlib import Path

from research.models.elo import fit_elo_model
from research.schemas import SportsBenchmarkCase


def write_elo_artifact(
    cases: list[SportsBenchmarkCase], output_path: str | Path
) -> Path:
    artifact = fit_elo_model(cases)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact.to_payload(), indent=2, sort_keys=True))
    return path

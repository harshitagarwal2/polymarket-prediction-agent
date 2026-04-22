from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping

from research.models.book_consensus import fit_book_consensus_artifact


def write_consensus_artifact(
    output_path: str | Path,
    *,
    half_life_seconds: float = 3600.0,
    model_version: str = "v1",
) -> Path:
    artifact = fit_book_consensus_artifact([], half_life_seconds=half_life_seconds)
    payload = {**artifact.to_payload(), "model_version": model_version}
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path


def write_consensus_artifact_from_rows(
    rows: Iterable[Mapping[str, object]],
    output_path: str | Path,
    *,
    half_life_seconds: float = 3600.0,
    model_version: str = "v1",
) -> Path:
    artifact = fit_book_consensus_artifact(rows, half_life_seconds=half_life_seconds)
    payload = {**artifact.to_payload(), "model_version": model_version}
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path

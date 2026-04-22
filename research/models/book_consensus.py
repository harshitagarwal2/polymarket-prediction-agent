from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from forecasting.consensus import weighted_consensus


@dataclass(frozen=True)
class BookConsensusArtifact:
    model: str = "consensus"
    model_version: str = "v1"
    half_life_seconds: float = 3600.0
    bookmaker_count: int = 0
    row_count: int = 0

    def to_payload(self) -> dict[str, object]:
        return {
            "model": self.model,
            "model_version": self.model_version,
            "half_life_seconds": self.half_life_seconds,
            "bookmaker_count": self.bookmaker_count,
            "row_count": self.row_count,
        }


def _payload_rows(rows: Iterable[Mapping[str, object]]) -> list[dict[str, object]]:
    return [dict(row) for row in rows]


def _as_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"consensus artifact {field_name} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"consensus artifact {field_name} must be numeric") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"consensus artifact {field_name} must be finite")
    return parsed


def _as_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"consensus artifact {field_name} must be an integer")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"consensus artifact {field_name} must be an integer") from exc
    if not math.isfinite(parsed) or not parsed.is_integer():
        raise ValueError(f"consensus artifact {field_name} must be an integer")
    return int(parsed)


def _as_string(value: object, *, field_name: str, default: str) -> str:
    if value in (None, ""):
        return default
    if not isinstance(value, str):
        raise ValueError(f"consensus artifact {field_name} must be a string")
    return value


def consensus_probability_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    half_life_seconds: float = 3600.0,
) -> float:
    payload_rows = _payload_rows(rows)
    for row in payload_rows:
        source_age_ms = row.get("source_age_ms")
        if "freshness_seconds" not in row and source_age_ms is not None:
            row["freshness_seconds"] = (
                _as_float(
                    source_age_ms,
                    field_name="source_age_ms",
                )
                / 1000.0
            )
    return weighted_consensus(payload_rows, half_life_seconds=half_life_seconds)


def fit_book_consensus_artifact(
    rows: Iterable[Mapping[str, object]],
    *,
    half_life_seconds: float = 3600.0,
) -> BookConsensusArtifact:
    payload_rows = _payload_rows(rows)
    bookmakers: set[str] = set()
    for row in payload_rows:
        metadata = row.get("metadata")
        source = row.get("source") or row.get("bookmaker")
        if source in (None, "") and isinstance(metadata, dict):
            source = metadata.get("source")
        if source not in (None, ""):
            bookmakers.add(str(source))
    bookmakers.discard("")
    return BookConsensusArtifact(
        half_life_seconds=float(half_life_seconds),
        bookmaker_count=len(bookmakers),
        row_count=len(payload_rows),
    )


def load_book_consensus_artifact(
    source: str | Path | Mapping[str, object],
) -> BookConsensusArtifact:
    payload: dict[str, object]
    if isinstance(source, (str, Path)):
        payload = json.loads(Path(source).read_text())
    elif isinstance(source, Mapping):
        payload = dict(source)
    else:
        raise ValueError("consensus artifact must be a path or mapping")

    half_life_seconds = _as_float(
        payload.get("half_life_seconds", 3600.0),
        field_name="half_life_seconds",
    )
    if half_life_seconds <= 0:
        raise ValueError("consensus artifact half_life_seconds must be positive")

    return BookConsensusArtifact(
        model=_as_string(
            payload.get("model"),
            field_name="model",
            default="consensus",
        ),
        model_version=_as_string(
            payload.get("model_version"),
            field_name="model_version",
            default="v1",
        ),
        half_life_seconds=half_life_seconds,
        bookmaker_count=_as_int(
            payload.get("bookmaker_count", 0) or 0,
            field_name="bookmaker_count",
        ),
        row_count=_as_int(
            payload.get("row_count", 0) or 0,
            field_name="row_count",
        ),
    )

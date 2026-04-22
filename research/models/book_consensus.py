from __future__ import annotations

from dataclasses import dataclass
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


def consensus_probability_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    half_life_seconds: float = 3600.0,
) -> float:
    payload_rows = _payload_rows(rows)
    for row in payload_rows:
        if "freshness_seconds" not in row and row.get("source_age_ms") is not None:
            row["freshness_seconds"] = float(row["source_age_ms"]) / 1000.0
        row.setdefault("half_life_seconds", half_life_seconds)
    return weighted_consensus(payload_rows)


def fit_book_consensus_artifact(
    rows: Iterable[Mapping[str, object]],
    *,
    half_life_seconds: float = 3600.0,
) -> BookConsensusArtifact:
    payload_rows = _payload_rows(rows)
    bookmakers = {
        str(source)
        for row in payload_rows
        for source in [
            row.get("source")
            or row.get("bookmaker")
            or (
                row.get("metadata", {}).get("source")
                if isinstance(row.get("metadata"), dict)
                else None
            )
        ]
        if source is not None
    }
    bookmakers.discard("")
    return BookConsensusArtifact(
        half_life_seconds=float(half_life_seconds),
        bookmaker_count=len(bookmakers),
        row_count=len(payload_rows),
    )

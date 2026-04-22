from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EvidenceMemo:
    summary: str
    citations: tuple[str, ...]
    key_points: tuple[str, ...]


def summarize_evidence(
    notes: list[str],
    *,
    citations: list[str] | None = None,
) -> EvidenceMemo:
    cleaned_notes = tuple(note.strip() for note in notes if note.strip())
    summary = cleaned_notes[0] if cleaned_notes else "No evidence available."
    return EvidenceMemo(
        summary=summary,
        citations=tuple(citations or ()),
        key_points=cleaned_notes,
    )

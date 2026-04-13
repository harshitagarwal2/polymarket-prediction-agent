from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class CaptureRecord:
    source: str
    captured_at: datetime = field(default_factory=_utc_now)
    payload: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "captured_at": self.captured_at.isoformat(),
            "payload": self.payload,
        }


@dataclass(frozen=True)
class TrainingSetRow:
    home_team: str
    away_team: str
    label: int
    metadata: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "home_team": self.home_team,
            "away_team": self.away_team,
            "label": int(self.label),
            "metadata": self.metadata,
        }

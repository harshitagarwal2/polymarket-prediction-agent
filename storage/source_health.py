from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .current_state_projectors import (  # pyright: ignore[reportMissingImports]
    SourceHealthUpdate,
    project_source_health_state,
)


@dataclass(frozen=True)
class SourceHealthRecord:
    source_name: str
    last_seen_at: str | None
    last_success_at: str | None
    stale_after_ms: int
    status: str
    details: dict[str, Any]


class SourceHealthStore:
    def __init__(
        self, path: str | Path = "runtime/data/current/source_health.json"
    ) -> None:
        self.path = Path(path)

    def read_all(self) -> dict[str, Any]:
        return self._load()

    def write_all(self, records: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(records, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def upsert(
        self,
        source_name: str,
        *,
        stale_after_ms: int,
        status: str,
        details: dict[str, Any] | None = None,
        success: bool = True,
        observed_at: datetime | None = None,
    ) -> SourceHealthRecord:
        projected = project_source_health_state(
            (
                SourceHealthUpdate(
                    source_name=source_name,
                    stale_after_ms=int(stale_after_ms),
                    status=status,
                    details=details or {},
                    success=success,
                    observed_at=observed_at or datetime.now(timezone.utc),
                ),
            ),
            existing=self.read_all(),
        )
        record = SourceHealthRecord(**projected[source_name])
        self.write_all(projected)
        return record

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

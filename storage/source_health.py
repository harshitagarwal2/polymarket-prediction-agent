from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceHealthRecord:
    source_name: str
    last_seen_at: str | None
    last_success_at: str | None
    stale_after_ms: int
    status: str
    details: dict[str, Any]


class SourceHealthStore:
    def __init__(self, path: str | Path = "runtime/data/current/source_health.json") -> None:
        self.path = Path(path)

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
        now = (observed_at or datetime.now(timezone.utc)).isoformat()
        existing = self._load()
        current = existing.get(source_name, {})
        record = SourceHealthRecord(
            source_name=source_name,
            last_seen_at=now,
            last_success_at=now if success else current.get("last_success_at"),
            stale_after_ms=int(stale_after_ms),
            status=status,
            details=details or {},
        )
        existing[source_name] = asdict(record)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8")
        return record

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

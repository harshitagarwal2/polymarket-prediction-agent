from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from contracts.mapping_schema import MAPPING_MANIFEST_SCHEMA_VERSION


@dataclass(frozen=True)
class MappingManifestBuild:
    schema_version: int = MAPPING_MANIFEST_SCHEMA_VERSION
    generated_at: datetime | None = None
    source: str = "unknown"
    values: dict[str, dict[str, object]] | None = None
    metadata: dict[str, object] | None = None

    def _metadata_payload(self) -> dict[str, object] | None:
        if not isinstance(self.metadata, dict):
            return None
        payload = dict(self.metadata)
        coverage_payload = payload.get("coverage")
        coverage = dict(coverage_payload) if isinstance(coverage_payload, dict) else {}
        coverage["value_count"] = len(self.values or {})
        payload["coverage"] = coverage
        return payload

    def to_payload(self) -> dict[str, object]:
        if self.generated_at is None:
            raise ValueError("generated_at is required")
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat().replace("+00:00", "Z"),
            "source": self.source,
            "values": self.values or {},
        }
        metadata = self._metadata_payload()
        if metadata:
            payload["metadata"] = metadata
        return payload

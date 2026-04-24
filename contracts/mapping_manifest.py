from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json

from contracts.mapping_schema import MAPPING_MANIFEST_SCHEMA_VERSION


@dataclass(frozen=True)
class MappingManifestBuild:
    schema_version: int = MAPPING_MANIFEST_SCHEMA_VERSION
    generated_at: datetime | None = None
    source: str = "unknown"
    values: dict[str, dict[str, object]] | None = None
    metadata: dict[str, object] | None = None
    manifest_id: str | None = None
    review_status: str = "generated"
    reviewer: str | None = None
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    superseded_by: str | None = None
    override_reason: str | None = None

    def _metadata_payload(self) -> dict[str, object] | None:
        payload = dict(self.metadata) if isinstance(self.metadata, dict) else {}
        coverage_payload = payload.get("coverage")
        coverage = dict(coverage_payload) if isinstance(coverage_payload, dict) else {}
        coverage["value_count"] = len(self.values or {})
        payload["coverage"] = coverage
        payload["governance"] = self._governance_payload()
        payload["provenance"] = self._provenance_payload(payload.get("provenance"))
        return payload or None

    def _governance_payload(self) -> dict[str, object]:
        if self.generated_at is None:
            raise ValueError("generated_at is required")
        manifest_id = self.manifest_id or self._default_manifest_id()
        payload: dict[str, object] = {
            "manifest_id": manifest_id,
            "review_status": self.review_status,
            "effective_from": (
                (self.effective_from or self.generated_at)
                .isoformat()
                .replace("+00:00", "Z")
            ),
        }
        if self.reviewer not in (None, ""):
            payload["reviewer"] = self.reviewer
        if self.effective_to is not None:
            payload["effective_to"] = self.effective_to.isoformat().replace(
                "+00:00", "Z"
            )
        if self.superseded_by not in (None, ""):
            payload["superseded_by"] = self.superseded_by
        if self.override_reason not in (None, ""):
            payload["override_reason"] = self.override_reason
        return payload

    def _provenance_payload(self, existing: object) -> dict[str, object]:
        payload = dict(existing) if isinstance(existing, dict) else {}
        payload["hash"] = self._provenance_hash()
        return payload

    def _default_manifest_id(self) -> str:
        if self.generated_at is None:
            raise ValueError("generated_at is required")
        return (
            f"mapping-manifest:{self.source}:"
            f"{self.generated_at.isoformat().replace('+00:00', 'Z')}"
        )

    def _provenance_hash(self) -> str:
        normalized_values = json.dumps(
            self.values or {},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return hashlib.sha256(normalized_values.encode("utf-8")).hexdigest()

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

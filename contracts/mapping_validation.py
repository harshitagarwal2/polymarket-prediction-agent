from __future__ import annotations

import math
from typing import Mapping

from contracts.mapping_schema import (
    MAPPING_MANIFEST_SCHEMA_VERSION,
    allowed_mapping_confidence_bands,
    allowed_mapping_manifest_review_statuses,
    allowed_mapping_statuses,
    required_mapping_target_fields,
)


def validate_mapping_manifest_schema_version(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("mapping manifest schema_version must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("mapping manifest schema_version must be an integer") from exc
    if str(parsed) != str(value).strip():
        raise ValueError("mapping manifest schema_version must be an integer")
    if parsed != MAPPING_MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            "unsupported mapping manifest schema_version: "
            f"{parsed} (expected {MAPPING_MANIFEST_SCHEMA_VERSION})"
        )
    return parsed


def _require_mapping(item: object, *, context: str) -> Mapping[str, object]:
    if not isinstance(item, Mapping):
        raise ValueError(f"{context} must be an object")
    return item


def _validate_confidence(confidence: object, *, market_key: object) -> None:
    payload = _require_mapping(
        confidence,
        context=f"mapping_confidence for market key: {market_key}",
    )
    band = payload.get("band")
    if band not in allowed_mapping_confidence_bands():
        raise ValueError(
            "mapping_confidence.band must be one of "
            f"{allowed_mapping_confidence_bands()} for market key: {market_key}"
        )
    score = payload.get("score")
    if score not in (None, ""):
        if isinstance(score, bool) or not isinstance(score, (int, float, str)):
            raise ValueError(
                f"mapping_confidence.score must be numeric for market key: {market_key}"
            )
        parsed = float(score)
        if not math.isfinite(parsed) or parsed < 0.0 or parsed > 1.0:
            raise ValueError(
                f"mapping_confidence.score must be finite between 0 and 1 for market key: {market_key}"
            )
    components = payload.get("components")
    if not isinstance(components, Mapping):
        raise ValueError(
            f"mapping_confidence.components must be an object for market key: {market_key}"
        )
    reasons = payload.get("reasons")
    if not isinstance(reasons, list) or not all(
        isinstance(item, str) for item in reasons
    ):
        raise ValueError(
            f"mapping_confidence.reasons must be a list of strings for market key: {market_key}"
        )


def _validate_blocked_reason(blocked_reason: object, *, market_key: object) -> None:
    if blocked_reason in (None, ""):
        return
    payload = _require_mapping(
        blocked_reason,
        context=f"blocked_reason for market key: {market_key}",
    )
    if payload.get("code") in (None, ""):
        raise ValueError(
            f"blocked_reason.code is required for market key: {market_key}"
        )
    if payload.get("message") in (None, ""):
        raise ValueError(
            f"blocked_reason.message is required for market key: {market_key}"
        )


def _validate_manifest_governance(governance: object) -> None:
    payload = _require_mapping(governance, context="mapping manifest governance")
    if payload.get("manifest_id") in (None, ""):
        raise ValueError("mapping manifest governance.manifest_id is required")
    review_status = payload.get("review_status")
    if review_status not in allowed_mapping_manifest_review_statuses():
        raise ValueError(
            "mapping manifest governance.review_status must be one of "
            f"{allowed_mapping_manifest_review_statuses()}"
        )
    if payload.get("effective_from") in (None, ""):
        raise ValueError("mapping manifest governance.effective_from is required")
    if review_status in {"reviewed", "approved", "superseded"} and payload.get(
        "reviewer"
    ) in (None, ""):
        raise ValueError(
            "mapping manifest governance.reviewer is required for reviewed manifests"
        )
    if review_status == "superseded" and payload.get("superseded_by") in (None, ""):
        raise ValueError(
            "mapping manifest governance.superseded_by is required for superseded manifests"
        )


def _validate_manifest_provenance(provenance: object) -> None:
    payload = _require_mapping(provenance, context="mapping manifest provenance")
    hash_value = payload.get("hash")
    if hash_value in (None, ""):
        raise ValueError("mapping manifest provenance.hash is required")
    hash_text = str(hash_value).strip().lower()
    if len(hash_text) != 64 or any(ch not in "0123456789abcdef" for ch in hash_text):
        raise ValueError(
            "mapping manifest provenance.hash must be a 64-char hex string"
        )


def validate_mapping_manifest_record(market_key: object, item: object) -> None:
    payload = _require_mapping(item, context=f"mapping record for {market_key}")
    status = payload.get("mapping_status")
    if status not in allowed_mapping_statuses():
        raise ValueError(
            f"mapping_status must be one of {allowed_mapping_statuses()} for market key: {market_key}"
        )
    target = _require_mapping(
        payload.get("target"),
        context=f"target for market key: {market_key}",
    )
    for field in required_mapping_target_fields():
        if target.get(field) in (None, ""):
            raise ValueError(f"target.{field} is required for market key: {market_key}")
    _validate_confidence(payload.get("mapping_confidence"), market_key=market_key)
    _validate_blocked_reason(payload.get("blocked_reason"), market_key=market_key)
    identity = payload.get("identity")
    if identity is not None and not isinstance(identity, Mapping):
        raise ValueError(f"identity must be an object for market key: {market_key}")
    semantics = payload.get("semantics")
    if semantics is not None and not isinstance(semantics, Mapping):
        raise ValueError(f"semantics must be an object for market key: {market_key}")


def validate_mapping_manifest_payload(payload: Mapping[str, object]) -> None:
    schema_version = payload.get("schema_version")
    if schema_version not in (None, ""):
        validate_mapping_manifest_schema_version(schema_version)
    if payload.get("generated_at") in (None, ""):
        raise ValueError("mapping manifest generated_at is required")
    values = payload.get("values")
    if not isinstance(values, Mapping):
        raise ValueError("mapping manifest must contain a values object")
    metadata = payload.get("metadata")
    if metadata is not None:
        metadata_payload = _require_mapping(
            metadata, context="mapping manifest metadata"
        )
        governance = metadata_payload.get("governance")
        if governance is not None:
            _validate_manifest_governance(governance)
        provenance = metadata_payload.get("provenance")
        if provenance is not None:
            _validate_manifest_provenance(provenance)
    for market_key, item in values.items():
        validate_mapping_manifest_record(market_key, item)

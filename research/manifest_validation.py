from __future__ import annotations

import math
from typing import Mapping

from research.manifest_schema import (
    FAIR_VALUE_MANIFEST_SCHEMA_VERSION,
    required_manifest_identity_fields,
)


def validate_manifest_schema_version(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("fair-value manifest schema_version must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("fair-value manifest schema_version must be an integer") from exc
    if str(parsed) != str(value).strip():
        raise ValueError("fair-value manifest schema_version must be an integer")
    if parsed != FAIR_VALUE_MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            "unsupported fair-value manifest schema_version: "
            f"{parsed} (expected {FAIR_VALUE_MANIFEST_SCHEMA_VERSION})"
        )
    return parsed


def validate_manifest_record(market_key: object, item: object) -> None:
    if not isinstance(item, Mapping):
        raise ValueError(f"manifest record for {market_key} must be an object")
    fair_value = item.get("fair_value")
    if fair_value in (None, ""):
        raise ValueError(f"manifest fair value missing for market key: {market_key}")
    if isinstance(fair_value, bool) or not isinstance(fair_value, (int, float, str)):
        raise ValueError(f"manifest fair value for market key: {market_key} must be numeric")
    parsed = float(fair_value)
    if not math.isfinite(parsed):
        raise ValueError(f"manifest fair value for market key: {market_key} must be finite")
    if item.get("generated_at") in (None, ""):
        raise ValueError(
            f"manifest generated_at missing for market key: {market_key}"
        )
    if not any(item.get(field) not in (None, "") for field in required_manifest_identity_fields()):
        raise ValueError(
            "manifest record must include event identity "
            f"(condition_id, event_key, or game_id) for market key: {market_key}"
        )


def validate_manifest_payload(payload: Mapping[str, object]) -> None:
    schema_version = payload.get("schema_version")
    if schema_version not in (None, ""):
        validate_manifest_schema_version(schema_version)
    if payload.get("generated_at") in (None, ""):
        raise ValueError("fair-value manifest generated_at is required")
    values = payload.get("values")
    if not isinstance(values, Mapping):
        raise ValueError("fair-value manifest must contain a values object")
    for market_key, item in values.items():
        validate_manifest_record(market_key, item)

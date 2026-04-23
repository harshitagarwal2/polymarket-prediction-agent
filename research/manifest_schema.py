from __future__ import annotations


FAIR_VALUE_MANIFEST_SCHEMA_VERSION = 1
REQUIRED_MANIFEST_IDENTITY_FIELDS = ("condition_id", "event_key", "game_id")


def required_manifest_identity_fields() -> tuple[str, ...]:
    return REQUIRED_MANIFEST_IDENTITY_FIELDS

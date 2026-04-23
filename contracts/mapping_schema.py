from __future__ import annotations


MAPPING_MANIFEST_SCHEMA_VERSION = 1
ALLOWED_MAPPING_STATUSES = (
    "exact_match",
    "normalized_match",
    "ambiguous_match",
    "blocked",
)
ALLOWED_MAPPING_CONFIDENCE_BANDS = ("high", "medium", "low", "unscored")
REQUIRED_MAPPING_TARGET_FIELDS = (
    "sportsbook_event_id",
    "sportsbook_market_type",
    "normalized_market_type",
)


def allowed_mapping_statuses() -> tuple[str, ...]:
    return ALLOWED_MAPPING_STATUSES


def allowed_mapping_confidence_bands() -> tuple[str, ...]:
    return ALLOWED_MAPPING_CONFIDENCE_BANDS


def required_mapping_target_fields() -> tuple[str, ...]:
    return REQUIRED_MAPPING_TARGET_FIELDS

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

from forecasting.fair_value_engine import (
    FairValueField,
    FairValueManifestEntry,
    ManifestFairValueProvider,
    StaticFairValueProvider,
)

SUPPORTED_FAIR_VALUE_MANIFEST_SCHEMA_VERSION = 1


class FairValueLookup(Protocol):
    def fair_value_for(self, market: object) -> float | None: ...


def _parse_fair_value_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_manifest_numeric(
    value: object,
    *,
    context: str,
    required: bool = False,
) -> float | None:
    if value in (None, ""):
        if required:
            raise RuntimeError(f"{context} is required")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        if required:
            raise RuntimeError(f"{context} must be numeric")
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        if required:
            raise RuntimeError(f"{context} must be numeric") from exc
        return None
    if not math.isfinite(parsed):
        if required:
            raise RuntimeError(f"{context} must be finite")
        return None
    return parsed


def _optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _parse_manifest_schema_version(payload: dict[str, object]) -> int | None:
    raw_version = payload.get("schema_version")
    if raw_version in (None, ""):
        return None
    if isinstance(raw_version, bool) or not isinstance(raw_version, (int, float, str)):
        raise RuntimeError("fair-value manifest schema_version must be an integer")
    try:
        parsed = int(raw_version)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("fair-value manifest schema_version must be an integer") from exc
    if str(parsed) != str(raw_version).strip():
        raise RuntimeError("fair-value manifest schema_version must be an integer")
    if parsed != SUPPORTED_FAIR_VALUE_MANIFEST_SCHEMA_VERSION:
        raise RuntimeError(
            "unsupported fair-value manifest schema_version: "
            f"{parsed} (expected {SUPPORTED_FAIR_VALUE_MANIFEST_SCHEMA_VERSION})"
        )
    return parsed


def _validate_manifest_record_identity(
    market_key: object,
    entry: FairValueManifestEntry,
) -> None:
    if any(
        value not in (None, "")
        for value in (entry.condition_id, entry.event_key, entry.game_id)
    ):
        return
    raise RuntimeError(
        "manifest record must include event identity "
        f"(condition_id, event_key, or game_id) for market key: {market_key}"
    )


def _parse_manifest_record(
    market_key: object,
    item: object,
) -> FairValueManifestEntry:
    if not isinstance(item, dict):
        return FairValueManifestEntry(fair_value=float(item))

    fair_value = _parse_manifest_numeric(
        item.get("fair_value"),
        context=f"manifest fair value missing for market key: {market_key}",
        required=True,
    )
    if fair_value is None:
        raise RuntimeError(
            f"manifest fair value missing for market key: {market_key} is required"
        )

    return FairValueManifestEntry(
        fair_value=fair_value,
        calibrated_fair_value=_parse_manifest_numeric(
            item.get("calibrated_fair_value"),
            context=f"manifest calibrated_fair_value for market key: {market_key}",
        ),
        generated_at=_parse_fair_value_timestamp(item.get("generated_at")),
        source=_optional_text(item.get("source")),
        condition_id=_optional_text(item.get("condition_id")),
        event_key=_optional_text(item.get("event_key")),
        sport=_optional_text(item.get("sport")),
        series=_optional_text(item.get("series")),
        game_id=_optional_text(item.get("game_id")),
        sports_market_type=_optional_text(item.get("sports_market_type")),
    )


def build_fair_value_provider(
    path: str,
    *,
    max_age_seconds: float | None = None,
    fair_value_field: FairValueField = "raw",
) -> ManifestFairValueProvider | StaticFairValueProvider:
    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("fair values file must contain a JSON object")

    manifest_values = payload.get("values")
    if not isinstance(manifest_values, dict):
        return StaticFairValueProvider(
            {str(key): float(value) for key, value in payload.items()}
        )
    schema_version = _parse_manifest_schema_version(payload)

    resolved_max_age = max_age_seconds
    if resolved_max_age is None and payload.get("max_age_seconds") not in (None, ""):
        resolved_max_age = float(payload["max_age_seconds"])

    records = {
        str(market_key): _parse_manifest_record(market_key, item)
        for market_key, item in manifest_values.items()
    }
    if schema_version is not None:
        generated_at = _parse_fair_value_timestamp(payload.get("generated_at"))
        if generated_at is None:
            raise RuntimeError(
                "fair-value manifest generated_at is required when schema_version is set"
            )
        for market_key, entry in records.items():
            _validate_manifest_record_identity(market_key, entry)
    return ManifestFairValueProvider(
        records=records,
        generated_at=_parse_fair_value_timestamp(payload.get("generated_at")),
        source=_optional_text(payload.get("source")),
        max_age_seconds=resolved_max_age,
        fair_value_field=fair_value_field,
    )


class ReloadingFairValueProvider:
    def __init__(
        self,
        loader: Callable[[], FairValueLookup],
        *,
        reload_interval_seconds: float,
    ):
        self.loader = loader
        self.reload_interval_seconds = max(0.0, reload_interval_seconds)
        self._provider = self.loader()
        self._loaded_at = datetime.now(timezone.utc)

    def _refresh_if_due(self) -> None:
        now = datetime.now(timezone.utc)
        age_seconds = (now - self._loaded_at).total_seconds()
        if age_seconds < self.reload_interval_seconds:
            return
        self._provider = self.loader()
        self._loaded_at = now

    def fair_value_for(self, market: object) -> float | None:
        self._refresh_if_due()
        return self._provider.fair_value_for(market)

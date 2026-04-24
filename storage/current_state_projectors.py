from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping


CAPTURE_OWNED_COMPATIBILITY_TABLES = (
    "polymarket_markets",
    "polymarket_bbo",
    "polymarket_orders",
    "polymarket_fills",
    "polymarket_positions",
    "polymarket_balance",
    "sportsbook_events",
    "sportsbook_odds",
    "source_health",
)

CAPTURE_OWNED_SOURCE_HEALTH_NAMES = frozenset(
    {
        "sportsbook_odds",
        "polymarket_market_catalog",
        "polymarket_market_channel",
        "polymarket_user_channel",
        "projection_sportsbook_odds",
        "projection_polymarket_market_catalog",
        "projection_polymarket_market_channel",
        "projection_polymarket_user_channel",
    }
)


@dataclass(frozen=True)
class SourceHealthUpdate:
    source_name: str
    stale_after_ms: int
    status: str
    details: Mapping[str, Any] | None = None
    success: bool = True
    observed_at: datetime | str | None = None


def _payload_dict(row: Any) -> dict[str, Any]:
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    if isinstance(row, Mapping):
        return dict(row)
    raise TypeError("current-state rows must be dataclass instances or mappings")


def _required_str(payload: Mapping[str, Any], *fields: str) -> str:
    for field in fields:
        value = payload.get(field)
        if value not in (None, ""):
            return str(value)
    raise KeyError(f"missing required key field from {fields!r}")


def _details_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _isoformat_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _project_by_key(
    rows: Iterable[Any],
    *,
    key_fields: tuple[str, ...],
    value_builder,
) -> dict[str, dict[str, Any]]:
    projected: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = _payload_dict(row)
        key = _required_str(payload, *key_fields)
        projected[key] = value_builder(payload)
    return projected


def _sportsbook_event_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_json = payload.get("raw_json")
    if isinstance(raw_json, Mapping) and raw_json:
        merged = dict(raw_json)
        for field_name in (
            "sportsbook_event_id",
            "source",
            "sport",
            "league",
            "home_team",
            "away_team",
            "start_time",
            "event_key",
            "series",
            "game_id",
        ):
            if payload.get(field_name) not in (None, ""):
                merged.setdefault(field_name, payload.get(field_name))
        if payload.get("sportsbook_event_id") not in (None, ""):
            merged.setdefault("id", payload.get("sportsbook_event_id"))
        return merged
    return dict(payload)


def project_polymarket_market_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("market_id",),
        value_builder=lambda payload: dict(payload),
    )


def project_polymarket_bbo_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("market_id",),
        value_builder=lambda payload: dict(payload),
    )


def project_sportsbook_event_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("sportsbook_event_id", "id"),
        value_builder=_sportsbook_event_payload,
    )


def sportsbook_quote_current_key(row: Any) -> str:
    payload = _payload_dict(row)
    return "|".join(
        [
            _required_str(payload, "sportsbook_event_id"),
            _required_str(payload, "source"),
            _required_str(payload, "market_type"),
            _required_str(payload, "selection"),
        ]
    )


def project_sportsbook_quote_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    projected: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = _payload_dict(row)
        projected[sportsbook_quote_current_key(payload)] = payload
    return projected


def project_polymarket_order_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("order_id",),
        value_builder=lambda payload: dict(payload),
    )


def project_polymarket_fill_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("fill_id", "fill_key"),
        value_builder=lambda payload: dict(payload),
    )


def project_polymarket_position_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("contract_key",),
        value_builder=lambda payload: dict(payload),
    )


def project_polymarket_balance_state(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    return _project_by_key(
        rows,
        key_fields=("balance_key",),
        value_builder=lambda payload: dict(payload),
    )


def project_source_health_state(
    rows: Iterable[Any],
    *,
    existing: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    projected: dict[str, dict[str, Any]] = {}
    if existing:
        for source_name, row in existing.items():
            payload = _payload_dict(row)
            key = (
                _required_str(payload, "source_name")
                if payload.get("source_name") not in (None, "")
                else str(source_name)
            )
            projected[key] = {
                "source_name": key,
                "last_seen_at": _isoformat_or_none(payload.get("last_seen_at")),
                "last_success_at": _isoformat_or_none(payload.get("last_success_at")),
                "stale_after_ms": int(payload.get("stale_after_ms", 0)),
                "status": str(payload.get("status") or ""),
                "details": _details_dict(payload.get("details")),
            }

    for row in rows:
        payload = _payload_dict(row)
        source_name = _required_str(payload, "source_name")
        if "last_seen_at" in payload:
            projected[source_name] = {
                "source_name": source_name,
                "last_seen_at": _isoformat_or_none(payload.get("last_seen_at")),
                "last_success_at": _isoformat_or_none(payload.get("last_success_at")),
                "stale_after_ms": int(payload.get("stale_after_ms", 0)),
                "status": str(payload.get("status") or ""),
                "details": _details_dict(payload.get("details")),
            }
            continue

        observed_at = _isoformat_or_none(payload.get("observed_at"))
        if observed_at is None:
            raise ValueError("source health updates require observed_at")
        current = projected.get(source_name, {})
        success = bool(payload.get("success", True))
        projected[source_name] = {
            "source_name": source_name,
            "last_seen_at": observed_at,
            "last_success_at": observed_at
            if success
            else current.get("last_success_at"),
            "stale_after_ms": int(payload.get("stale_after_ms", 0)),
            "status": str(payload.get("status") or ""),
            "details": _details_dict(payload.get("details")),
        }
    return projected


def merge_source_health_state(
    rows: Iterable[Any],
    *,
    existing: Mapping[str, Any] | None = None,
    owned_source_names: Iterable[str] = CAPTURE_OWNED_SOURCE_HEALTH_NAMES,
) -> dict[str, dict[str, Any]]:
    owned_names = {str(source_name) for source_name in owned_source_names}
    normalized_existing = project_source_health_state((), existing=existing)
    merged = {
        source_name: dict(payload)
        for source_name, payload in normalized_existing.items()
        if source_name not in owned_names
    }
    merged.update(
        {
            source_name: dict(payload)
            for source_name, payload in project_source_health_state(rows).items()
            if source_name in owned_names
        }
    )
    return merged


__all__ = [
    "CAPTURE_OWNED_COMPATIBILITY_TABLES",
    "CAPTURE_OWNED_SOURCE_HEALTH_NAMES",
    "merge_source_health_state",
    "SourceHealthUpdate",
    "project_polymarket_bbo_state",
    "project_polymarket_balance_state",
    "project_polymarket_fill_state",
    "project_polymarket_market_state",
    "project_polymarket_order_state",
    "project_polymarket_position_state",
    "project_source_health_state",
    "project_sportsbook_event_state",
    "project_sportsbook_quote_state",
    "sportsbook_quote_current_key",
]

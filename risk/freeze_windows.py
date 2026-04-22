from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from adapters import MarketSummary


@dataclass(frozen=True)
class FreezeWindowPolicy:
    freeze_minutes_before_start: int = 10
    freeze_minutes_before_expiry: int = 0
    freeze_when_inactive: bool = True
    freeze_when_resolved: bool = True
    freeze_when_source_unhealthy: bool = True
    unhealthy_source_statuses: tuple[str, ...] = ("error", "red", "down", "stale")


def _coerce_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _source_reason(
    required_sources: Iterable[str],
    *,
    source_health: Mapping[str, Any] | None,
    now: datetime,
    policy: FreezeWindowPolicy,
) -> str | None:
    if not policy.freeze_when_source_unhealthy:
        return None
    health_payload = source_health or {}
    for source_name in required_sources:
        payload = health_payload.get(source_name)
        if not isinstance(payload, dict):
            return f"source health missing for {source_name}"
        status = str(payload.get("status") or "").strip().lower()
        if status in policy.unhealthy_source_statuses:
            return f"source {source_name} unhealthy"
        stale_after_ms = payload.get("stale_after_ms")
        last_success_at = _coerce_datetime(payload.get("last_success_at"))
        if isinstance(stale_after_ms, int) and last_success_at is not None:
            age_ms = int((now - last_success_at).total_seconds() * 1000)
            if age_ms > stale_after_ms:
                return f"source {source_name} stale"
    return None


def freeze_reasons_for_state(
    *,
    policy: FreezeWindowPolicy,
    now: datetime | None = None,
    event_start_time: datetime | None = None,
    market_end_time: datetime | None = None,
    market_active: bool | None = None,
    market_resolved: bool | None = None,
    required_sources: Iterable[str] = (),
    source_health: Mapping[str, Any] | None = None,
) -> list[str]:
    current = now or datetime.now(timezone.utc)
    reasons: list[str] = []
    if event_start_time is not None:
        if event_start_time.tzinfo is None:
            event_start_time = event_start_time.replace(tzinfo=timezone.utc)
        seconds_to_start = (event_start_time - current).total_seconds()
        if seconds_to_start <= policy.freeze_minutes_before_start * 60:
            reasons.append("market within pre-start freeze window")
    if market_end_time is not None and policy.freeze_minutes_before_expiry > 0:
        if market_end_time.tzinfo is None:
            market_end_time = market_end_time.replace(tzinfo=timezone.utc)
        seconds_to_end = (market_end_time - current).total_seconds()
        if seconds_to_end <= policy.freeze_minutes_before_expiry * 60:
            reasons.append("market within pre-expiry freeze window")
    if policy.freeze_when_inactive and market_active is False:
        reasons.append("market inactive")
    if policy.freeze_when_resolved and market_resolved is True:
        reasons.append("market resolved")
    source_reason = _source_reason(
        required_sources,
        source_health=source_health,
        now=current,
        policy=policy,
    )
    if source_reason is not None:
        reasons.append(source_reason)
    return reasons


def freeze_reason_for_market(
    market: MarketSummary,
    *,
    policy: FreezeWindowPolicy,
    now: datetime | None = None,
) -> str | None:
    reasons = freeze_reasons_for_state(
        policy=policy,
        now=now,
        event_start_time=market.start_time,
        market_end_time=market.expires_at,
        market_active=market.active,
        market_resolved=(market.active is False and market.expires_at is not None),
    )
    return reasons[0] if reasons else None

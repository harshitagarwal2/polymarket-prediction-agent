from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from adapters import MarketSummary
from adapters.polymarket.normalize import parse_datetime_value


@dataclass(frozen=True)
class ContractRuleFreezePolicy:
    freeze_before_expiry_seconds: float | None = 3600.0
    freeze_when_closed: bool = True
    freeze_when_inactive: bool = True
    freeze_when_not_accepting_orders: bool = True
    freeze_when_order_book_disabled: bool = True


@dataclass(frozen=True)
class ParsedContractRules:
    active: bool | None = None
    closed: bool | None = None
    accepting_orders: bool | None = None
    order_book_enabled: bool | None = None
    expires_at: datetime | None = None
    description: str | None = None
    resolution_source: str | None = None
    resolved_by: str | None = None


def _payload_bool(payload: dict[str, Any], *keys: str) -> bool | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "yes", "1"}:
                return True
            if normalized in {"false", "no", "0"}:
                return False
        if isinstance(value, (int, float)):
            return bool(value)
    return None


def _payload_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _market_payload(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("market"), dict):
        return raw["market"]
    return raw


def parse_contract_rules(market: MarketSummary) -> ParsedContractRules:
    payload = _market_payload(market.raw)
    if payload is None:
        return ParsedContractRules(expires_at=market.expires_at, active=market.active)

    expires_at = market.expires_at or parse_datetime_value(
        payload.get("endDateIso")
        or payload.get("endDate")
        or payload.get("end_date_iso")
        or payload.get("end_date")
    )

    return ParsedContractRules(
        active=_payload_bool(payload, "active"),
        closed=_payload_bool(payload, "closed"),
        accepting_orders=_payload_bool(
            payload, "acceptingOrders", "accepting_orders"
        ),
        order_book_enabled=_payload_bool(
            payload, "enableOrderBook", "enable_order_book"
        ),
        expires_at=expires_at,
        description=_payload_text(payload, "description", "rules"),
        resolution_source=_payload_text(
            payload, "resolutionSource", "resolution_source"
        ),
        resolved_by=_payload_text(payload, "resolvedBy", "resolved_by"),
    )


def contract_freeze_reasons(
    market: MarketSummary,
    *,
    policy: ContractRuleFreezePolicy,
    now: datetime | None = None,
) -> list[str]:
    now = now or datetime.now(timezone.utc)
    rules = parse_contract_rules(market)
    reasons: list[str] = []

    if policy.freeze_when_closed and rules.closed is True:
        reasons.append("market marked closed in contract rules")
    if policy.freeze_when_inactive and (
        rules.active is False or (rules.active is None and not market.active)
    ):
        reasons.append("market marked inactive in contract rules")
    if policy.freeze_when_not_accepting_orders and rules.accepting_orders is False:
        reasons.append("market not accepting orders in contract rules")
    if policy.freeze_when_order_book_disabled and rules.order_book_enabled is False:
        reasons.append("market order book disabled in contract rules")

    expires_at = rules.expires_at
    if expires_at is not None:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        seconds_to_expiry = (expires_at - now).total_seconds()
        if seconds_to_expiry <= 0:
            reasons.append("market expired in contract rules")
        elif (
            policy.freeze_before_expiry_seconds is not None
            and seconds_to_expiry <= policy.freeze_before_expiry_seconds
        ):
            reasons.append(
                "market within expiry freeze window "
                f"({seconds_to_expiry:.0f}s <= {policy.freeze_before_expiry_seconds:.0f}s)"
            )

    return reasons

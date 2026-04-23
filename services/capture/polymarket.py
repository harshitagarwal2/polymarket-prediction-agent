from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Protocol, Sequence

from adapters.polymarket.market_catalog import PolymarketMarketCatalogClient
from adapters.polymarket.normalizer import normalize_bbo_event, normalize_market_row
from storage.raw import RawStore
from storage.postgres import (
    SourceHealthRepository,
    append_raw_capture_event,
    read_capture_checkpoint,
    upsert_capture_checkpoint,
)


class KeyedRowRepository(Protocol):
    def upsert(self, key: str, row: Any) -> dict[str, Any]: ...

    def read_all(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class PolymarketMarketSnapshotRequest:
    root: str
    sport: str | None = None
    market_type: str | None = None
    limit: int = 500
    stale_after_ms: int = 60_000


@dataclass(frozen=True)
class PolymarketCaptureStores:
    source_health: KeyedRowRepository
    postgres_root: Path
    raw: RawStore | None = None

    @classmethod
    def from_root(cls, root: str | Path) -> PolymarketCaptureStores:
        root_path = Path(root)
        postgres_root = root_path / "postgres"
        return cls(
            source_health=SourceHealthRepository(postgres_root),
            postgres_root=postgres_root,
            raw=RawStore(root_path / "raw"),
        )


def _optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _payload_hash(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _market_token_ids(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    for key in ("clobTokenIds", "clob_token_ids", "tokenIds", "token_ids"):
        values = payload.get(key)
        if isinstance(values, list):
            yes = _optional_text(values[0]) if len(values) >= 1 else None
            no = _optional_text(values[1]) if len(values) >= 2 else None
            return yes, no
    return None, None


def _dsn_optional_failure(exc: RuntimeError) -> bool:
    message = str(exc)
    return "Postgres DSN" in message or "Could not resolve a Postgres DSN" in message


def sanitize_polymarket_capture_error(error: Exception) -> dict[str, str]:
    kind = error.__class__.__name__
    if kind in {"ConnectionClosed", "ConnectionClosedError", "TimeoutError"}:
        message = "polymarket stream disconnected"
    elif kind == "JSONDecodeError":
        message = "polymarket stream payload invalid"
    else:
        message = f"{kind} during polymarket capture"
    return {"kind": kind, "message": message}


def _safe_append_raw_capture_event(
    stores: PolymarketCaptureStores,
    *,
    source: str,
    layer: str,
    entity_type: str,
    operation: str,
    payload: dict[str, Any],
    entity_key: str | None = None,
    captured_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_captured_at = captured_at or datetime.now(timezone.utc)
    try:
        return append_raw_capture_event(
            source=source,
            layer=layer,
            entity_type=entity_type,
            operation=operation,
            payload=payload,
            entity_key=entity_key,
            captured_at=resolved_captured_at,
            metadata=metadata,
            root=stores.postgres_root,
        )
    except RuntimeError as exc:
        if not _dsn_optional_failure(exc):
            raise
        if stores.raw is not None:
            stores.raw.write(source, layer, resolved_captured_at, dict(payload))
        return {
            "source": source,
            "layer": layer,
            "entity_type": entity_type,
            "entity_key": entity_key,
            "operation": operation,
            "payload": dict(payload),
            "metadata": dict(metadata or {}),
            "captured_at": resolved_captured_at.isoformat(),
        }


def _safe_upsert_checkpoint(
    checkpoint_name: str,
    source_name: str,
    checkpoint_value: str | None,
    *,
    checkpoint_ts: str | None,
    metadata: dict[str, Any] | None,
    root: str | Path,
) -> dict[str, Any]:
    try:
        return upsert_capture_checkpoint(
            checkpoint_name,
            source_name,
            checkpoint_value,
            checkpoint_ts=checkpoint_ts,
            metadata=metadata,
            root=root,
        )
    except RuntimeError as exc:
        if not _dsn_optional_failure(exc):
            raise
        return {
            "checkpoint_name": checkpoint_name,
            "source_name": source_name,
            "checkpoint_value": checkpoint_value,
            "checkpoint_ts": checkpoint_ts,
            "metadata": dict(metadata or {}),
        }


def _safe_read_checkpoint(
    checkpoint_name: str,
    source_name: str,
    *,
    root: str | Path,
) -> dict[str, Any] | None:
    try:
        return read_capture_checkpoint(
            checkpoint_name,
            source_name,
            root=root,
        )
    except RuntimeError as exc:
        if not _dsn_optional_failure(exc):
            raise
        return None


def write_polymarket_source_health(
    stores: PolymarketCaptureStores,
    *,
    source_name: str,
    stale_after_ms: int,
    status: str,
    observed_at: datetime,
    details: dict[str, Any] | None = None,
    success: bool = True,
) -> dict[str, Any]:
    now = observed_at.astimezone(timezone.utc).isoformat()
    current = stores.source_health.read_all().get(source_name, {})
    payload = {
        "source_name": source_name,
        "last_seen_at": now,
        "last_success_at": now if success else current.get("last_success_at"),
        "stale_after_ms": stale_after_ms,
        "status": status,
        "details": dict(details or {}),
    }
    return dict(stores.source_health.upsert(source_name, payload))


def record_polymarket_capture_failure(
    stores: PolymarketCaptureStores,
    *,
    source_name: str,
    stale_after_ms: int,
    error: Exception,
    observed_at: datetime,
    checkpoint_name: str | None = None,
    final: bool = False,
) -> dict[str, Any]:
    checkpoint = (
        _safe_read_checkpoint(
            checkpoint_name or source_name,
            source_name,
            root=stores.postgres_root,
        )
        if checkpoint_name is not None
        else None
    )
    sanitized_error = sanitize_polymarket_capture_error(error)
    health = write_polymarket_source_health(
        stores,
        source_name=source_name,
        stale_after_ms=stale_after_ms,
        status="down" if final else "red",
        observed_at=observed_at,
        details={
            "error_kind": sanitized_error["kind"],
            "error_message": sanitized_error["message"],
            "gap_suspected": True,
            "checkpoint": checkpoint,
        },
        success=False,
    )
    return {
        "ok": False,
        "error_kind": sanitized_error["kind"],
        "error_message": sanitized_error["message"],
        "checkpoint": checkpoint,
        "source_health": health,
    }


def hydrate_polymarket_market_snapshot(
    request: PolymarketMarketSnapshotRequest,
    *,
    client: Any | None = None,
    stores: PolymarketCaptureStores | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    capture_time = observed_at or datetime.now(timezone.utc)
    resolved_stores = stores or PolymarketCaptureStores.from_root(request.root)
    catalog_client = client or PolymarketMarketCatalogClient()
    markets = catalog_client.fetch_open_markets()
    if request.sport:
        markets = [
            item
            for item in markets
            if str(item.get("sport") or "").lower() == request.sport.lower()
        ]
    if request.market_type:
        markets = [
            item
            for item in markets
            if request.market_type.lower()
            in str(
                item.get("sports_market_type") or item.get("market_type") or ""
            ).lower()
        ]
    markets = markets[: request.limit]
    _safe_append_raw_capture_event(
        resolved_stores,
        source="polymarket",
        layer="market_catalog",
        entity_type="market_catalog_snapshot",
        entity_key=request.sport or request.market_type or "all",
        operation="snapshot",
        payload={"markets": markets},
        captured_at=capture_time,
        metadata={"market_count": len(markets)},
    )
    rows: list[dict[str, Any]] = []
    for market in markets:
        row = normalize_market_row(market)
        token_id_yes, token_id_no = _market_token_ids(market)
        payload = {**row, "token_id_yes": token_id_yes, "token_id_no": token_id_no}
        rows.append(payload)
    checkpoint_value = capture_time.astimezone(timezone.utc).isoformat()
    checkpoint = _safe_upsert_checkpoint(
        "market_catalog_snapshot",
        "polymarket_market_catalog",
        checkpoint_value,
        checkpoint_ts=checkpoint_value,
        metadata={
            "market_count": len(rows),
            "sport": request.sport,
            "market_type": request.market_type,
        },
        root=resolved_stores.postgres_root,
    )
    health = write_polymarket_source_health(
        resolved_stores,
        source_name="polymarket_market_catalog",
        stale_after_ms=request.stale_after_ms,
        status="ok",
        observed_at=capture_time,
        details={"market_count": len(rows), "checkpoint": checkpoint},
        success=True,
    )
    return {
        "ok": True,
        "market_count": len(rows),
        "rows": rows,
        "checkpoint": checkpoint,
        "source_health": health,
    }


def persist_polymarket_bbo_rows(
    *,
    raw_messages: Sequence[dict[str, Any]],
    normalized_rows: Sequence[dict[str, Any]],
    stores: PolymarketCaptureStores,
    observed_at: datetime,
    stale_after_ms: int,
    source_name: str = "polymarket_market_channel",
    checkpoint_value: str | None = None,
    checkpoint_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    for message in raw_messages:
        _safe_append_raw_capture_event(
            stores,
            source="polymarket",
            layer="market_channel",
            entity_type="market_stream_envelope",
            entity_key=None,
            operation="append",
            payload=message,
            captured_at=observed_at,
            metadata=checkpoint_metadata or {},
        )
    checkpoint_ts = checkpoint_value or observed_at.astimezone(timezone.utc).isoformat()
    checkpoint = _safe_upsert_checkpoint(
        "market_stream",
        source_name,
        checkpoint_value or checkpoint_ts,
        checkpoint_ts=checkpoint_ts,
        metadata=dict(checkpoint_metadata or {}),
        root=stores.postgres_root,
    )
    health = write_polymarket_source_health(
        stores,
        source_name=source_name,
        stale_after_ms=stale_after_ms,
        status="ok",
        observed_at=observed_at,
        details={
            "normalized_row_count": len(normalized_rows),
            "checkpoint": checkpoint,
        },
        success=True,
    )
    return {
        "ok": True,
        "normalized_row_count": len(normalized_rows),
        "normalized_rows": [dict(row) for row in normalized_rows],
        "checkpoint": checkpoint,
        "source_health": health,
    }


def persist_polymarket_bbo_input_events(
    events: Sequence[dict[str, Any]],
    *,
    root: str,
    stores: PolymarketCaptureStores | None = None,
    observed_at: datetime | None = None,
    stale_after_ms: int = 4_000,
) -> dict[str, Any]:
    capture_time = observed_at or datetime.now(timezone.utc)
    resolved_stores = stores or PolymarketCaptureStores.from_root(root)
    normalized_rows: list[dict[str, Any]] = []
    for event in events:
        row = normalize_bbo_event(event)
        row["raw_hash"] = _payload_hash(event)
        normalized_rows.append(row)
    checkpoint_value = max(
        [str(row.get("book_ts") or "") for row in normalized_rows]
        or [capture_time.isoformat()]
    )
    return persist_polymarket_bbo_rows(
        raw_messages=list(events),
        normalized_rows=normalized_rows,
        stores=resolved_stores,
        observed_at=capture_time,
        stale_after_ms=stale_after_ms,
        checkpoint_value=checkpoint_value,
        checkpoint_metadata={"input_mode": True},
    )


def _payload_checkpoint_value(payload: dict[str, Any], observed_at: datetime) -> str:
    for key in (
        "trade_id",
        "tradeId",
        "fill_id",
        "fillId",
        "match_id",
        "matchId",
        "order_id",
        "orderId",
        "id",
        "timestamp",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return observed_at.astimezone(timezone.utc).isoformat()


def persist_polymarket_user_message(
    message: dict[str, Any],
    *,
    stores: PolymarketCaptureStores,
    observed_at: datetime,
    stale_after_ms: int,
    order_payloads: Sequence[dict[str, Any]] = (),
    fill_payloads: Sequence[dict[str, Any]] = (),
) -> dict[str, Any]:
    _safe_append_raw_capture_event(
        stores,
        source="polymarket",
        layer="user_channel",
        entity_type="user_stream_envelope",
        entity_key=None,
        operation="append",
        payload=message,
        captured_at=observed_at,
        metadata={"order_count": len(order_payloads), "fill_count": len(fill_payloads)},
    )
    checkpoint_candidates: list[str] = []
    for payload in order_payloads:
        _safe_append_raw_capture_event(
            stores,
            source="polymarket",
            layer="user_channel",
            entity_type="user_order",
            entity_key=_optional_text(
                payload.get("id") or payload.get("order_id") or payload.get("orderId")
            ),
            operation="append",
            payload=payload,
            captured_at=observed_at,
        )
        checkpoint_candidates.append(_payload_checkpoint_value(payload, observed_at))
    for payload in fill_payloads:
        _safe_append_raw_capture_event(
            stores,
            source="polymarket",
            layer="user_channel",
            entity_type="user_fill",
            entity_key=_optional_text(
                payload.get("trade_id")
                or payload.get("fill_id")
                or payload.get("match_id")
                or payload.get("id")
            ),
            operation="append",
            payload=payload,
            captured_at=observed_at,
        )
        checkpoint_candidates.append(_payload_checkpoint_value(payload, observed_at))
    checkpoint_value = (
        checkpoint_candidates[-1]
        if checkpoint_candidates
        else _payload_checkpoint_value(message, observed_at)
    )
    checkpoint = _safe_upsert_checkpoint(
        "user_stream",
        "polymarket_user_channel",
        checkpoint_value,
        checkpoint_ts=observed_at.astimezone(timezone.utc).isoformat(),
        metadata={"order_count": len(order_payloads), "fill_count": len(fill_payloads)},
        root=stores.postgres_root,
    )
    health = write_polymarket_source_health(
        stores,
        source_name="polymarket_user_channel",
        stale_after_ms=stale_after_ms,
        status="ok",
        observed_at=observed_at,
        details={
            "order_count": len(order_payloads),
            "fill_count": len(fill_payloads),
            "checkpoint": checkpoint,
        },
        success=True,
    )
    return {
        "ok": True,
        "order_count": len(order_payloads),
        "fill_count": len(fill_payloads),
        "checkpoint": checkpoint,
        "source_health": health,
    }

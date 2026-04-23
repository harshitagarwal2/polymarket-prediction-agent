from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Protocol

from adapters.sportsbooks import TheOddsApiClient, normalize_odds_event
from research.data.odds_api import load_event_map
from storage import (
    FileBackedCurrentStateStore,
    ParquetStore,
    RawStore,
    SourceHealthUpdate,
    SourceHealthStore,
    SportsbookEventRecord,
    SportsbookEventRepository,
    SportsbookOddsRecord,
    SportsbookOddsRepository,
    materialize_source_health_state,
    materialize_sportsbook_event_state,
    materialize_sportsbook_quote_state,
    project_source_health_state,
)
from storage.postgres import (
    SourceHealthRepository,
    append_raw_capture_event,
    upsert_capture_checkpoint,
)


def _row_payload(row: Any) -> dict[str, Any]:
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    if isinstance(row, dict):
        return dict(row)
    raise TypeError("row must be a dataclass instance or dict")


class _JsonKeyedRepository:
    def __init__(self, path: Path) -> None:
        self.path = path

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        existing[str(key)] = payload
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(existing, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return payload

    def read_all(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))


class _JsonAppendRepository(_JsonKeyedRepository):
    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        existing[str(len(existing))] = payload
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(existing, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return payload


def sanitize_capture_error(error: Exception) -> dict[str, str]:
    kind = error.__class__.__name__
    if isinstance(error, FileNotFoundError):
        message = "event map file could not be read"
    elif isinstance(error, PermissionError):
        message = "capture file access denied"
    elif kind in {"HTTPStatusError", "ConnectError", "ReadTimeout", "URLError"}:
        message = "sportsbook fetch failed"
    else:
        message = f"{kind} during sportsbook capture"
    return {"kind": kind, "message": message}


class SportsbookCaptureSource(Protocol):
    provider_name: str

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]: ...


class KeyedRowRepository(Protocol):
    def upsert(self, key: str, row: Any) -> dict[str, Any]: ...

    def read_all(self) -> dict[str, Any]: ...


class AppendRowRepository(Protocol):
    def append(self, row: Any) -> dict[str, Any]: ...


class TheOddsApiCaptureSource:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        client: TheOddsApiClient | None = None,
    ) -> None:
        self.provider_name = "theoddsapi"
        if client is None:
            if api_key in (None, ""):
                raise ValueError("api_key is required when client is not provided")
            client = TheOddsApiClient(api_key=api_key)
        self._client = client

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]:
        return self._client.fetch_upcoming(sport, market_type)


@dataclass(frozen=True)
class SportsbookCaptureRequest:
    root: str
    sport: str
    market: str
    event_map_file: str | None = None
    stale_after_ms: int = 60_000


@dataclass(frozen=True)
class SportsbookCaptureStores:
    raw: RawStore
    parquet: ParquetStore
    current: FileBackedCurrentStateStore
    sportsbook_events: KeyedRowRepository
    sportsbook_odds: AppendRowRepository
    current_health: SourceHealthStore
    postgres_health: KeyedRowRepository

    @classmethod
    def from_root(
        cls,
        root: str | Path,
        *,
        require_postgres: bool = False,
    ) -> SportsbookCaptureStores:
        root_path = Path(root)
        postgres_root = root_path / "postgres"
        try:
            sportsbook_events = SportsbookEventRepository(postgres_root)
            sportsbook_odds = SportsbookOddsRepository(postgres_root)
            postgres_health = SourceHealthRepository(postgres_root)
        except RuntimeError as exc:
            if "Could not resolve a Postgres DSN" not in str(exc):
                raise
            if require_postgres:
                raise
            sportsbook_events = _JsonKeyedRepository(
                postgres_root / "sportsbook_events.json"
            )
            sportsbook_odds = _JsonAppendRepository(
                postgres_root / "sportsbook_odds.json"
            )
            postgres_health = _JsonKeyedRepository(postgres_root / "source_health.json")
        return cls(
            raw=RawStore(root_path / "raw"),
            parquet=ParquetStore(root_path / "parquet"),
            current=FileBackedCurrentStateStore(root_path / "current"),
            sportsbook_events=sportsbook_events,
            sportsbook_odds=sportsbook_odds,
            current_health=SourceHealthStore(
                root_path / "current" / "source_health.json"
            ),
            postgres_health=postgres_health,
        )


def _dsn_optional_failure(exc: RuntimeError) -> bool:
    message = str(exc)
    return "Postgres DSN" in message or "Could not resolve a Postgres DSN" in message


def _safe_append_raw_capture_event(
    stores: SportsbookCaptureStores,
    *,
    source_name: str,
    layer: str,
    entity_type: str,
    payload: dict[str, Any],
    entity_key: str | None,
    captured_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return append_raw_capture_event(
            source=source_name,
            layer=layer,
            entity_type=entity_type,
            entity_key=entity_key,
            operation="append",
            payload=payload,
            captured_at=captured_at,
            metadata=metadata,
            root=stores.current.root.parent / "postgres",
        )
    except RuntimeError as exc:
        if not _dsn_optional_failure(exc):
            raise
        stores.raw.write(source_name, layer, captured_at, payload)
        return {
            "source": source_name,
            "layer": layer,
            "entity_type": entity_type,
            "entity_key": entity_key,
            "operation": "append",
            "payload": dict(payload),
            "metadata": dict(metadata or {}),
            "captured_at": captured_at.isoformat(),
        }


def _safe_upsert_checkpoint(
    stores: SportsbookCaptureStores,
    *,
    checkpoint_name: str,
    source_name: str,
    checkpoint_value: str | None,
    checkpoint_ts: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return upsert_capture_checkpoint(
            checkpoint_name,
            source_name,
            checkpoint_value,
            checkpoint_ts=checkpoint_ts,
            metadata=metadata,
            root=stores.current.root.parent / "postgres",
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


def _write_source_health(
    stores: SportsbookCaptureStores,
    *,
    stale_after_ms: int,
    status: str,
    details: dict[str, Any],
    success: bool,
    observed_at: datetime,
    materialize_current: bool = True,
) -> dict[str, Any]:
    update = SourceHealthUpdate(
        source_name="sportsbook_odds",
        stale_after_ms=stale_after_ms,
        status=status,
        details=details,
        success=success,
        observed_at=observed_at,
    )
    if materialize_current:
        projected = materialize_source_health_state(stores.current_health, [update])
        record = projected["sportsbook_odds"]
    else:
        read_all = getattr(stores.postgres_health, "read_all", None)
        existing_rows = read_all() if callable(read_all) else {}
        existing = (
            existing_rows.get("sportsbook_odds", {})
            if isinstance(existing_rows, dict)
            else {}
        )
        projected = project_source_health_state(
            [update], existing={"sportsbook_odds": existing}
        )
        record = projected["sportsbook_odds"]
    stores.postgres_health.upsert("sportsbook_odds", record)
    return dict(record)


def record_sportsbook_capture_failure(
    stores: SportsbookCaptureStores,
    request: SportsbookCaptureRequest,
    source: SportsbookCaptureSource,
    *,
    error: Exception,
    observed_at: datetime | None = None,
    materialize_current: bool = True,
) -> dict[str, Any]:
    capture_time = observed_at or datetime.now(timezone.utc)
    sanitized_error = sanitize_capture_error(error)
    health = _write_source_health(
        stores,
        stale_after_ms=request.stale_after_ms,
        status="red",
        details={
            "provider": source.provider_name,
            "sport": request.sport,
            "market": request.market,
            "error_kind": sanitized_error["kind"],
            "error_message": sanitized_error["message"],
        },
        success=False,
        observed_at=capture_time,
        materialize_current=materialize_current,
    )
    return {
        "ok": False,
        "error_kind": sanitized_error["kind"],
        "error_message": sanitized_error["message"],
        "provider": source.provider_name,
        "sport": request.sport,
        "market": request.market,
        "root": request.root,
        "source_health": health,
    }


def capture_sportsbook_odds_once(
    request: SportsbookCaptureRequest,
    *,
    source: SportsbookCaptureSource,
    stores: SportsbookCaptureStores | None = None,
    observed_at: datetime | None = None,
    materialize_current: bool = True,
) -> dict[str, object]:
    resolved_stores = stores or SportsbookCaptureStores.from_root(request.root)
    capture_time = observed_at or datetime.now(timezone.utc)
    events = source.fetch_upcoming(request.sport, request.market)
    event_map = load_event_map(request.event_map_file)

    current_event_rows: list[SportsbookEventRecord] = []
    current_quote_rows: list[SportsbookOddsRecord] = []
    normalized_rows: list[dict[str, Any]] = []

    for event in events:
        event_payload = dict(event)
        event_payload["sport_key"] = request.sport
        event_identity = event_map.get(str(event_payload.get("id") or ""), {})
        normalized_rows.extend(
            normalize_odds_event(
                event_payload,
                source=source.provider_name,
                market_type=request.market,
                captured_at=capture_time,
            )
        )
        resolved_stores.raw.write("sportsbook", "odds", capture_time, event_payload)

        enriched_event_payload = dict(event_payload)
        for field in ("event_key", "game_id", "sport", "series"):
            if event_identity.get(field) not in (None, ""):
                enriched_event_payload[field] = event_identity[field]

        _safe_append_raw_capture_event(
            resolved_stores,
            source_name="sportsbook",
            layer="odds_api",
            entity_type="sportsbook_odds_envelope",
            entity_key=str(event_payload.get("id") or "") or None,
            payload=enriched_event_payload,
            captured_at=capture_time,
            metadata={
                "provider": source.provider_name,
                "sport": request.sport,
                "market": request.market,
            },
        )

        event_record = SportsbookEventRecord(
            sportsbook_event_id=str(event_payload.get("id") or ""),
            source=source.provider_name,
            sport=request.sport,
            league=(
                str(event_payload.get("sport_title"))
                if event_payload.get("sport_title") not in (None, "")
                else None
            ),
            home_team=(
                str(event_payload.get("home_team"))
                if event_payload.get("home_team") not in (None, "")
                else None
            ),
            away_team=(
                str(event_payload.get("away_team"))
                if event_payload.get("away_team") not in (None, "")
                else None
            ),
            start_time=str(event_payload.get("commence_time") or ""),
            raw_json=enriched_event_payload,
        )
        resolved_stores.sportsbook_events.upsert(
            event_record.sportsbook_event_id,
            event_record,
        )
        current_event_rows.append(event_record)

    for row in normalized_rows:
        record = SportsbookOddsRecord(
            sportsbook_event_id=str(row["sportsbook_event_id"]),
            source=str(row["source"]),
            market_type=str(row["market_type"]),
            selection=str(row["selection"]),
            price_decimal=(
                float(row["price_decimal"])
                if row.get("price_decimal") not in (None, "")
                else None
            ),
            implied_prob=(
                float(row["implied_prob"])
                if row.get("implied_prob") not in (None, "")
                else None
            ),
            overround=(
                float(row["overround"])
                if row.get("overround") not in (None, "")
                else None
            ),
            quote_ts=str(row["quote_ts"]),
            source_age_ms=int(row["source_age_ms"]),
            raw_json=dict(row["raw_json"]),
            provider=(
                str(row.get("provider"))
                if row.get("provider") not in (None, "")
                else source.provider_name
            ),
            source_ts=(
                str(row.get("source_ts"))
                if row.get("source_ts") not in (None, "")
                else None
            ),
            capture_ts=(
                str(row.get("capture_ts"))
                if row.get("capture_ts") not in (None, "")
                else None
            ),
        )
        resolved_stores.sportsbook_odds.append(record)
        current_quote_rows.append(record)

    latest_source_ts = max(
        [
            str(row.get("source_ts") or "")
            for row in normalized_rows
            if row.get("source_ts")
        ],
        default=capture_time.isoformat(),
    )
    checkpoint = _safe_upsert_checkpoint(
        resolved_stores,
        checkpoint_name="sportsbook_odds",
        source_name=source.provider_name,
        checkpoint_value=latest_source_ts,
        checkpoint_ts=latest_source_ts,
        metadata={
            "provider": source.provider_name,
            "sport": request.sport,
            "market": request.market,
            "event_count": len(events),
            "row_count": len(normalized_rows),
        },
    )

    if materialize_current:
        materialize_sportsbook_event_state(resolved_stores.current, current_event_rows)
        materialize_sportsbook_quote_state(resolved_stores.current, current_quote_rows)
    resolved_stores.parquet.append_records(
        "odds_snapshots", capture_time, normalized_rows
    )

    health = _write_source_health(
        resolved_stores,
        stale_after_ms=request.stale_after_ms,
        status="ok",
        details={
            "provider": source.provider_name,
            "sport": request.sport,
            "market": request.market,
            "event_count": len(events),
            "row_count": len(normalized_rows),
            "checkpoint": checkpoint,
        },
        success=True,
        observed_at=capture_time,
        materialize_current=materialize_current,
    )

    return {
        "ok": True,
        "provider": source.provider_name,
        "sport": request.sport,
        "market": request.market,
        "event_count": len(events),
        "row_count": len(normalized_rows),
        "root": request.root,
        "checkpoint": checkpoint,
        "source_health": health,
    }

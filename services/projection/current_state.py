from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any, Protocol, Sequence

from adapters.types import (
    deserialize_balance_snapshot,
    deserialize_fill_snapshot,
    deserialize_normalized_order,
    deserialize_position_snapshot,
    serialize_balance_snapshot,
    serialize_fill_snapshot,
    serialize_normalized_order,
    serialize_position_snapshot,
)
from adapters.polymarket.normalizer import normalize_bbo_event, normalize_market_row
from adapters.sportsbooks import normalize_odds_event
from storage import (
    BBORepository,
    CAPTURE_OWNED_COMPATIBILITY_TABLES,
    FileBackedCurrentStateStore,
    MarketRepository,
    PolymarketBalanceRepository,
    PolymarketFillRepository,
    PolymarketOrderRepository,
    PolymarketPositionRepository,
    ProjectedCurrentStateReadAdapter,
    SourceHealthStore,
    SportsbookEventRecord,
    SportsbookEventRepository,
    SportsbookOddsRecord,
    SportsbookOddsRepository,
    materialize_capture_owned_source_health_state,
)
from storage.postgres import (
    SourceHealthRepository,
    list_raw_capture_events,
    read_capture_checkpoint,
    upsert_capture_checkpoint,
)


def _parse_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _market_token_ids(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    for key in ("clobTokenIds", "clob_token_ids", "tokenIds", "token_ids"):
        values = payload.get(key)
        if isinstance(values, list):
            yes = (
                str(values[0]).strip()
                if len(values) >= 1 and values[0] not in (None, "")
                else None
            )
            no = (
                str(values[1]).strip()
                if len(values) >= 2 and values[1] not in (None, "")
                else None
            )
            return yes, no
    return None, None


def _payload_hash(payload: object) -> str:
    return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CurrentProjectionStores:
    root: Path
    current: FileBackedCurrentStateStore
    current_health: SourceHealthStore
    markets: "KeyedProjectionRepository"
    bbo: "KeyedProjectionRepository"
    sportsbook_events: "KeyedProjectionRepository"
    sportsbook_odds: "AppendProjectionRepository"
    source_health: "KeyedProjectionRepository"
    polymarket_orders: "ReplaceableProjectionRepository | None" = None
    polymarket_fills: "ReplaceableProjectionRepository | None" = None
    polymarket_positions: "ReplaceableProjectionRepository | None" = None
    polymarket_balance: "ReplaceableProjectionRepository | None" = None

    @classmethod
    def from_root(cls, root: str | Path) -> "CurrentProjectionStores":
        root_path = Path(root)
        postgres_root = root_path / "postgres"
        return cls(
            root=root_path,
            current=FileBackedCurrentStateStore(root_path / "current"),
            current_health=SourceHealthStore(
                root_path / "current" / "source_health.json"
            ),
            markets=MarketRepository(postgres_root),
            bbo=BBORepository(postgres_root),
            sportsbook_events=SportsbookEventRepository(postgres_root),
            sportsbook_odds=SportsbookOddsRepository(postgres_root),
            source_health=SourceHealthRepository(postgres_root),
            polymarket_orders=PolymarketOrderRepository(postgres_root),
            polymarket_fills=PolymarketFillRepository(postgres_root),
            polymarket_positions=PolymarketPositionRepository(postgres_root),
            polymarket_balance=PolymarketBalanceRepository(postgres_root),
        )

    def read_adapter(self) -> ProjectedCurrentStateReadAdapter:
        return ProjectedCurrentStateReadAdapter(
            opportunities=_NullCurrentRepository(),
            mappings=_NullCurrentRepository(),
            fair_values=_NullCurrentRepository(),
            bbo_rows=self.bbo,
            sportsbook_events=self.sportsbook_events,
            sportsbook_odds=self.sportsbook_odds,
            source_health=self.source_health,
            polymarket_markets=self.markets,
            polymarket_orders=self.polymarket_orders,
            polymarket_fills=self.polymarket_fills,
            polymarket_positions=self.polymarket_positions,
            polymarket_balance=self.polymarket_balance,
        )


class _NullCurrentRepository:
    def read_all(self) -> dict[str, object]:
        return {}

    def read_current(self) -> dict[str, object]:
        return {}


class KeyedProjectionRepository(Protocol):
    def upsert(self, key: str, row: Any) -> dict[str, object]: ...

    def read_all(self) -> dict[str, object]: ...

    def read_current(self) -> dict[str, object]: ...


class AppendProjectionRepository(Protocol):
    def append(self, row: Any) -> dict[str, object]: ...

    def read_all(self) -> dict[str, object]: ...

    def read_current(self) -> dict[str, object]: ...


class ReplaceableProjectionRepository(Protocol):
    def replace_all(self, rows: dict[str, Any]) -> None: ...

    def read_all(self) -> dict[str, object]: ...

    def read_current(self) -> dict[str, object]: ...


PROJECTOR_SOURCE = "current_projection"
SPORTSBOOK_PROJECTION_CHECKPOINT = "projection_sportsbook_odds"
POLYMARKET_MARKET_PROJECTION_CHECKPOINT = "projection_polymarket_market_catalog"
POLYMARKET_BBO_PROJECTION_CHECKPOINT = "projection_polymarket_market_channel"
POLYMARKET_ACCOUNT_PROJECTION_CHECKPOINT = "projection_polymarket_user_channel"


def _checkpoint_capture_id(root: Path, checkpoint_name: str) -> int:
    checkpoint = read_capture_checkpoint(
        checkpoint_name, PROJECTOR_SOURCE, root=root / "postgres"
    )
    if checkpoint is None:
        return 0
    value = checkpoint.get("checkpoint_value")
    return int(str(value)) if value not in (None, "") else 0


def _write_projection_checkpoint(
    root: Path,
    checkpoint_name: str,
    *,
    capture_id: int,
    checkpoint_ts: str | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return upsert_capture_checkpoint(
        checkpoint_name,
        PROJECTOR_SOURCE,
        str(capture_id),
        checkpoint_ts=checkpoint_ts,
        metadata=metadata,
        root=root / "postgres",
    )


def materialize_current_compatibility_tables(
    stores: CurrentProjectionStores,
    tables: Sequence[str] | None = None,
) -> dict[str, dict[str, Any]]:
    adapter = stores.read_adapter()
    table_names = tuple(tables or CAPTURE_OWNED_COMPATIBILITY_TABLES)
    materialized: dict[str, dict[str, Any]] = {}
    for table in table_names:
        payload = adapter.read_table(table)
        if table == "source_health":
            materialized[table] = materialize_capture_owned_source_health_state(
                stores.current_health,
                payload.values(),
            )
        else:
            materialized[table] = payload
            stores.current.write_table(table, payload)
    return materialized


def _project_sportsbook_capture_events(
    events: Sequence[dict[str, Any]],
    stores: CurrentProjectionStores,
) -> tuple[int, int]:
    row_count = 0
    for event in events:
        payload = dict(event.get("payload") or {})
        metadata = dict(event.get("metadata") or {})
        if not payload:
            continue
        provider = str(metadata.get("provider") or "theoddsapi")
        market_type = str(metadata.get("market") or "h2h")
        captured_at = _parse_timestamp(event.get("captured_at")) or datetime.now(
            timezone.utc
        )
        normalized_rows = normalize_odds_event(
            payload,
            source=provider,
            market_type=market_type,
            captured_at=captured_at,
        )
        event_record = SportsbookEventRecord(
            sportsbook_event_id=str(payload.get("id") or ""),
            source=provider,
            sport=str(metadata.get("sport") or payload.get("sport_key") or ""),
            league=str(payload.get("sport_title"))
            if payload.get("sport_title") not in (None, "")
            else None,
            home_team=str(payload.get("home_team"))
            if payload.get("home_team") not in (None, "")
            else None,
            away_team=str(payload.get("away_team"))
            if payload.get("away_team") not in (None, "")
            else None,
            start_time=str(payload.get("commence_time") or ""),
            raw_json=payload,
        )
        if event_record.sportsbook_event_id:
            stores.sportsbook_events.upsert(
                event_record.sportsbook_event_id, event_record
            )
        for row in normalized_rows:
            stores.sportsbook_odds.append(
                SportsbookOddsRecord(
                    sportsbook_event_id=str(row["sportsbook_event_id"]),
                    source=str(row["source"]),
                    market_type=str(row["market_type"]),
                    selection=str(row["selection"]),
                    price_decimal=float(row["price_decimal"])
                    if row.get("price_decimal") not in (None, "")
                    else None,
                    implied_prob=float(row["implied_prob"])
                    if row.get("implied_prob") not in (None, "")
                    else None,
                    overround=float(row["overround"])
                    if row.get("overround") not in (None, "")
                    else None,
                    quote_ts=str(row["quote_ts"]),
                    source_age_ms=int(row["source_age_ms"]),
                    raw_json=dict(row["raw_json"]),
                    provider=str(row.get("provider"))
                    if row.get("provider") not in (None, "")
                    else provider,
                    source_ts=str(row.get("source_ts"))
                    if row.get("source_ts") not in (None, "")
                    else None,
                    capture_ts=str(row.get("capture_ts"))
                    if row.get("capture_ts") not in (None, "")
                    else None,
                )
            )
        row_count += len(normalized_rows)
    return len(events), row_count


def _project_polymarket_market_catalog_events(
    events: Sequence[dict[str, Any]],
    stores: CurrentProjectionStores,
) -> tuple[int, int]:
    row_count = 0
    for event in events:
        payload = dict(event.get("payload") or {})
        markets = payload.get("markets")
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            normalized = normalize_market_row(market)
            token_id_yes, token_id_no = _market_token_ids(market)
            stores.markets.upsert(
                str(normalized.get("market_id") or ""),
                {
                    **normalized,
                    "token_id_yes": token_id_yes,
                    "token_id_no": token_id_no,
                },
            )
            row_count += 1
    return len(events), row_count


def _project_polymarket_bbo_events(
    events: Sequence[dict[str, Any]],
    stores: CurrentProjectionStores,
) -> tuple[int, int]:
    row_count = 0
    for event in events:
        payload = dict(event.get("payload") or {})
        if not payload:
            continue
        normalized = normalize_bbo_event(payload)
        normalized["raw_hash"] = _payload_hash(payload)
        market_id = str(normalized.get("market_id") or "")
        if not market_id:
            continue
        stores.bbo.upsert(market_id, normalized)
        row_count += 1
    return len(events), row_count


def _project_polymarket_account_snapshot_events(
    events: Sequence[dict[str, Any]],
    stores: CurrentProjectionStores,
) -> tuple[int, int]:
    latest_snapshot: dict[str, Any] | None = None
    latest_seen_at: str | None = None
    latest_capture_id: int | None = None
    for event in events:
        payload = dict(event.get("payload") or {})
        if not payload:
            continue
        observed_at = str(payload.get("observed_at") or "")
        if latest_snapshot is None or observed_at >= (latest_seen_at or ""):
            latest_snapshot = payload
            latest_seen_at = observed_at
            latest_capture_id = int(event.get("capture_id") or 0)
    if latest_snapshot is None:
        if events:
            raise RuntimeError(
                "projected account snapshot missing from user-channel batch"
            )
        return len(events), 0
    if not bool(latest_snapshot.get("complete", True)):
        issues = latest_snapshot.get("issues", [])
        issue_text = (
            ", ".join(str(item) for item in issues)
            if isinstance(issues, list)
            else str(issues)
        )
        raise RuntimeError(
            "projected account snapshot incomplete"
            + (f": {issue_text}" if issue_text else "")
        )
    if (
        stores.polymarket_orders is None
        or stores.polymarket_fills is None
        or stores.polymarket_positions is None
        or stores.polymarket_balance is None
    ):
        raise RuntimeError("projected account-truth repositories are not configured")

    snapshot_observed_at = str(
        latest_snapshot.get("observed_at") or latest_seen_at or ""
    )
    snapshot_cohort_id = (
        f"user_account_snapshot:{latest_capture_id or snapshot_observed_at}"
    )

    order_rows: dict[str, Any] = {}
    for payload in latest_snapshot.get("open_orders", []):
        if not isinstance(payload, dict):
            continue
        order = deserialize_normalized_order(payload)
        serialized = serialize_normalized_order(order)
        serialized["contract_key"] = order.contract.market_key
        serialized["snapshot_cohort_id"] = snapshot_cohort_id
        serialized["snapshot_observed_at"] = snapshot_observed_at
        order_rows[order.order_id] = serialized
    stores.polymarket_orders.replace_all(order_rows)

    fill_rows: dict[str, Any] = {}
    for payload in latest_snapshot.get("fills", []):
        if not isinstance(payload, dict):
            continue
        fill = deserialize_fill_snapshot(payload)
        serialized = serialize_fill_snapshot(fill)
        serialized["fill_key"] = fill.fill_key
        serialized["contract_key"] = fill.contract.market_key
        serialized["snapshot_cohort_id"] = snapshot_cohort_id
        serialized["snapshot_observed_at"] = snapshot_observed_at
        fill_rows[fill.fill_key] = serialized
    stores.polymarket_fills.replace_all(fill_rows)

    position_rows: dict[str, Any] = {}
    for payload in latest_snapshot.get("positions", []):
        if not isinstance(payload, dict):
            continue
        position = deserialize_position_snapshot(payload)
        serialized = serialize_position_snapshot(position)
        serialized["contract_key"] = position.contract.market_key
        serialized["snapshot_cohort_id"] = snapshot_cohort_id
        serialized["snapshot_observed_at"] = snapshot_observed_at
        position_rows[position.contract.market_key] = serialized
    stores.polymarket_positions.replace_all(position_rows)

    balance_payload = latest_snapshot.get("balance")
    balance_rows: dict[str, Any] = {}
    if isinstance(balance_payload, dict):
        balance = deserialize_balance_snapshot(balance_payload)
        serialized = serialize_balance_snapshot(balance)
        balance_key = f"{balance.venue.value}:{balance.currency}"
        serialized["balance_key"] = balance_key
        serialized["snapshot_cohort_id"] = snapshot_cohort_id
        serialized["snapshot_observed_at"] = snapshot_observed_at
        balance_rows[balance_key] = serialized
    stores.polymarket_balance.replace_all(balance_rows)

    row_count = (
        len(order_rows) + len(fill_rows) + len(position_rows) + len(balance_rows)
    )
    return len(events), row_count


def _project_lane(
    stores: CurrentProjectionStores,
    *,
    checkpoint_name: str,
    source: str,
    layer: str,
    entity_types: Sequence[str],
    processor,
    materialize_tables: Sequence[str],
    max_events: int,
    require_matching_event: bool = False,
) -> dict[str, Any]:
    after_capture_id = _checkpoint_capture_id(stores.root, checkpoint_name)
    events = list_raw_capture_events(
        source=source,
        layer=layer,
        after_capture_id=after_capture_id,
        limit=max_events,
        root=stores.root / "postgres",
    )
    events = [
        event
        for event in events
        if str(event.get("source") or "") == source
        and str(event.get("layer") or "") == layer
    ]
    projected_events = [
        event for event in events if str(event.get("entity_type") or "") in entity_types
    ]
    checkpoint_metadata = {
        "source": source,
        "layer": layer,
        "event_count": 0,
        "row_count": 0,
    }
    try:
        if require_matching_event and events and not projected_events:
            raise RuntimeError(
                f"projection lane {checkpoint_name} received source events without a matching entity"
            )
        if (
            require_matching_event
            and projected_events
            and int(projected_events[-1]["capture_id"]) != int(events[-1]["capture_id"])
        ):
            raise RuntimeError(
                f"projection lane {checkpoint_name} received newer non-matching events after the latest snapshot"
            )
        event_count, row_count = processor(projected_events, stores)
        checkpoint_capture_id: int | None = None
        checkpoint_ts: str | None = None
        checkpoint_metadata = {
            "source": source,
            "layer": layer,
            "event_count": event_count,
            "row_count": row_count,
        }
        if projected_events and require_matching_event:
            last_event = projected_events[-1]
            checkpoint_capture_id = int(last_event["capture_id"])
            checkpoint_ts = str(last_event.get("captured_at") or "")
        elif events:
            last_event = events[-1]
            checkpoint_capture_id = int(last_event["capture_id"])
            checkpoint_ts = str(last_event.get("captured_at") or "")
        materialized = materialize_current_compatibility_tables(
            stores, materialize_tables
        )
        if checkpoint_capture_id is not None:
            checkpoint = _write_projection_checkpoint(
                stores.root,
                checkpoint_name,
                capture_id=checkpoint_capture_id,
                checkpoint_ts=checkpoint_ts,
                metadata=checkpoint_metadata,
            )
        else:
            checkpoint = None
        stores.source_health.upsert(
            checkpoint_name,
            {
                "source_name": checkpoint_name,
                "last_seen_at": datetime.now(timezone.utc).isoformat(),
                "last_success_at": datetime.now(timezone.utc).isoformat(),
                "stale_after_ms": 60_000,
                "status": "ok",
                "details": {
                    **checkpoint_metadata,
                    "checkpoint": checkpoint,
                },
            },
        )
    except Exception as exc:
        stores.source_health.upsert(
            checkpoint_name,
            {
                "source_name": checkpoint_name,
                "last_seen_at": datetime.now(timezone.utc).isoformat(),
                "last_success_at": None,
                "stale_after_ms": 60_000,
                "status": "red",
                "details": {
                    **checkpoint_metadata,
                    "checkpoint": None,
                    "error_kind": exc.__class__.__name__,
                },
            },
        )
        materialize_capture_owned_source_health_state(
            stores.current_health,
            stores.source_health.read_current().values(),
        )
        raise
    if "source_health" in materialized:
        materialized["source_health"] = materialize_capture_owned_source_health_state(
            stores.current_health,
            stores.source_health.read_current().values(),
        )
    return {
        "checkpoint_name": checkpoint_name,
        "source": source,
        "layer": layer,
        "event_count": event_count,
        "row_count": row_count,
        "checkpoint": checkpoint,
        "materialized_tables": {
            table: len(materialized[table]) for table in materialize_tables
        },
    }


def project_current_state_once(
    root: str | Path,
    *,
    max_events_per_lane: int = 1000,
) -> dict[str, Any]:
    stores = CurrentProjectionStores.from_root(root)
    sportsbook = _project_lane(
        stores,
        checkpoint_name=SPORTSBOOK_PROJECTION_CHECKPOINT,
        source="sportsbook",
        layer="odds_api",
        entity_types=("sportsbook_odds_envelope",),
        processor=_project_sportsbook_capture_events,
        materialize_tables=("sportsbook_events", "sportsbook_odds", "source_health"),
        max_events=max_events_per_lane,
    )
    polymarket_markets = _project_lane(
        stores,
        checkpoint_name=POLYMARKET_MARKET_PROJECTION_CHECKPOINT,
        source="polymarket",
        layer="market_catalog",
        entity_types=("market_catalog_snapshot",),
        processor=_project_polymarket_market_catalog_events,
        materialize_tables=("polymarket_markets", "source_health"),
        max_events=max_events_per_lane,
    )
    polymarket_bbo = _project_lane(
        stores,
        checkpoint_name=POLYMARKET_BBO_PROJECTION_CHECKPOINT,
        source="polymarket",
        layer="market_channel",
        entity_types=("market_stream_envelope",),
        processor=_project_polymarket_bbo_events,
        materialize_tables=("polymarket_bbo", "source_health"),
        max_events=max_events_per_lane,
    )
    polymarket_account = _project_lane(
        stores,
        checkpoint_name=POLYMARKET_ACCOUNT_PROJECTION_CHECKPOINT,
        source="polymarket",
        layer="user_channel",
        entity_types=("user_account_snapshot",),
        processor=_project_polymarket_account_snapshot_events,
        materialize_tables=(
            "polymarket_orders",
            "polymarket_fills",
            "polymarket_positions",
            "polymarket_balance",
            "source_health",
        ),
        max_events=max_events_per_lane,
        require_matching_event=True,
    )
    return {
        "ok": True,
        "root": str(Path(root)),
        "lanes": {
            "sportsbook": sportsbook,
            "polymarket_markets": polymarket_markets,
            "polymarket_bbo": polymarket_bbo,
            "polymarket_account": polymarket_account,
        },
    }


__all__ = [
    "CAPTURE_OWNED_COMPATIBILITY_TABLES",
    "CurrentProjectionStores",
    "materialize_current_compatibility_tables",
    "project_current_state_once",
    "SPORTSBOOK_PROJECTION_CHECKPOINT",
    "POLYMARKET_MARKET_PROJECTION_CHECKPOINT",
    "POLYMARKET_BBO_PROJECTION_CHECKPOINT",
    "POLYMARKET_ACCOUNT_PROJECTION_CHECKPOINT",
]

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .current_state_projectors import (  # pyright: ignore[reportMissingImports]
    project_polymarket_balance_state,
    project_polymarket_bbo_state,
    project_polymarket_fill_state,
    project_polymarket_market_state,
    project_polymarket_order_state,
    project_polymarket_position_state,
    project_source_health_state,
    project_sportsbook_event_state,
    project_sportsbook_quote_state,
)
from storage.postgres.repositories import (
    BBORepository,
    FairValueRepository,
    MappingRepository,
    MarketRepository,
    OpportunityRepository,
    PolymarketBalanceRepository,
    PolymarketFillRepository,
    PolymarketOrderRepository,
    PolymarketPositionRepository,
    SourceHealthRepository,
    SportsbookEventRepository,
    SportsbookOddsRepository,
)


CURRENT_STATE_TABLE_NAMES = (
    "opportunities",
    "market_mappings",
    "fair_values",
    "polymarket_bbo",
    "polymarket_orders",
    "polymarket_fills",
    "polymarket_positions",
    "polymarket_balance",
    "sportsbook_events",
    "sportsbook_odds",
    "source_health",
    "polymarket_markets",
)


def _coerce_table_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    return payload


def _payload_dict(row: object) -> dict[str, object]:
    if isinstance(row, dict):
        return dict(row)
    return {}


def _read_current_if_available(
    repository: CurrentStateTableRepository,
    *,
    connection: Any | None = None,
) -> dict[str, object] | None:
    read_current = getattr(repository, "read_current", None)
    if not callable(read_current):
        return None
    if connection is not None:
        try:
            return _coerce_table_payload(read_current(connection=connection))
        except TypeError as exc:
            if "connection" not in str(exc):
                raise
    return _coerce_table_payload(read_current())


def _read_all_with_optional_connection(
    repository: CurrentStateTableRepository,
    *,
    connection: Any | None = None,
) -> dict[str, object]:
    read_all = getattr(repository, "read_all")
    if connection is not None:
        try:
            return _coerce_table_payload(read_all(connection=connection))
        except TypeError as exc:
            if "connection" not in str(exc):
                raise
    return _coerce_table_payload(read_all())


def _project_latest_rows(
    rows: dict[str, object],
    *,
    key_builder,
    timestamp_field: str | None = None,
) -> dict[str, object]:
    projected: dict[str, object] = {}
    latest_timestamps: dict[str, str] = {}
    for row in rows.values():
        payload = _payload_dict(row)
        key = key_builder(payload)
        if key in (None, ""):
            continue
        if timestamp_field is None:
            projected[str(key)] = payload
            continue
        timestamp = str(payload.get(timestamp_field) or "")
        if (
            str(key) not in latest_timestamps
            or timestamp >= latest_timestamps[str(key)]
        ):
            latest_timestamps[str(key)] = timestamp
            projected[str(key)] = payload
    return projected


class CurrentStateReadAdapter(Protocol):
    def read_table(
        self, table: str, *, connection: Any | None = None
    ) -> dict[str, object]: ...


class CurrentStateTableRepository(Protocol):
    def read_all(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class FileCurrentStateReadAdapter:
    root: Path

    @classmethod
    def from_opportunity_root(cls, root: str | Path) -> "FileCurrentStateReadAdapter":
        return cls(Path(root) / "current")

    def read_table(
        self, table: str, *, connection: Any | None = None
    ) -> dict[str, object]:
        del connection
        path = self.root / f"{table}.json"
        if not path.exists():
            return {}
        return _coerce_table_payload(json.loads(path.read_text(encoding="utf-8")))


@dataclass(frozen=True)
class ProjectedCurrentStateReadAdapter:
    opportunities: CurrentStateTableRepository
    mappings: CurrentStateTableRepository
    fair_values: CurrentStateTableRepository
    bbo_rows: CurrentStateTableRepository
    sportsbook_events: CurrentStateTableRepository
    source_health: CurrentStateTableRepository
    polymarket_markets: CurrentStateTableRepository
    sportsbook_odds: CurrentStateTableRepository | None = None
    polymarket_orders: CurrentStateTableRepository | None = None
    polymarket_fills: CurrentStateTableRepository | None = None
    polymarket_positions: CurrentStateTableRepository | None = None
    polymarket_balance: CurrentStateTableRepository | None = None

    @classmethod
    def from_root(
        cls, root: str | Path = "runtime/data/postgres"
    ) -> "ProjectedCurrentStateReadAdapter":
        root_path = Path(root)
        postgres_root = (
            root_path if root_path.name == "postgres" else root_path / "postgres"
        )
        return cls(
            opportunities=OpportunityRepository(postgres_root),
            mappings=MappingRepository(postgres_root),
            fair_values=FairValueRepository(postgres_root),
            bbo_rows=BBORepository(postgres_root),
            sportsbook_events=SportsbookEventRepository(postgres_root),
            sportsbook_odds=SportsbookOddsRepository(postgres_root),
            source_health=SourceHealthRepository(postgres_root),
            polymarket_markets=MarketRepository(postgres_root),
            polymarket_orders=PolymarketOrderRepository(postgres_root),
            polymarket_fills=PolymarketFillRepository(postgres_root),
            polymarket_positions=PolymarketPositionRepository(postgres_root),
            polymarket_balance=PolymarketBalanceRepository(postgres_root),
        )

    def read_table(
        self, table: str, *, connection: Any | None = None
    ) -> dict[str, object]:
        if table == "sportsbook_odds":
            if self.sportsbook_odds is None:
                return {}
            current_rows = _read_current_if_available(
                self.sportsbook_odds, connection=connection
            )
            rows = (
                current_rows
                if current_rows is not None
                else _read_all_with_optional_connection(
                    self.sportsbook_odds, connection=connection
                )
            )
            return _coerce_table_payload(project_sportsbook_quote_state(rows.values()))
        if table == "polymarket_markets":
            return _coerce_table_payload(
                project_polymarket_market_state(
                    _read_all_with_optional_connection(
                        self.polymarket_markets, connection=connection
                    ).values()
                )
            )
        if table == "polymarket_bbo":
            return _coerce_table_payload(
                project_polymarket_bbo_state(
                    _read_all_with_optional_connection(
                        self.bbo_rows, connection=connection
                    ).values()
                )
            )
        if table == "sportsbook_events":
            return _coerce_table_payload(
                project_sportsbook_event_state(
                    _read_all_with_optional_connection(
                        self.sportsbook_events, connection=connection
                    ).values()
                )
            )
        if table == "source_health":
            current_rows = _read_current_if_available(
                self.source_health, connection=connection
            )
            rows = (
                current_rows
                if current_rows is not None
                else _read_all_with_optional_connection(
                    self.source_health, connection=connection
                )
            )
            return _coerce_table_payload(project_source_health_state((), existing=rows))
        if table == "polymarket_orders":
            if self.polymarket_orders is None:
                return {}
            current_rows = _read_current_if_available(
                self.polymarket_orders, connection=connection
            )
            rows = (
                current_rows
                if current_rows is not None
                else _read_all_with_optional_connection(
                    self.polymarket_orders, connection=connection
                )
            )
            return _coerce_table_payload(project_polymarket_order_state(rows.values()))
        if table == "polymarket_fills":
            if self.polymarket_fills is None:
                return {}
            current_rows = _read_current_if_available(
                self.polymarket_fills, connection=connection
            )
            rows = (
                current_rows
                if current_rows is not None
                else _read_all_with_optional_connection(
                    self.polymarket_fills, connection=connection
                )
            )
            return _coerce_table_payload(project_polymarket_fill_state(rows.values()))
        if table == "polymarket_positions":
            if self.polymarket_positions is None:
                return {}
            current_rows = _read_current_if_available(
                self.polymarket_positions, connection=connection
            )
            rows = (
                current_rows
                if current_rows is not None
                else _read_all_with_optional_connection(
                    self.polymarket_positions, connection=connection
                )
            )
            return _coerce_table_payload(
                project_polymarket_position_state(rows.values())
            )
        if table == "polymarket_balance":
            if self.polymarket_balance is None:
                return {}
            current_rows = _read_current_if_available(
                self.polymarket_balance, connection=connection
            )
            rows = (
                current_rows
                if current_rows is not None
                else _read_all_with_optional_connection(
                    self.polymarket_balance, connection=connection
                )
            )
            return _coerce_table_payload(
                project_polymarket_balance_state(rows.values())
            )
        if table == "market_mappings":
            current_rows = _read_current_if_available(self.mappings, connection=connection)
            if current_rows is not None:
                return current_rows
            return _project_latest_rows(
                _read_all_with_optional_connection(self.mappings, connection=connection),
                key_builder=lambda payload: "|".join(
                    [
                        str(payload.get("polymarket_market_id") or ""),
                        str(payload.get("sportsbook_event_id") or ""),
                    ]
                ),
            )
        if table == "fair_values":
            current_rows = _read_current_if_available(
                self.fair_values, connection=connection
            )
            if current_rows is not None:
                return current_rows
            return _project_latest_rows(
                _read_all_with_optional_connection(
                    self.fair_values, connection=connection
                ),
                key_builder=lambda payload: str(payload.get("market_id") or ""),
                timestamp_field="as_of",
            )
        if table == "opportunities":
            current_rows = _read_current_if_available(
                self.opportunities, connection=connection
            )
            if current_rows is not None:
                return current_rows
            return _project_latest_rows(
                _read_all_with_optional_connection(
                    self.opportunities, connection=connection
                ),
                key_builder=lambda payload: "|".join(
                    [
                        str(payload.get("market_id") or ""),
                        str(payload.get("side") or ""),
                    ]
                ),
                timestamp_field="as_of",
            )
        raise KeyError(f"unsupported current-state table: {table}")


__all__ = [
    "CURRENT_STATE_TABLE_NAMES",
    "CurrentStateReadAdapter",
    "CurrentStateTableRepository",
    "FileCurrentStateReadAdapter",
    "ProjectedCurrentStateReadAdapter",
]

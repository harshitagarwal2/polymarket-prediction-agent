from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .current_state_projectors import (  # pyright: ignore[reportMissingImports]
    project_polymarket_bbo_state,
    project_polymarket_market_state,
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
    SourceHealthRepository,
    SportsbookEventRepository,
    SportsbookOddsRepository,
)


CURRENT_STATE_TABLE_NAMES = (
    "opportunities",
    "market_mappings",
    "fair_values",
    "polymarket_bbo",
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
) -> dict[str, object] | None:
    read_current = getattr(repository, "read_current", None)
    if not callable(read_current):
        return None
    return _coerce_table_payload(read_current())


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
    def read_table(self, table: str) -> dict[str, object]: ...


class CurrentStateTableRepository(Protocol):
    def read_all(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class FileCurrentStateReadAdapter:
    root: Path

    @classmethod
    def from_opportunity_root(cls, root: str | Path) -> "FileCurrentStateReadAdapter":
        return cls(Path(root) / "current")

    def read_table(self, table: str) -> dict[str, object]:
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

    @classmethod
    def from_root(
        cls, root: str | Path = "runtime/data/postgres"
    ) -> "ProjectedCurrentStateReadAdapter":
        return cls(
            opportunities=OpportunityRepository(root),
            mappings=MappingRepository(root),
            fair_values=FairValueRepository(root),
            bbo_rows=BBORepository(root),
            sportsbook_events=SportsbookEventRepository(root),
            sportsbook_odds=SportsbookOddsRepository(root),
            source_health=SourceHealthRepository(root),
            polymarket_markets=MarketRepository(root),
        )

    def read_table(self, table: str) -> dict[str, object]:
        if table == "sportsbook_odds":
            if self.sportsbook_odds is None:
                return {}
            current_rows = _read_current_if_available(self.sportsbook_odds)
            if current_rows is not None:
                return current_rows
            return _coerce_table_payload(
                project_sportsbook_quote_state(self.sportsbook_odds.read_all().values())
            )
        if table == "polymarket_markets":
            return _coerce_table_payload(
                project_polymarket_market_state(
                    self.polymarket_markets.read_all().values()
                )
            )
        if table == "polymarket_bbo":
            return _coerce_table_payload(
                project_polymarket_bbo_state(self.bbo_rows.read_all().values())
            )
        if table == "sportsbook_events":
            return _coerce_table_payload(
                project_sportsbook_event_state(
                    self.sportsbook_events.read_all().values()
                )
            )
        if table == "source_health":
            current_rows = _read_current_if_available(self.source_health)
            if current_rows is not None:
                return current_rows
            return _coerce_table_payload(self.source_health.read_all())
        if table == "market_mappings":
            current_rows = _read_current_if_available(self.mappings)
            if current_rows is not None:
                return current_rows
            return _project_latest_rows(
                self.mappings.read_all(),
                key_builder=lambda payload: "|".join(
                    [
                        str(payload.get("polymarket_market_id") or ""),
                        str(payload.get("sportsbook_event_id") or ""),
                    ]
                ),
            )
        if table == "fair_values":
            current_rows = _read_current_if_available(self.fair_values)
            if current_rows is not None:
                return current_rows
            return _project_latest_rows(
                self.fair_values.read_all(),
                key_builder=lambda payload: str(payload.get("market_id") or ""),
                timestamp_field="as_of",
            )
        if table == "opportunities":
            current_rows = _read_current_if_available(self.opportunities)
            if current_rows is not None:
                return current_rows
            return _project_latest_rows(
                self.opportunities.read_all(),
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

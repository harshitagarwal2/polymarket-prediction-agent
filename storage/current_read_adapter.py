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
)
from storage.postgres.repositories import (
    BBORepository,
    FairValueRepository,
    MappingRepository,
    MarketRepository,
    OpportunityRepository,
    SourceHealthRepository,
    SportsbookEventRepository,
)


CURRENT_STATE_TABLE_NAMES = (
    "opportunities",
    "market_mappings",
    "fair_values",
    "polymarket_bbo",
    "sportsbook_events",
    "source_health",
    "polymarket_markets",
)


def _coerce_table_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    return payload


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
            source_health=SourceHealthRepository(root),
            polymarket_markets=MarketRepository(root),
        )

    def read_table(self, table: str) -> dict[str, object]:
        repository_by_table = {
            "opportunities": self.opportunities,
            "market_mappings": self.mappings,
            "fair_values": self.fair_values,
            "polymarket_bbo": self.bbo_rows,
            "sportsbook_events": self.sportsbook_events,
            "source_health": self.source_health,
            "polymarket_markets": self.polymarket_markets,
        }
        repository = repository_by_table[table]
        if table == "polymarket_markets":
            return _coerce_table_payload(
                project_polymarket_market_state(repository.read_all().values())
            )
        if table == "polymarket_bbo":
            return _coerce_table_payload(
                project_polymarket_bbo_state(repository.read_all().values())
            )
        if table == "sportsbook_events":
            return _coerce_table_payload(
                project_sportsbook_event_state(repository.read_all().values())
            )
        return _coerce_table_payload(repository.read_all())


__all__ = [
    "CURRENT_STATE_TABLE_NAMES",
    "CurrentStateReadAdapter",
    "CurrentStateTableRepository",
    "FileCurrentStateReadAdapter",
    "ProjectedCurrentStateReadAdapter",
]

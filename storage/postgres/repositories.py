from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


def _row_payload(row: Any) -> dict[str, Any]:
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    if isinstance(row, dict):
        return dict(row)
    raise TypeError("row must be a dataclass instance or dict")


class _JsonRepository:
    table_name = "table"

    def __init__(self, root: str | Path = "runtime/data/postgres") -> None:
        self.root = Path(root)

    @property
    def path(self) -> Path:
        return self.root / f"{self.table_name}.json"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        existing[str(key)] = payload
        self.write_all(existing)
        return payload

    def write_all(self, rows: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8"
        )

    def read_all(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))


class MarketRepository(_JsonRepository):
    table_name = "polymarket_markets"


class BBORepository(_JsonRepository):
    table_name = "polymarket_bbo"


class SportsbookEventRepository(_JsonRepository):
    table_name = "sportsbook_events"


class SportsbookOddsRepository(_JsonRepository):
    table_name = "sportsbook_odds"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        index = str(len(existing))
        existing[index] = payload
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8"
        )
        return payload


class MappingRepository(_JsonRepository):
    table_name = "market_mappings"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        index = str(len(existing))
        existing[index] = payload
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8"
        )
        return payload


class SourceHealthRepository(_JsonRepository):
    table_name = "source_health"


class FairValueRepository(_JsonRepository):
    table_name = "fair_values"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        key = "|".join(
            [
                str(payload["market_id"]),
                str(payload["as_of"]),
                str(payload["model_name"]),
                str(payload["model_version"]),
            ]
        )
        return self.upsert(key, payload)


class OpportunityRepository(_JsonRepository):
    table_name = "opportunities"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        key = "|".join(
            [
                str(payload["market_id"]),
                str(payload["as_of"]),
                str(payload["side"]),
            ]
        )
        return self.upsert(key, payload)


class TradeAttributionRepository(_JsonRepository):
    table_name = "trade_attribution"


class ModelRegistryRepository(_JsonRepository):
    table_name = "model_registry"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        key = "|".join([str(payload["model_name"]), str(payload["model_version"])])
        return self.upsert(key, payload)

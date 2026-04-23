from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from forecasting.fair_value_engine import FairValueProvider
from storage.postgres import ModelRegistryRecord, ModelRegistryRepository


class ModelRegistryWriter(Protocol):
    def append(self, row: Any) -> dict[str, Any]: ...


class _JsonModelRegistryRepository:
    def __init__(self, root: str | Path) -> None:
        self.path = Path(root) / "model_registry.json"

    def append(self, row: Any) -> dict[str, Any]:
        payload = self._row_payload(row)
        existing = self.read_all()
        key = "|".join(
            [
                str(payload.get("model_name") or ""),
                str(payload.get("model_version") or ""),
            ]
        )
        existing[key] = payload
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

    @staticmethod
    def _row_payload(row: Any) -> dict[str, Any]:
        if is_dataclass(row) and not isinstance(row, type):
            return asdict(row)
        if isinstance(row, dict):
            return dict(row)
        raise TypeError("row must be a dataclass instance or dict")


def build_model_registry_repository(root: str | Path) -> ModelRegistryWriter:
    try:
        return ModelRegistryRepository(root)
    except RuntimeError as exc:
        if "Could not resolve a Postgres DSN" not in str(exc):
            raise
        return _JsonModelRegistryRepository(root)


@dataclass(frozen=True)
class ForecastModelSpec:
    name: str
    domain: str
    description: str
    loader: Callable[[], FairValueProvider]


@dataclass
class ForecastModelRegistry:
    specs: dict[str, ForecastModelSpec] = field(default_factory=dict)
    repository: ModelRegistryWriter | None = None

    def register(self, spec: ForecastModelSpec) -> None:
        self.specs[spec.name] = spec

    def resolve(self, name: str) -> ForecastModelSpec:
        if name not in self.specs:
            raise KeyError(f"unknown forecast model: {name}")
        return self.specs[name]

    def load_provider(self, name: str) -> FairValueProvider:
        return self.resolve(name).loader()

    def persist_artifact(
        self,
        *,
        model_name: str,
        model_version: str,
        feature_spec: dict,
        metrics: dict,
        artifact_uri: str,
    ) -> ModelRegistryRecord:
        repository = self.repository or build_model_registry_repository(
            Path("runtime/data/postgres")
        )
        record = ModelRegistryRecord(
            model_name=model_name,
            model_version=model_version,
            created_at=datetime.now(timezone.utc).isoformat(),
            feature_spec=dict(feature_spec),
            metrics=dict(metrics),
            artifact_uri=artifact_uri,
        )
        repository.append(record)
        return record

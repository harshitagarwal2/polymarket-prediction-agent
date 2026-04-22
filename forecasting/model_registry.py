from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from forecasting.fair_value_engine import FairValueProvider
from storage.postgres import ModelRegistryRecord, ModelRegistryRepository


@dataclass(frozen=True)
class ForecastModelSpec:
    name: str
    domain: str
    description: str
    loader: Callable[[], FairValueProvider]


@dataclass
class ForecastModelRegistry:
    specs: dict[str, ForecastModelSpec] = field(default_factory=dict)
    repository: ModelRegistryRepository | None = None

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
        repository = self.repository or ModelRegistryRepository(
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

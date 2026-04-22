from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from forecasting.fair_value_engine import FairValueProvider


@dataclass(frozen=True)
class ForecastModelSpec:
    name: str
    domain: str
    description: str
    loader: Callable[[], FairValueProvider]


@dataclass
class ForecastModelRegistry:
    specs: dict[str, ForecastModelSpec] = field(default_factory=dict)

    def register(self, spec: ForecastModelSpec) -> None:
        self.specs[spec.name] = spec

    def resolve(self, name: str) -> ForecastModelSpec:
        if name not in self.specs:
            raise KeyError(f"unknown forecast model: {name}")
        return self.specs[name]

    def load_provider(self, name: str) -> FairValueProvider:
        return self.resolve(name).loader()

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class AnalysisOutput:
    data: Any | None = None
    metadata: dict[str, Any] | None = None


class Analysis(ABC):
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @abstractmethod
    def run(self) -> AnalysisOutput:
        raise NotImplementedError

    def save_json(self, output_dir: Path | str, payload: dict[str, Any]) -> Path:
        import json

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{self.name}.json"
        output_path.write_text(json.dumps(payload, indent=2, default=str))
        return output_path

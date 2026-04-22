from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ResearchStoragePaths:
    root: Path
    raw_root: Path
    raw_polymarket_root: Path
    raw_sportsbook_root: Path
    processed_root: Path
    processed_training_root: Path
    processed_inference_root: Path
    artifacts_root: Path
    model_artifacts_root: Path
    calibration_artifacts_root: Path

    def create_dirs(self) -> None:
        for path in (
            self.raw_polymarket_root,
            self.raw_sportsbook_root,
            self.processed_training_root,
            self.processed_inference_root,
            self.model_artifacts_root,
            self.calibration_artifacts_root,
        ):
            path.mkdir(parents=True, exist_ok=True)


def build_research_storage_paths(
    root: str | Path = "runtime/data",
) -> ResearchStoragePaths:
    base = Path(root)
    raw_root = base / "raw"
    processed_root = base / "processed"
    artifacts_root = base / "artifacts"
    return ResearchStoragePaths(
        root=base,
        raw_root=raw_root,
        raw_polymarket_root=raw_root / "polymarket",
        raw_sportsbook_root=raw_root / "sportsbook",
        processed_root=processed_root,
        processed_training_root=processed_root / "training",
        processed_inference_root=processed_root / "inference",
        artifacts_root=artifacts_root,
        model_artifacts_root=artifacts_root / "models",
        calibration_artifacts_root=artifacts_root / "calibration",
    )

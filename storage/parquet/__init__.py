from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from storage.parquet_store import ParquetStore

class ParquetStorage:
    """Small optional Parquet writer influenced by upstream chunked market storage."""

    def __init__(self, data_dir: Path | str = "research/data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def append_records(self, name: str, records: list[Any]) -> Path:
        if not records:
            raise ValueError("records must not be empty")
        try:
            import pandas as pd
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("pandas is required for ParquetStorage") from exc

        normalized = []
        fetched_at = datetime.now(timezone.utc)
        for record in records:
            if is_dataclass(record) and not isinstance(record, type):
                payload = asdict(record)
            elif isinstance(record, dict):
                payload = dict(record)
            else:
                payload = {"value": record}
            payload["_fetched_at"] = fetched_at.isoformat()
            normalized.append(payload)

        df = pd.DataFrame(normalized)
        output_path = self.data_dir / f"{name}_{int(fetched_at.timestamp())}.parquet"
        df.to_parquet(output_path, index=False)
        return output_path


class PartitionedParquetStorage(ParquetStorage):
    def append_partitioned_records(
        self,
        *,
        dataset: str,
        partition_key: str,
        records: list[Any],
    ) -> Path:
        if not partition_key:
            raise ValueError("partition_key must not be empty")
        partition_dir = self.data_dir / dataset / partition_key
        partition_dir.mkdir(parents=True, exist_ok=True)
        original = self.data_dir
        try:
            self.data_dir = partition_dir
            return self.append_records(dataset.replace("/", "_"), records)
        finally:
            self.data_dir = original


__all__ = ["ParquetStorage", "PartitionedParquetStorage", "ParquetStore"]

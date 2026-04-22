from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


class ParquetStore:
    def __init__(self, root: str | Path = "runtime/data/parquet") -> None:
        self.root = Path(root)

    def append_records(self, dataset: str, dt: datetime, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        import pandas as pd

        path = (
            self.root
            / dataset
            / f"year={dt:%Y}"
            / f"month={dt:%m}"
            / f"day={dt:%d}"
            / f"hour={dt:%H}"
        )
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / f"{dataset}-{dt:%Y%m%dT%H%M%S}.parquet"
        pd.DataFrame(records).to_parquet(file_path, index=False)

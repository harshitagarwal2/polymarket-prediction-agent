from __future__ import annotations

import json
from pathlib import Path
import tempfile
from typing import Any


class FileBackedCurrentStateStore:
    def __init__(self, root: str | Path = "runtime/data/current") -> None:
        self.root = Path(root)

    def upsert(self, table: str, key: str, payload: dict[str, Any]) -> None:
        table_path = self.root / f"{table}.json"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        existing: dict[str, Any] = {}
        if table_path.exists():
            existing = json.loads(table_path.read_text(encoding="utf-8"))
        existing[str(key)] = payload
        self.write_table(table, existing)

    def write_table(self, table: str, payload: dict[str, Any]) -> None:
        table_path = self.root / f"{table}.json"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            dir=table_path.parent,
            prefix=f"{table_path.name}.",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
        ) as handle:
            handle.write(json.dumps(payload, indent=2, sort_keys=True))
            temp_path = Path(handle.name)
        temp_path.replace(table_path)

    def read_table(self, table: str) -> dict[str, Any]:
        table_path = self.root / f"{table}.json"
        if not table_path.exists():
            return {}
        return json.loads(table_path.read_text(encoding="utf-8"))

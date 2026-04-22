from __future__ import annotations

import json
from pathlib import Path
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
        table_path.write_text(
            json.dumps(existing, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def read_table(self, table: str) -> dict[str, Any]:
        table_path = self.root / f"{table}.json"
        if not table_path.exists():
            return {}
        return json.loads(table_path.read_text(encoding="utf-8"))

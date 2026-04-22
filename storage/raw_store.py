from __future__ import annotations

import gzip
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from storage.journal import normalize_for_json


class RawStore:
    def __init__(self, root: str | Path = "runtime/data/raw") -> None:
        self.root = Path(root)

    def write(
        self,
        source: str,
        stream: str,
        event_ts: datetime,
        payload: dict[str, Any],
    ) -> str:
        ts = event_ts
        path = (
            self.root
            / source
            / stream
            / f"{ts:%Y}"
            / f"{ts:%m}"
            / f"{ts:%d}"
            / f"{ts:%H}"
            / f"{ts:%M}.jsonl.gz"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "at", encoding="utf-8") as handle:
            handle.write(json.dumps(normalize_for_json(payload), sort_keys=True) + "\n")
        return str(path)

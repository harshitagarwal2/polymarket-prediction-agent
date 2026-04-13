from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class PolymarketCaptureEnvelope:
    layer: str
    captured_at: datetime = field(default_factory=_utc_now)
    records: list[dict[str, object]] = field(default_factory=list)

    def to_payload(self) -> dict[str, object]:
        return {
            "layer": self.layer,
            "captured_at": self.captured_at.isoformat(),
            "records": self.records,
        }


def build_polymarket_capture(
    payload: object,
    *,
    layer: str,
    captured_at: datetime | None = None,
) -> PolymarketCaptureEnvelope:
    if isinstance(payload, list):
        records = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        records = [payload]
    else:
        records = []
    return PolymarketCaptureEnvelope(
        layer=layer,
        captured_at=captured_at or _utc_now(),
        records=records,
    )


def write_polymarket_capture(
    envelope: PolymarketCaptureEnvelope, output_path: str | Path
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope.to_payload(), indent=2, sort_keys=True))
    return path

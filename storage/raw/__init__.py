from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from storage.journal import normalize_for_json, write_json
from storage.raw_store import RawStore


@dataclass(frozen=True)
class RawCaptureEnvelope:
    source: str
    layer: str
    captured_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    payload: Any = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "layer": self.layer,
            "captured_at": self.captured_at,
            "payload": normalize_for_json(self.payload),
        }


def build_raw_capture(payload: Any, *, source: str, layer: str) -> RawCaptureEnvelope:
    return RawCaptureEnvelope(source=source, layer=layer, payload=payload)


def write_raw_capture(
    envelope: RawCaptureEnvelope,
    path: str | Path,
) -> Path:
    return write_json(path, envelope.to_payload())


__all__ = ["RawCaptureEnvelope", "RawStore", "build_raw_capture", "write_raw_capture"]

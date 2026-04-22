from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


REQUIRED_FIELDS = (
    "ts",
    "level",
    "trace_id",
    "component",
    "action",
    "market_id",
    "event_id",
    "status",
    "latency_ms",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "trace_id": getattr(record, "trace_id", None),
            "component": getattr(record, "component", record.name),
            "action": getattr(record, "action", None),
            "market_id": getattr(record, "market_id", None),
            "event_id": getattr(record, "event_id", None),
            "status": getattr(record, "status", None),
            "latency_ms": getattr(record, "latency_ms", None),
            "message": record.getMessage(),
        }
        return json.dumps(payload, sort_keys=True)


def build_structured_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def structured_log(
    logger: logging.Logger,
    *,
    action: str,
    status: str,
    message: str,
    trace_id: str | None = None,
    market_id: str | None = None,
    event_id: str | None = None,
    latency_ms: float | None = None,
    level: int = logging.INFO,
) -> None:
    logger.log(
        level,
        message,
        extra={
            "trace_id": trace_id,
            "component": logger.name,
            "action": action,
            "market_id": market_id,
            "event_id": event_id,
            "status": status,
            "latency_ms": latency_ms,
        },
    )

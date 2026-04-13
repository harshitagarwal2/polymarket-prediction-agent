from __future__ import annotations

import importlib
import json
from typing import Any

SPORTS_WS_SUPPORTED = True


def connect_sports_websocket(url: str, *, timeout: float = 10.0):
    try:
        ws_module = importlib.import_module("websockets.sync.client")
        connect = getattr(ws_module, "connect")
    except ImportError as exc:
        raise RuntimeError(
            "websockets is required for sports websocket support"
        ) from exc
    return connect(url, open_timeout=timeout)


def sports_message_payload(message: Any) -> dict[str, Any]:
    if isinstance(message, dict):
        return message
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    if isinstance(message, str):
        payload = json.loads(message)
        if isinstance(payload, dict):
            return payload
    raise ValueError("sports websocket message must decode to an object")


def send_sports_pong(websocket, message: str = "PONG") -> None:
    send = getattr(websocket, "send", None)
    if callable(send):
        send(message)


def describe_boundary() -> dict[str, object]:
    return {
        "supported": SPORTS_WS_SUPPORTED,
        "transport": "websocket",
        "current_path": "channel boundary for sports event streams; ingestion wiring remains caller-owned",
        "required_behaviors": [
            "connect",
            "decode JSON messages",
            "reply to server heartbeat or ping events as required by upstream",
        ],
    }

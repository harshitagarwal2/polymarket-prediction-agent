from __future__ import annotations

import asyncio
import importlib
import json
from typing import Any, Awaitable, Callable


class PolymarketMarketStream:
    def __init__(
        self,
        *,
        host: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    ) -> None:
        self.host = host

    async def run(
        self,
        asset_ids: list[str],
        on_event: Callable[[dict[str, Any]], Awaitable[None] | None],
    ) -> None:
        websockets = importlib.import_module("websockets")
        connect = getattr(websockets, "connect")
        async with connect(self.host) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "type": "market",
                        "assets_ids": list(asset_ids),
                        "initial_dump": True,
                        "level": 2,
                    }
                )
            )
            async for message in websocket:
                payload = json.loads(message)
                result = on_event(payload)
                if asyncio.iscoroutine(result):
                    await result

from __future__ import annotations

from typing import Any

from adapters.polymarket.gamma_client import fetch_markets


class PolymarketMarketCatalogClient:
    def __init__(self, *, host: str | None = None, timeout_seconds: float = 30.0, client: Any | None = None) -> None:
        self.host = host
        self.timeout_seconds = timeout_seconds
        self.client = client

    def fetch_open_markets(self) -> list[dict]:
        markets = fetch_markets(
            host=self.host or "https://gamma-api.polymarket.com",
            timeout_seconds=self.timeout_seconds,
            client=self.client,
        )
        return [market for market in markets if bool(market.get("active", True))]

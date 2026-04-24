from __future__ import annotations

import unittest
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - environment-dependent optional extra
    httpx = None

from adapters.polymarket import (
    PolymarketAdapter,
    PolymarketConfig,
    clob_client,
    gamma_client,
)


class GammaClientTests(unittest.TestCase):
    def test_fetch_markets_uses_httpx_transport_and_filters_objects(self):
        seen: dict[str, object] = {}
        if httpx is None:
            self.skipTest("httpx not installed")
        httpx_module = httpx

        def handler(request: Any) -> Any:
            seen["url"] = str(request.url)
            seen["user_agent"] = request.headers.get("User-Agent")
            return httpx_module.Response(
                200,
                json=[{"question": "A"}, "skip", {"question": "B"}],
            )

        client = httpx_module.Client(transport=httpx_module.MockTransport(handler))
        try:
            markets = gamma_client.fetch_markets(limit=2, client=client)
        finally:
            client.close()

        self.assertEqual(markets, [{"question": "A"}, {"question": "B"}])
        self.assertEqual(
            seen["url"], "https://gamma-api.polymarket.com/markets?limit=2"
        )
        self.assertEqual(seen["user_agent"], "prediction-market-agent/0.1.0")

    def test_list_markets_falls_back_to_public_gamma_when_client_call_raises(self):
        class FailingAdapter(PolymarketAdapter):
            def _call_client(self, operation: str, method_name: str):
                raise RuntimeError(f"{operation}:{method_name}:boom")

        adapter = FailingAdapter(PolymarketConfig(request_timeout_seconds=7.5))

        with patch.object(
            gamma_client,
            "fetch_markets",
            return_value=[
                {
                    "question": "Will Team A win?",
                    "conditionId": "condition-1",
                    "tokens": [
                        {"token_id": "yes-token", "outcome": "Yes", "midpoint": 0.61},
                        {"token_id": "no-token", "outcome": "No", "midpoint": 0.39},
                    ],
                    "active": True,
                }
            ],
        ) as fetch_markets:
            markets = gamma_client.list_markets(adapter, limit=12)

        fetch_markets.assert_called_once_with(limit=12, timeout_seconds=7.5)
        self.assertEqual(
            [market.contract.symbol for market in markets], ["yes-token", "no-token"]
        )


@dataclass
class _Config:
    data_api_host: str = "https://data-api.polymarket.com"
    request_timeout_seconds: float = 2.5
    retry_max_attempts: int = 3
    retry_backoff_seconds: float = 0.0
    retry_backoff_multiplier: float = 1.0
    retry_max_backoff_seconds: float = 0.0


class _Adapter:
    def __init__(self) -> None:
        self.config = _Config()


class ClobClientTests(unittest.TestCase):
    def test_fetch_data_api_uses_httpx_transport_and_omits_none_params(self):
        seen: dict[str, object] = {}
        if httpx is None:
            self.skipTest("httpx not installed")
        httpx_module = httpx

        def handler(request: Any) -> Any:
            seen["url"] = str(request.url)
            seen["user_agent"] = request.headers.get("User-Agent")
            return httpx_module.Response(200, json={"ok": True})

        client = httpx_module.Client(transport=httpx_module.MockTransport(handler))
        try:
            payload = clob_client.fetch_data_api(
                _Adapter(),
                "/positions",
                {"limit": 10, "cursor": None},
                client=client,
            )
        finally:
            client.close()

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(
            seen["url"], "https://data-api.polymarket.com/positions?limit=10"
        )
        self.assertEqual(seen["user_agent"], "prediction-market-agent/0.1.0")


if __name__ == "__main__":
    unittest.main()

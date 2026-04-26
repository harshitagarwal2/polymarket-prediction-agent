from __future__ import annotations

import itertools
import unittest
from unittest.mock import patch

from engine import http_client
from engine.http_client import get_json


class _StubResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _StubClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, follow_redirects=True):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
                "follow_redirects": follow_redirects,
            }
        )
        return _StubResponse(self.payload)


class HttpClientTests(unittest.TestCase):
    def test_get_json_uses_injected_client_without_httpx(self):
        client = _StubClient({"ok": True})

        payload = get_json(
            "https://example.test/feed.json",
            params={"sport": "basketball_nba"},
            headers={"X-Test": "1"},
            timeout_seconds=12,
            client=client,
            follow_redirects=False,
        )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["url"], "https://example.test/feed.json")
        self.assertEqual(client.calls[0]["params"], {"sport": "basketball_nba"})
        self.assertEqual(client.calls[0]["headers"]["X-Test"], "1")
        self.assertFalse(client.calls[0]["follow_redirects"])

    def test_get_json_applies_per_host_min_interval_throttle(self):
        client = _StubClient({"ok": True})
        monotonic_values = iter([0.0, 0.0, 0.1, 0.1, 0.4])
        sleeps: list[float] = []
        http_client._LAST_REQUEST_AT.clear()

        with (
            patch.dict(
                "os.environ",
                {"PREDICTION_MARKET_HTTP_MIN_INTERVAL_SECONDS": "0.25"},
                clear=False,
            ),
            patch.object(
                http_client.time,
                "monotonic",
                side_effect=lambda: next(monotonic_values),
            ),
            patch.object(
                http_client.time,
                "sleep",
                side_effect=lambda value: sleeps.append(value),
            ),
        ):
            get_json("https://example.test/feed.json", client=client, timeout_seconds=1)
            get_json("https://example.test/feed.json", client=client, timeout_seconds=1)

        self.assertEqual(len(client.calls), 2)
        self.assertEqual(sleeps, [0.25])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()

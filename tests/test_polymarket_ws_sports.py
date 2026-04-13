from __future__ import annotations

import unittest

from adapters.polymarket import ws_sports


class FakeWebSocket:
    def __init__(self):
        self.messages: list[str] = []

    def send(self, message: str) -> None:
        self.messages.append(message)


class PolymarketSportsWebSocketTests(unittest.TestCase):
    def test_describe_boundary_reports_current_contract(self):
        payload = ws_sports.describe_boundary()
        self.assertTrue(payload["supported"])
        self.assertEqual(payload["transport"], "websocket")

    def test_sports_message_payload_accepts_json_text_and_bytes(self):
        self.assertEqual(
            ws_sports.sports_message_payload('{"channel": "sports", "type": "ping"}'),
            {"channel": "sports", "type": "ping"},
        )
        self.assertEqual(
            ws_sports.sports_message_payload(b'{"channel": "sports", "type": "pong"}'),
            {"channel": "sports", "type": "pong"},
        )

    def test_send_sports_pong_uses_websocket_send(self):
        websocket = FakeWebSocket()
        ws_sports.send_sports_pong(websocket, message="heartbeat")
        self.assertEqual(websocket.messages, ["heartbeat"])


if __name__ == "__main__":
    unittest.main()

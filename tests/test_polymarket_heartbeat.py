from __future__ import annotations

import time
import unittest
from threading import Event

from adapters.polymarket import HeartbeatStatus, PolymarketAdapter, PolymarketConfig


class FakeHeartbeatClient:
    def __init__(self, effects: list[object]):
        self.effects = list(effects)
        self.calls: list[str | None] = []
        self.called = Event()

    def post_heartbeat(self, heartbeat_id: str | None):
        self.calls.append(heartbeat_id)
        self.called.set()
        effect = self.effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


class StubHeartbeatAdapter(PolymarketAdapter):
    def __init__(self, client: FakeHeartbeatClient):
        super().__init__(
            PolymarketConfig(
                private_key="pk",
                heartbeat_interval_seconds=0.01,
                heartbeat_max_consecutive_failures=2,
                request_timeout_seconds=0.1,
            )
        )
        self._stub_client = client

    def _ensure_client(self):
        return self._stub_client


class PolymarketHeartbeatTests(unittest.TestCase):
    def test_start_and_stop_heartbeat_tracks_success(self):
        client = FakeHeartbeatClient(
            [{"heartbeat_id": "hb-1"}, {"heartbeat_id": "hb-2"}]
        )
        adapter = StubHeartbeatAdapter(client)

        status = adapter.start_heartbeat()

        self.assertTrue(status.active)
        self.assertTrue(status.healthy_for_trading)
        self.assertIsNotNone(status.last_success_at)
        self.assertEqual(client.calls[0], None)
        self.assertTrue(client.called.wait(0.05))
        deadline = time.time() + 0.2
        while len(client.calls) < 2 and time.time() < deadline:
            time.sleep(0.01)

        self.assertGreaterEqual(len(client.calls), 2)
        self.assertEqual(client.calls[1], "hb-1")

        stopped = adapter.stop_heartbeat()

        self.assertFalse(stopped.required)
        self.assertFalse(stopped.active)
        self.assertFalse(stopped.running)

    def test_heartbeat_becomes_unhealthy_after_repeated_failures(self):
        client = FakeHeartbeatClient(
            [RuntimeError("heartbeat failed"), RuntimeError("heartbeat failed")]
        )
        adapter = StubHeartbeatAdapter(client)

        adapter.start_heartbeat()

        deadline = time.time() + 0.2
        status = adapter.heartbeat_status()
        while not status.unhealthy and time.time() < deadline:
            time.sleep(0.01)
            status = adapter.heartbeat_status()

        self.assertTrue(status.unhealthy)
        self.assertFalse(status.healthy_for_trading)
        self.assertIn("heartbeat failed", status.last_error or "")

        adapter.stop_heartbeat()

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
import unittest
from unittest.mock import patch

from scripts import run_agent_loop


class FakeAdapter:
    def __init__(self):
        self.stop_heartbeat_calls = 0
        self.close_calls = 0

    def live_state_status(self):
        return SimpleNamespace(
            active=False,
            running=False,
            initialized=False,
            fresh=False,
            fills_initialized=False,
            fills_fresh=False,
            fills_last_update_at=None,
            cached_fill_count=0,
            last_fills_source="rest",
            last_fills_fallback_reason="fill_cache_cold",
            snapshot_open_order_overlay_count=0,
            snapshot_open_order_overlay_source="rest_only",
            snapshot_open_order_overlay_reason="live_state_inactive",
            snapshot_fill_overlay_count=0,
            snapshot_fill_overlay_source="rest_only",
            snapshot_fill_overlay_reason="fill_cache_cold",
            last_error=None,
            subscribed_markets=(),
        )

    def stop_heartbeat(self):
        self.stop_heartbeat_calls += 1

    def close(self):
        self.close_calls += 1


class RunAgentLoopTests(unittest.TestCase):
    def test_main_wires_lifecycle_manager_by_default(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=False, paused=False)
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_cycle = SimpleNamespace(selected=None)

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "load_fair_values",
                return_value={"token-1:yes": 0.60},
            ),
            patch.object(run_agent_loop, "StaticFairValueProvider"),
            patch.object(
                run_agent_loop,
                "TradingEngine",
                return_value=fake_engine,
            ),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(
                run_agent_loop,
                "PollingAgentLoop",
            ) as polling_loop,
        ):
            polling_loop.return_value.run.return_value = [fake_cycle]

            with patch(
                "sys.argv",
                [
                    "run_agent_loop.py",
                    "--venue",
                    "polymarket",
                    "--fair-values-file",
                    "runtime/fair-values.json",
                ],
            ):
                result = run_agent_loop.main()

        self.assertEqual(result, 0)
        lifecycle_manager = polling_loop.call_args.kwargs["lifecycle_manager"]
        self.assertIsInstance(
            lifecycle_manager,
            run_agent_loop.OrderLifecycleManager,
        )
        self.assertIsInstance(
            lifecycle_manager.policy,
            run_agent_loop.OrderLifecyclePolicy,
        )
        self.assertEqual(adapter.stop_heartbeat_calls, 1)
        self.assertEqual(adapter.close_calls, 1)

    def test_validate_runtime_rejects_missing_fair_values_file(self):
        args = SimpleNamespace(
            venue="polymarket",
            fair_values_file="runtime/missing.json",
            journal="runtime/events.jsonl",
            state_file="runtime/safety-state.json",
        )

        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "fair values file not found"):
                run_agent_loop.validate_runtime(args)

    def test_validate_runtime_rejects_missing_required_env_vars(self):
        args = SimpleNamespace(
            venue="polymarket",
            fair_values_file=__file__,
            journal="runtime/events.jsonl",
            state_file="runtime/safety-state.json",
        )

        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "POLYMARKET_PRIVATE_KEY"):
                run_agent_loop.validate_runtime(args)

    def test_validate_runtime_creates_output_directories(self):
        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            with patch.object(run_agent_loop.Path, "mkdir") as mkdir:
                args = SimpleNamespace(
                    venue="polymarket",
                    fair_values_file=__file__,
                    journal="runtime/events/events.jsonl",
                    state_file="runtime/state/safety-state.json",
                )

                run_agent_loop.validate_runtime(args)

        self.assertEqual(mkdir.call_count, 2)

    def test_build_adapter_parses_polymarket_live_user_markets(self):
        args = SimpleNamespace(
            polymarket_live_user_markets="cond-1, cond-2",
            polymarket_user_ws_host="wss://example.invalid/ws/user",
        )

        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            adapter = run_agent_loop.build_adapter("polymarket", args)

        self.assertIsInstance(adapter, run_agent_loop.PolymarketAdapter)
        config = cast(run_agent_loop.PolymarketConfig, adapter.config)
        self.assertEqual(config.live_user_markets, ["cond-1", "cond-2"])
        self.assertEqual(
            config.user_ws_host,
            "wss://example.invalid/ws/user",
        )

    def test_main_stops_heartbeat_in_finally(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=False, paused=False)
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=True,
            heartbeat_healthy_for_trading=False,
        )
        fake_engine.request_cancel_order = lambda order, reason: None

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "load_fair_values",
                return_value={"token-1:yes": 0.60},
            ),
            patch.object(run_agent_loop, "StaticFairValueProvider"),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
        ):
            polling_loop.return_value.run.side_effect = RuntimeError("loop failure")

            with (
                patch(
                    "sys.argv",
                    [
                        "run_agent_loop.py",
                        "--venue",
                        "polymarket",
                        "--fair-values-file",
                        "runtime/fair-values.json",
                    ],
                ),
                self.assertRaisesRegex(RuntimeError, "loop failure"),
            ):
                run_agent_loop.main()

        self.assertEqual(adapter.stop_heartbeat_calls, 1)
        self.assertEqual(adapter.close_calls, 1)

    def test_main_output_includes_live_state_fields(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=False, paused=False)
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
            pending_cancels=[],
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_cycle = SimpleNamespace(selected=None)

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "load_fair_values",
                return_value={"token-1:yes": 0.60},
            ),
            patch.object(run_agent_loop, "StaticFairValueProvider"),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            patch("sys.stdout") as stdout,
        ):
            polling_loop.return_value.run.return_value = [fake_cycle]

            with patch(
                "sys.argv",
                [
                    "run_agent_loop.py",
                    "--venue",
                    "polymarket",
                    "--fair-values-file",
                    "runtime/fair-values.json",
                ],
            ):
                run_agent_loop.main()

        rendered = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertIn('"live_state_active": false', rendered)
        self.assertIn('"live_fills_fresh": false', rendered)
        self.assertIn('"live_last_fills_source": "rest"', rendered)
        self.assertIn('"snapshot_open_order_overlay_source": "rest_only"', rendered)
        self.assertIn('"snapshot_fill_overlay_source": "rest_only"', rendered)


if __name__ == "__main__":
    unittest.main()

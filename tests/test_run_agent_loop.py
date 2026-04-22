from __future__ import annotations

import io
import json
import tempfile
from types import SimpleNamespace
from typing import cast
import unittest
from unittest.mock import patch

from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from engine.discovery import ManifestFairValueProvider
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
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
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
                    "--quiet",
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

    def test_validate_runtime_rejects_missing_policy_file(self):
        args = SimpleNamespace(
            venue="polymarket",
            fair_values_file=__file__,
            policy_file="runtime/missing-policy.json",
            journal="runtime/events.jsonl",
            state_file="runtime/safety-state.json",
        )

        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "policy file not found"):
                run_agent_loop.validate_runtime(args)

    def test_main_can_apply_runtime_defaults_from_config_file(self):
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
            tempfile.NamedTemporaryFile("w+", suffix=".json") as fair_values,
            tempfile.NamedTemporaryFile("w+", suffix=".yaml") as config_file,
        ):
            json.dump({"token-1:yes": 0.6}, fair_values)
            fair_values.flush()
            config_file.write(
                "runtime:\n"
                "  policy_file: configs/runtime_policy.preview.json\n"
                "  preview_only: false\n"
            )
            config_file.flush()

            with patch.dict(
                "os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False
            ):
                with (
                    patch.object(run_agent_loop, "build_adapter", return_value=adapter),
                    patch.object(
                        run_agent_loop, "validate_runtime"
                    ) as validate_runtime,
                    patch.object(
                        run_agent_loop,
                        "build_fair_value_provider",
                        return_value=SimpleNamespace(),
                    ),
                    patch.object(
                        run_agent_loop,
                        "TradingEngine",
                        return_value=fake_engine,
                    ),
                    patch.object(run_agent_loop, "AgentOrchestrator"),
                    patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
                ):
                    polling_loop.return_value.run.return_value = [fake_cycle]

                    with patch(
                        "sys.argv",
                        [
                            "run_agent_loop.py",
                            "--venue",
                            "polymarket",
                            "--fair-values-file",
                            fair_values.name,
                            "--config-file",
                            config_file.name,
                            "--quiet",
                        ],
                    ):
                        result = run_agent_loop.main()

        self.assertEqual(result, 0)
        validated_args = validate_runtime.call_args.args[0]
        self.assertEqual(
            validated_args.policy_file,
            "configs/runtime_policy.preview.json",
        )
        self.assertEqual(polling_loop.call_args.kwargs["config"].mode, "run")

    def test_build_adapter_parses_polymarket_live_user_markets(self):
        args = SimpleNamespace(
            polymarket_live_user_markets="cond-1, cond-2",
            polymarket_user_ws_host="wss://example.invalid/ws/user",
        )

        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            adapter = run_agent_loop.build_adapter("polymarket", args)

        self.assertIsInstance(adapter, PolymarketAdapter)
        config = cast(PolymarketConfig, adapter.config)
        self.assertEqual(config.live_user_markets, ["cond-1", "cond-2"])
        self.assertEqual(
            config.user_ws_host,
            "wss://example.invalid/ws/user",
        )

    def test_build_adapter_applies_polymarket_depth_admission_policy(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 1,
                    "venues": {
                        "polymarket": {
                            "depth_admission_levels": 4,
                            "depth_admission_liquidity_fraction": 0.65,
                            "depth_admission_max_expected_slippage_bps": 20,
                        }
                    },
                },
                handle,
            )
            handle.flush()
            policy = run_agent_loop.load_runtime_policy(handle.name)

        args = SimpleNamespace(
            polymarket_live_user_markets=None,
            polymarket_user_ws_host=None,
        )
        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            adapter = run_agent_loop.build_adapter("polymarket", args, policy=policy)

        self.assertIsInstance(adapter, PolymarketAdapter)
        config = cast(PolymarketConfig, adapter.config)
        self.assertEqual(config.depth_admission_levels, 4)
        self.assertEqual(config.depth_admission_liquidity_fraction, 0.65)
        self.assertEqual(config.depth_admission_max_expected_slippage_bps, 20.0)

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
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
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
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
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

    def test_main_quiet_suppresses_stdout(self):
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
        stdout = io.StringIO()

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            patch("sys.stdout", stdout),
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
                    "--quiet",
                ],
            ):
                run_agent_loop.main()

        self.assertEqual(stdout.getvalue(), "")

    def test_main_wires_ranker_filters_from_cli(self):
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
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator") as orchestrator_ctor,
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
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
                    "--categories",
                    "sports, nba",
                    "--taker-fee-rate",
                    "0.03",
                    "--min-volume",
                    "2500",
                    "--max-spread",
                    "0.08",
                    "--min-hours-to-expiry",
                    "2",
                    "--max-hours-to-expiry",
                    "24",
                    "--max-fair-value-age-seconds",
                    "900",
                    "--quiet",
                ],
            ):
                run_agent_loop.main()

        ranker = orchestrator_ctor.call_args.kwargs["ranker"]
        pair_ranker = orchestrator_ctor.call_args.kwargs["pair_ranker"]
        self.assertEqual(ranker.allowed_categories, ("sports", "nba"))
        self.assertEqual(ranker.taker_fee_rate, 0.03)
        self.assertEqual(ranker.min_volume, 2500.0)
        self.assertEqual(ranker.max_spread, 0.08)
        self.assertEqual(ranker.min_hours_to_expiry, 2.0)
        self.assertEqual(ranker.max_hours_to_expiry, 24.0)
        self.assertEqual(pair_ranker.allowed_categories, ("sports", "nba"))
        self.assertEqual(pair_ranker.taker_fee_rate, 0.03)
        self.assertEqual(pair_ranker.min_volume, 2500.0)
        self.assertEqual(pair_ranker.max_spread, 0.08)

    def test_main_supports_pair_mode_last_selected_summary(self):
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
        fake_cycle = SimpleNamespace(selected=SimpleNamespace(market_key="event-1"))

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
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
                    "--mode",
                    "pair-preview",
                ],
            ):
                run_agent_loop.main()

        rendered = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertIn('"last_selected": "event-1"', rendered)

    def test_main_passes_quantity_into_polling_config_for_pair_mode(self):
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
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
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
                    "--mode",
                    "pair-run",
                    "--quantity",
                    "2.5",
                    "--quiet",
                ],
            ):
                run_agent_loop.main()

        config = polling_loop.call_args.kwargs["config"]
        self.assertEqual(config.quantity, 2.5)

    def test_main_wraps_provider_with_reloader_when_requested(self):
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
                "build_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
            patch.object(run_agent_loop, "ReloadingFairValueProvider") as reloader_ctor,
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
        ):
            reloader_ctor.return_value = SimpleNamespace()
            polling_loop.return_value.run.return_value = [fake_cycle]

            with patch(
                "sys.argv",
                [
                    "run_agent_loop.py",
                    "--venue",
                    "polymarket",
                    "--fair-values-file",
                    "runtime/fair-values.json",
                    "--fair-values-reload-seconds",
                    "30",
                    "--quiet",
                ],
            ):
                run_agent_loop.main()

        reloader_ctor.assert_called_once()
        self.assertEqual(
            reloader_ctor.call_args.kwargs["reload_interval_seconds"], 30.0
        )

    def test_main_uses_policy_file_for_thresholds_and_manifest_event_registry(self):
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
            tempfile.NamedTemporaryFile("w+", suffix=".json") as fair_values_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as policy_handle,
        ):
            json.dump(
                {
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.61,
                            "event_key": "Event 1",
                        }
                    },
                },
                fair_values_handle,
            )
            fair_values_handle.flush()
            json.dump(
                {
                    "schema_version": 1,
                    "fair_value": {"field": "calibrated"},
                    "strategy": {
                        "base_quantity": 2.5,
                        "edge_threshold": 0.07,
                    },
                    "risk_limits": {
                        "max_contracts_per_market": 4,
                        "max_global_contracts": 9,
                        "max_contracts_per_event": 5,
                    },
                    "opportunity_ranker": {
                        "allowed_categories": ["sports"],
                        "taker_fee_rate": 0.02,
                        "min_volume": 2500,
                    },
                    "pair_opportunity_ranker": {
                        "edge_threshold": 0.04,
                        "allowed_categories": ["sports"],
                    },
                    "execution_policy_gate": {
                        "max_open_orders_global": 3,
                    },
                    "trading_engine": {
                        "overlay_max_age_seconds": 11,
                    },
                    "order_lifecycle_policy": {
                        "max_order_age_seconds": 44,
                    },
                },
                policy_handle,
            )
            policy_handle.flush()

            with (
                patch.object(run_agent_loop, "build_adapter", return_value=adapter),
                patch.object(run_agent_loop, "validate_runtime"),
                patch.object(
                    run_agent_loop,
                    "TradingEngine",
                    return_value=fake_engine,
                ) as trading_engine_ctor,
                patch.object(run_agent_loop, "AgentOrchestrator") as orchestrator_ctor,
                patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            ):
                polling_loop.return_value.run.return_value = [fake_cycle]

                with patch(
                    "sys.argv",
                    [
                        "run_agent_loop.py",
                        "--venue",
                        "polymarket",
                        "--fair-values-file",
                        fair_values_handle.name,
                        "--policy-file",
                        policy_handle.name,
                        "--quantity",
                        "9",
                        "--edge-threshold",
                        "0.99",
                        "--max-contracts-per-market",
                        "99",
                        "--max-global-contracts",
                        "99",
                        "--categories",
                        "politics",
                        "--taker-fee-rate",
                        "0.99",
                        "--min-volume",
                        "1",
                        "--quiet",
                    ],
                ):
                    run_agent_loop.main()

        risk_engine = trading_engine_ctor.call_args.kwargs["risk_engine"]
        strategy = trading_engine_ctor.call_args.kwargs["strategy"]
        ranker = orchestrator_ctor.call_args.kwargs["ranker"]
        pair_ranker = orchestrator_ctor.call_args.kwargs["pair_ranker"]
        policy_gate = orchestrator_ctor.call_args.kwargs["policy_gate"]
        sizer = orchestrator_ctor.call_args.kwargs["sizer"]
        config = polling_loop.call_args.kwargs["config"]
        lifecycle_manager = polling_loop.call_args.kwargs["lifecycle_manager"]
        provider = orchestrator_ctor.call_args.kwargs["fair_value_provider"]

        self.assertEqual(strategy.quantity, 2.5)
        self.assertEqual(strategy.edge_threshold, 0.07)
        self.assertEqual(risk_engine.limits.max_contracts_per_market, 4)
        self.assertEqual(risk_engine.limits.max_global_contracts, 9)
        self.assertEqual(risk_engine.limits.max_contracts_per_event, 5)
        self.assertEqual(
            risk_engine.market_key_to_event_exposure_key["token-1:yes"],
            "event:event-1",
        )
        self.assertEqual(ranker.allowed_categories, ("sports",))
        self.assertEqual(ranker.taker_fee_rate, 0.02)
        self.assertEqual(ranker.min_volume, 2500.0)
        self.assertEqual(pair_ranker.edge_threshold, 0.04)
        self.assertEqual(policy_gate.max_open_orders_global, 3)
        self.assertEqual(sizer.base_quantity, 2.5)
        self.assertEqual(sizer.edge_unit, 0.07)
        self.assertEqual(config.quantity, 2.5)
        self.assertEqual(lifecycle_manager.policy.max_order_age_seconds, 44.0)
        self.assertIsInstance(provider, ManifestFairValueProvider)
        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")
        self.assertEqual(provider.fair_value_field, "calibrated")
        self.assertEqual(
            trading_engine_ctor.call_args.kwargs["overlay_max_age_seconds"],
            11.0,
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import io
import json
import tempfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import cast
import unittest
from unittest.mock import patch

from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from engine.discovery import ManifestFairValueProvider
from risk.kill_switch import KillSwitchState
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


class FakeExecutionLock:
    def __init__(self, name: str, acquired: bool):
        self.name = name
        self._acquired = acquired
        self.acquire_calls = 0
        self.release_calls = 0

    def acquire(self) -> bool:
        self.acquire_calls += 1
        return self._acquired

    def release(self) -> None:
        self.release_calls += 1


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

    def test_main_records_execution_ledger_for_run_mode(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=False, paused=False)
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_cycle = SimpleNamespace(
            selected=SimpleNamespace(contract=SimpleNamespace(market_key="token-1:yes")),
            execution=None,
            policy_allowed=False,
            policy_reasons=[],
            gate_trace=[],
            shadow_quote_plan=None,
        )
        stdout = io.StringIO()

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "_build_runtime_kill_switch",
                return_value=(KillSwitchState(), ()),
            ),
            patch.object(
                run_agent_loop,
                "_build_projected_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
            patch.object(
                run_agent_loop,
                "_build_projected_account_snapshot_provider",
                return_value=lambda contract: None,
            ),
            patch.object(
                run_agent_loop,
                "_build_preview_order_proposals",
                return_value=([], []),
            ),
            patch.object(
                run_agent_loop,
                "TradingEngine",
                return_value=fake_engine,
            ),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(
                run_agent_loop,
                "persist_runtime_execution_ledger",
            ) as record_ledger,
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            patch("sys.stdout", stdout),
        ):
            polling_loop.return_value.run.return_value = [fake_cycle]
            record_ledger.return_value = {
                "cycle_count": 1,
                "decision_count": 0,
                "order_count": 0,
                "fill_count": 2,
            }

            with patch(
                "sys.argv",
                [
                    "run_agent_loop.py",
                    "--venue",
                    "polymarket",
                    "--mode",
                    "run",
                    "--opportunity-root",
                    "runtime/data",
                ],
            ):
                result = run_agent_loop.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        record_ledger.assert_called_once()
        self.assertEqual(payload["ledger_summary"]["cycle_count"], 1)
        self.assertEqual(payload["ledger_summary"]["fill_count"], 2)

    def test_main_fails_closed_when_ledger_persistence_fails(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=False, paused=False)
        )
        halt_reasons: list[str] = []
        fake_engine.halt = lambda reason: halt_reasons.append(reason)
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_cycle = SimpleNamespace(
            selected=SimpleNamespace(contract=SimpleNamespace(market_key="token-1:yes")),
            execution=None,
            policy_allowed=False,
            policy_reasons=[],
            gate_trace=[],
            shadow_quote_plan=None,
        )
        stdout = io.StringIO()

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "_build_runtime_kill_switch",
                return_value=(KillSwitchState(), ()),
            ),
            patch.object(
                run_agent_loop,
                "_build_projected_fair_value_provider",
                return_value=SimpleNamespace(),
            ),
            patch.object(
                run_agent_loop,
                "_build_projected_account_snapshot_provider",
                return_value=lambda contract: None,
            ),
            patch.object(
                run_agent_loop,
                "_build_preview_order_proposals",
                return_value=([], []),
            ),
            patch.object(
                run_agent_loop,
                "TradingEngine",
                return_value=fake_engine,
            ),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(
                run_agent_loop,
                "persist_runtime_execution_ledger",
                side_effect=RuntimeError("ledger write failed"),
            ),
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
                    "--mode",
                    "run",
                    "--opportunity-root",
                    "runtime/data",
                ],
            ):
                result = run_agent_loop.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 1)
        self.assertTrue(halt_reasons)
        self.assertEqual(payload["ledger_summary"]["error_kind"], "RuntimeError")

    def test_validate_runtime_rejects_missing_fair_values_file(self):
        args = SimpleNamespace(
            venue="polymarket",
            fair_values_file="runtime/missing.json",
            mode="preview",
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
            with self.assertRaisesRegex(RuntimeError, "POLYMARKET_PRIVATE_KEY_FILE"):
                run_agent_loop.validate_runtime(args)

    def test_validate_runtime_accepts_polymarket_private_key_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            fair_values = Path(temp_dir) / "fair-values.json"
            fair_values.write_text("{}", encoding="utf-8")
            key_file = Path(temp_dir) / "polymarket.key"
            key_file.write_text("file-private-key", encoding="utf-8")
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=str(fair_values),
                journal=str(journal),
                state_file=str(state),
                policy_file=None,
                mode="preview",
                opportunity_root=None,
            )

            with patch.dict(
                "os.environ",
                {
                    "POLYMARKET_PRIVATE_KEY": "",
                    "POLYMARKET_PRIVATE_KEY_FILE": str(key_file),
                    "POLYMARKET_ROUTE_LABEL": "eu-proxy-1",
                    "POLYMARKET_GEO_COMPLIANCE_ACK": "true",
                },
                clear=True,
            ):
                run_agent_loop.validate_runtime(args)

    def test_validate_runtime_rejects_live_mode_without_geo_routing_ack(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=None,
                policy_file=None,
                journal=str(journal),
                state_file=str(state),
                mode="run",
                opportunity_root=str(Path(temp_dir) / "data"),
            )

            with (
                patch.dict(
                    "os.environ",
                    {"POLYMARKET_PRIVATE_KEY": "pk"},
                    clear=True,
                ),
                patch.object(
                    run_agent_loop,
                    "build_current_state_read_adapter",
                    return_value=object(),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "POLYMARKET_ROUTE_LABEL"):
                    run_agent_loop.validate_runtime(args)

    def test_validate_runtime_rejects_private_order_flow_without_private_host(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal = Path(temp_dir) / "runtime" / "events.jsonl"
            state = Path(temp_dir) / "runtime" / "safety-state.json"
            args = SimpleNamespace(
                venue="polymarket",
                fair_values_file=None,
                policy_file=None,
                journal=str(journal),
                state_file=str(state),
                mode="run",
                opportunity_root=str(Path(temp_dir) / "data"),
            )

            with (
                patch.dict(
                    "os.environ",
                    {
                        "POLYMARKET_PRIVATE_KEY": "pk",
                        "POLYMARKET_ROUTE_LABEL": "eu-proxy-1",
                        "POLYMARKET_GEO_COMPLIANCE_ACK": "true",
                        "POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED": "true",
                        "POLYMARKET_CLOB_HOST": PolymarketConfig.host,
                    },
                    clear=True,
                ),
                patch.object(
                    run_agent_loop,
                    "build_current_state_read_adapter",
                    return_value=object(),
                ),
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "non-default POLYMARKET_CLOB_HOST"
                ):
                    run_agent_loop.validate_runtime(args)

    def test_validate_autonomous_mode_requires_guardrail_contracts(self):
        args = SimpleNamespace(
            autonomous_mode=True,
            mode="run",
            execution_lock_name=None,
            drift_report_file=None,
        )
        with self.assertRaisesRegex(RuntimeError, "--execution-lock-name"):
            run_agent_loop._validate_autonomous_mode(args, None)

    def test_validate_autonomous_mode_accepts_complete_guardrail_contract(self):
        args = SimpleNamespace(
            autonomous_mode=True,
            mode="run",
            execution_lock_name="primary-loop",
            drift_report_file="runtime/data/current/model_drift.json",
        )
        policy = SimpleNamespace(
            trading_engine=SimpleNamespace(
                autonomous_mode=True,
                max_active_wallet_balance=250.0,
            ),
            risk_limits=SimpleNamespace(max_weekly_loss=10.0, max_cumulative_loss=50.0),
        )
        with patch.dict(
            "os.environ",
            {"POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED": "true"},
            clear=False,
        ):
            run_agent_loop._validate_autonomous_mode(args, policy)

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
            tempfile.TemporaryDirectory() as temp_dir,
        ):
            json.dump({"token-1:yes": 0.6}, fair_values)
            fair_values.flush()
            config_file.write(
                "runtime:\n"
                "  policy_file: configs/runtime_policy.preview.json\n"
                "  preview_only: false\n"
                f"  opportunity_root: {Path(temp_dir) / 'data'}\n"
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
                        "_build_projected_fair_value_provider",
                        return_value=ManifestFairValueProvider(records={}),
                    ),
                    patch.object(
                        run_agent_loop,
                        "build_current_state_read_adapter",
                        return_value=SimpleNamespace(read_table=lambda table: {}),
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
        self.assertEqual(
            validated_args.opportunity_root,
            str(Path(temp_dir) / "data"),
        )

    def test_main_can_apply_fair_values_and_opportunity_root_from_config_file(self):
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
            tempfile.TemporaryDirectory() as temp_dir,
        ):
            json.dump(
                {
                    "schema_version": 1,
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.6,
                            "event_key": "event-1",
                        }
                    },
                },
                fair_values,
            )
            fair_values.flush()
            config_file.write(
                "runtime:\n"
                f"  fair_values_file: {fair_values.name}\n"
                f"  opportunity_root: {Path(temp_dir) / 'data'}\n"
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
                    ) as build_provider,
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
                            "--config-file",
                            config_file.name,
                            "--quiet",
                        ],
                    ):
                        result = run_agent_loop.main()

        self.assertEqual(result, 0)
        validated_args = validate_runtime.call_args.args[0]
        self.assertEqual(validated_args.fair_values_file, fair_values.name)
        self.assertEqual(
            validated_args.opportunity_root,
            str(Path(temp_dir) / "data"),
        )
        self.assertEqual(build_provider.call_args.args[0], fair_values.name)

    def test_build_projected_fair_value_provider_uses_current_state_tables(self):
        adapter = SimpleNamespace(
            read_table=lambda table: {
                "fair_values": {
                    "pm-1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                        "fair_yes_prob": 0.61,
                        "calibrated_fair_yes_prob": 0.63,
                    }
                },
                "market_mappings": {
                    "pm-1|sb-1": {
                        "polymarket_market_id": "pm-1",
                        "sportsbook_event_id": "sb-1",
                        "event_key": "event-1",
                        "sport": "nba",
                        "series": "playoffs",
                        "game_id": "game-1",
                        "normalized_market_type": "moneyline_full_game",
                        "match_confidence": 0.98,
                        "resolution_risk": 0.05,
                        "is_active": True,
                    }
                },
                "polymarket_markets": {
                    "pm-1": {
                        "market_id": "pm-1",
                        "raw_json": {
                            "conditionId": "cond-1",
                            "tokenIds": ["token-yes", "token-no"],
                        },
                    }
                },
            }[table]
        )

        with patch.object(
            run_agent_loop,
            "build_current_state_read_adapter",
            return_value=adapter,
        ):
            provider = run_agent_loop._build_projected_fair_value_provider(
                SimpleNamespace(
                    opportunity_root="runtime/data",
                    max_fair_value_age_seconds=900.0,
                ),
                fair_value_field="calibrated",
            )

        self.assertIsInstance(provider, ManifestFairValueProvider)
        if not isinstance(provider, ManifestFairValueProvider):
            self.fail("expected manifest fair value provider")
        self.assertEqual(provider.max_age_seconds, 900.0)
        self.assertEqual(provider.fair_value_field, "calibrated")
        self.assertIn("token-yes:yes", provider.records)
        self.assertIn("token-no:no", provider.records)
        self.assertEqual(provider.records["token-yes:yes"].condition_id, "cond-1")
        self.assertAlmostEqual(provider.records["token-no:no"].fair_value, 0.39)

    def test_build_projected_fair_value_provider_rejects_missing_probability(self):
        adapter = SimpleNamespace(
            read_table=lambda table: {
                "fair_values": {
                    "pm-1": {
                        "market_id": "pm-1",
                        "as_of": "2026-04-21T18:00:00+00:00",
                    }
                },
                "market_mappings": {},
                "polymarket_markets": {
                    "pm-1": {
                        "market_id": "pm-1",
                        "raw_json": {
                            "conditionId": "cond-1",
                            "tokenIds": ["token-yes", "token-no"],
                        },
                    }
                },
            }[table]
        )

        with patch.object(
            run_agent_loop,
            "build_current_state_read_adapter",
            return_value=adapter,
        ):
            with self.assertRaisesRegex(RuntimeError, "missing fair_yes_prob"):
                run_agent_loop._build_projected_fair_value_provider(
                    SimpleNamespace(
                        opportunity_root="runtime/data",
                        max_fair_value_age_seconds=900.0,
                    ),
                    fair_value_field="raw",
                )

    def test_projected_account_snapshot_provider_reads_current_state_tables(self):
        now_iso = datetime.now(timezone.utc).isoformat()
        adapter = SimpleNamespace(
            read_table=lambda table: {
                "source_health": {
                    "polymarket_user_channel": {
                        "status": "ok",
                        "last_success_at": now_iso,
                        "stale_after_ms": 60000,
                        "details": {
                            "account_snapshot": True,
                            "account_snapshot_complete": True,
                            "account_snapshot_issues": [],
                        },
                    },
                    "projection_polymarket_user_channel": {
                        "status": "ok",
                        "last_success_at": now_iso,
                        "stale_after_ms": 60000,
                    },
                },
                "polymarket_balance": {
                    "polymarket:USDC": {
                        "venue": "polymarket",
                        "available": 100.0,
                        "total": 100.0,
                        "currency": "USDC",
                        "snapshot_cohort_id": "cohort-1",
                        "snapshot_observed_at": now_iso,
                    }
                },
                "polymarket_orders": {
                    "order-1": {
                        "order_id": "order-1",
                        "contract": {
                            "venue": "polymarket",
                            "symbol": "asset-1",
                            "outcome": "yes",
                            "title": None,
                        },
                        "action": "buy",
                        "price": 0.45,
                        "quantity": 2.0,
                        "remaining_quantity": 2.0,
                        "status": "resting",
                        "created_at": "2026-04-21T18:00:00+00:00",
                        "updated_at": "2026-04-21T18:00:00+00:00",
                        "post_only": False,
                        "reduce_only": False,
                        "expiration_ts": None,
                        "client_order_id": None,
                        "snapshot_cohort_id": "cohort-1",
                        "snapshot_observed_at": now_iso,
                    }
                },
                "polymarket_positions": {
                    "asset-1:yes": {
                        "contract": {
                            "venue": "polymarket",
                            "symbol": "asset-1",
                            "outcome": "yes",
                            "title": None,
                        },
                        "quantity": 1.0,
                        "average_price": 0.44,
                        "mark_price": 0.46,
                        "snapshot_cohort_id": "cohort-1",
                        "snapshot_observed_at": now_iso,
                    }
                },
                "polymarket_fills": {
                    "fill-1": {
                        "order_id": "order-1",
                        "contract": {
                            "venue": "polymarket",
                            "symbol": "asset-1",
                            "outcome": "yes",
                            "title": None,
                        },
                        "action": "buy",
                        "price": 0.45,
                        "quantity": 0.5,
                        "fee": 0.0,
                        "fill_id": "fill-1",
                        "snapshot_cohort_id": "cohort-1",
                        "snapshot_observed_at": now_iso,
                    }
                },
            }[table]
        )

        with patch.object(
            run_agent_loop,
            "build_current_state_read_adapter",
            return_value=adapter,
        ):
            provider = run_agent_loop._build_projected_account_snapshot_provider(
                SimpleNamespace(opportunity_root="runtime/data")
            )
            snapshot = provider(None)

        self.assertTrue(snapshot.complete)
        self.assertEqual(snapshot.balance.available, 100.0)
        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(len(snapshot.positions), 1)
        self.assertEqual(len(snapshot.fills), 1)

    def test_projected_account_snapshot_provider_rejects_mixed_snapshot_cohorts(self):
        now_iso = datetime.now(timezone.utc).isoformat()
        adapter = SimpleNamespace(
            read_table=lambda table: {
                "source_health": {
                    "polymarket_user_channel": {
                        "status": "ok",
                        "last_success_at": now_iso,
                        "stale_after_ms": 60000,
                        "details": {
                            "account_snapshot": True,
                            "account_snapshot_complete": True,
                            "account_snapshot_issues": [],
                        },
                    },
                    "projection_polymarket_user_channel": {
                        "status": "ok",
                        "last_success_at": now_iso,
                        "stale_after_ms": 60000,
                    },
                },
                "polymarket_balance": {
                    "polymarket:USDC": {
                        "venue": "polymarket",
                        "available": 100.0,
                        "total": 100.0,
                        "currency": "USDC",
                        "snapshot_cohort_id": "cohort-1",
                        "snapshot_observed_at": now_iso,
                    }
                },
                "polymarket_orders": {
                    "order-1": {
                        "order_id": "order-1",
                        "contract": {
                            "venue": "polymarket",
                            "symbol": "asset-1",
                            "outcome": "yes",
                            "title": None,
                        },
                        "action": "buy",
                        "price": 0.45,
                        "quantity": 2.0,
                        "remaining_quantity": 2.0,
                        "status": "resting",
                        "snapshot_cohort_id": "cohort-2",
                        "snapshot_observed_at": now_iso,
                    }
                },
                "polymarket_positions": {},
                "polymarket_fills": {},
            }[table]
        )

        with patch.object(
            run_agent_loop,
            "build_current_state_read_adapter",
            return_value=adapter,
        ):
            provider = run_agent_loop._build_projected_account_snapshot_provider(
                SimpleNamespace(opportunity_root="runtime/data")
            )
            snapshot = provider(None)

        self.assertFalse(snapshot.complete)
        self.assertIn(
            "projected account truth spans multiple snapshot cohorts",
            snapshot.issues,
        )

    def test_main_run_mode_uses_projected_fair_values_without_manifest(self):
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
        current_state_adapter = SimpleNamespace(read_table=lambda table: {})

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "build_current_state_read_adapter",
                return_value=current_state_adapter,
            ) as build_read_adapter,
            patch.object(
                run_agent_loop,
                "_build_projected_fair_value_provider",
                return_value=ManifestFairValueProvider(records={}),
            ) as build_projected_provider,
            patch.object(
                run_agent_loop,
                "build_fair_value_provider",
                side_effect=AssertionError("manifest loader should not be used"),
            ),
            patch.object(
                run_agent_loop,
                "TradingEngine",
                return_value=fake_engine,
            ),
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
                    "--mode",
                    "run",
                    "--opportunity-root",
                    "runtime/data",
                    "--quiet",
                ],
            ):
                result = run_agent_loop.main()

        self.assertEqual(result, 0)
        build_projected_provider.assert_called_once()
        build_read_adapter.assert_called()
        provider = orchestrator_ctor.call_args.kwargs["fair_value_provider"]
        self.assertIsInstance(provider, ManifestFairValueProvider)

    def test_main_run_mode_enters_watcher_mode_when_execution_lock_is_held(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(
                halted=False,
                paused=False,
                hold_new_orders=False,
                hold_reason=None,
            )
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
            pending_cancels=[],
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_engine.set_new_order_hold = lambda reason: (
            setattr(fake_engine.safety_state, "hold_new_orders", True),
            setattr(fake_engine.safety_state, "hold_reason", reason),
        )
        fake_engine.clear_new_order_hold = lambda: None
        fake_cycle = SimpleNamespace(selected=None)
        current_state_adapter = SimpleNamespace(read_table=lambda table: {})
        lock = FakeExecutionLock("primary-loop", acquired=False)
        stdout = io.StringIO()

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "build_current_state_read_adapter",
                return_value=current_state_adapter,
            ),
            patch.object(
                run_agent_loop,
                "_build_projected_fair_value_provider",
                return_value=ManifestFairValueProvider(records={}),
            ),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            patch.object(run_agent_loop, "_build_execution_lock", return_value=lock),
            patch("sys.stdout", stdout),
        ):
            polling_loop.return_value.run.return_value = [fake_cycle]

            with patch(
                "sys.argv",
                [
                    "run_agent_loop.py",
                    "--venue",
                    "polymarket",
                    "--mode",
                    "run",
                    "--opportunity-root",
                    "runtime/data",
                ],
            ):
                result = run_agent_loop.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        self.assertTrue(fake_engine.safety_state.hold_new_orders)
        self.assertIn(
            "watcher mode: execution lock 'primary-loop'",
            fake_engine.safety_state.hold_reason,
        )
        self.assertTrue(payload["watcher_mode"])
        self.assertFalse(payload["execution_lock_acquired"])
        self.assertEqual(lock.acquire_calls, 1)
        self.assertEqual(lock.release_calls, 1)
        lifecycle_manager = polling_loop.call_args.kwargs["lifecycle_manager"]
        self.assertIsNotNone(lifecycle_manager.cancel_handler)
        self.assertIs(lifecycle_manager.cancel_handler, run_agent_loop._noop_cancel_handler)

    def test_main_run_mode_releases_execution_lock_when_acquired(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(
                halted=False,
                paused=False,
                hold_new_orders=True,
                hold_reason="watcher mode: execution lock 'primary-loop' held by another process",
            )
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
            pending_cancels=[],
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_engine.set_new_order_hold = lambda reason: None
        fake_engine.clear_new_order_hold = lambda: (
            setattr(fake_engine.safety_state, "hold_new_orders", False),
            setattr(fake_engine.safety_state, "hold_reason", None),
        )
        fake_cycle = SimpleNamespace(selected=None)
        current_state_adapter = SimpleNamespace(read_table=lambda table: {})
        lock = FakeExecutionLock("primary-loop", acquired=True)

        with (
            patch.object(run_agent_loop, "build_adapter", return_value=adapter),
            patch.object(run_agent_loop, "validate_runtime"),
            patch.object(
                run_agent_loop,
                "build_current_state_read_adapter",
                return_value=current_state_adapter,
            ),
            patch.object(
                run_agent_loop,
                "_build_projected_fair_value_provider",
                return_value=ManifestFairValueProvider(records={}),
            ),
            patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            patch.object(run_agent_loop, "_build_execution_lock", return_value=lock),
        ):
            polling_loop.return_value.run.return_value = [fake_cycle]

            with patch(
                "sys.argv",
                [
                    "run_agent_loop.py",
                    "--venue",
                    "polymarket",
                    "--mode",
                    "run",
                    "--opportunity-root",
                    "runtime/data",
                    "--quiet",
                ],
            ):
                result = run_agent_loop.main()

        self.assertEqual(result, 0)
        self.assertFalse(fake_engine.safety_state.hold_new_orders)
        self.assertIsNone(fake_engine.safety_state.hold_reason)
        self.assertEqual(lock.acquire_calls, 1)
        self.assertEqual(lock.release_calls, 1)

    def test_main_can_apply_venue_from_config_file(self):
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
                f"venue: polymarket\nruntime:\n  fair_values_file: {fair_values.name}\n"
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
                            "--config-file",
                            config_file.name,
                            "--quiet",
                        ],
                    ):
                        result = run_agent_loop.main()

        self.assertEqual(result, 0)
        validated_args = validate_runtime.call_args.args[0]
        self.assertEqual(validated_args.venue, "polymarket")

    def test_main_can_apply_runtime_timing_defaults_from_config_file(self):
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
                "venue: polymarket\n"
                "runtime:\n"
                f"  fair_values_file: {fair_values.name}\n"
                "  max_fair_value_age_seconds: 900\n"
                "  fair_values_reload_seconds: 30\n"
                "  interval_seconds: 15\n"
                "  max_cycles: 100\n"
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
                            "--config-file",
                            config_file.name,
                            "--quiet",
                        ],
                    ):
                        result = run_agent_loop.main()

        self.assertEqual(result, 0)
        validated_args = validate_runtime.call_args.args[0]
        self.assertEqual(validated_args.max_fair_value_age_seconds, 900.0)
        self.assertEqual(validated_args.fair_values_reload_seconds, 30.0)
        self.assertEqual(validated_args.interval_seconds, 15.0)
        self.assertEqual(validated_args.max_cycles, 100)

    def test_build_adapter_parses_polymarket_live_user_markets(self):
        args = SimpleNamespace(
            polymarket_live_user_markets="cond-1, cond-2",
            polymarket_user_ws_host="wss://example.invalid/ws/user",
        )

        with patch.dict("os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False):
            adapter = run_agent_loop.build_adapter("polymarket", args)

        self.assertIsInstance(adapter, PolymarketAdapter)
        polymarket_adapter = cast(PolymarketAdapter, adapter)
        config = cast(PolymarketConfig, polymarket_adapter.config)
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
        polymarket_adapter = cast(PolymarketAdapter, adapter)
        config = cast(PolymarketConfig, polymarket_adapter.config)
        self.assertEqual(config.depth_admission_levels, 4)
        self.assertEqual(config.depth_admission_liquidity_fraction, 0.65)
        self.assertEqual(config.depth_admission_max_expected_slippage_bps, 20.0)

    def test_build_adapter_derives_polymarket_live_user_markets_from_manifest(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as fair_values:
            json.dump(
                {
                    "schema_version": 1,
                    "generated_at": "2026-04-07T12:00:00Z",
                    "values": {
                        "token-1:yes": {
                            "fair_value": 0.6,
                            "condition_id": "cond-1",
                        },
                        "token-2:yes": {
                            "fair_value": 0.55,
                            "condition_id": "cond-2",
                        },
                    },
                },
                fair_values,
            )
            fair_values.flush()

            args = SimpleNamespace(
                fair_values_file=fair_values.name,
                polymarket_live_user_markets=None,
                polymarket_user_ws_host=None,
            )

            with patch.dict(
                "os.environ", {"POLYMARKET_PRIVATE_KEY": "pk"}, clear=False
            ):
                adapter = run_agent_loop.build_adapter("polymarket", args)

        self.assertIsInstance(adapter, PolymarketAdapter)
        polymarket_adapter = cast(PolymarketAdapter, adapter)
        config = cast(PolymarketConfig, polymarket_adapter.config)
        self.assertEqual(config.live_user_markets, ["cond-1", "cond-2"])

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
                "_build_projected_fair_value_provider",
                return_value=ManifestFairValueProvider(records={}),
            ),
            patch.object(
                run_agent_loop,
                "build_current_state_read_adapter",
                return_value=SimpleNamespace(read_table=lambda table: {}),
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
                    "--mode",
                    "pair-run",
                    "--opportunity-root",
                    "runtime/data",
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

    def test_main_can_emit_preview_order_proposals_from_opportunity_store(self):
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

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "start_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )

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
                        "--opportunity-root",
                        str(Path(temp_dir) / "data"),
                    ],
                ):
                    run_agent_loop.main()

            preview_snapshot = json.loads(
                (
                    Path(temp_dir) / "data" / "current" / "preview_order_context.json"
                ).read_text(encoding="utf-8")
            )

        payload = json.loads(
            "".join(call.args[0] for call in stdout.write.call_args_list)
        )
        self.assertEqual(payload["preview_order_proposal_count"], 1)
        proposal = payload["preview_order_proposals"][0]
        self.assertEqual(proposal["market_id"], "pm-1")
        self.assertEqual(proposal["edge_buy_after_costs_bps"], 1285.0)
        self.assertEqual(proposal["edge_sell_after_costs_bps"], -1515.0)
        self.assertEqual(proposal["blocked_reasons"], [])
        self.assertEqual(preview_snapshot["preview_order_proposal_count"], 1)
        self.assertEqual(preview_snapshot["preview_order_blocked_count"], 0)
        self.assertEqual(
            preview_snapshot["preview_order_proposals"][0]["market_id"],
            "pm-1",
        )

    def test_main_preview_prefers_best_mapping_per_market(self):
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

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-good": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-good",
                            "match_confidence": 0.99,
                            "resolution_risk": 0.01,
                            "is_active": True,
                        },
                        "pm-1|sb-bad": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-bad",
                            "match_confidence": 0.20,
                            "resolution_risk": 0.50,
                            "is_active": True,
                        },
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-good": {
                            "sportsbook_event_id": "sb-good",
                            "start_time": "2026-04-30T19:00:00Z",
                        },
                        "sb-bad": {
                            "sportsbook_event_id": "sb-bad",
                            "start_time": "2026-04-21T00:04:00Z",
                        },
                    }
                )
            )

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
                        "--opportunity-root",
                        str(Path(temp_dir) / "data"),
                    ],
                ):
                    run_agent_loop.main()

        rendered = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertIn('"preview_order_proposal_count": 1', rendered)
        self.assertNotIn(
            '"blocked_reason": "market within pre-start freeze window"', rendered
        )

    def test_main_preview_uses_current_bbo_depth_for_proposal_size(self):
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

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "best_bid_yes_size": 9.0,
                            "best_ask_yes_size": 2.0,
                            "source_age_ms": 500,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "start_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )

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
                        "--opportunity-root",
                        str(Path(temp_dir) / "data"),
                    ],
                ):
                    run_agent_loop.main()

        rendered = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertIn('"preview_order_proposal_count": 1', rendered)
        self.assertIn('"size": 2.0', rendered)

    def test_build_preview_order_proposals_preserves_zero_snapshot_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.0,
                            "best_bid_yes": 0.0,
                            "best_ask_yes": 0.0,
                            "edge_buy_bps": 125.0,
                            "edge_sell_bps": -125.0,
                            "edge_buy_after_costs_bps": 0.0,
                            "edge_sell_after_costs_bps": 0.0,
                            "edge_after_costs_bps": 0.0,
                            "fillable_size": 0.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "best_bid_yes_size": 9.0,
                            "best_ask_yes_size": 6.0,
                            "source_age_ms": 500,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "start_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )
            (runtime_root / "polymarket_markets.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "status": "open",
                            "end_time": "2026-04-30T22:00:00Z",
                        }
                    }
                )
            )

            proposals, blocked = run_agent_loop._build_preview_order_proposals(
                SimpleNamespace(opportunity_root=str(Path(temp_dir) / "data")),
                None,
            )

        self.assertEqual(proposals, [])
        self.assertEqual(len(blocked), 1)
        blocked_payload = blocked[0]
        self.assertEqual(blocked_payload["fair_yes_prob"], 0.0)
        self.assertEqual(blocked_payload["best_bid_yes"], 0.0)
        self.assertEqual(blocked_payload["best_ask_yes"], 0.0)
        self.assertEqual(blocked_payload["edge_buy_after_costs_bps"], 0.0)
        self.assertEqual(blocked_payload["edge_sell_after_costs_bps"], 0.0)
        self.assertEqual(blocked_payload["fillable_size"], 0.0)
        blocked_reasons = blocked_payload["blocked_reasons"]
        self.assertIsInstance(blocked_reasons, list)
        if not isinstance(blocked_reasons, list):
            self.fail("expected blocked_reasons list")
        self.assertIn("insufficient visible depth", blocked_reasons)

    def test_build_preview_order_proposals_uses_commence_time_for_freeze_window(self):
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

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "best_bid_yes_size": 9.0,
                            "best_ask_yes_size": 5.0,
                            "source_age_ms": 500,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "commence_time": (
                                datetime.now(timezone.utc) + timedelta(minutes=5)
                            ).isoformat(),
                        }
                    }
                )
            )

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
                        "--opportunity-root",
                        str(Path(temp_dir) / "data"),
                    ],
                ):
                    run_agent_loop.main()

        rendered = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertIn('"preview_order_proposal_count": 0', rendered)
        self.assertIn("market within pre-start freeze window", rendered)

    def test_build_preview_order_proposals_uses_current_state_read_adapter(self):
        current_state_adapter = object()
        preview_context = SimpleNamespace(
            preview_order_proposals=({"market_id": "pm-1"},),
            blocked_preview_orders=({"market_id": "pm-2"},),
        )

        with (
            patch.object(
                run_agent_loop,
                "build_current_state_read_adapter",
                return_value=current_state_adapter,
            ) as build_read_adapter,
            patch.object(
                run_agent_loop,
                "build_preview_runtime_context",
                return_value=preview_context,
            ) as build_preview_context,
        ):
            proposals, blocked = run_agent_loop._build_preview_order_proposals(
                SimpleNamespace(opportunity_root="runtime/data"),
                None,
            )

        self.assertEqual(proposals, [{"market_id": "pm-1"}])
        self.assertEqual(blocked, [{"market_id": "pm-2"}])
        build_read_adapter.assert_called_once_with("runtime/data")
        build_preview_context.assert_called_once_with(
            "runtime/data",
            policy=None,
            read_adapter=current_state_adapter,
        )

    def test_build_runtime_kill_switch_reads_projected_source_health(self):
        adapter = SimpleNamespace(
            read_table=lambda table: {
                "sportsbook_odds": {
                    "source_name": "sportsbook_odds",
                    "status": "red",
                }
            }
            if table == "source_health"
            else {}
        )

        with patch.object(
            run_agent_loop,
            "build_current_state_read_adapter",
            return_value=adapter,
        ):
            state, reasons = run_agent_loop._build_runtime_kill_switch(
                SimpleNamespace(mode="run", opportunity_root="runtime/data")
            )

        self.assertTrue(state.active)
        self.assertEqual(reasons, ("source health red",))

    def test_main_reports_kill_switch_when_source_health_is_red(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(
                halted=False,
                paused=False,
                hold_new_orders=False,
            )
        )

        def _halt(reason: str) -> None:
            fake_engine.safety_state.halted = True
            fake_engine.safety_state.reason = reason

        fake_engine.halt = _halt
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
            pending_cancels=[],
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_cycle = SimpleNamespace(selected=None)
        current_state_adapter = SimpleNamespace(
            read_table=lambda table: {
                "sportsbook_odds": {
                    "source_name": "sportsbook_odds",
                    "status": "red",
                }
            }
            if table == "source_health"
            else {}
        )

        stdout = io.StringIO()
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
                "build_current_state_read_adapter",
                return_value=current_state_adapter,
            ),
            patch.object(
                run_agent_loop,
                "TradingEngine",
                return_value=fake_engine,
            ),
            patch.object(run_agent_loop, "AgentOrchestrator"),
            patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            patch.object(
                run_agent_loop,
                "_build_preview_order_proposals",
                return_value=([], []),
            ),
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
                    __file__,
                    "--mode",
                    "preview",
                    "--opportunity-root",
                    "runtime/data",
                ],
            ):
                result = run_agent_loop.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        self.assertTrue(payload["engine_halted"])
        self.assertTrue(payload["kill_switch_active"])
        self.assertEqual(payload["kill_switch_reasons"], ["source health red"])

    def test_build_runtime_kill_switch_uses_projected_source_health(self):
        current_state_adapter = SimpleNamespace(
            read_table=lambda table: {
                "sportsbook_odds": {
                    "source_name": "sportsbook_odds",
                    "status": "red",
                }
            }
            if table == "source_health"
            else {}
        )

        with patch.object(
            run_agent_loop,
            "build_current_state_read_adapter",
            return_value=current_state_adapter,
        ):
            state, reasons = run_agent_loop._build_runtime_kill_switch(
                SimpleNamespace(mode="run", opportunity_root="runtime/data")
            )

        self.assertIsInstance(state, KillSwitchState)
        self.assertTrue(state.active)
        self.assertIn("source health red", reasons)

    def test_main_halts_engine_when_runtime_kill_switch_is_active(self):
        adapter = FakeAdapter()
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=True, paused=False),
            halt_reason=None,
        )

        def _halt(reason):
            fake_engine.halt_reason = reason

        fake_engine.halt = _halt
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
            pending_cancels=[],
            halted=True,
            pause_reason=None,
            hold_new_orders=False,
            hold_reason=None,
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_cycle = SimpleNamespace(selected=None)
        read_adapter = SimpleNamespace(
            read_table=lambda table: {
                "sportsbook_odds": {
                    "source_name": "sportsbook_odds",
                    "status": "red",
                }
            }
            if table == "source_health"
            else {}
        )

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
                "build_current_state_read_adapter",
                return_value=read_adapter,
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
                    __file__,
                    "--mode",
                    "preview",
                    "--opportunity-root",
                    "runtime/data",
                    "--quiet",
                ],
            ):
                result = run_agent_loop.main()

        self.assertEqual(result, 0)
        self.assertEqual(fake_engine.halt_reason, "kill switch: source health red")

    def test_main_preview_blocks_when_current_bbo_is_stale(self):
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

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "best_bid_yes_size": 9.0,
                            "best_ask_yes_size": 5.0,
                            "source_age_ms": 9000,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "start_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )

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
                        "--opportunity-root",
                        str(Path(temp_dir) / "data"),
                    ],
                ):
                    run_agent_loop.main()

        rendered = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertIn('"preview_order_proposal_count": 0', rendered)
        self.assertIn('"preview_order_blocked_count": 1', rendered)
        self.assertIn("source data stale", rendered)

    def test_main_reports_blocked_preview_orders_and_persists_runtime_metrics(self):
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

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "data" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_markets.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "status": "open",
                            "end_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "start_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )
            (runtime_root / "source_health.json").write_text(
                json.dumps(
                    {
                        "polymarket_market_channel": {
                            "status": "red",
                            "last_success_at": "2026-04-22T00:00:00Z",
                            "stale_after_ms": 4000,
                        }
                    }
                )
            )

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
                        "--opportunity-root",
                        str(Path(temp_dir) / "data"),
                    ],
                ):
                    run_agent_loop.main()

            preview_snapshot = json.loads(
                (runtime_root / "preview_order_context.json").read_text()
            )
            metrics_payload = json.loads(
                (runtime_root / "runtime_metrics.json").read_text()
            )

        payload = json.loads(
            "".join(call.args[0] for call in stdout.write.call_args_list)
        )
        self.assertEqual(payload["preview_order_proposal_count"], 0)
        self.assertEqual(payload["preview_order_blocked_count"], 1)
        blocked = payload["preview_order_blocked"][0]
        self.assertEqual(
            blocked["blocked_reason"], "source polymarket_market_channel unhealthy"
        )
        self.assertEqual(
            blocked["blocked_reasons"],
            ["source polymarket_market_channel unhealthy"],
        )
        self.assertEqual(blocked["edge_buy_after_costs_bps"], 1285.0)
        self.assertEqual(preview_snapshot["preview_order_proposal_count"], 0)
        self.assertEqual(preview_snapshot["preview_order_blocked_count"], 1)
        self.assertEqual(
            preview_snapshot["preview_order_blocked"][0]["blocked_reason"],
            "source polymarket_market_channel unhealthy",
        )
        self.assertIn("run_agent_loop:preview_proposals", metrics_payload["metrics"])

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

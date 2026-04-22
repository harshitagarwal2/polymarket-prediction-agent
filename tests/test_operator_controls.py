from __future__ import annotations

import argparse
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from adapters.base import AdapterHealth
from adapters.polymarket import PolymarketAdapter
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderAction,
    OrderBookSnapshot,
    OrderStatus,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from engine.runner import TradingEngine
from engine.safety_state import PendingCancelState
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits
from scripts import operator_cli
from research.storage import read_jsonl_events


class PauseAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self.contract = Contract(
            venue=self.venue, symbol="token-1", outcome=OutcomeSide.YES
        )

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return []

    def get_order_book(self, contract: Contract):
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=10)],
        )

    def list_open_orders(self, contract: Contract | None = None):
        return []

    def list_positions(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [PositionSnapshot(contract=contract, quantity=0.0)]

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract):
        return PositionSnapshot(contract=contract, quantity=0.0)

    def get_balance(self):
        return BalanceSnapshot(venue=self.venue, available=100.0, total=100.0)

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=[],
            fills=[],
        )

    def place_limit_order(self, intent) -> PlacementResult:
        raise AssertionError("paused engine should not place orders")

    def cancel_order(self, order_id: str):
        return True

    def cancel_all(self, contract: Contract | None = None) -> int:
        return 0

    def close(self):
        return None


class CancelTrackingAdapter(PauseAdapter):
    def __init__(self):
        super().__init__()
        self._open_orders = [
            NormalizedOrder(
                order_id="cancel-1",
                contract=self.contract,
                action=OrderAction.BUY,
                price=0.5,
                quantity=1.0,
                remaining_quantity=1.0,
            )
        ]

    def list_open_orders(self, contract: Contract | None = None):
        return list(self._open_orders)

    def cancel_all(self, contract: Contract | None = None) -> int:
        count = len(self._open_orders)
        self._open_orders = []
        return count


class StatusDriftAdapter(PauseAdapter):
    def __init__(self):
        super().__init__()
        self._open_orders: list[NormalizedOrder] = []
        self._fills: list[FillSnapshot] = []

    def list_open_orders(self, contract: Contract | None = None):
        return list(self._open_orders)

    def list_fills(self, contract: Contract | None = None):
        return list(self._fills)

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=self.list_open_orders(contract),
            fills=self.list_fills(contract),
        )

    def live_state_status(self):
        return SimpleNamespace(
            active=True,
            running=True,
            initialized=True,
            fresh=True,
            fills_initialized=True,
            fills_fresh=True,
            fills_last_update_at=None,
            cached_fill_count=1,
            last_fills_source="live_cache",
            last_fills_fallback_reason=None,
            last_error=None,
            subscribed_markets=("condition-1",),
        )

    def market_state_status(self):
        return SimpleNamespace(
            active=True,
            running=True,
            mode="healthy",
            fresh=True,
            last_error=None,
            degraded_reason=None,
            recovery_attempts=1,
            last_recovery_at=None,
            snapshot_book_overlay_source="rest_plus_live_market",
            snapshot_book_overlay_reason=None,
            snapshot_book_overlay_applied=True,
            subscribed_assets=("token-1",),
        )


class PlaceableAdapter(PauseAdapter):
    def __init__(self):
        super().__init__()
        self.place_calls = 0

    def place_limit_order(self, intent):
        self.place_calls += 1
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.RESTING)


class OperatorControlTests(unittest.TestCase):
    def test_build_adapter_parses_polymarket_live_user_markets(self):
        with patch.dict(
            os.environ,
            {
                "POLYMARKET_PRIVATE_KEY": "pk",
                "POLYMARKET_LIVE_USER_MARKETS": "cond-1, cond-2",
                "POLYMARKET_USER_WS_HOST": "wss://example.invalid/ws/user",
            },
            clear=False,
        ):
            adapter = operator_cli._build_adapter("polymarket")

        self.assertIsInstance(adapter, PolymarketAdapter)
        self.assertEqual(adapter.config.live_user_markets, ["cond-1", "cond-2"])
        self.assertEqual(adapter.config.user_ws_host, "wss://example.invalid/ws/user")

    def test_pause_blocks_run_and_persists(self):
        adapter = PauseAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
                ),
                safety_state_path=state_path,
            )

            engine.pause("manual maintenance")
            result = engine.run_once(adapter.contract, fair_value=0.60)

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
                ),
                safety_state_path=state_path,
            )

            self.assertFalse(result.placements)
            self.assertTrue(result.risk.rejected)
            self.assertIn("manual maintenance", result.risk.rejected[0].reason)
            self.assertTrue(restarted.status_snapshot().paused)
            self.assertEqual(
                restarted.status_snapshot().pause_reason, "manual maintenance"
            )

    def test_clear_pause_persists(self):
        adapter = PauseAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
                ),
                safety_state_path=state_path,
            )

            engine.pause("manual maintenance")
            engine.clear_pause()

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
                ),
                safety_state_path=state_path,
            )

            self.assertFalse(restarted.status_snapshot().paused)
            self.assertIsNone(restarted.status_snapshot().pause_reason)

    def test_resume_command_clears_halt_after_clean_confirmation(self):
        adapter = PauseAdapter()
        adapter.get_account_snapshot = lambda contract=None: AccountSnapshot(
            venue=adapter.venue,
            balance=adapter.get_balance(),
            positions=[PositionSnapshot(contract=adapter.contract, quantity=0.0)],
            open_orders=[],
            fills=[],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            store = operator_cli.SafetyStateStore(state_path)
            state = store.load()
            state.halted = True
            state.reason = "previous drift"
            state.contract_key = adapter.contract.market_key
            store.save(state)

            args = argparse.Namespace(
                venue="polymarket",
                symbol=adapter.contract.symbol,
                outcome="yes",
                state_file=str(state_path),
                journal=None,
                resume_confirmation_required=1,
            )
            stdout = io.StringIO()

            with (
                patch.object(operator_cli, "_build_adapter", return_value=adapter),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.cmd_resume(args)

            resumed = operator_cli.SafetyStateStore(state_path).load()
            self.assertEqual(result, 0)
            self.assertFalse(resumed.halted)
            self.assertIsNone(resumed.reason)
            self.assertIn('"action": "ok"', stdout.getvalue())

    def test_recent_execution_status_marks_unresolved_orders(self):
        contract = Contract(
            venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
        )
        snapshot = AccountSnapshot(
            venue=Venue.POLYMARKET,
            balance=BalanceSnapshot(
                venue=Venue.POLYMARKET, available=100.0, total=100.0
            ),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=[
                NormalizedOrder(
                    order_id="open-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.5,
                    quantity=1.0,
                    remaining_quantity=1.0,
                )
            ],
            fills=[
                FillSnapshot(
                    order_id="filled-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.5,
                    quantity=1.0,
                )
            ],
        )

        status = operator_cli._recent_execution_status(
            {"last_execution_order_ids": ["open-1", "filled-1", "missing-1"]},
            snapshot,
        )

        self.assertEqual(status["acknowledged_order_ids"], ["filled-1", "open-1"])
        self.assertEqual(status["unresolved_order_ids"], ["missing-1"])

    def test_operator_actions_get_cycle_id_in_journal(self):
        adapter = PauseAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            journal_path = Path(temp_dir) / "events.jsonl"
            args = argparse.Namespace(
                state_file=str(state_path),
                reason="manual maintenance",
                journal=str(journal_path),
            )

            result = operator_cli.cmd_pause(args)
            events = read_jsonl_events(journal_path)

            self.assertEqual(result, 0)
            self.assertEqual(events[0]["event_type"], "operator_pause")
            self.assertTrue(events[0]["payload"]["cycle_id"])

    def test_cancel_all_registers_pending_cancel_state(self):
        adapter = CancelTrackingAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            args = argparse.Namespace(
                venue="polymarket",
                symbol=adapter.contract.symbol,
                outcome="yes",
                journal=None,
                state_file=str(state_path),
                stable_polls=1,
                verify_sleep_seconds=0.0,
                max_wait_seconds=0.0,
            )

            with patch.object(operator_cli, "_build_adapter", return_value=adapter):
                result = operator_cli.cmd_cancel_all(args)

            state = operator_cli.SafetyStateStore(state_path).load()
            self.assertEqual(result, 0)
            self.assertEqual(len(state.pending_cancels), 1)
            self.assertEqual(state.pending_cancels[0].order_id, "cancel-1")
            self.assertFalse(state.pending_cancels[0].acknowledged)

    def test_status_includes_pending_cancel_visibility(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            store = operator_cli.SafetyStateStore(state_path)
            state = store.load()
            state.pending_cancels.append(
                PendingCancelState(
                    order_id="cancel-1",
                    contract_key="token-1:yes",
                    requested_at=datetime.now(timezone.utc),
                    reason="operator cancel all",
                    last_attempt_at=datetime.now(timezone.utc),
                    attempt_count=1,
                    post_cancel_fill_seen=True,
                )
            )
            store.save(state)
            args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue=None,
                symbol=None,
                outcome="unknown",
            )
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_status(args)

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload["pending_cancels"][0]["order_id"], "cancel-1")
            self.assertTrue(payload["pending_cancels"][0]["post_cancel_fill_seen"])
            self.assertIn("recovery_items", payload)

    def test_status_includes_reconciliation_detail_against_persisted_truth(self):
        adapter = StatusDriftAdapter()
        adapter._open_orders = [
            NormalizedOrder(
                order_id="local-1",
                contract=adapter.contract,
                action=OrderAction.BUY,
                price=0.5,
                quantity=1.0,
                remaining_quantity=1.0,
                status=OrderStatus.RESTING,
            )
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
                ),
                safety_state_path=state_path,
            )
            engine.preview_once(adapter.contract, fair_value=0.60)
            adapter._open_orders = []
            args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue="polymarket",
                symbol=adapter.contract.symbol,
                outcome="yes",
            )
            stdout = io.StringIO()

            with (
                patch.object(operator_cli, "_build_adapter", return_value=adapter),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.cmd_status(args)

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload["reconciliation"]["missing_on_venue"], ["local-1"])
            self.assertTrue(payload["live_state"]["active"])
            self.assertEqual(payload["market_state"]["mode"], "healthy")
            self.assertTrue(payload["live_state"]["fills_initialized"])
            self.assertEqual(payload["live_state"]["last_fills_source"], "live_cache")

    def test_status_includes_last_depth_assessment(self):
        adapter = StatusDriftAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            store = operator_cli.SafetyStateStore(state_path)
            state = store.load()
            state.last_depth_assessment = {
                "requested_quantity": 2.0,
                "visible_quantity": 1.0,
                "max_admissible_quantity": 0.5,
                "expected_slippage_bps": 25.0,
                "depth_levels_used": 2,
            }
            store.save(state)
            args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue="polymarket",
                symbol=adapter.contract.symbol,
                outcome="yes",
            )
            stdout = io.StringIO()

            with (
                patch.object(operator_cli, "_build_adapter", return_value=adapter),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.cmd_status(args)

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(
                payload["depth_assessment"]["max_admissible_quantity"], 0.5
            )

    def test_hold_new_orders_command_persists_and_status_reports_hold(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            hold_args = argparse.Namespace(
                state_file=str(state_path),
                reason="risk desk hold",
                journal=None,
            )
            status_args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue=None,
                symbol=None,
                outcome="unknown",
            )
            stdout = io.StringIO()

            self.assertEqual(operator_cli.cmd_hold_new_orders(hold_args), 0)
            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_status(status_args)

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertTrue(payload["safety_state"]["hold_new_orders"])
            self.assertEqual(payload["runtime_health"]["state"], "hold_new_orders")
            self.assertFalse(payload["runtime_health"]["resume_trading_eligible"])

    def test_force_refresh_command_persists_scoped_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue="polymarket",
                symbol="token-1",
                outcome="yes",
                reason="manual refresh",
            )

            result = operator_cli.cmd_force_refresh(args)
            state = operator_cli.SafetyStateStore(state_path).load()

            self.assertEqual(result, 0)
            self.assertEqual(len(state.pending_refresh_requests), 1)
            self.assertEqual(state.pending_refresh_requests[0].scope, "token-1:yes")
            self.assertTrue(
                any(
                    item.item_type == "market-refresh-needed"
                    for item in state.recovery_items
                )
            )

    def test_status_reports_open_recovery_items(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            store = operator_cli.SafetyStateStore(state_path)
            state = store.load()
            now = datetime.now(timezone.utc)
            state.recovery_items.append(
                operator_cli.RecoveryItemState(
                    recovery_id="submit-uncertain:token-1:yes",
                    item_type="submit-uncertain",
                    scope="token-1:yes",
                    reason="ambiguous submission outcome",
                    clear_source="authoritative_observation",
                    opened_at=now,
                    last_evidence_at=now,
                    last_evidence="ambiguous submission outcome",
                )
            )
            store.save(state)
            args = argparse.Namespace(
                state_file=str(state_path),
                journal=None,
                venue=None,
                symbol=None,
                outcome="unknown",
            )
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_status(args)

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload["runtime_health"]["state"], "recovering")
            self.assertEqual(payload["runtime_health"]["open_recovery_count"], 1)
            self.assertTrue(payload["recovery_items"])

    def test_running_engine_syncs_hold_new_orders_from_store(self):
        adapter = PlaceableAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
                ),
                safety_state_path=state_path,
            )

            hold_args = argparse.Namespace(
                state_file=str(state_path),
                reason="risk desk hold",
                journal=None,
            )
            operator_cli.cmd_hold_new_orders(hold_args)
            result = engine.run_once(adapter.contract, fair_value=0.60)

            self.assertEqual(adapter.place_calls, 0)
            self.assertTrue(result.risk.rejected)
            self.assertIn("risk desk hold", result.risk.rejected[0].reason)


if __name__ == "__main__":
    unittest.main()

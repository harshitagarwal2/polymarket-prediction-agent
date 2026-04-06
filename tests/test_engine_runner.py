from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderAction,
    OrderBookSnapshot,
    OrderIntent,
    OrderStatus,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from adapters import MarketSummary
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits


class SequencedAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self.contract = Contract(
            venue=Venue.POLYMARKET, symbol="token-1", outcome=OutcomeSide.YES
        )
        self.other_contract = Contract(
            venue=Venue.POLYMARKET, symbol="token-2", outcome=OutcomeSide.YES
        )
        self.phase = 0

    def health(self) -> AdapterHealth:
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100) -> list[MarketSummary]:
        return []

    def get_order_book(self, contract: Contract) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=10)],
        )

    def list_open_orders(
        self, contract: Contract | None = None
    ) -> list[NormalizedOrder]:
        return self.get_account_snapshot(contract).open_orders

    def list_positions(
        self, contract: Contract | None = None
    ) -> list[PositionSnapshot]:
        return self.get_account_snapshot(contract).positions

    def list_fills(self, contract: Contract | None = None):
        return self.get_account_snapshot(contract).fills

    def get_position(self, contract: Contract) -> PositionSnapshot:
        positions = self.list_positions(contract)
        return (
            positions[0]
            if positions
            else PositionSnapshot(contract=contract, quantity=0.0)
        )

    def get_balance(self) -> BalanceSnapshot:
        return self.get_account_snapshot(self.contract).balance

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        if self.phase == 0:
            return AccountSnapshot(
                venue=self.venue,
                balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
                positions=[PositionSnapshot(contract=contract, quantity=0.0)],
                open_orders=[],
                fills=[],
            )
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=95.0, total=95.0),
            positions=[PositionSnapshot(contract=contract, quantity=10.0)],
            open_orders=[],
            fills=[],
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        self.phase = 1
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.RESTING)

    def cancel_order(self, order_id: str) -> bool:
        return True

    def cancel_all(self, contract: Contract | None = None) -> int:
        return 0

    def close(self) -> None:
        return None


class IncompleteResumeAdapter(SequencedAdapter):
    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        if self.phase == 0:
            return super().get_account_snapshot(contract)
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=95.0, total=95.0),
            positions=[PositionSnapshot(contract=contract, quantity=10.0)],
            open_orders=[],
            fills=[],
            complete=False,
            issues=["resume snapshot incomplete"],
        )


class StaleResumeAdapter(SequencedAdapter):
    def __init__(self):
        super().__init__()
        self.fixed_observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        snapshot = super().get_account_snapshot(contract)
        if self.phase == 0:
            return snapshot
        snapshot.observed_at = self.fixed_observed_at
        return snapshot


class GlobalExposurePreviewAdapter(SequencedAdapter):
    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        if contract is None:
            return AccountSnapshot(
                venue=self.venue,
                balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
                positions=[
                    PositionSnapshot(contract=self.contract, quantity=0.0),
                    PositionSnapshot(contract=self.other_contract, quantity=2.0),
                ],
                open_orders=[
                    NormalizedOrder(
                        order_id="other-open",
                        contract=self.other_contract,
                        action=OrderAction.BUY,
                        price=0.45,
                        quantity=1.0,
                        remaining_quantity=1.0,
                        status=OrderStatus.RESTING,
                    )
                ],
                fills=[],
            )
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=[],
            fills=[],
        )


class LiveDeltaPreviewAdapter(SequencedAdapter):
    def __init__(self):
        super().__init__()
        self.include_delta = True

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=[],
            fills=[],
        )

    def live_user_state_delta(self, contract: Contract | None = None):
        if not self.include_delta:
            return None
        contract = contract or self.contract
        return SimpleNamespace(
            source="polymarket_live_user_state",
            observed_at=datetime.now(timezone.utc),
            open_orders=(
                NormalizedOrder(
                    order_id="live-order-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.49,
                    quantity=2.0,
                    remaining_quantity=2.0,
                    status=OrderStatus.RESTING,
                ),
            ),
            fills=(
                FillSnapshot(
                    order_id="live-order-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.49,
                    quantity=0.5,
                    fill_id="live-fill-1",
                ),
            ),
            terminal_order_ids=(),
        )


class IncompleteLiveDeltaAdapter(LiveDeltaPreviewAdapter):
    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        snapshot = super().get_account_snapshot(contract)
        snapshot.complete = False
        snapshot.issues = ["incomplete venue truth"]
        return snapshot


class TerminalDeltaAdapter(LiveDeltaPreviewAdapter):
    def __init__(self):
        super().__init__()
        self.include_terminal = True
        self.snapshot_has_order = True

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        open_orders = []
        if self.snapshot_has_order:
            open_orders = [
                NormalizedOrder(
                    order_id="live-order-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.49,
                    quantity=2.0,
                    remaining_quantity=2.0,
                    status=OrderStatus.RESTING,
                )
            ]
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=open_orders,
            fills=[],
        )

    def live_user_state_delta(self, contract: Contract | None = None):
        if not self.include_terminal:
            return None
        contract = contract or self.contract
        return SimpleNamespace(
            source="polymarket_live_user_state",
            observed_at=datetime.now(timezone.utc),
            open_orders=(),
            fills=(),
            terminal_order_ids=("live-order-1",),
        )


class ErrorLiveStateAdapter(LiveDeltaPreviewAdapter):
    def __init__(self):
        super().__init__()
        self.live_error: str | None = "socket reset"
        self.live_mode = "degraded"
        self.recovery_confirmed_at = None

    def live_state_status(self):
        return SimpleNamespace(
            active=True,
            running=True,
            mode=self.live_mode,
            initialized=True,
            fresh=True,
            last_update_at=datetime.now(timezone.utc),
            last_error=self.live_error,
            degraded_reason=self.live_error,
            subscribed_markets=("condition-1",),
            fills_last_update_at=datetime.now(timezone.utc),
        )

    def mark_live_state_degraded(self, reason: str):
        self.live_mode = "degraded"
        self.live_error = reason
        return self.live_state_status()

    def confirm_live_state_recovery(self, observed_at: datetime):
        self.live_mode = "healthy"
        self.live_error = None
        self.recovery_confirmed_at = observed_at
        return self.live_state_status()


class RefreshThenRetryAdmissionAdapter(SequencedAdapter):
    def __init__(self):
        super().__init__()
        self.place_calls = 0

    def admit_limit_order(self, intent: OrderIntent):
        return SimpleNamespace(
            action="refresh_then_retry",
            reason="live market overlay unavailable: market_state_recovering",
            scope=intent.contract.market_key,
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        self.place_calls += 1
        raise AssertionError("runner should not place when admission asks for refresh")


class AmbiguousSubmitAdapter(SequencedAdapter):
    def __init__(self):
        super().__init__()
        self.place_calls = 0
        self.visible_order = False
        self.cancel_calls = 0

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        open_orders = []
        if self.visible_order:
            open_orders = [
                NormalizedOrder(
                    order_id="placed-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=1.0,
                    remaining_quantity=1.0,
                    status=OrderStatus.RESTING,
                )
            ]
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=open_orders,
            fills=[],
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        self.place_calls += 1
        raise RuntimeError("timeout after send")

    def cancel_order(self, order_id: str):
        self.cancel_calls += 1
        return True


class AcceptedPendingAdapter(SequencedAdapter):
    def __init__(self):
        super().__init__()
        self.place_calls = 0
        self.visible_order = False

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        open_orders = []
        if self.visible_order:
            open_orders = [
                NormalizedOrder(
                    order_id="placed-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=1.0,
                    remaining_quantity=1.0,
                    status=OrderStatus.RESTING,
                )
            ]
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=open_orders,
            fills=[],
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        self.place_calls += 1
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.RESTING)


class PendingCancelSubmitAdapter(SequencedAdapter):
    def __init__(self):
        super().__init__()
        self.place_calls = 0
        self.cancel_calls = 0

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=[
                NormalizedOrder(
                    order_id="cancel-1",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.50,
                    quantity=1.0,
                    remaining_quantity=1.0,
                    status=OrderStatus.RESTING,
                )
            ],
            fills=[],
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        self.place_calls += 1
        raise AssertionError("runner should not place while cancel is unresolved")

    def cancel_order(self, order_id: str):
        self.cancel_calls += 1
        return True


class PendingCancelLiveTerminalAdapter(PendingCancelSubmitAdapter):
    def live_user_state_delta(self, contract: Contract | None = None):
        return SimpleNamespace(
            source="polymarket_live_user_state",
            observed_at=datetime.now(timezone.utc),
            open_orders=(),
            fills=(),
            terminal_order_ids=("cancel-1",),
        )


class EngineRunnerTests(unittest.TestCase):
    def test_run_once_reconciles_against_updated_venue_state(self):
        adapter = SequencedAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
            ),
            resume_confirmation_required=2,
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(result.context.balance.available, 100.0)
        reconciliation_after = result.reconciliation_after
        self.assertIsNotNone(reconciliation_after)
        if reconciliation_after is None:
            self.fail("expected reconciliation_after")
        self.assertEqual(reconciliation_after.position_drift, 10.0)
        self.assertEqual(reconciliation_after.balance_drift, -5.0)
        self.assertEqual(reconciliation_after.policy.action, "halt")
        self.assertTrue(engine.safety_state.halted)
        self.assertEqual(
            engine.account_state.position_for(adapter.contract).quantity, 10.0
        )

    def test_engine_blocks_after_halt_until_resume(self):
        adapter = SequencedAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
            ),
            resume_confirmation_required=2,
        )

        first = engine.run_once(adapter.contract, fair_value=0.60)
        second = engine.run_once(adapter.contract, fair_value=0.60)
        first_resume = engine.try_resume(adapter.contract)
        self.assertTrue(engine.safety_state.halted)
        self.assertEqual(engine.safety_state.clean_resume_streak, 1)
        blocked_while_confirming = engine.run_once(adapter.contract, fair_value=0.60)
        second_resume = engine.try_resume(adapter.contract)
        self.assertFalse(engine.safety_state.halted)
        third = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(first.placements[0].order_id, "placed-1")
        self.assertFalse(second.placements)
        self.assertTrue(second.risk.rejected)
        self.assertIn("Position drift detected", second.risk.rejected[0].reason)
        self.assertEqual(first_resume.policy.action, "ok")
        self.assertFalse(blocked_while_confirming.placements)
        self.assertTrue(blocked_while_confirming.risk.rejected)
        self.assertEqual(second_resume.policy.action, "ok")
        self.assertEqual(third.placements[0].order_id, "placed-1")

    def test_try_resume_stays_halted_when_snapshot_is_incomplete(self):
        adapter = IncompleteResumeAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
            ),
            resume_confirmation_required=2,
        )

        engine.run_once(adapter.contract, fair_value=0.60)

        resume_report = engine.try_resume(adapter.contract)

        self.assertTrue(engine.safety_state.halted)
        self.assertEqual(resume_report.policy.action, "ok")
        self.assertIn("resume snapshot incomplete", engine.safety_state.reason or "")

    def test_try_resume_wrong_contract_keeps_existing_halt(self):
        adapter = SequencedAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
            ),
            resume_confirmation_required=2,
        )

        engine.run_once(adapter.contract, fair_value=0.60)
        original_reason = engine.safety_state.reason

        resume_report = engine.try_resume(adapter.other_contract)

        self.assertTrue(engine.safety_state.halted)
        self.assertEqual(engine.safety_state.contract_key, adapter.contract.market_key)
        self.assertEqual(engine.safety_state.reason, original_reason)
        self.assertTrue(resume_report.healthy)

    def test_try_resume_requires_fresh_observation(self):
        adapter = StaleResumeAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
            ),
            resume_confirmation_required=2,
        )

        engine.run_once(adapter.contract, fair_value=0.60)
        first_resume = engine.try_resume(adapter.contract)
        second_resume = engine.try_resume(adapter.contract)

        self.assertEqual(first_resume.policy.action, "ok")
        self.assertTrue(engine.safety_state.halted)
        self.assertEqual(engine.safety_state.clean_resume_streak, 0)
        self.assertEqual(second_resume.policy.action, "ok")
        self.assertEqual(engine.safety_state.reason, "resume evidence is not fresh")

    def test_preview_once_uses_global_positions_and_open_orders_for_risk(self):
        adapter = GlobalExposurePreviewAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=3)
            ),
        )

        result = engine.preview_once(adapter.contract, fair_value=0.60)

        self.assertFalse(result.risk.approved)
        self.assertTrue(result.risk.rejected)
        self.assertEqual(result.risk.rejected[0].reason, "global exposure cap exceeded")

    def test_build_context_applies_live_user_state_delta_after_snapshot(self):
        adapter = LiveDeltaPreviewAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        context = engine.build_context(adapter.contract, fair_value=0.60)

        self.assertEqual(
            [order.order_id for order in context.open_orders], ["live-order-1"]
        )
        self.assertEqual(
            engine.account_state.fills_for(adapter.contract)[0].fill_id, "live-fill-1"
        )
        status = engine.status_snapshot()
        self.assertEqual(status.last_live_delta_order_upserts, 1)
        self.assertEqual(status.last_live_delta_fill_upserts, 1)
        self.assertEqual(status.last_live_delta_source, "polymarket_live_user_state")

    def test_next_snapshot_records_correction_when_live_delta_disappears(self):
        adapter = LiveDeltaPreviewAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        engine.build_context(adapter.contract, fair_value=0.60)
        adapter.include_delta = False
        engine.build_context(adapter.contract, fair_value=0.60)

        status = engine.status_snapshot()
        self.assertGreaterEqual(status.last_snapshot_correction_order_count, 1)
        self.assertGreaterEqual(status.last_snapshot_correction_fill_count, 1)

    def test_incomplete_snapshot_does_not_apply_live_delta_fast_path(self):
        adapter = IncompleteLiveDeltaAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        context = engine.build_context(adapter.contract, fair_value=0.60)

        self.assertFalse(context.metadata["account_snapshot_complete"])
        self.assertFalse(engine.account_state.open_orders)
        self.assertFalse(engine.account_state.fills)

    def test_live_terminal_marker_removes_order_until_snapshot_confirms(self):
        adapter = TerminalDeltaAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        context = engine.build_context(adapter.contract, fair_value=0.60)
        first_status = engine.status_snapshot()
        adapter.include_terminal = False
        adapter.snapshot_has_order = False
        engine.build_context(adapter.contract, fair_value=0.60)

        status = engine.status_snapshot()
        self.assertFalse(context.open_orders)
        self.assertEqual(first_status.last_live_terminal_marker_applied_count, 1)
        self.assertEqual(status.last_snapshot_terminal_confirmation_count, 1)
        self.assertEqual(status.last_snapshot_terminal_reversal_count, 0)

    def test_snapshot_can_reverse_terminal_marker(self):
        adapter = TerminalDeltaAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        engine.build_context(adapter.contract, fair_value=0.60)
        adapter.include_terminal = False
        adapter.snapshot_has_order = True
        context = engine.build_context(adapter.contract, fair_value=0.60)

        status = engine.status_snapshot()
        self.assertEqual(
            [order.order_id for order in context.open_orders], ["live-order-1"]
        )
        self.assertEqual(status.last_snapshot_terminal_confirmation_count, 0)
        self.assertEqual(status.last_snapshot_terminal_reversal_count, 1)

    def test_live_delta_is_suppressed_when_overlay_health_degrades(self):
        adapter = ErrorLiveStateAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        context = engine.build_context(adapter.contract, fair_value=0.60)
        status = engine.status_snapshot()

        self.assertFalse(context.open_orders)
        self.assertTrue(status.overlay_degraded)
        self.assertTrue(status.overlay_delta_suppressed)
        self.assertEqual(
            status.overlay_degraded_reason, "live state error: socket reset"
        )
        self.assertEqual(status.overlay_forced_snapshot_count, 1)
        self.assertEqual(status.overlay_last_forced_snapshot_scope, "account")

    def test_overlay_health_recovers_after_clean_snapshot(self):
        adapter = ErrorLiveStateAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        engine.build_context(adapter.contract, fair_value=0.60)
        adapter.live_error = None
        adapter.live_mode = "recovering"
        context = engine.build_context(adapter.contract, fair_value=0.60)
        status = engine.status_snapshot()

        self.assertFalse(status.overlay_degraded)
        self.assertFalse(status.overlay_delta_suppressed)
        self.assertIsNotNone(status.overlay_last_confirmed_snapshot_at)
        self.assertEqual(status.overlay_last_recovery_outcome, "snapshot_confirmed")
        self.assertEqual(status.overlay_last_recovery_scope, "account")
        self.assertIsNotNone(status.overlay_last_recovery_at)
        self.assertIsNotNone(status.overlay_last_suppression_duration_seconds)
        self.assertEqual(adapter.live_mode, "healthy")
        self.assertIsNotNone(adapter.recovery_confirmed_at)
        self.assertEqual(
            [order.order_id for order in context.open_orders], ["live-order-1"]
        )

    def test_duplicate_refresh_requests_are_coalesced_by_scope(self):
        adapter = SequencedAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        engine.request_authoritative_refresh("live state error: socket reset")
        engine.request_authoritative_refresh("live state error: socket reset")

        forced, reason = engine.consume_authoritative_refresh_request()

        self.assertFalse(forced)
        self.assertIsNone(reason)
        self.assertEqual(engine.status_snapshot().overlay_forced_snapshot_count, 1)

    def test_run_once_defers_placement_when_admission_requests_refresh(self):
        adapter = RefreshThenRetryAdmissionAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)
        status = engine.status_snapshot()

        self.assertEqual(adapter.place_calls, 0)
        self.assertEqual(len(result.placements), 1)
        self.assertIn(
            "deferred pending authoritative refresh", result.placements[0].message or ""
        )
        self.assertEqual(status.overlay_forced_snapshot_count, 1)
        self.assertEqual(
            status.overlay_last_forced_snapshot_scope, adapter.contract.market_key
        )

    def test_ambiguous_submit_becomes_pending_submission_and_requests_refresh(self):
        adapter = AmbiguousSubmitAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)
        status = engine.status_snapshot()

        self.assertEqual(adapter.place_calls, 1)
        self.assertEqual(len(engine.pending_submissions(unresolved_only=True)), 1)
        self.assertEqual(
            engine.pending_submissions(unresolved_only=True)[0].status,
            "needs_recovery",
        )
        self.assertIn("placement uncertain", result.placements[0].message or "")
        self.assertEqual(status.overlay_forced_snapshot_count, 1)

    def test_duplicate_pending_submission_blocks_retry(self):
        adapter = AmbiguousSubmitAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        engine.run_once(adapter.contract, fair_value=0.60)
        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(adapter.place_calls, 1)
        self.assertIn(
            "ambiguous submission outcome",
            result.placements[0].message or "",
        )

    def test_authoritative_observation_clears_pending_submission(self):
        adapter = AcceptedPendingAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        first = engine.run_once(adapter.contract, fair_value=0.60)
        adapter.visible_order = True
        context = engine.build_context(adapter.contract, fair_value=0.60)

        self.assertFalse(engine.pending_submissions(unresolved_only=True))
        self.assertEqual(first.placements[0].order_id, "placed-1")
        self.assertEqual(
            [order.order_id for order in context.open_orders], ["placed-1"]
        )

    def test_request_cancel_order_triggers_targeted_refresh(self):
        adapter = PendingCancelSubmitAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        order = adapter.get_account_snapshot(adapter.contract).open_orders[0]

        engine.request_cancel_order(order, "manual cancel")
        status = engine.status_snapshot()

        self.assertEqual(len(engine.pending_cancels(unresolved_only=True)), 1)
        self.assertEqual(status.overlay_forced_snapshot_count, 1)
        self.assertEqual(
            status.overlay_last_forced_snapshot_scope, adapter.contract.market_key
        )

    def test_live_terminal_does_not_remove_pending_cancel_order_early(self):
        adapter = PendingCancelLiveTerminalAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        order = adapter.get_account_snapshot(adapter.contract).open_orders[0]

        engine.request_cancel_order(order, "manual cancel")
        context = engine.build_context(adapter.contract, fair_value=0.60)

        self.assertEqual(
            [existing.order_id for existing in context.open_orders], ["cancel-1"]
        )
        self.assertEqual(len(engine.pending_cancels(unresolved_only=True)), 1)

    def test_run_once_defers_new_submit_while_pending_cancel_unresolved(self):
        adapter = PendingCancelSubmitAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        order = adapter.get_account_snapshot(adapter.contract).open_orders[0]

        engine.request_cancel_order(order, "manual cancel")
        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(adapter.place_calls, 0)
        self.assertTrue(result.risk.rejected)
        self.assertIn(
            "pending cancel awaiting authoritative observation",
            result.risk.rejected[0].reason,
        )

    def test_cancel_request_recovers_first_while_submit_uncertain(self):
        adapter = AmbiguousSubmitAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        engine.run_once(adapter.contract, fair_value=0.60)
        order = NormalizedOrder(
            order_id="cancel-1",
            contract=adapter.contract,
            action=OrderAction.BUY,
            price=0.50,
            quantity=1.0,
            remaining_quantity=1.0,
            status=OrderStatus.RESTING,
        )
        record = engine.request_cancel_order(order, "manual cancel")

        self.assertEqual(record.status, "needs_recovery")
        self.assertEqual(adapter.cancel_calls, 0)

    def test_duplicate_pending_cancel_is_held(self):
        adapter = PendingCancelSubmitAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        order = adapter.get_account_snapshot(adapter.contract).open_orders[0]

        engine.request_cancel_order(order, "manual cancel")
        engine.request_cancel_order(order, "manual cancel again")

        self.assertEqual(adapter.cancel_calls, 1)
        self.assertEqual(len(engine.pending_cancels(unresolved_only=True)), 1)


if __name__ == "__main__":
    unittest.main()

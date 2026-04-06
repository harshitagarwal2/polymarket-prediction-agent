from __future__ import annotations

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
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits


class IncompleteTruthAdapter:
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
        return [PositionSnapshot(contract=self.contract, quantity=0.0)]

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract):
        return PositionSnapshot(contract=contract, quantity=0.0)

    def get_balance(self):
        return BalanceSnapshot(venue=self.venue, available=100.0, total=100.0)

    def get_account_snapshot(self, contract: Contract | None = None):
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=self.list_positions(contract),
            open_orders=[],
            fills=[],
            complete=False,
            issues=["incomplete venue truth"],
        )

    def place_limit_order(self, intent):
        raise AssertionError("run_once should fail closed before placing an order")

    def cancel_order(self, order_id: str):
        return False

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


class PlacementFailClosedAdapter:
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
            open_orders=self.list_open_orders(contract),
            fills=self.list_fills(contract),
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        raise RuntimeError("transient post_order failure")

    def cancel_order(self, order_id: str):
        return False

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


class AcceptedWithoutOrderIdAdapter(PlacementFailClosedAdapter):
    def place_limit_order(self, intent: OrderIntent):
        return PlacementResult(True, status=OrderStatus.PENDING)


class MissingVenueAcknowledgementAdapter(PlacementFailClosedAdapter):
    def place_limit_order(self, intent: OrderIntent):
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.RESTING)


class VisibleVenueAcknowledgementAdapter(PlacementFailClosedAdapter):
    def list_open_orders(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [
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

    def place_limit_order(self, intent: OrderIntent):
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.RESTING)


class FilledVenueAcknowledgementAdapter(PlacementFailClosedAdapter):
    def list_fills(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [
            FillSnapshot(
                order_id="placed-1",
                contract=contract,
                action=OrderAction.BUY,
                price=0.50,
                quantity=1.0,
            )
        ]

    def place_limit_order(self, intent: OrderIntent):
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.FILLED)


class UnhealthyHeartbeatAdapter(PlacementFailClosedAdapter):
    def heartbeat_status(self):
        return SimpleNamespace(
            required=True,
            active=True,
            running=False,
            healthy_for_trading=False,
            unhealthy=True,
            last_success_at=None,
            consecutive_failures=2,
            last_error="heartbeat failed",
            last_heartbeat_id="hb-1",
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        raise AssertionError("run_once should block before placing on heartbeat fault")


class PendingCancelAttentionAdapter(PlacementFailClosedAdapter):
    def __init__(self):
        super().__init__()
        self.cancel_attempts = 0

    def list_open_orders(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [
            NormalizedOrder(
                order_id="cancel-1",
                contract=contract,
                action=OrderAction.BUY,
                price=0.50,
                quantity=1.0,
                remaining_quantity=1.0,
                status=OrderStatus.RESTING,
            )
        ]

    def cancel_order(self, order_id: str):
        self.cancel_attempts += 1
        return False

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        raise AssertionError("run_once should block before placing with pending cancel")


class FailClosedTests(unittest.TestCase):
    def test_run_once_blocks_trading_on_incomplete_snapshot(self):
        adapter = IncompleteTruthAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertFalse(result.placements)
        self.assertFalse(result.risk.approved)
        self.assertTrue(result.risk.rejected)
        self.assertIn(
            "venue account snapshot incomplete", result.risk.rejected[0].reason
        )

    def test_run_once_converts_placement_exception_into_rejected_outcome(self):
        adapter = PlacementFailClosedAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(len(result.placements), 1)
        self.assertFalse(result.placements[0].accepted)
        self.assertEqual(result.placements[0].status, OrderStatus.REJECTED)
        self.assertIn("placement exception", result.placements[0].message or "")
        self.assertTrue(engine.safety_state.halted)
        self.assertIn("raised an exception", engine.safety_state.reason or "")

    def test_run_once_halts_on_accepted_without_order_id(self):
        adapter = AcceptedWithoutOrderIdAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)
        status = engine.status_snapshot()

        self.assertEqual(len(result.placements), 1)
        self.assertTrue(result.placements[0].accepted)
        self.assertIsNone(result.placements[0].order_id)
        self.assertFalse(engine.safety_state.halted)
        self.assertEqual(len(engine.pending_submissions(unresolved_only=True)), 1)
        self.assertEqual(
            engine.pending_submissions(unresolved_only=True)[0].status,
            "needs_recovery",
        )
        self.assertEqual(status.overlay_forced_snapshot_count, 1)

    def test_run_once_halts_when_accepted_order_is_not_acknowledged(self):
        adapter = MissingVenueAcknowledgementAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)
        status = engine.status_snapshot()

        self.assertEqual(len(result.placements), 1)
        self.assertEqual(result.placements[0].order_id, "placed-1")
        reconciliation_after = result.reconciliation_after
        self.assertIsNotNone(reconciliation_after)
        if reconciliation_after is None:
            self.fail("expected reconciliation_after")
        self.assertEqual(reconciliation_after.policy.action, "resync")
        self.assertFalse(engine.safety_state.halted)
        self.assertEqual(len(engine.pending_submissions(unresolved_only=True)), 1)
        self.assertEqual(
            engine.pending_submissions(unresolved_only=True)[0].status,
            "needs_recovery",
        )
        self.assertEqual(status.overlay_forced_snapshot_count, 1)

    def test_run_once_does_not_halt_when_accepted_order_is_visible(self):
        adapter = VisibleVenueAcknowledgementAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(len(result.placements), 1)
        self.assertFalse(engine.safety_state.halted)
        reconciliation_after = result.reconciliation_after
        self.assertIsNotNone(reconciliation_after)
        if reconciliation_after is None:
            self.fail("expected reconciliation_after")
        self.assertEqual(reconciliation_after.policy.action, "ok")

    def test_run_once_does_not_halt_when_fill_acknowledges_order(self):
        adapter = FilledVenueAcknowledgementAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertEqual(len(result.placements), 1)
        self.assertFalse(engine.safety_state.halted)
        reconciliation_after = result.reconciliation_after
        self.assertIsNotNone(reconciliation_after)
        if reconciliation_after is None:
            self.fail("expected reconciliation_after")
        self.assertEqual(reconciliation_after.policy.action, "resync")

    def test_run_once_halts_before_placement_when_heartbeat_is_unhealthy(self):
        adapter = UnhealthyHeartbeatAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertFalse(result.placements)
        self.assertFalse(result.risk.approved)
        self.assertTrue(result.risk.rejected)
        self.assertTrue(engine.safety_state.halted)
        self.assertIn("heartbeat unhealthy", result.risk.rejected[0].reason)
        self.assertIn("heartbeat failed", engine.safety_state.reason or "")

    def test_run_once_halts_when_pending_cancel_requires_operator_attention(self):
        adapter = PendingCancelAttentionAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        engine.cancel_retry_interval_seconds = 0.0
        engine.cancel_retry_max_attempts = 1
        engine.cancel_attention_timeout_seconds = 0.0
        engine.track_cancel_request("cancel-1", adapter.contract, "stale cancel")

        result = engine.run_once(adapter.contract, fair_value=0.60)

        self.assertFalse(result.placements)
        self.assertTrue(result.risk.rejected)
        self.assertTrue(engine.safety_state.halted)
        self.assertEqual(adapter.cancel_attempts, 0)
        self.assertIn("operator attention", result.risk.rejected[0].reason)


if __name__ == "__main__":
    unittest.main()

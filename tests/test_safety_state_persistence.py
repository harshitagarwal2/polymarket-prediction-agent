from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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


class PersistedHaltAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self.contract = Contract(
            venue=self.venue, symbol="token-1", outcome=OutcomeSide.YES
        )
        self.phase = 0

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
        quantity = 0.0 if self.phase == 0 else 10.0
        return [PositionSnapshot(contract=contract, quantity=quantity)]

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract):
        return self.list_positions(contract)[0]

    def get_balance(self):
        available = 100.0 if self.phase == 0 else 95.0
        return BalanceSnapshot(venue=self.venue, available=available, total=available)

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=self.list_positions(contract),
            open_orders=[],
            fills=[],
        )

    def place_limit_order(self, intent: OrderIntent):
        self.phase = 1
        return PlacementResult(True, order_id="placed-1", status=OrderStatus.RESTING)

    def cancel_order(self, order_id: str):
        return True

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


class PersistedUnexpectedFillAdapter(PersistedHaltAdapter):
    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        fills = []
        if self.phase > 0:
            fills = [
                FillSnapshot(
                    order_id="venue-fill-order",
                    contract=contract,
                    action=OrderAction.BUY,
                    price=0.41,
                    quantity=1.0,
                    fill_id="venue-fill-1",
                )
            ]
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=self.list_positions(contract),
            open_orders=[],
            fills=fills,
        )


class PendingCancelResolutionAdapter(PersistedHaltAdapter):
    def __init__(self):
        super().__init__()
        self.order_visible = True
        self.cancel_calls = 0

    def list_open_orders(self, contract: Contract | None = None):
        contract = contract or self.contract
        if not self.order_visible:
            return []
        return [
            NormalizedOrder(
                order_id="cancel-1",
                contract=contract,
                action=OrderAction.BUY,
                price=0.5,
                quantity=1.0,
                remaining_quantity=1.0,
                status=OrderStatus.RESTING,
            )
        ]

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=self.list_positions(contract),
            open_orders=self.list_open_orders(contract),
            fills=[],
        )

    def cancel_order(self, order_id: str):
        self.cancel_calls += 1
        return True


class PendingSubmissionPersistenceAdapter(PersistedHaltAdapter):
    def __init__(self):
        super().__init__()
        self.place_calls = 0

    def place_limit_order(self, intent: OrderIntent):
        self.place_calls += 1
        raise RuntimeError("timeout after send")


class SafetyStatePersistenceTests(unittest.TestCase):
    def test_halt_persists_across_restart(self):
        adapter = PersistedHaltAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            engine.run_once(adapter.contract, fair_value=0.60)

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            self.assertTrue(state_path.exists())
            self.assertTrue(restarted.safety_state.halted)
            self.assertEqual(
                restarted.safety_state.contract_key, adapter.contract.market_key
            )
            self.assertIn(
                "Position drift detected", restarted.safety_state.reason or ""
            )

    def test_clear_halt_persists_clean_state(self):
        adapter = PersistedHaltAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            engine.run_once(adapter.contract, fair_value=0.60)
            engine.clear_halt()

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            self.assertFalse(restarted.safety_state.halted)
            self.assertIsNone(restarted.safety_state.reason)

    def test_truth_summary_persists_across_restart(self):
        adapter = PersistedHaltAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            engine.preview_once(adapter.contract, fair_value=0.60)

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            self.assertTrue(restarted.safety_state.last_truth_complete)
            self.assertEqual(restarted.safety_state.last_truth_positions, 1)
            self.assertEqual(restarted.safety_state.last_truth_open_orders, 0)
            self.assertEqual(restarted.safety_state.last_truth_open_order_notional, 0.0)
            self.assertEqual(
                restarted.safety_state.last_truth_reserved_buy_notional, 0.0
            )
            self.assertIsNotNone(restarted.safety_state.last_truth_observed_at)

    def test_pending_cancel_persists_across_restart(self):
        adapter = PersistedHaltAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            engine.track_cancel_request(
                "placed-1", adapter.contract, "operator cancel all"
            )

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            self.assertEqual(len(restarted.safety_state.pending_cancels), 1)
            self.assertEqual(
                restarted.safety_state.pending_cancels[0].order_id,
                "placed-1",
            )

    def test_pending_submission_persists_across_restart(self):
        adapter = PendingSubmissionPersistenceAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            engine.run_once(adapter.contract, fair_value=0.60)

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            self.assertEqual(
                len(restarted.pending_submissions(unresolved_only=True)), 1
            )
            self.assertEqual(
                restarted.pending_submissions(unresolved_only=True)[0].status,
                "needs_recovery",
            )
            self.assertEqual(
                restarted.pending_submissions(unresolved_only=True)[0].order_id,
                None,
            )

    def test_pending_cancel_resolves_when_order_disappears(self):
        adapter = PendingCancelResolutionAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            order = adapter.list_open_orders(adapter.contract)[0]
            engine.request_cancel_order(order, "operator cancel all")
            adapter.order_visible = False

            engine.observe_polled_snapshot(
                adapter.get_account_snapshot(adapter.contract)
            )

            self.assertFalse(engine.safety_state.pending_cancels)
            self.assertNotIn("cancel-1", engine.order_state.pending_cancel_ids)

    def test_resume_reconciles_against_persisted_truth_before_sync(self):
        adapter = PersistedUnexpectedFillAdapter()
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            engine = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            engine.preview_once(adapter.contract, fair_value=0.60)
            engine.halt("manual resume required", adapter.contract)
            adapter.phase = 1

            restarted = TradingEngine(
                adapter=adapter,
                strategy=FairValueBandStrategy(quantity=10, edge_threshold=0.03),
                risk_engine=RiskEngine(
                    RiskLimits(max_contracts_per_market=20, max_global_contracts=20)
                ),
                safety_state_path=state_path,
            )

            report = restarted.try_resume(adapter.contract)

            self.assertEqual(report.policy.action, "halt")
            self.assertTrue(restarted.safety_state.halted)
            self.assertIn(
                "Venue fill venue-fill-1", restarted.safety_state.reason or ""
            )
            self.assertFalse(restarted.account_state.fills)


if __name__ == "__main__":
    unittest.main()

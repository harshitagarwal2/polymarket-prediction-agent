from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from adapters.base import AdapterHealth
from adapters.polymarket import HeartbeatStatus
from adapters.polymarket import LiveStateStatus
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    NormalizedOrder,
    OrderBookSnapshot,
    OrderAction,
    OrderStatus,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from adapters import MarketSummary
from engine.discovery import (
    AgentOrchestrator,
    OpportunityRanker,
    PairOpportunityRanker,
    PollingAgentLoop,
    PollingLoopConfig,
    StaticFairValueProvider,
)
from engine import OrderLifecycleManager, OrderLifecyclePolicy
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits


class LoopAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self.contract = Contract(
            venue=self.venue, symbol="token-1", outcome=OutcomeSide.YES
        )
        self.placed = 0
        self._acknowledged_orders: list[NormalizedOrder] = []

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return [
            MarketSummary(
                contract=self.contract,
                title="Loop market",
                best_bid=0.45,
                best_ask=0.50,
                active=True,
            )
        ]

    def get_order_book(self, contract: Contract):
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=10)],
        )

    def list_open_orders(self, contract: Contract | None = None):
        return list(self._acknowledged_orders)

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
        open_orders = self.list_open_orders(contract)
        self._acknowledged_orders = []
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=open_orders,
            fills=[],
        )

    def place_limit_order(self, intent):
        self.placed += 1
        self._acknowledged_orders = [
            NormalizedOrder(
                order_id=f"placed-{self.placed}",
                contract=intent.contract,
                action=intent.action,
                price=intent.price,
                quantity=intent.quantity,
                remaining_quantity=intent.quantity,
                status=OrderStatus.RESTING,
            )
        ]
        return PlacementResult(
            True, order_id=f"placed-{self.placed}", status=OrderStatus.RESTING
        )

    def cancel_order(self, order_id: str):
        return True

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


class StaleOrderLoopAdapter(LoopAdapter):
    def __init__(self):
        super().__init__()
        now = datetime.now(timezone.utc)
        self._open_orders = [
            NormalizedOrder(
                order_id="stale-1",
                contract=self.contract,
                action=OrderAction.BUY,
                price=0.49,
                quantity=1.0,
                remaining_quantity=1.0,
                status=OrderStatus.RESTING,
                created_at=now - timedelta(seconds=120),
                updated_at=now - timedelta(seconds=120),
            )
        ]
        self.cancelled: list[str] = []

    def list_open_orders(self, contract: Contract | None = None):
        return list(self._open_orders)

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=self.list_open_orders(contract),
            fills=[],
        )

    def cancel_order(self, order_id: str):
        self.cancelled.append(order_id)
        return True


class IncompleteTruthLoopAdapter(LoopAdapter):
    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=[],
            fills=[],
            complete=False,
            issues=["account truth unavailable"],
        )


class HeartbeatLoopAdapter(LoopAdapter):
    def __init__(self, *, healthy_for_trading: bool, unhealthy: bool = False):
        super().__init__()
        self.start_calls = 0
        self.stop_calls = 0
        self._healthy_for_trading = healthy_for_trading
        self._unhealthy = unhealthy
        self._heartbeat_status = HeartbeatStatus(
            supported=True,
            required=False,
            active=False,
            running=False,
            healthy_for_trading=True,
            unhealthy=False,
            last_success_at=None,
            consecutive_failures=0,
            last_error=None,
            last_heartbeat_id=None,
        )

    def start_heartbeat(self):
        self.start_calls += 1
        self._heartbeat_status = HeartbeatStatus(
            supported=True,
            required=True,
            active=True,
            running=True,
            healthy_for_trading=self._healthy_for_trading,
            unhealthy=self._unhealthy,
            last_success_at=datetime.now(timezone.utc)
            if self._healthy_for_trading
            else None,
            consecutive_failures=2 if self._unhealthy else 0,
            last_error="heartbeat unhealthy" if self._unhealthy else None,
            last_heartbeat_id="hb-1" if self._healthy_for_trading else None,
        )
        return self._heartbeat_status

    def stop_heartbeat(self):
        self.stop_calls += 1
        self._heartbeat_status = HeartbeatStatus(
            supported=True,
            required=False,
            active=False,
            running=False,
            healthy_for_trading=True,
            unhealthy=self._unhealthy,
            last_success_at=self._heartbeat_status.last_success_at,
            consecutive_failures=self._heartbeat_status.consecutive_failures,
            last_error=self._heartbeat_status.last_error,
            last_heartbeat_id=self._heartbeat_status.last_heartbeat_id,
        )
        return self._heartbeat_status

    def heartbeat_status(self):
        return self._heartbeat_status


class PairLoopAdapter(LoopAdapter):
    def __init__(self):
        super().__init__()
        self.no_contract = Contract(
            venue=self.venue, symbol="token-2", outcome=OutcomeSide.NO
        )
        self.last_quantities: list[float] = []

    def list_markets(self, limit: int = 100):
        return [
            MarketSummary(
                contract=self.contract,
                title="Pair market",
                best_bid=0.44,
                best_ask=0.47,
                event_key="event-1",
                active=True,
            ),
            MarketSummary(
                contract=self.no_contract,
                title="Pair market",
                best_bid=0.44,
                best_ask=0.48,
                event_key="event-1",
                active=True,
            ),
        ]

    def get_account_snapshot(self, contract: Contract | None = None):
        open_orders = self.list_open_orders(contract)
        self._acknowledged_orders = []
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[
                PositionSnapshot(contract=self.contract, quantity=0.0),
                PositionSnapshot(contract=self.no_contract, quantity=0.0),
            ],
            open_orders=open_orders,
            fills=[],
        )

    def place_limit_order(self, intent):
        self.last_quantities.append(intent.quantity)
        return super().place_limit_order(intent)


class LiveStateLoopAdapter(LoopAdapter):
    def __init__(self):
        super().__init__()
        self.start_live_calls = 0
        self.stop_live_calls = 0
        self.start_market_calls = 0
        self.stop_market_calls = 0
        self._live_state_status = LiveStateStatus(
            supported=True,
            active=False,
            running=False,
            mode="inactive",
            initialized=False,
            fresh=False,
            last_update_at=None,
            last_error=None,
            subscribed_markets=("condition-1",),
        )

    def start_live_user_state(self):
        self.start_live_calls += 1
        self._live_state_status = LiveStateStatus(
            supported=True,
            active=True,
            running=True,
            mode="healthy",
            initialized=True,
            fresh=True,
            last_update_at=datetime.now(timezone.utc),
            last_error=None,
            subscribed_markets=("condition-1",),
        )
        return self._live_state_status

    def stop_live_user_state(self):
        self.stop_live_calls += 1
        self._live_state_status = LiveStateStatus(
            supported=True,
            active=False,
            running=False,
            mode="inactive",
            initialized=self._live_state_status.initialized,
            fresh=False,
            last_update_at=self._live_state_status.last_update_at,
            last_error=self._live_state_status.last_error,
            subscribed_markets=self._live_state_status.subscribed_markets,
        )
        return self._live_state_status

    def live_state_status(self):
        return self._live_state_status

    def start_live_market_state(self):
        self.start_market_calls += 1
        return SimpleNamespace(
            active=True,
            running=True,
            mode="healthy",
            fresh=True,
            subscribed_assets=("token-1",),
        )

    def stop_live_market_state(self):
        self.stop_market_calls += 1
        return SimpleNamespace(
            active=False,
            running=False,
            mode="inactive",
            fresh=False,
            subscribed_assets=(),
        )


class PollingLoopTests(unittest.TestCase):
    def test_preview_mode_runs_multiple_cycles_without_placing(self):
        adapter = LoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="preview", market_limit=10, interval_seconds=0, max_cycles=3
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 3)
        self.assertEqual(adapter.placed, 0)

    def test_forced_authoritative_refresh_skips_sleep_backoff(self):
        adapter = LoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        sleeps: list[float] = []
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="preview", market_limit=10, interval_seconds=5.0, max_cycles=2
            ),
            sleep_fn=sleeps.append,
        )

        engine.request_authoritative_refresh("test refresh")
        loop.run()

        self.assertEqual(sleeps, [0.0])

    def test_run_mode_places_in_each_cycle(self):
        adapter = LoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="run", market_limit=10, interval_seconds=0, max_cycles=2
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 2)
        self.assertEqual(adapter.placed, 2)

    def test_pair_run_mode_places_both_legs(self):
        adapter = PairLoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider({}),
            ranker=OpportunityRanker(edge_threshold=0.03),
            pair_ranker=PairOpportunityRanker(edge_threshold=0.01),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="pair-run", market_limit=10, interval_seconds=0, max_cycles=1
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 1)
        self.assertEqual(adapter.placed, 2)

    def test_pair_run_mode_uses_configured_quantity(self):
        adapter = PairLoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider({}),
            ranker=OpportunityRanker(edge_threshold=0.03),
            pair_ranker=PairOpportunityRanker(edge_threshold=0.01),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="pair-run",
                market_limit=10,
                interval_seconds=0,
                max_cycles=1,
                quantity=2.5,
            ),
            sleep_fn=lambda _: None,
        )

        loop.run()

        self.assertEqual(adapter.last_quantities, [2.5, 2.5])

    def test_pause_skips_new_scan_cycles(self):
        adapter = LoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        engine.pause("operator pause")
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="run", market_limit=10, interval_seconds=0, max_cycles=2
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 2)
        self.assertEqual(adapter.placed, 0)
        self.assertEqual(results[0].markets, [])

    def test_loop_cancels_stale_orders_before_cycle(self):
        adapter = StaleOrderLoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        lifecycle_manager = OrderLifecycleManager(
            adapter=adapter,
            policy=OrderLifecyclePolicy(max_order_age_seconds=30),
            cancel_handler=engine.request_cancel_order,
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="preview", market_limit=10, interval_seconds=0, max_cycles=1
            ),
            sleep_fn=lambda _: None,
            lifecycle_manager=lifecycle_manager,
        )

        loop.run()

        self.assertEqual(adapter.cancelled, ["stale-1"])
        self.assertEqual(len(engine.safety_state.pending_cancels), 1)
        self.assertEqual(engine.safety_state.pending_cancels[0].order_id, "stale-1")
        self.assertFalse(engine.safety_state.pending_cancels[0].acknowledged)

    def test_loop_blocks_scan_when_account_truth_incomplete(self):
        adapter = IncompleteTruthLoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="run", market_limit=10, interval_seconds=0, max_cycles=1
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 1)
        self.assertEqual(adapter.placed, 0)
        self.assertFalse(results[0].policy_allowed)
        self.assertIn("incomplete account truth", results[0].policy_reasons[0])

    def test_run_mode_starts_and_stops_heartbeat(self):
        adapter = HeartbeatLoopAdapter(healthy_for_trading=True)
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="run", market_limit=10, interval_seconds=0, max_cycles=1
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 1)
        self.assertEqual(adapter.start_calls, 1)
        self.assertGreaterEqual(adapter.stop_calls, 1)
        self.assertEqual(adapter.placed, 1)

    def test_run_mode_starts_and_stops_live_user_state(self):
        adapter = LiveStateLoopAdapter()
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="run", market_limit=10, interval_seconds=0, max_cycles=1
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 1)
        self.assertEqual(adapter.start_live_calls, 1)
        self.assertGreaterEqual(adapter.stop_live_calls, 1)
        self.assertEqual(adapter.start_market_calls, 1)
        self.assertGreaterEqual(adapter.stop_market_calls, 1)
        self.assertEqual(adapter.placed, 1)

    def test_run_mode_blocks_when_heartbeat_is_unhealthy(self):
        adapter = HeartbeatLoopAdapter(healthy_for_trading=False, unhealthy=True)
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider(
                {adapter.contract.market_key: 0.60}
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode="run", market_limit=10, interval_seconds=0, max_cycles=1
            ),
            sleep_fn=lambda _: None,
        )

        results = loop.run()

        self.assertEqual(len(results), 1)
        self.assertEqual(adapter.placed, 0)
        self.assertTrue(engine.safety_state.halted)
        self.assertFalse(results[0].policy_allowed)
        self.assertIn("heartbeat unhealthy", results[0].policy_reasons[0])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
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
    DeterministicSizer,
    ExecutionPolicyGate,
    OpportunityRanker,
    StaticFairValueProvider,
)
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits


class OrchestratorAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self.contract = Contract(
            venue=self.venue, symbol="token-1", outcome=OutcomeSide.YES
        )
        self.placed = 0

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return [
            MarketSummary(
                contract=self.contract,
                title="Demo market",
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

    def place_limit_order(self, intent):
        self.placed += 1
        return PlacementResult(
            True, order_id=f"placed-{self.placed}", status=OrderStatus.RESTING
        )

    def cancel_order(self, order_id: str):
        return True

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


class ExistingPositionAdapter(OrchestratorAdapter):
    def get_position(self, contract: Contract):
        return PositionSnapshot(contract=contract, quantity=1.0)

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=1.0)],
            open_orders=[],
            fills=[],
        )


class ThinLiquidityAdapter(OrchestratorAdapter):
    def get_order_book(self, contract: Contract):
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=0.25)],
        )


class MultiLevelLiquidityAdapter(OrchestratorAdapter):
    def get_order_book(self, contract: Contract):
        return OrderBookSnapshot(
            contract=contract,
            bids=[
                PriceLevel(price=0.45, quantity=0.5),
                PriceLevel(price=0.44, quantity=0.5),
                PriceLevel(price=0.43, quantity=1.0),
            ],
            asks=[
                PriceLevel(price=0.50, quantity=0.25),
                PriceLevel(price=0.51, quantity=0.50),
                PriceLevel(price=0.52, quantity=1.25),
            ],
        )


class ExistingOrderAdapter(OrchestratorAdapter):
    def list_open_orders(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [
            NormalizedOrder(
                order_id="existing-order",
                contract=contract,
                action=OrderAction.BUY,
                price=0.49,
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
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=self.list_open_orders(contract),
            fills=[],
        )


class PartialFillAdapter(ExistingOrderAdapter):
    def list_fills(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [
            FillSnapshot(
                order_id="existing-order",
                contract=contract,
                action=OrderAction.BUY,
                price=0.49,
                quantity=0.4,
            )
        ]

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=self.list_open_orders(contract),
            fills=self.list_fills(contract),
        )


class FallbackCandidateAdapter(OrchestratorAdapter):
    def __init__(self):
        super().__init__()
        self.second_contract = Contract(
            venue=self.venue, symbol="token-2", outcome=OutcomeSide.YES
        )

    def list_markets(self, limit: int = 100):
        return [
            MarketSummary(
                contract=self.contract,
                title="Blocked top market",
                best_bid=0.45,
                best_ask=0.50,
                active=True,
            ),
            MarketSummary(
                contract=self.second_contract,
                title="Fallback market",
                best_bid=0.44,
                best_ask=0.52,
                active=True,
            ),
        ]

    def get_order_book(self, contract: Contract):
        ask_price = 0.50 if contract.market_key == self.contract.market_key else 0.52
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=ask_price, quantity=10)],
        )

    def list_open_orders(self, contract: Contract | None = None):
        contract = contract or self.contract
        if contract.market_key != self.contract.market_key:
            return []
        return [
            NormalizedOrder(
                order_id="existing-order",
                contract=contract,
                action=OrderAction.BUY,
                price=0.49,
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
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=self.list_open_orders(contract),
            fills=self.list_fills(contract),
        )


class OrchestratorTests(unittest.TestCase):
    def test_preview_top_selects_and_previews_best_candidate(self):
        adapter = OrchestratorAdapter()
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

        result = orchestrator.preview_top()

        self.assertIsNotNone(result.selected)
        self.assertIsNotNone(result.execution)
        selected = result.selected
        execution = result.execution
        if selected is None or execution is None:
            self.fail("expected selected candidate and execution preview")
        self.assertEqual(selected.contract.market_key, adapter.contract.market_key)
        self.assertTrue(execution.risk.approved)

    def test_preview_top_applies_deterministic_sizing(self):
        adapter = OrchestratorAdapter()
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
            sizer=DeterministicSizer(
                base_quantity=2.0,
                max_quantity=2.0,
                edge_unit=0.03,
                liquidity_fraction=1.0,
            ),
        )

        result = orchestrator.preview_top()

        execution = result.execution
        if execution is None:
            self.fail("expected execution preview")
        self.assertTrue(execution.risk.approved)
        self.assertEqual(execution.risk.approved[0].quantity, 2.0)

    def test_preview_top_falls_back_to_next_executable_candidate(self):
        adapter = FallbackCandidateAdapter()
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
                {
                    adapter.contract.market_key: 0.60,
                    adapter.second_contract.market_key: 0.60,
                }
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )

        result = orchestrator.preview_top()

        self.assertTrue(result.policy_allowed)
        self.assertIsNotNone(result.selected)
        if result.selected is None:
            self.fail("expected selected candidate")
        self.assertEqual(
            result.selected.contract.market_key,
            adapter.second_contract.market_key,
        )
        self.assertEqual(
            result.skipped_candidates[0]["market_key"], adapter.contract.market_key
        )
        self.assertTrue(result.skipped_candidates[0]["reasons"])

    def test_sizer_uses_multi_level_visible_depth(self):
        adapter = MultiLevelLiquidityAdapter()
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
            sizer=DeterministicSizer(
                base_quantity=2.0,
                max_quantity=2.0,
                edge_unit=0.03,
                liquidity_fraction=1.0,
                depth_levels=3,
            ),
        )

        result = orchestrator.preview_top()

        execution = result.execution
        if execution is None:
            self.fail("expected execution preview")
        self.assertTrue(execution.risk.approved)
        self.assertEqual(execution.risk.approved[0].quantity, 2.0)

    def test_run_top_places_order_for_top_candidate(self):
        adapter = OrchestratorAdapter()
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

        result = orchestrator.run_top()

        self.assertEqual(adapter.placed, 1)
        self.assertIsNotNone(result.execution)
        execution = result.execution
        if execution is None:
            self.fail("expected execution result")
        self.assertTrue(execution.placements)

    def test_policy_gate_blocks_duplicate_position_entry(self):
        adapter = ExistingPositionAdapter()
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
            policy_gate=ExecutionPolicyGate(),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(result.policy_reasons)
        self.assertIn("existing position already open", result.policy_reasons[0])
        self.assertEqual(adapter.placed, 0)

    def test_run_top_falls_back_to_next_candidate_when_top_candidate_blocked(self):
        adapter = FallbackCandidateAdapter()
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
                {
                    adapter.contract.market_key: 0.60,
                    adapter.second_contract.market_key: 0.60,
                }
            ),
            ranker=OpportunityRanker(edge_threshold=0.03),
        )

        result = orchestrator.run_top()

        self.assertEqual(adapter.placed, 1)
        self.assertTrue(result.policy_allowed)
        self.assertIsNotNone(result.selected)
        if result.selected is None:
            self.fail("expected selected candidate")
        self.assertEqual(
            result.selected.contract.market_key,
            adapter.second_contract.market_key,
        )
        self.assertEqual(
            result.skipped_candidates[0]["market_key"], adapter.contract.market_key
        )
        self.assertTrue(result.skipped_candidates[0]["reasons"])

    def test_policy_gate_blocks_thin_liquidity(self):
        adapter = ThinLiquidityAdapter()
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
            policy_gate=ExecutionPolicyGate(min_top_level_liquidity=1.0),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(result.policy_reasons)
        self.assertIn("top-level liquidity too low", result.policy_reasons[0])
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_visible_depth_too_thin(self):
        adapter = MultiLevelLiquidityAdapter()
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
            policy_gate=ExecutionPolicyGate(
                min_top_level_liquidity=0.0,
                depth_levels_for_liquidity=2,
                max_visible_liquidity_consumption=1.0,
            ),
            sizer=DeterministicSizer(
                base_quantity=1.0,
                max_quantity=1.0,
                edge_unit=0.03,
                liquidity_fraction=1.0,
                depth_levels=3,
            ),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any("visible depth too thin" in reason for reason in result.policy_reasons)
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_open_order_limit_reached(self):
        adapter = ExistingOrderAdapter()
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
            policy_gate=ExecutionPolicyGate(max_open_orders_per_contract=1),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "open-order count limit reached" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_capital_at_risk_limit_reached(self):
        adapter = ExistingPositionAdapter()
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
            policy_gate=ExecutionPolicyGate(
                prevent_same_side_duplicate=False,
                max_contract_capital_at_risk=0.55,
            ),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(result.policy_reasons)
        self.assertIn("capital-at-risk limit reached", result.policy_reasons[0])
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_uses_sized_intent_notional_for_capital_at_risk(self):
        adapter = ExistingPositionAdapter()
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
            sizer=DeterministicSizer(
                base_quantity=2.0,
                max_quantity=2.0,
                edge_unit=0.03,
                liquidity_fraction=1.0,
            ),
            policy_gate=ExecutionPolicyGate(
                prevent_same_side_duplicate=False,
                max_contract_capital_at_risk=1.40,
            ),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "capital-at-risk limit reached" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_global_open_order_limit_reached(self):
        adapter = ExistingOrderAdapter()
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
            policy_gate=ExecutionPolicyGate(max_open_orders_global=1),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "global open-order count limit reached" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_global_open_order_notional_limit_reached(self):
        adapter = ExistingOrderAdapter()
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
            policy_gate=ExecutionPolicyGate(
                prevent_same_side_duplicate=False,
                max_global_open_order_notional=0.60,
            ),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "global open-order notional limit reached" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_uses_sized_intent_notional_for_global_open_orders(self):
        adapter = ExistingOrderAdapter()
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
            sizer=DeterministicSizer(
                base_quantity=2.0,
                max_quantity=2.0,
                edge_unit=0.03,
                liquidity_fraction=1.0,
            ),
            policy_gate=ExecutionPolicyGate(
                prevent_same_side_duplicate=False,
                max_global_open_order_notional=1.40,
            ),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "global open-order notional limit reached" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_contract_has_partial_fill(self):
        adapter = PartialFillAdapter()
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
            policy_gate=ExecutionPolicyGate(block_on_contract_partial_fills=True),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "unresolved partial fills exist for candidate contract" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)

    def test_policy_gate_blocks_when_global_partial_fill_limit_reached(self):
        adapter = PartialFillAdapter()
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
            policy_gate=ExecutionPolicyGate(
                block_on_contract_partial_fills=False,
                max_partial_fills_global=1,
            ),
        )

        result = orchestrator.run_top()

        self.assertFalse(result.policy_allowed)
        self.assertTrue(
            any(
                "global partial-fill limit reached" in reason
                for reason in result.policy_reasons
            )
        )
        self.assertEqual(adapter.placed, 0)


if __name__ == "__main__":
    unittest.main()

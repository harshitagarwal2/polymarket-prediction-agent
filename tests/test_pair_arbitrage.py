from __future__ import annotations

import unittest

from adapters import MarketSummary
from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    OrderBookSnapshot,
    OrderStatus,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from engine.discovery import (
    AgentOrchestrator,
    PairOpportunityRanker,
    StaticFairValueProvider,
)
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits


class PairArbAdapter:
    venue = Venue.POLYMARKET

    def __init__(self, *, second_leg_accepts: bool = True):
        self.yes_contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-yes",
            outcome=OutcomeSide.YES,
        )
        self.no_contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-no",
            outcome=OutcomeSide.NO,
        )
        self.second_leg_accepts = second_leg_accepts
        self.placements: list[str] = []

    def health(self) -> AdapterHealth:
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100) -> list[MarketSummary]:
        return [
            MarketSummary(
                contract=self.yes_contract,
                title="Will home team win?",
                best_bid=0.44,
                best_ask=0.47,
                event_key="event-1",
                active=True,
            ),
            MarketSummary(
                contract=self.no_contract,
                title="Will home team win?",
                best_bid=0.44,
                best_ask=0.48,
                event_key="event-1",
                active=True,
            ),
        ]

    def get_order_book(self, contract: Contract) -> OrderBookSnapshot:
        ask = 0.47 if contract.market_key == self.yes_contract.market_key else 0.48
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.44, quantity=10.0)],
            asks=[PriceLevel(price=ask, quantity=10.0)],
        )

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        return AccountSnapshot(
            venue=self.venue,
            balance=BalanceSnapshot(venue=self.venue, available=100.0, total=100.0),
            positions=[
                PositionSnapshot(contract=self.yes_contract, quantity=0.0),
                PositionSnapshot(contract=self.no_contract, quantity=0.0),
            ],
            open_orders=[],
            fills=[],
        )

    def get_balance(self) -> BalanceSnapshot:
        return BalanceSnapshot(venue=self.venue, available=100.0, total=100.0)

    def list_positions(self, contract: Contract | None = None):
        return self.get_account_snapshot(contract).positions

    def list_open_orders(self, contract: Contract | None = None):
        return []

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract) -> PositionSnapshot:
        return PositionSnapshot(contract=contract, quantity=0.0)

    def place_limit_order(self, intent) -> PlacementResult:
        self.placements.append(intent.contract.market_key)
        if (
            not self.second_leg_accepts
            and intent.contract.market_key == self.no_contract.market_key
        ):
            return PlacementResult(
                False,
                status=OrderStatus.REJECTED,
                message="second leg rejected",
            )
        return PlacementResult(
            True,
            order_id=f"placed-{len(self.placements)}",
            status=OrderStatus.RESTING,
        )

    def cancel_order(self, order_id: str) -> bool:
        return True

    def cancel_all(self, contract: Contract | None = None) -> int:
        return 0

    def close(self) -> None:
        return None


class PairArbitrageTests(unittest.TestCase):
    def _orchestrator(self, adapter: PairArbAdapter) -> AgentOrchestrator:
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(quantity=1.0, edge_threshold=0.03),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )
        return AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=StaticFairValueProvider({}),
            pair_ranker=PairOpportunityRanker(edge_threshold=0.01),
        )

    def test_preview_best_pair_returns_two_leg_candidate(self):
        adapter = PairArbAdapter()
        orchestrator = self._orchestrator(adapter)

        result = orchestrator.preview_best_pair(quantity=1.0)

        self.assertIsNotNone(result.selected)
        self.assertTrue(result.policy_allowed)
        self.assertEqual(len(result.intents), 2)
        self.assertEqual(len(result.risk.approved if result.risk else []), 2)

    def test_run_best_pair_places_both_legs(self):
        adapter = PairArbAdapter()
        orchestrator = self._orchestrator(adapter)

        result = orchestrator.run_best_pair(quantity=1.0)

        self.assertEqual(len(result.placements), 2)
        self.assertTrue(all(placement.accepted for placement in result.placements))
        self.assertEqual(adapter.placements, ["token-yes:yes", "token-no:no"])
        pending_submissions = orchestrator.engine.pending_submissions(
            unresolved_only=True
        )
        self.assertEqual(len(pending_submissions), 2)
        self.assertTrue(
            all(
                submission.status == "needs_recovery"
                for submission in pending_submissions
            )
        )
        pair_ids = {submission.pair_id for submission in pending_submissions}
        self.assertEqual(len(pair_ids), 1)
        pair_id = next(iter(pair_ids))
        self.assertIsNotNone(pair_id)
        self.assertTrue(
            any(
                item.item_type == "pair-submit-uncertain" and item.scope == pair_id
                for item in orchestrator.engine.recovery_items(open_only=True)
            )
        )

    def test_run_best_pair_halts_when_second_leg_fails(self):
        adapter = PairArbAdapter(second_leg_accepts=False)
        orchestrator = self._orchestrator(adapter)

        result = orchestrator.run_best_pair(quantity=1.0)

        self.assertEqual(len(result.placements), 2)
        self.assertFalse(result.placements[1].accepted)
        self.assertTrue(orchestrator.engine.status_snapshot().halted)

    def test_pair_ranker_respects_sports_filters(self):
        adapter = PairArbAdapter()
        crypto_yes = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="crypto-yes", outcome=OutcomeSide.YES
            ),
            title="Crypto pair",
            best_bid=0.40,
            best_ask=0.44,
            category="crypto",
            active=True,
        )
        crypto_no = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="crypto-no", outcome=OutcomeSide.NO
            ),
            title="Crypto pair",
            best_bid=0.40,
            best_ask=0.45,
            category="crypto",
            active=True,
        )
        markets = adapter.list_markets() + [crypto_yes, crypto_no]

        candidates = PairOpportunityRanker(
            edge_threshold=0.01,
            allowed_categories=("sports", "event-1"),
        ).rank(markets)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].market_key, "event-1")


if __name__ == "__main__":
    unittest.main()

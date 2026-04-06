from __future__ import annotations

import unittest

from adapters import MarketSummary
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import Contract, OrderAction, OutcomeSide, Venue
from engine.discovery import OpportunityRanker, StaticFairValueProvider


class FakePolymarketClient:
    def get_simplified_markets(self):
        return {
            "data": [
                {
                    "question": "Will BTC be above 100k?",
                    "category": "crypto",
                    "active": True,
                    "volume": 1234,
                    "tokens": [
                        {
                            "token_id": "yes-token",
                            "outcome": "YES",
                            "best_bid": 0.45,
                            "best_ask": 0.50,
                        },
                        {
                            "token_id": "no-token",
                            "outcome": "NO",
                            "best_bid": 0.48,
                            "best_ask": 0.53,
                        },
                    ],
                }
            ]
        }


class FakePolymarketAdapter(PolymarketAdapter):
    def _ensure_client(self):
        return FakePolymarketClient()


class DiscoveryTests(unittest.TestCase):
    def test_polymarket_list_markets_parses_tokens(self):
        adapter = FakePolymarketAdapter(PolymarketConfig())

        markets = adapter.list_markets(limit=10)

        self.assertEqual(len(markets), 2)
        self.assertEqual(markets[0].contract.symbol, "yes-token")
        self.assertEqual(markets[0].contract.outcome, OutcomeSide.YES)
        self.assertEqual(markets[1].contract.outcome, OutcomeSide.NO)

    def test_opportunity_ranker_ranks_best_edge_first(self):
        markets = [
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
                ),
                title="A",
                best_bid=0.45,
                best_ask=0.50,
                active=True,
            ),
            MarketSummary(
                contract=Contract(
                    venue=Venue.POLYMARKET, symbol="token-b", outcome=OutcomeSide.YES
                ),
                title="B",
                best_bid=0.40,
                best_ask=0.60,
                active=True,
            ),
        ]
        provider = StaticFairValueProvider(
            fair_values={
                "token-a:yes": 0.60,
                "token-b:yes": 0.75,
            }
        )

        candidates = OpportunityRanker(edge_threshold=0.03).rank(markets, provider)

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0].contract.symbol, "token-b")
        self.assertEqual(candidates[0].action, OrderAction.BUY)
        self.assertGreater(candidates[0].score, candidates[1].score)


if __name__ == "__main__":
    unittest.main()

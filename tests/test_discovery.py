from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from adapters import MarketSummary
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import Contract, OrderAction, OutcomeSide, Venue
from engine.discovery import (
    FairValueManifestEntry,
    ManifestFairValueProvider,
    OpportunityRanker,
    StaticFairValueProvider,
)


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
                    "series": "crypto",
                    "sport": "crypto",
                    "tags": "btc,macro",
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
        self.assertEqual(markets[0].series, "crypto")
        self.assertEqual(markets[0].sport, "crypto")
        self.assertEqual(markets[0].tags, ("btc", "macro"))

    def test_polymarket_list_markets_parses_json_encoded_sports_tokens(self):
        class JsonSportsClient:
            def get_simplified_markets(self):
                return {
                    "data": [
                        {
                            "question": "Who wins?",
                            "category": "sports",
                            "sport": "nba",
                            "series": "nba",
                            "sportsMarketType": "moneyline",
                            "gameStartTime": "2026-04-07T19:00:00Z",
                            "gameId": "game-1",
                            "slug": "nba-finals-game-1",
                            "tags": "games, nba",
                            "conditionId": "condition-1",
                            "outcomes": '["Yes", "No"]',
                            "outcomePrices": '["0.61", "0.39"]',
                            "clobTokenIds": '["token-yes", "token-no"]',
                            "active": True,
                        }
                    ]
                }

        class JsonSportsAdapter(PolymarketAdapter):
            def _ensure_client(self):
                return JsonSportsClient()

        adapter = JsonSportsAdapter(PolymarketConfig())

        markets = adapter.list_markets(limit=10)

        self.assertEqual(len(markets), 2)
        self.assertEqual(markets[0].contract.symbol, "token-yes")
        self.assertEqual(markets[0].sport, "nba")
        self.assertEqual(markets[0].series, "nba")
        self.assertEqual(markets[0].event_key, "nba-finals-game-1")
        self.assertEqual(markets[0].game_id, "game-1")
        self.assertEqual(markets[0].sports_market_type, "moneyline")
        self.assertEqual(markets[0].tags, ("games", "nba"))
        self.assertEqual(markets[0].midpoint, 0.61)
        self.assertEqual(markets[0].contract.outcome, OutcomeSide.YES)

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

    def test_opportunity_ranker_filters_to_sports_volume_and_expiry_window(self):
        now = datetime.now(timezone.utc)
        sports_contract = Contract(
            venue=Venue.POLYMARKET, symbol="sports-1", outcome=OutcomeSide.YES
        )
        late_contract = Contract(
            venue=Venue.POLYMARKET, symbol="sports-2", outcome=OutcomeSide.YES
        )
        crypto_contract = Contract(
            venue=Venue.POLYMARKET, symbol="crypto-1", outcome=OutcomeSide.YES
        )
        markets = [
            MarketSummary(
                contract=sports_contract,
                title="Sports A",
                best_bid=0.48,
                best_ask=0.50,
                volume=2500,
                category="Sports",
                expires_at=now + timedelta(hours=12),
                active=True,
            ),
            MarketSummary(
                contract=late_contract,
                title="Sports B",
                best_bid=0.48,
                best_ask=0.50,
                volume=2500,
                series="nba",
                expires_at=now + timedelta(hours=48),
                active=True,
            ),
            MarketSummary(
                contract=crypto_contract,
                title="Crypto A",
                best_bid=0.48,
                best_ask=0.50,
                volume=5000,
                category="crypto",
                expires_at=now + timedelta(hours=12),
                active=True,
            ),
        ]
        provider = StaticFairValueProvider(
            {
                sports_contract.market_key: 0.60,
                late_contract.market_key: 0.60,
                crypto_contract.market_key: 0.60,
            }
        )

        candidates = OpportunityRanker(
            edge_threshold=0.03,
            allowed_categories=("sports",),
            min_volume=1000,
            min_hours_to_expiry=6,
            max_hours_to_expiry=24,
        ).rank(markets, provider)

        self.assertEqual(
            [candidate.contract.symbol for candidate in candidates], ["sports-1"]
        )

    def test_opportunity_ranker_matches_allowed_categories_against_series_and_tags(
        self,
    ):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="sports-1", outcome=OutcomeSide.YES
            ),
            title="Sports A",
            best_bid=0.48,
            best_ask=0.50,
            volume=2500,
            category="sports",
            sport="nba",
            series="nba",
            tags=("games", "playoffs"),
            active=True,
        )
        provider = StaticFairValueProvider({market.contract.market_key: 0.60})

        candidates = OpportunityRanker(
            edge_threshold=0.03,
            allowed_categories=("playoffs",),
        ).rank([market], provider)

        self.assertEqual(
            [candidate.contract.symbol for candidate in candidates], ["sports-1"]
        )

    def test_opportunity_ranker_boosts_buy_pair_discount(self):
        discounted_yes = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="discount-yes", outcome=OutcomeSide.YES
            ),
            title="Discounted Pair",
            best_bid=0.43,
            best_ask=0.47,
            active=True,
            raw={"market": {"condition_id": "condition-1"}},
        )
        discounted_no = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="discount-no", outcome=OutcomeSide.NO
            ),
            title="Discounted Pair",
            best_bid=0.44,
            best_ask=0.48,
            active=True,
            raw={"market": {"condition_id": "condition-1"}},
        )
        standalone = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="standalone", outcome=OutcomeSide.YES
            ),
            title="Standalone",
            best_bid=0.43,
            best_ask=0.46,
            active=True,
        )
        provider = StaticFairValueProvider(
            {
                discounted_yes.contract.market_key: 0.53,
                discounted_no.contract.market_key: 0.52,
                standalone.contract.market_key: 0.53,
            }
        )

        candidates = OpportunityRanker(
            edge_threshold=0.03,
            spread_penalty_weight=0.0,
        ).rank([standalone, discounted_yes, discounted_no], provider)

        discounted_yes_candidate = next(
            candidate
            for candidate in candidates
            if candidate.contract.symbol == "discount-yes"
        )

        self.assertIn("paired_ask_discount", discounted_yes_candidate.rationale)
        self.assertGreater(
            discounted_yes_candidate.score, discounted_yes_candidate.edge
        )
        self.assertLessEqual(
            discounted_yes_candidate.score - discounted_yes_candidate.edge,
            0.005,
        )

    def test_opportunity_ranker_bonus_does_not_swamp_meaningfully_better_raw_edge(self):
        discounted_yes = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="discount-yes", outcome=OutcomeSide.YES
            ),
            title="Discounted Pair",
            best_bid=0.43,
            best_ask=0.47,
            active=True,
            raw={"market": {"condition_id": "condition-1"}},
        )
        discounted_no = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="discount-no", outcome=OutcomeSide.NO
            ),
            title="Discounted Pair",
            best_bid=0.44,
            best_ask=0.48,
            active=True,
            raw={"market": {"condition_id": "condition-1"}},
        )
        stronger_edge = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="stronger-edge", outcome=OutcomeSide.YES
            ),
            title="Standalone",
            best_bid=0.43,
            best_ask=0.44,
            active=True,
        )
        provider = StaticFairValueProvider(
            {
                discounted_yes.contract.market_key: 0.53,
                discounted_no.contract.market_key: 0.52,
                stronger_edge.contract.market_key: 0.53,
            }
        )

        candidates = OpportunityRanker(edge_threshold=0.03).rank(
            [discounted_yes, discounted_no, stronger_edge], provider
        )

        self.assertEqual(candidates[0].contract.symbol, "stronger-edge")

    def test_opportunity_ranker_applies_taker_fee_to_net_edge(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            title="A",
            best_bid=0.45,
            best_ask=0.50,
            active=True,
        )
        provider = StaticFairValueProvider({market.contract.market_key: 0.535})

        gross_candidates = OpportunityRanker(edge_threshold=0.03).rank(
            [market], provider
        )
        net_candidates = OpportunityRanker(
            edge_threshold=0.03,
            taker_fee_rate=0.03,
        ).rank([market], provider)

        self.assertEqual(len(gross_candidates), 1)
        self.assertEqual(net_candidates, [])

    def test_opportunity_ranker_rationale_includes_fee_drag(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            title="A",
            best_bid=0.45,
            best_ask=0.50,
            active=True,
        )
        provider = StaticFairValueProvider({market.contract.market_key: 0.54})

        candidates = OpportunityRanker(
            edge_threshold=0.03,
            taker_fee_rate=0.01,
        ).rank([market], provider)

        self.assertEqual(len(candidates), 1)
        self.assertIn("fee_drag", candidates[0].rationale)

    def test_manifest_fair_value_provider_rejects_stale_record(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            active=True,
        )
        provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                    generated_at=datetime.now(timezone.utc) - timedelta(hours=2),
                )
            },
            max_age_seconds=60,
        )

        self.assertIsNone(provider.fair_value_for(market))

    def test_manifest_fair_value_provider_rejects_condition_mismatch(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            active=True,
            raw={"market": {"condition_id": "condition-2"}},
        )
        provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                    condition_id="condition-1",
                )
            }
        )

        self.assertIsNone(provider.fair_value_for(market))

    def test_manifest_fair_value_provider_rejects_event_key_mismatch(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            active=True,
            event_key="event-2",
        )
        provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                    event_key="event-1",
                )
            }
        )

        self.assertIsNone(provider.fair_value_for(market))

    def test_manifest_fair_value_provider_rejects_extended_market_identity_mismatch(
        self,
    ):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            active=True,
            sport="nba",
            series="nba-finals",
            game_id="game-1",
            sports_market_type="moneyline",
        )
        provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                    sport="nba",
                    series="nba-finals",
                    game_id="game-1",
                    sports_market_type="moneyline",
                )
            }
        )

        self.assertEqual(provider.fair_value_for(market), 0.61)
        self.assertIsNone(
            provider.fair_value_for(
                MarketSummary(
                    contract=market.contract,
                    active=True,
                    sport="nfl",
                    series="nba-finals",
                    game_id="game-1",
                    sports_market_type="moneyline",
                )
            )
        )
        self.assertIsNone(
            provider.fair_value_for(
                MarketSummary(
                    contract=market.contract,
                    active=True,
                    sport="nba",
                    series="eastern-conference",
                    game_id="game-1",
                    sports_market_type="moneyline",
                )
            )
        )
        self.assertIsNone(
            provider.fair_value_for(
                MarketSummary(
                    contract=market.contract,
                    active=True,
                    sport="nba",
                    series="nba-finals",
                    game_id="game-2",
                    sports_market_type="moneyline",
                )
            )
        )
        self.assertIsNone(
            provider.fair_value_for(
                MarketSummary(
                    contract=market.contract,
                    active=True,
                    sport="nba",
                    series="nba-finals",
                    game_id="game-1",
                    sports_market_type="spread",
                )
            )
        )

    def test_manifest_fair_value_provider_can_select_calibrated_field(self):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            active=True,
        )
        raw_provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                    calibrated_fair_value=0.67,
                )
            },
            fair_value_field="raw",
        )
        calibrated_provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                    calibrated_fair_value=0.67,
                )
            },
            fair_value_field="calibrated",
        )

        self.assertEqual(raw_provider.fair_value_for(market), 0.61)
        self.assertEqual(calibrated_provider.fair_value_for(market), 0.67)

    def test_manifest_fair_value_provider_falls_back_to_raw_when_calibrated_missing(
        self,
    ):
        market = MarketSummary(
            contract=Contract(
                venue=Venue.POLYMARKET, symbol="token-a", outcome=OutcomeSide.YES
            ),
            active=True,
        )
        provider = ManifestFairValueProvider(
            records={
                market.contract.market_key: FairValueManifestEntry(
                    fair_value=0.61,
                )
            },
            fair_value_field="calibrated",
        )

        self.assertEqual(provider.fair_value_for(market), 0.61)


if __name__ == "__main__":
    unittest.main()

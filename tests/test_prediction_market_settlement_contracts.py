from __future__ import annotations

import unittest

import test_discovery as discovery_tests


class PredictionMarketSettlementContractTests(unittest.TestCase):
    def test_contract_rules_parser_reads_resolution_state(self):
        case = discovery_tests.DiscoveryTests(
            methodName="test_contract_rules_parser_reads_resolution_and_trading_flags"
        )
        case.test_contract_rules_parser_reads_resolution_and_trading_flags()

    def test_contract_rules_freeze_reasons_cover_resolution_state(self):
        case = discovery_tests.DiscoveryTests(
            methodName="test_contract_rules_freeze_reasons_cover_resolution_state_and_expiry"
        )
        case.test_contract_rules_freeze_reasons_cover_resolution_state_and_expiry()

    def test_opportunity_ranker_skips_market_frozen_by_contract_rules(self):
        case = discovery_tests.DiscoveryTests(
            methodName="test_opportunity_ranker_skips_market_frozen_by_contract_rules"
        )
        case.test_opportunity_ranker_skips_market_frozen_by_contract_rules()

    def test_pair_ranker_skips_pair_frozen_by_contract_rules(self):
        case = discovery_tests.DiscoveryTests(
            methodName="test_pair_ranker_skips_pairs_frozen_by_contract_rules"
        )
        case.test_pair_ranker_skips_pairs_frozen_by_contract_rules()


if __name__ == "__main__":
    unittest.main()

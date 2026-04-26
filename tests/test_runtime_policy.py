from __future__ import annotations

import json
import tempfile
import unittest

from adapters.polymarket import PolymarketConfig
from engine.runtime_policy import RuntimePolicyError, load_runtime_policy


class RuntimePolicyTests(unittest.TestCase):
    def test_load_runtime_policy_builds_expected_owner_configs(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 1,
                    "fair_value": {"field": "calibrated"},
                    "strategy": {
                        "base_quantity": 2.5,
                        "edge_threshold": 0.07,
                    },
                    "risk_limits": {
                        "max_contracts_per_market": 8,
                        "max_global_contracts": 20,
                        "max_contracts_per_event": 12,
                        "max_notional_per_event": 2.5,
                    },
                    "opportunity_ranker": {
                        "allowed_categories": ["sports", "nba"],
                        "taker_fee_rate": 0.02,
                        "time_lock_penalty_weight": 0.05,
                        "time_lock_penalty_saturation_hours": 72,
                        "min_volume": 5000,
                        "contract_rules": {
                            "freeze_before_expiry_seconds": 1800,
                            "freeze_when_not_accepting_orders": False,
                        },
                        "freeze_windows": {
                            "freeze_minutes_before_start": 20,
                            "freeze_when_resolved": False,
                        },
                    },
                    "pair_opportunity_ranker": {
                        "edge_threshold": 0.03,
                        "allowed_categories": ["sports"],
                        "time_lock_penalty_weight": 0.02,
                        "time_lock_penalty_saturation_hours": 48,
                        "contract_rules": {
                            "freeze_when_closed": False,
                        },
                        "freeze_windows": {
                            "freeze_minutes_before_start": 15,
                            "freeze_minutes_before_expiry": 5,
                        },
                    },
                    "execution_policy_gate": {
                        "max_open_orders_global": 3,
                        "max_book_age_seconds": 4,
                    },
                    "trading_engine": {
                        "overlay_max_age_seconds": 12,
                        "max_active_wallet_balance": 250.0,
                        "autonomous_mode": True,
                        "cancel_retry_max_attempts": 5,
                    },
                    "proposal_planner": {
                        "min_match_confidence": 0.9,
                        "max_source_age_ms": 2500,
                        "freeze_minutes_before_start": 5,
                        "freeze_minutes_before_expiry": 30,
                        "block_on_unhealthy_source": False,
                    },
                    "order_lifecycle_policy": {
                        "max_order_age_seconds": 45,
                    },
                    "venues": {
                        "polymarket": {
                            "depth_admission_levels": 4,
                            "depth_admission_liquidity_fraction": 0.6,
                            "depth_admission_max_expected_slippage_bps": 35,
                        }
                    },
                },
                handle,
            )
            handle.flush()

            policy = load_runtime_policy(handle.name)

        self.assertEqual(policy.fair_value.field, "calibrated")
        self.assertEqual(policy.strategy.base_quantity, 2.5)
        self.assertEqual(policy.strategy.edge_threshold, 0.07)
        self.assertEqual(policy.risk_limits.max_notional_per_event, 2.5)
        self.assertEqual(policy.risk_limits.max_contracts_per_event, 12)
        self.assertEqual(
            policy.opportunity_ranker.allowed_categories,
            ("sports", "nba"),
        )
        self.assertEqual(policy.opportunity_ranker.time_lock_penalty_weight, 0.05)
        self.assertEqual(
            policy.opportunity_ranker.time_lock_penalty_saturation_hours, 72
        )
        self.assertEqual(
            policy.opportunity_ranker.contract_rule_freeze.freeze_before_expiry_seconds,
            1800,
        )
        self.assertFalse(
            policy.opportunity_ranker.contract_rule_freeze.freeze_when_not_accepting_orders
        )
        self.assertEqual(
            policy.opportunity_ranker.freeze_window_policy.freeze_minutes_before_start,
            20,
        )
        self.assertFalse(
            policy.opportunity_ranker.freeze_window_policy.freeze_when_resolved
        )
        self.assertEqual(policy.pair_opportunity_ranker.edge_threshold, 0.03)
        self.assertEqual(policy.pair_opportunity_ranker.time_lock_penalty_weight, 0.02)
        self.assertEqual(
            policy.pair_opportunity_ranker.time_lock_penalty_saturation_hours, 48
        )
        self.assertFalse(
            policy.pair_opportunity_ranker.contract_rule_freeze.freeze_when_closed
        )
        self.assertEqual(
            policy.pair_opportunity_ranker.freeze_window_policy.freeze_minutes_before_start,
            15,
        )
        self.assertEqual(
            policy.pair_opportunity_ranker.freeze_window_policy.freeze_minutes_before_expiry,
            5,
        )
        self.assertEqual(policy.execution_policy_gate.max_open_orders_global, 3)
        self.assertEqual(policy.trading_engine.overlay_max_age_seconds, 12.0)
        self.assertEqual(policy.trading_engine.max_active_wallet_balance, 250.0)
        self.assertTrue(policy.trading_engine.autonomous_mode)
        self.assertEqual(policy.proposal_planner.min_match_confidence, 0.9)
        self.assertEqual(policy.proposal_planner.max_source_age_ms, 2500)
        self.assertEqual(policy.proposal_planner.freeze_minutes_before_start, 5)
        self.assertEqual(policy.proposal_planner.freeze_minutes_before_expiry, 30)
        self.assertFalse(policy.proposal_planner.block_on_unhealthy_source)
        self.assertEqual(policy.order_lifecycle_policy.max_order_age_seconds, 45.0)

        strategy = policy.strategy.build_strategy()
        sizer = policy.strategy.build_sizer()
        limits = policy.risk_limits.build()
        gate = policy.execution_policy_gate.build()
        ranker = policy.opportunity_ranker.build()
        planner = policy.proposal_planner.build()
        venue_config = policy.venues.polymarket.apply(PolymarketConfig())

        self.assertEqual(strategy.quantity, 2.5)
        self.assertEqual(strategy.edge_threshold, 0.07)
        self.assertEqual(sizer.base_quantity, 2.5)
        self.assertEqual(sizer.edge_unit, 0.07)
        self.assertEqual(limits.max_contracts_per_event, 12)
        self.assertEqual(limits.max_notional_per_event, 2.5)
        self.assertEqual(gate.max_open_orders_global, 3)
        self.assertEqual(
            policy.trading_engine.build_kwargs()["max_active_wallet_balance"], 250.0
        )
        self.assertTrue(policy.trading_engine.build_kwargs()["autonomous_mode"])
        self.assertEqual(planner.freeze_minutes_before_start, 5)
        self.assertEqual(planner.freeze_minutes_before_expiry, 30)
        self.assertFalse(planner.block_on_unhealthy_source)
        self.assertEqual(ranker.time_lock_penalty_weight, 0.05)
        self.assertEqual(
            ranker.contract_rule_freeze.freeze_before_expiry_seconds,
            1800,
        )
        self.assertEqual(ranker.freeze_window_policy.freeze_minutes_before_start, 20)
        self.assertFalse(ranker.freeze_window_policy.freeze_when_resolved)
        pair_ranker = policy.pair_opportunity_ranker.build()
        self.assertEqual(pair_ranker.time_lock_penalty_weight, 0.02)
        self.assertEqual(
            pair_ranker.freeze_window_policy.freeze_minutes_before_start, 15
        )
        self.assertEqual(
            pair_ranker.freeze_window_policy.freeze_minutes_before_expiry, 5
        )
        self.assertEqual(venue_config.depth_admission_levels, 4)
        self.assertEqual(venue_config.depth_admission_liquidity_fraction, 0.6)
        self.assertEqual(
            venue_config.depth_admission_max_expected_slippage_bps,
            35.0,
        )

    def test_load_runtime_policy_rejects_invalid_schema_version(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump({"schema_version": 2}, handle)
            handle.flush()

            with self.assertRaisesRegex(
                RuntimePolicyError,
                "schema_version must be 1",
            ):
                load_runtime_policy(handle.name)

    def test_load_runtime_policy_rejects_unknown_fair_value_field(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {"schema_version": 1, "fair_value": {"field": "adjusted"}},
                handle,
            )
            handle.flush()

            with self.assertRaisesRegex(
                RuntimePolicyError,
                "fair_value.field must be one of: raw, calibrated",
            ):
                load_runtime_policy(handle.name)


if __name__ == "__main__":
    unittest.main()

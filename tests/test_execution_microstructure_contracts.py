from __future__ import annotations

import unittest

import test_opportunity_layers as opportunity_layer_tests
import test_orchestrator as orchestrator_tests


class ExecutionMicrostructureContractTests(unittest.TestCase):
    def test_fee_drag_is_applied_to_executable_edge(self):
        case = opportunity_layer_tests.OpportunityLayerTests(
            methodName="test_assess_executable_edge_applies_fee_drag"
        )
        case.test_assess_executable_edge_applies_fee_drag()

    def test_after_cost_edge_can_flip_selected_side(self):
        case = opportunity_layer_tests.OpportunityLayerTests(
            methodName="test_opportunity_from_prices_chooses_side_from_after_cost_edge"
        )
        case.test_opportunity_from_prices_chooses_side_from_after_cost_edge()

    def test_partial_fill_blocks_candidate_contract_when_configured(self):
        case = orchestrator_tests.OrchestratorTests(
            methodName="test_policy_gate_blocks_when_contract_has_partial_fill"
        )
        case.test_policy_gate_blocks_when_contract_has_partial_fill()

    def test_global_partial_fill_limit_blocks_new_actions(self):
        case = orchestrator_tests.OrchestratorTests(
            methodName="test_policy_gate_blocks_when_global_partial_fill_limit_reached"
        )
        case.test_policy_gate_blocks_when_global_partial_fill_limit_reached()


if __name__ == "__main__":
    unittest.main()

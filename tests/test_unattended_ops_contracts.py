from __future__ import annotations

import unittest

import test_run_agent_loop as run_agent_loop_tests
import test_runtime_bootstrap as runtime_bootstrap_tests
import test_safety_state_persistence as safety_state_tests


class UnattendedOpsContractTests(unittest.TestCase):
    def test_autonomous_mode_requires_guardrail_contracts(self):
        case = run_agent_loop_tests.RunAgentLoopTests(
            methodName="test_validate_autonomous_mode_requires_guardrail_contracts"
        )
        case.test_validate_autonomous_mode_requires_guardrail_contracts()

    def test_autonomous_mode_accepts_complete_guardrail_contract(self):
        case = run_agent_loop_tests.RunAgentLoopTests(
            methodName="test_validate_autonomous_mode_accepts_complete_guardrail_contract"
        )
        case.test_validate_autonomous_mode_accepts_complete_guardrail_contract()

    def test_live_mode_requires_route_and_compliance_attestation(self):
        case = runtime_bootstrap_tests.RunAgentLoopPostgresAuthorityTests(
            methodName="test_validate_polymarket_live_routing_requires_route_label_and_ack"
        )
        case.test_validate_polymarket_live_routing_requires_route_label_and_ack()

    def test_weekly_and_cumulative_loss_guards_persist_across_restart(self):
        case = safety_state_tests.SafetyStatePersistenceTests(
            methodName="test_weekly_and_cumulative_loss_state_persist_and_block_after_restart"
        )
        case.test_weekly_and_cumulative_loss_state_persist_and_block_after_restart()


if __name__ == "__main__":
    unittest.main()

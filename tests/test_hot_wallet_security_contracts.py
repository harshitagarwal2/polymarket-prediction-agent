from __future__ import annotations

import unittest

import test_engine_runner as engine_runner_tests
import test_polymarket_capture_worker as capture_worker_tests
import test_runtime_bootstrap as runtime_bootstrap_tests


class HotWalletSecurityContractTests(unittest.TestCase):
    def test_runtime_bootstrap_accepts_file_backed_key(self):
        case = runtime_bootstrap_tests.RunAgentLoopPostgresAuthorityTests(
            methodName="test_build_adapter_can_load_polymarket_private_key_from_file"
        )
        case.test_build_adapter_can_load_polymarket_private_key_from_file()

    def test_runtime_bootstrap_accepts_command_backed_key(self):
        case = runtime_bootstrap_tests.RunAgentLoopPostgresAuthorityTests(
            methodName="test_build_adapter_can_load_polymarket_private_key_from_command"
        )
        case.test_build_adapter_can_load_polymarket_private_key_from_command()

    def test_user_capture_accepts_file_backed_key(self):
        case = capture_worker_tests.PolymarketCaptureWorkerTests(
            methodName="test_user_cli_accepts_polymarket_private_key_file"
        )
        case.test_user_cli_accepts_polymarket_private_key_file()

    def test_user_capture_accepts_command_backed_key(self):
        case = capture_worker_tests.PolymarketCaptureWorkerTests(
            methodName="test_user_cli_accepts_polymarket_private_key_command"
        )
        case.test_user_cli_accepts_polymarket_private_key_command()

    def test_engine_blocks_when_wallet_balance_exceeds_cap(self):
        case = engine_runner_tests.EngineRunnerTests(
            methodName="test_run_once_blocks_when_active_wallet_balance_exceeds_cap"
        )
        case.test_run_once_blocks_when_active_wallet_balance_exceeds_cap()


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

import test_engine_runner as engine_runner_tests


class ExecutionCrashWindowTests(unittest.TestCase):
    def test_submit_uncertain_becomes_pending_submission(self):
        case = engine_runner_tests.EngineRunnerTests(
            methodName="test_ambiguous_submit_becomes_pending_submission_and_requests_refresh"
        )
        case.test_ambiguous_submit_becomes_pending_submission_and_requests_refresh()

    def test_duplicate_pending_submission_blocks_retry(self):
        case = engine_runner_tests.EngineRunnerTests(
            methodName="test_duplicate_pending_submission_blocks_retry"
        )
        case.test_duplicate_pending_submission_blocks_retry()

    def test_authoritative_observation_clears_pending_submission(self):
        case = engine_runner_tests.EngineRunnerTests(
            methodName="test_authoritative_observation_clears_pending_submission"
        )
        case.test_authoritative_observation_clears_pending_submission()

    def test_cancel_request_recovers_first_while_submit_uncertain(self):
        case = engine_runner_tests.EngineRunnerTests(
            methodName="test_cancel_request_recovers_first_while_submit_uncertain"
        )
        case.test_cancel_request_recovers_first_while_submit_uncertain()


if __name__ == "__main__":
    unittest.main()

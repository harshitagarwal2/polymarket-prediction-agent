from __future__ import annotations

import unittest

import test_refresh_sports_fair_values as refresh_tests


class DataQualityGatesTests(unittest.TestCase):
    def test_refresh_cycle_records_velocity_metadata_within_threshold(self):
        case = refresh_tests.RefreshSportsFairValuesTests(
            methodName="test_run_refresh_cycle_records_velocity_metadata_when_within_threshold"
        )
        case.test_run_refresh_cycle_records_velocity_metadata_when_within_threshold()

    def test_refresh_cycle_blocks_implausible_velocity_jump(self):
        case = refresh_tests.RefreshSportsFairValuesTests(
            methodName="test_run_refresh_cycle_blocks_implausible_velocity_jump"
        )
        case.test_run_refresh_cycle_blocks_implausible_velocity_jump()


if __name__ == "__main__":
    unittest.main()

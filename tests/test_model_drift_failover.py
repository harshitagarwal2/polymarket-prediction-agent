from __future__ import annotations

import unittest

import test_model_drift_fallback as drift_fallback_tests


class ModelDriftFailoverTests(unittest.TestCase):
    def test_build_model_drift_reports_threshold_breach(self):
        case = drift_fallback_tests.ModelDriftFallbackTests(
            methodName="test_build_model_drift_reports_threshold_breach"
        )
        case.test_build_model_drift_reports_threshold_breach()

    def test_run_mode_holds_on_unhealthy_drift_report(self):
        case = drift_fallback_tests.ModelDriftFallbackTests(
            methodName="test_run_mode_holds_on_unhealthy_drift_report"
        )
        case.test_run_mode_holds_on_unhealthy_drift_report()


if __name__ == "__main__":
    unittest.main()

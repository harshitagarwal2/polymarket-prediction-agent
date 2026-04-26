from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import operator_cli, run_agent_loop


class ModelDriftFallbackTests(unittest.TestCase):
    def test_build_model_drift_reports_threshold_breach(self):
        benchmark_payload = {
            "fair_value_report": {
                "forecast_score": {
                    "brier_score": 0.42,
                    "expected_calibration_error": 0.18,
                }
            }
        }
        with tempfile.NamedTemporaryFile("w+") as handle:
            json.dump(benchmark_payload, handle)
            handle.flush()
            stdout = io.StringIO()
            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_build_model_drift(
                    argparse.Namespace(
                        benchmark_report_file=handle.name,
                        output=None,
                        max_brier_score=0.20,
                        max_expected_calibration_error=0.10,
                        quiet=False,
                    )
                )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(result, 0)
        self.assertFalse(payload["ok"])
        self.assertEqual(len(payload["reasons"]), 2)

    def test_run_mode_holds_on_unhealthy_drift_report(self):
        adapter = SimpleNamespace(stop_heartbeat=lambda: None, close=lambda: None)
        fake_engine = SimpleNamespace(
            safety_state=SimpleNamespace(halted=False, paused=False, hold_reason=None)
        )
        fake_engine.status_snapshot = lambda: SimpleNamespace(
            heartbeat_active=False,
            heartbeat_healthy_for_trading=True,
            pending_cancels=[],
        )
        fake_engine.request_cancel_order = lambda order, reason: None
        fake_engine.set_new_order_hold = lambda reason: setattr(
            fake_engine.safety_state, "hold_reason", reason
        )
        fake_engine.clear_new_order_hold = lambda: None
        current_state_adapter = SimpleNamespace(read_table=lambda table: {})
        fake_cycle = SimpleNamespace(selected=None)

        with tempfile.NamedTemporaryFile("w+") as drift_report:
            json.dump(
                {"ok": False, "reasons": ["brier score exceeds threshold"]},
                drift_report,
            )
            drift_report.flush()
            with (
                patch.object(run_agent_loop, "build_adapter", return_value=adapter),
                patch.object(run_agent_loop, "validate_runtime"),
                patch.object(
                    run_agent_loop,
                    "build_current_state_read_adapter",
                    return_value=current_state_adapter,
                ),
                patch.object(
                    run_agent_loop,
                    "_build_projected_fair_value_provider",
                    return_value=SimpleNamespace(),
                ),
                patch.object(run_agent_loop, "TradingEngine", return_value=fake_engine),
                patch.object(run_agent_loop, "AgentOrchestrator"),
                patch.object(run_agent_loop, "PollingAgentLoop") as polling_loop,
            ):
                polling_loop.return_value.run.return_value = [fake_cycle]
                with patch(
                    "sys.argv",
                    [
                        "run_agent_loop.py",
                        "--venue",
                        "polymarket",
                        "--mode",
                        "run",
                        "--opportunity-root",
                        "runtime/data",
                        "--drift-report-file",
                        drift_report.name,
                        "--quiet",
                    ],
                ):
                    result = run_agent_loop.main()

        self.assertEqual(result, 0)
        self.assertIn(
            "model drift report blocked live mode",
            fake_engine.safety_state.hold_reason,
        )


if __name__ == "__main__":
    unittest.main()

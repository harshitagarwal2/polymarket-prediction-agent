from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.alerting import build_runtime_alerts, send_alerts
from engine.safety_store import SafetyStateStore
from scripts import operator_cli


class AlertingContractTests(unittest.TestCase):
    def test_build_runtime_alerts_emits_critical_and_warning_alerts(self):
        payload = {
            "runtime_health": {
                "state": "halted",
                "reasons": ["kill switch tripped"],
                "kill_switch_active": True,
                "kill_switch_reasons": ["source unhealthy"],
                "pending_cancel_count": 1,
            },
            "pending_cancel_operator_attention_required": True,
            "last_truth_summary": {
                "complete": False,
                "issues": ["missing balance"],
            },
            "recent_execution_status": {"unresolved_order_ids": ["order-1"]},
        }

        alert_payload = build_runtime_alerts(payload)

        self.assertGreaterEqual(alert_payload["alert_count"], 4)
        severities = {alert["severity"] for alert in alert_payload["alerts"]}
        self.assertIn("critical", severities)
        self.assertIn("warning", severities)

    def test_send_alerts_dry_run_dedupes_alerts(self):
        payload = {
            "alerts": [
                {
                    "key": "critical:runtime:Runtime halted",
                    "severity": "critical",
                    "category": "runtime",
                    "summary": "Runtime halted",
                    "details": {},
                    "dedupe_hash": "abc",
                }
            ]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            dedupe = Path(temp_dir) / "dedupe.json"
            first = send_alerts(
                payload,
                webhook_url="https://example.invalid/webhook",
                dedupe_state_file=dedupe,
                dry_run=True,
            )
            second = send_alerts(
                payload,
                webhook_url="https://example.invalid/webhook",
                dedupe_state_file=dedupe,
                dry_run=True,
            )

        self.assertEqual(first["sent_alert_count"], 1)
        self.assertEqual(second["sent_alert_count"], 0)

    def test_build_alerts_command_reads_runtime_status_and_writes_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_status = Path(temp_dir) / "runtime_status.json"
            runtime_status.write_text(
                json.dumps(
                    {
                        "runtime_health": {
                            "state": "hold_new_orders",
                            "reasons": ["manual hold"],
                            "kill_switch_active": False,
                            "kill_switch_reasons": [],
                        },
                        "pending_cancel_operator_attention_required": False,
                        "last_truth_summary": {"complete": True, "issues": []},
                        "recent_execution_status": {"unresolved_order_ids": []},
                    }
                ),
                encoding="utf-8",
            )
            output = Path(temp_dir) / "runtime_alerts.json"
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_build_alerts(
                    argparse.Namespace(
                        runtime_status_file=str(runtime_status),
                        output=str(output),
                        quiet=False,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload, json.loads(output.read_text(encoding="utf-8")))
            self.assertEqual(payload["alerts"][0]["severity"], "warning")

    def test_send_alerts_command_dry_run_writes_dedupe_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            alerts_file = Path(temp_dir) / "alerts.json"
            alerts_file.write_text(
                json.dumps(
                    {
                        "alerts": [
                            {
                                "key": "warning:runtime:Runtime state is degraded",
                                "severity": "warning",
                                "category": "runtime",
                                "summary": "Runtime state is degraded",
                                "details": {},
                                "dedupe_hash": "hash-1",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            dedupe_state = Path(temp_dir) / "dedupe.json"
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_send_alerts(
                    argparse.Namespace(
                        alerts_file=str(alerts_file),
                        webhook_url="https://example.invalid/webhook",
                        minimum_severity="warning",
                        dedupe_state_file=str(dedupe_state),
                        dry_run=True,
                        quiet=False,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload["sent_alert_count"], 1)
            self.assertTrue(dedupe_state.exists())

    def test_status_payload_can_feed_alert_pipeline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            output_path = Path(temp_dir) / "runtime_status.json"
            store = SafetyStateStore(state_path)
            state = store.load()
            state.hold_new_orders = True
            state.hold_reason = "operator hold"
            store.save(state)
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                operator_cli.cmd_status(
                    argparse.Namespace(
                        state_file=str(state_path),
                        journal=None,
                        llm_advisory_file=None,
                        venue=None,
                        symbol=None,
                        outcome="unknown",
                        output=str(output_path),
                    )
                )

            alert_payload = build_runtime_alerts(
                json.loads(output_path.read_text(encoding="utf-8"))
            )
            self.assertGreaterEqual(alert_payload["alert_count"], 1)


if __name__ == "__main__":
    unittest.main()

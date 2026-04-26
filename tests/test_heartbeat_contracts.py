from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.alerting import build_runtime_heartbeat, send_heartbeat
from engine.safety_store import SafetyStateStore
from scripts import operator_cli


class HeartbeatContractTests(unittest.TestCase):
    def test_build_runtime_heartbeat_extracts_runtime_and_heartbeat_fields(self):
        payload = {
            "runtime_health": {
                "state": "healthy",
                "resume_trading_eligible": True,
            },
            "safety_state": {
                "heartbeat_required": True,
                "heartbeat_active": True,
                "heartbeat_running": True,
                "heartbeat_healthy_for_trading": True,
                "heartbeat_last_success_at": "2026-04-24T21:00:00+00:00",
                "heartbeat_last_error": None,
                "heartbeat_last_id": "hb-1",
            },
            "journal_summary": {"market_count": 5, "candidate_count": 2},
            "recent_runtime": {"selected_market_key": "pm-1"},
        }

        heartbeat = build_runtime_heartbeat(payload)

        self.assertEqual(heartbeat["runtime_state"], "healthy")
        self.assertTrue(heartbeat["heartbeat_active"])
        self.assertEqual(heartbeat["market_count"], 5)
        self.assertEqual(heartbeat["selected_market_key"], "pm-1")

    def test_send_heartbeat_supports_dry_run(self):
        result = send_heartbeat(
            {"runtime_state": "healthy"},
            webhook_url="https://example.invalid/heartbeat",
            dry_run=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["runtime_state"], "healthy")

    def test_build_heartbeat_command_reads_runtime_status_and_writes_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_status = Path(temp_dir) / "runtime_status.json"
            runtime_status.write_text(
                json.dumps(
                    {
                        "runtime_health": {
                            "state": "healthy",
                            "resume_trading_eligible": True,
                        },
                        "safety_state": {
                            "heartbeat_required": True,
                            "heartbeat_active": True,
                            "heartbeat_running": True,
                            "heartbeat_healthy_for_trading": True,
                            "heartbeat_last_success_at": None,
                            "heartbeat_last_error": None,
                            "heartbeat_last_id": None,
                        },
                        "journal_summary": {},
                        "recent_runtime": None,
                    }
                ),
                encoding="utf-8",
            )
            output = Path(temp_dir) / "runtime_heartbeat.json"
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_build_heartbeat(
                    argparse.Namespace(
                        runtime_status_file=str(runtime_status),
                        output=str(output),
                        quiet=False,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload, json.loads(output.read_text(encoding="utf-8")))
            self.assertEqual(payload["runtime_state"], "healthy")

    def test_send_heartbeat_command_dry_run_reports_runtime_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            heartbeat_file = Path(temp_dir) / "heartbeat.json"
            heartbeat_file.write_text(
                json.dumps({"runtime_state": "healthy"}),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with patch("sys.stdout", stdout):
                result = operator_cli.cmd_send_heartbeat(
                    argparse.Namespace(
                        heartbeat_file=str(heartbeat_file),
                        webhook_url="https://example.invalid/heartbeat",
                        dry_run=True,
                        quiet=False,
                    )
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(payload["runtime_state"], "healthy")

    def test_status_payload_can_feed_heartbeat_pipeline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "safety-state.json"
            status_path = Path(temp_dir) / "runtime_status.json"
            store = SafetyStateStore(state_path)
            state = store.load()
            state.heartbeat_required = True
            state.heartbeat_active = True
            state.heartbeat_running = True
            state.heartbeat_healthy_for_trading = True
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
                        output=str(status_path),
                        quiet=False,
                    )
                )

            heartbeat = build_runtime_heartbeat(
                json.loads(status_path.read_text(encoding="utf-8"))
            )
            self.assertTrue(heartbeat["heartbeat_active"])


if __name__ == "__main__":
    unittest.main()

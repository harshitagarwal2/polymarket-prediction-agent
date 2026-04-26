from __future__ import annotations

import argparse
import io
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from engine.safety_store import SafetyStateStore
from scripts import operator_cli


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic alerting baseline smoke verification."
    )
    parser.add_argument("--root", default="runtime/data")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        state_path = root / "safety-state.json"
        status_path = root / "runtime_status.json"
        alerts_path = root / "runtime_alerts.json"
        dedupe_path = root / "alert_dedupe.json"

        store = SafetyStateStore(state_path)
        state = store.load()
        state.halted = True
        state.reason = "smoke halt"
        state.hold_new_orders = True
        state.hold_reason = "operator hold"
        state.last_truth_complete = False
        state.last_truth_issues = ["truth missing balance"]
        store.save(state)

        with patch("sys.stdout", io.StringIO()):
            operator_cli.cmd_status(
                argparse.Namespace(
                    state_file=str(state_path),
                    journal=None,
                    llm_advisory_file=None,
                    venue=None,
                    symbol=None,
                    outcome="unknown",
                    output=str(status_path),
                )
            )

        with patch("sys.stdout", io.StringIO()):
            operator_cli.cmd_build_alerts(
                argparse.Namespace(
                    runtime_status_file=str(status_path),
                    output=str(alerts_path),
                    quiet=True,
                )
            )

        stdout = io.StringIO()
        with patch("sys.stdout", stdout):
            operator_cli.cmd_send_alerts(
                argparse.Namespace(
                    alerts_file=str(alerts_path),
                    webhook_url="https://example.invalid/hooks/runtime",
                    minimum_severity="warning",
                    dedupe_state_file=str(dedupe_path),
                    dry_run=True,
                    quiet=False,
                )
            )
        result = json.loads(stdout.getvalue())
        if result["sent_alert_count"] < 1:
            raise RuntimeError("smoke alerting did not emit any alerts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

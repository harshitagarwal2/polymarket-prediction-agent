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
        description="Run deterministic heartbeat baseline smoke verification."
    )
    parser.add_argument("--root", default="runtime/data")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        state_path = root / "safety-state.json"
        status_path = root / "runtime_status.json"
        heartbeat_path = root / "runtime_heartbeat.json"

        store = SafetyStateStore(state_path)
        state = store.load()
        state.heartbeat_required = True
        state.heartbeat_active = True
        state.heartbeat_running = True
        state.heartbeat_healthy_for_trading = True
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
                    quiet=False,
                )
            )

        with patch("sys.stdout", io.StringIO()):
            operator_cli.cmd_build_heartbeat(
                argparse.Namespace(
                    runtime_status_file=str(status_path),
                    output=str(heartbeat_path),
                    quiet=True,
                )
            )

        stdout = io.StringIO()
        with patch("sys.stdout", stdout):
            operator_cli.cmd_send_heartbeat(
                argparse.Namespace(
                    heartbeat_file=str(heartbeat_path),
                    webhook_url="https://example.invalid/heartbeat",
                    dry_run=True,
                    quiet=False,
                )
            )
        result = json.loads(stdout.getvalue())
        if result["runtime_state"] != "healthy":
            raise RuntimeError("heartbeat smoke did not report healthy runtime state")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import io
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from scripts import operator_cli


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic model-drift baseline smoke verification."
    )
    parser.add_argument("--root", default="runtime/data")
    return parser


def main(argv: list[str] | None = None) -> int:
    _ = build_parser().parse_args(argv)
    with tempfile.TemporaryDirectory() as temp_dir:
        benchmark_report = Path(temp_dir) / "benchmark_report.json"
        drift_report = Path(temp_dir) / "model_drift.json"
        benchmark_report.write_text(
            json.dumps(
                {
                    "fair_value_report": {
                        "forecast_score": {
                            "brier_score": 0.42,
                            "expected_calibration_error": 0.18,
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        stdout = io.StringIO()
        with patch("sys.stdout", stdout):
            operator_cli.cmd_build_model_drift(
                argparse.Namespace(
                    benchmark_report_file=str(benchmark_report),
                    output=str(drift_report),
                    max_brier_score=0.20,
                    max_expected_calibration_error=0.10,
                    quiet=False,
                )
            )
        payload = json.loads(stdout.getvalue())
        if payload.get("ok") is not False:
            raise RuntimeError("model drift smoke expected an unhealthy drift report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

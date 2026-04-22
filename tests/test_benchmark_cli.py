from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import run_sports_benchmark


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "research"
    / "fixtures"
    / "sports_benchmark_tiny.json"
)


class BenchmarkCliTests(unittest.TestCase):
    def test_cli_quiet_suppresses_stdout(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle:
            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "run_sports_benchmark.py",
                        "--fixture",
                        "sports_benchmark_tiny.json",
                        "--output",
                        output_handle.name,
                        "--quiet",
                    ],
                ),
                patch("sys.stdout", stdout),
            ):
                run_sports_benchmark.main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(payload["case_name"], "sports-benchmark-tiny")

    def test_cli_runs_packaged_fixture_and_writes_report(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle:
            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark.py",
                    "--fixture",
                    "sports_benchmark_tiny.json",
                    "--output",
                    output_handle.name,
                ],
            ):
                run_sports_benchmark.main()

            output_handle.seek(0)
            payload = json.load(output_handle)

        self.assertEqual(payload["case_name"], "sports-benchmark-tiny")
        self.assertEqual(payload["replay"]["score"]["trade_count"], 1)

    def test_cli_runs_explicit_case_and_writes_manifest(self):
        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as output_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as manifest_handle,
        ):
            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark.py",
                    "--case",
                    str(FIXTURE_PATH),
                    "--output",
                    output_handle.name,
                    "--write-manifest",
                    manifest_handle.name,
                ],
            ):
                run_sports_benchmark.main()

            output_handle.seek(0)
            manifest_handle.seek(0)
            report_payload = json.load(output_handle)
            manifest_payload = json.load(manifest_handle)

        self.assertEqual(report_payload["case_name"], "sports-benchmark-tiny")
        self.assertEqual(
            sorted(manifest_payload["values"].keys()),
            ["token-home:no", "token-home:yes"],
        )

    def test_cli_rejects_write_manifest_for_replay_only_case(self):
        replay_only_payload = {
            "name": "replay-only",
            "replay_case": json.loads(FIXTURE_PATH.read_text())["replay_case"],
        }

        with (
            tempfile.NamedTemporaryFile("w+", suffix=".json") as case_handle,
            tempfile.NamedTemporaryFile("w+", suffix=".json") as manifest_handle,
        ):
            json.dump(replay_only_payload, case_handle)
            case_handle.flush()

            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark.py",
                    "--case",
                    case_handle.name,
                    "--write-manifest",
                    manifest_handle.name,
                ],
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "benchmark case did not produce a fair-value manifest",
                ):
                    run_sports_benchmark.main()

    def test_cli_rejects_unknown_packaged_fixture_name(self):
        with patch(
            "sys.argv",
            [
                "run_sports_benchmark.py",
                "--fixture",
                "../../not-a-fixture.json",
            ],
        ):
            with self.assertRaises(SystemExit):
                run_sports_benchmark.main()


if __name__ == "__main__":
    unittest.main()

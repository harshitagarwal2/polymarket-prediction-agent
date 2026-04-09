from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import run_sports_benchmark_suite


FIXTURES_DIR = Path(__file__).resolve().parents[1] / "research" / "fixtures"


class BenchmarkSuiteCliTests(unittest.TestCase):
    def test_suite_cli_writes_json_and_markdown_summaries(self):
        with tempfile.TemporaryDirectory() as output_dir:
            with patch(
                "sys.argv",
                [
                    "run_sports_benchmark_suite.py",
                    "--fixtures-dir",
                    str(FIXTURES_DIR),
                    "--output-dir",
                    output_dir,
                ],
            ):
                run_sports_benchmark_suite.main()

            self.assertTrue(
                (Path(output_dir) / "benchmark_suite_summary.json").exists()
            )
            self.assertTrue((Path(output_dir) / "benchmark_suite_summary.md").exists())


if __name__ == "__main__":
    unittest.main()

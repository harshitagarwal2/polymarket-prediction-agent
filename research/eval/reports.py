from __future__ import annotations

from pathlib import Path

from research.benchmark_suite import (
    WalkForwardBenchmarkSuiteReport,
    write_walk_forward_suite_report,
)


def write_walk_forward_report_artifacts(
    report: WalkForwardBenchmarkSuiteReport, output_dir: str | Path
) -> Path:
    return write_walk_forward_suite_report(report, output_dir)

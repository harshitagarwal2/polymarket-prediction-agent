from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class ExecutionMetricsSummary:
    trade_count: int
    filled_trade_count: int
    fill_rate: float
    average_expected_edge_bps: float
    average_realized_edge_bps: float
    average_realized_slippage_bps: float
    stale_data_count: int


def summarize_execution_metrics(
    rows: Iterable[Mapping[str, object]],
) -> ExecutionMetricsSummary:
    payload_rows = [dict(row) for row in rows]
    filled_rows = [row for row in payload_rows if bool(row.get("filled"))]

    def _average(values: list[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    return ExecutionMetricsSummary(
        trade_count=len(payload_rows),
        filled_trade_count=len(filled_rows),
        fill_rate=(len(filled_rows) / len(payload_rows)) if payload_rows else 0.0,
        average_expected_edge_bps=_average(
            [float(row.get("expected_edge_bps", 0.0) or 0.0) for row in payload_rows]
        ),
        average_realized_edge_bps=_average(
            [float(row.get("realized_edge_bps", 0.0) or 0.0) for row in payload_rows]
        ),
        average_realized_slippage_bps=_average(
            [float(row.get("slippage_bps", 0.0) or 0.0) for row in filled_rows]
        ),
        stale_data_count=sum(bool(row.get("stale_data_flag")) for row in payload_rows),
    )

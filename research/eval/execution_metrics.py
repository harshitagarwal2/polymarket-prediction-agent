from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class ExecutionMetricsSummary:
    trade_count: int
    filled_trade_count: int
    complete_fill_count: int
    partial_fill_count: int
    fill_rate: float
    complete_fill_rate: float
    partial_fill_rate: float
    average_fill_ratio: float
    average_requested_quantity: float
    average_filled_quantity: float
    average_wait_steps: float
    average_expected_edge_bps: float
    average_realized_edge_bps: float
    average_realized_slippage_bps: float
    average_visible_fill_ratio: float
    stale_data_count: int

    def to_payload(self) -> dict[str, object]:
        return {
            "trade_count": self.trade_count,
            "filled_trade_count": self.filled_trade_count,
            "complete_fill_count": self.complete_fill_count,
            "partial_fill_count": self.partial_fill_count,
            "fill_rate": self.fill_rate,
            "complete_fill_rate": self.complete_fill_rate,
            "partial_fill_rate": self.partial_fill_rate,
            "average_fill_ratio": self.average_fill_ratio,
            "average_requested_quantity": self.average_requested_quantity,
            "average_filled_quantity": self.average_filled_quantity,
            "average_wait_steps": self.average_wait_steps,
            "average_expected_edge_bps": self.average_expected_edge_bps,
            "average_realized_edge_bps": self.average_realized_edge_bps,
            "average_realized_slippage_bps": self.average_realized_slippage_bps,
            "average_visible_fill_ratio": self.average_visible_fill_ratio,
            "stale_data_count": self.stale_data_count,
        }


def summarize_execution_metrics(
    rows: Iterable[Mapping[str, object]],
) -> ExecutionMetricsSummary:
    payload_rows = [dict(row) for row in rows]
    filled_rows = [row for row in payload_rows if bool(row.get("filled"))]
    complete_rows = [row for row in filled_rows if not bool(row.get("partial_fill"))]
    partial_rows = [row for row in filled_rows if bool(row.get("partial_fill"))]

    def _float(row: Mapping[str, object], key: str) -> float:
        value = row.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        return 0.0

    def _average(values: list[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    visible_fill_ratios = []
    for row in filled_rows:
        visible_quantity = _float(row, "visible_quantity")
        if visible_quantity > 0:
            visible_fill_ratios.append(
                _float(row, "filled_quantity") / visible_quantity
            )

    return ExecutionMetricsSummary(
        trade_count=len(payload_rows),
        filled_trade_count=len(filled_rows),
        complete_fill_count=len(complete_rows),
        partial_fill_count=len(partial_rows),
        fill_rate=(len(filled_rows) / len(payload_rows)) if payload_rows else 0.0,
        complete_fill_rate=(len(complete_rows) / len(payload_rows))
        if payload_rows
        else 0.0,
        partial_fill_rate=(len(partial_rows) / len(payload_rows))
        if payload_rows
        else 0.0,
        average_fill_ratio=_average(
            [_float(row, "fill_ratio") for row in payload_rows]
        ),
        average_requested_quantity=_average(
            [_float(row, "requested_quantity") for row in payload_rows]
        ),
        average_filled_quantity=_average(
            [_float(row, "filled_quantity") for row in payload_rows]
        ),
        average_wait_steps=_average(
            [_float(row, "wait_steps") for row in payload_rows]
        ),
        average_expected_edge_bps=_average(
            [_float(row, "expected_edge_bps") for row in payload_rows]
        ),
        average_realized_edge_bps=_average(
            [_float(row, "realized_edge_bps") for row in filled_rows]
        ),
        average_realized_slippage_bps=_average(
            [_float(row, "slippage_bps") for row in filled_rows]
        ),
        average_visible_fill_ratio=_average(visible_fill_ratios),
        stale_data_count=sum(bool(row.get("stale_data_flag")) for row in payload_rows),
    )

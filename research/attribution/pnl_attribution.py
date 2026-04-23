from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from adapters.types import OrderAction
from storage.postgres import TradeAttributionRecord, TradeAttributionRepository


@dataclass(frozen=True)
class TradeAttribution:
    trade_id: str
    market_id: str
    expected_edge_bps: float
    realized_edge_bps: float
    slippage_bps: float
    pnl: float
    model_error: float
    stale_data_flag: bool
    mapping_risk: float
    signal_edge_bps: float | None = None
    execution_drag_bps: float | None = None
    model_residual_bps: float | None = None
    closing_edge_bps: float | None = None
    value_capture_bps: float | None = None
    requested_quantity: float | None = None
    filled_quantity: float | None = None
    fill_ratio: float | None = None
    wait_steps: int | None = None
    resting: bool = False
    notes: dict[str, object] | None = None

    def to_record(self) -> TradeAttributionRecord:
        notes = dict(self.notes or {})
        decomposition: dict[str, object] = {}
        optional_fields = {
            "signal_edge_bps": self.signal_edge_bps,
            "execution_drag_bps": self.execution_drag_bps,
            "model_residual_bps": self.model_residual_bps,
            "closing_edge_bps": self.closing_edge_bps,
            "value_capture_bps": self.value_capture_bps,
            "requested_quantity": self.requested_quantity,
            "filled_quantity": self.filled_quantity,
            "fill_ratio": self.fill_ratio,
            "wait_steps": self.wait_steps,
            "resting": self.resting,
        }
        for key, value in optional_fields.items():
            if value is not None:
                decomposition[key] = value
        if decomposition:
            notes.setdefault("decomposition", decomposition)
        return TradeAttributionRecord(
            trade_id=self.trade_id,
            market_id=self.market_id,
            expected_edge_bps=self.expected_edge_bps,
            realized_edge_bps=self.realized_edge_bps,
            slippage_bps=self.slippage_bps,
            pnl=self.pnl,
            model_error=self.model_error,
            stale_data_flag=self.stale_data_flag,
            mapping_risk=self.mapping_risk,
            notes=notes,
        )

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "trade_id": self.trade_id,
            "market_id": self.market_id,
            "expected_edge_bps": self.expected_edge_bps,
            "realized_edge_bps": self.realized_edge_bps,
            "slippage_bps": self.slippage_bps,
            "pnl": self.pnl,
            "model_error": self.model_error,
            "stale_data_flag": self.stale_data_flag,
            "mapping_risk": self.mapping_risk,
            "resting": self.resting,
        }
        optional_fields = {
            "signal_edge_bps": self.signal_edge_bps,
            "execution_drag_bps": self.execution_drag_bps,
            "model_residual_bps": self.model_residual_bps,
            "closing_edge_bps": self.closing_edge_bps,
            "value_capture_bps": self.value_capture_bps,
            "requested_quantity": self.requested_quantity,
            "filled_quantity": self.filled_quantity,
            "fill_ratio": self.fill_ratio,
            "wait_steps": self.wait_steps,
        }
        for key, value in optional_fields.items():
            if value is not None:
                payload[key] = value
        if self.notes:
            payload["notes"] = dict(self.notes)
        return payload


@dataclass(frozen=True)
class ReplayAttributionSummary:
    trade_count: int
    total_pnl: float
    average_signal_edge_bps: float
    average_realized_edge_bps: float
    average_execution_drag_bps: float
    average_model_residual_bps: float
    average_closing_edge_bps: float
    average_value_capture_bps: float
    average_fill_ratio: float
    stale_data_count: int
    resting_trade_count: int

    def to_payload(self) -> dict[str, object]:
        return {
            "trade_count": self.trade_count,
            "total_pnl": self.total_pnl,
            "average_signal_edge_bps": self.average_signal_edge_bps,
            "average_realized_edge_bps": self.average_realized_edge_bps,
            "average_execution_drag_bps": self.average_execution_drag_bps,
            "average_model_residual_bps": self.average_model_residual_bps,
            "average_closing_edge_bps": self.average_closing_edge_bps,
            "average_value_capture_bps": self.average_value_capture_bps,
            "average_fill_ratio": self.average_fill_ratio,
            "stale_data_count": self.stale_data_count,
            "resting_trade_count": self.resting_trade_count,
        }


@dataclass(frozen=True)
class ClosingValueBreakdown:
    closing_edge_bps: float
    value_capture_bps: float


def attribute_trade(
    *,
    trade_id: str,
    market_id: str,
    expected_edge_bps: float,
    realized_edge_bps: float,
    pnl: float,
    mapping_risk: float,
    stale_data_flag: bool = False,
    signal_edge_bps: float | None = None,
    execution_drag_bps: float | None = None,
    model_residual_bps: float | None = None,
    closing_edge_bps: float | None = None,
    value_capture_bps: float | None = None,
    requested_quantity: float | None = None,
    filled_quantity: float | None = None,
    fill_ratio: float | None = None,
    wait_steps: int | None = None,
    resting: bool = False,
    notes: dict[str, object] | None = None,
) -> TradeAttribution:
    return TradeAttribution(
        trade_id=trade_id,
        market_id=market_id,
        expected_edge_bps=expected_edge_bps,
        realized_edge_bps=realized_edge_bps,
        slippage_bps=realized_edge_bps - expected_edge_bps,
        pnl=pnl,
        model_error=expected_edge_bps - realized_edge_bps,
        stale_data_flag=stale_data_flag,
        mapping_risk=mapping_risk,
        signal_edge_bps=signal_edge_bps
        if signal_edge_bps is not None
        else expected_edge_bps,
        execution_drag_bps=execution_drag_bps,
        model_residual_bps=model_residual_bps,
        closing_edge_bps=closing_edge_bps,
        value_capture_bps=value_capture_bps,
        requested_quantity=requested_quantity,
        filled_quantity=filled_quantity,
        fill_ratio=fill_ratio,
        wait_steps=wait_steps,
        resting=resting,
        notes=notes,
    )


def persist_trade_attribution(
    attribution: TradeAttribution,
    *,
    root: str | Path = "runtime/data/postgres",
) -> TradeAttributionRecord:
    repository = TradeAttributionRepository(root)
    record = attribution.to_record()
    repository.upsert(record.trade_id, record)
    return record


def attribute_replay_result(
    replay_result,
    *,
    fair_value_by_market: Mapping[str, float] | None = None,
) -> tuple[TradeAttribution, ...]:
    attributes: list[TradeAttribution] = []
    expected_fair_values = fair_value_by_market or {}
    execution_ledger = getattr(replay_result, "execution_ledger", None)
    if execution_ledger is None:
        execution_ledger = tuple(
            trade for event in replay_result.events for trade in event.trades
        )
    for row_index, trade in enumerate(execution_ledger):
        if not trade.filled or trade.quantity <= 0:
            continue
        market_id = trade.contract.market_key
        terminal_mark = replay_result.mark_prices.get(market_id)
        if terminal_mark is None:
            terminal_mark = trade.decision_midpoint or trade.price
        fair_value = trade.decision_fair_value
        if fair_value is None:
            fair_value = expected_fair_values.get(market_id)
        if fair_value is None:
            fair_value = terminal_mark
        decision_price = trade.decision_reference_price or trade.price
        direction = 1.0 if trade.action is OrderAction.BUY else -1.0
        signal_edge_bps = direction * (fair_value - decision_price) * 10_000.0
        realized_edge_bps = direction * (fair_value - trade.price) * 10_000.0
        execution_drag_bps = realized_edge_bps - signal_edge_bps
        closing_value = evaluate_closing_value(
            signal_price=trade.price,
            closing_price=terminal_mark,
            fair_value=fair_value,
            side="buy_yes" if trade.action is OrderAction.BUY else "sell_yes",
        )
        model_residual_bps = closing_value.closing_edge_bps - realized_edge_bps
        pnl = direction * (terminal_mark - trade.price) * trade.quantity
        requested_quantity = trade.requested_quantity or trade.quantity
        fill_ratio = (
            trade.quantity / requested_quantity if requested_quantity > 0 else 0.0
        )
        mapping_risk = 0.0
        if trade.metadata:
            mapping_risk = float(trade.metadata.get("mapping_risk", 0.0) or 0.0)
        attributes.append(
            attribute_trade(
                trade_id=_replay_trade_row_id(trade, row_index=row_index),
                market_id=market_id,
                expected_edge_bps=round(signal_edge_bps, 4),
                realized_edge_bps=round(realized_edge_bps, 4),
                pnl=round(pnl, 6),
                mapping_risk=mapping_risk,
                stale_data_flag=trade.stale_data_flag,
                signal_edge_bps=round(signal_edge_bps, 4),
                execution_drag_bps=round(execution_drag_bps, 4),
                model_residual_bps=round(model_residual_bps, 4),
                closing_edge_bps=closing_value.closing_edge_bps,
                value_capture_bps=closing_value.value_capture_bps,
                requested_quantity=requested_quantity,
                filled_quantity=trade.quantity,
                fill_ratio=round(fill_ratio, 6),
                wait_steps=trade.wait_steps,
                resting=trade.resting,
                notes={
                    "reason": trade.reason,
                    "submitted_step": trade.submitted_step,
                    "fill_step": trade.fill_step,
                },
            )
        )
    return tuple(attributes)


def summarize_trade_attributions(
    attributions: tuple[TradeAttribution, ...],
) -> ReplayAttributionSummary:
    values = list(attributions)

    def _average(items: list[float]) -> float:
        return sum(items) / len(items) if items else 0.0

    return ReplayAttributionSummary(
        trade_count=len(values),
        total_pnl=round(sum(item.pnl for item in values), 6),
        average_signal_edge_bps=round(
            _average([item.signal_edge_bps or 0.0 for item in values]),
            4,
        ),
        average_realized_edge_bps=round(
            _average([item.realized_edge_bps for item in values]),
            4,
        ),
        average_execution_drag_bps=round(
            _average([item.execution_drag_bps or 0.0 for item in values]),
            4,
        ),
        average_model_residual_bps=round(
            _average([item.model_residual_bps or 0.0 for item in values]),
            4,
        ),
        average_closing_edge_bps=round(
            _average([item.closing_edge_bps or 0.0 for item in values]),
            4,
        ),
        average_value_capture_bps=round(
            _average([item.value_capture_bps or 0.0 for item in values]),
            4,
        ),
        average_fill_ratio=round(
            _average([item.fill_ratio or 0.0 for item in values]),
            6,
        ),
        stale_data_count=sum(item.stale_data_flag for item in values),
        resting_trade_count=sum(item.resting for item in values),
    )


def evaluate_closing_value(
    *,
    signal_price: float,
    closing_price: float,
    side: str,
    fair_value: float | None = None,
) -> ClosingValueBreakdown:
    if side == "sell_yes":
        closing_edge_bps = (signal_price - closing_price) * 10_000.0
        value_capture_bps = (
            (signal_price - fair_value) * 10_000.0 if fair_value is not None else 0.0
        )
    else:
        closing_edge_bps = (closing_price - signal_price) * 10_000.0
        value_capture_bps = (
            (fair_value - signal_price) * 10_000.0 if fair_value is not None else 0.0
        )
    return ClosingValueBreakdown(
        closing_edge_bps=round(closing_edge_bps, 4),
        value_capture_bps=round(value_capture_bps, 4),
    )


def _replay_trade_row_id(trade, *, row_index: int) -> str:
    fill_step = trade.fill_step if trade.fill_step is not None else "na"
    return f"{trade.order_id}:fill-{fill_step}:{row_index}"

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

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
    notes: dict[str, object] | None = None

    def to_record(self) -> TradeAttributionRecord:
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
            notes=dict(self.notes or {}),
        )


def attribute_trade(
    *,
    trade_id: str,
    market_id: str,
    expected_edge_bps: float,
    realized_edge_bps: float,
    pnl: float,
    mapping_risk: float,
    stale_data_flag: bool = False,
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
    for event in replay_result.events:
        book = event.book
        fallback_mark = book.midpoint
        if fallback_mark is None:
            if book.best_bid is not None and book.best_ask is not None:
                fallback_mark = (book.best_bid + book.best_ask) / 2.0
            else:
                fallback_mark = book.best_bid or book.best_ask or 0.0
        fair_value = expected_fair_values.get(book.contract.market_key, fallback_mark)
        mapping_risk = float(event.book.raw.get("mapping_risk", 0.0)) if isinstance(event.book.raw, dict) else 0.0
        for trade in event.trades:
            if not trade.filled or trade.quantity <= 0:
                continue
            if trade.action.value == "buy":
                expected_edge_bps = (fair_value - (book.best_ask or trade.price)) * 10_000.0
                realized_edge_bps = (fair_value - trade.price) * 10_000.0
                pnl = (fallback_mark - trade.price) * trade.quantity
            else:
                expected_edge_bps = ((book.best_bid or trade.price) - fair_value) * 10_000.0
                realized_edge_bps = (trade.price - fair_value) * 10_000.0
                pnl = (trade.price - fallback_mark) * trade.quantity
            attributes.append(
                attribute_trade(
                    trade_id=trade.order_id,
                    market_id=trade.contract.market_key,
                    expected_edge_bps=expected_edge_bps,
                    realized_edge_bps=realized_edge_bps,
                    pnl=pnl,
                    mapping_risk=mapping_risk,
                    stale_data_flag=False,
                    notes={
                        "step_index": event.step_index,
                        "reason": trade.reason,
                    },
                )
            )
    return tuple(attributes)

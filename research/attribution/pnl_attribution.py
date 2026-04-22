from __future__ import annotations

from dataclasses import dataclass


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


def attribute_trade(
    *,
    trade_id: str,
    market_id: str,
    expected_edge_bps: float,
    realized_edge_bps: float,
    pnl: float,
    mapping_risk: float,
    stale_data_flag: bool = False,
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
    )

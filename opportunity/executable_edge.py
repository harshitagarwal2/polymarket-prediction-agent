from __future__ import annotations

from dataclasses import dataclass

from adapters.types import OrderAction
from opportunity.models import Opportunity


@dataclass(frozen=True)
class ExecutableEdge:
    action: OrderAction
    quoted_price: float
    executable_price: float
    fee_drag: float
    slippage_bps: float
    edge: float


def compute_edge(
    fair_yes_prob: float,
    best_bid_yes: float,
    best_ask_yes: float,
    fee_bps: float,
    slippage_bps: float,
) -> dict[str, float]:
    fee = max(0.0, fee_bps + slippage_bps) / 10_000.0
    edge_buy_raw_bps = (fair_yes_prob - best_ask_yes) * 10_000.0
    edge_sell_raw_bps = (best_bid_yes - fair_yes_prob) * 10_000.0
    return {
        "edge_buy_raw_bps": edge_buy_raw_bps,
        "edge_sell_raw_bps": edge_sell_raw_bps,
        "edge_after_costs_bps": max(edge_buy_raw_bps, edge_sell_raw_bps) - (fee * 10_000.0),
    }


def assess_executable_edge(
    *,
    fair_value: float,
    quoted_price: float,
    action: OrderAction,
    fee_rate: float = 0.0,
    slippage_bps: float = 0.0,
) -> ExecutableEdge:
    fee_drag = max(0.0, fee_rate * quoted_price * (1.0 - quoted_price))
    slippage = max(0.0, slippage_bps) / 10_000
    if action is OrderAction.BUY:
        executable_price = quoted_price * (1.0 + slippage)
        edge = fair_value - executable_price - fee_drag
    else:
        executable_price = quoted_price * (1.0 - slippage)
        edge = executable_price - fair_value - fee_drag
    return ExecutableEdge(
        action=action,
        quoted_price=quoted_price,
        executable_price=executable_price,
        fee_drag=fee_drag,
        slippage_bps=max(0.0, slippage_bps),
        edge=edge,
    )


def opportunity_from_prices(
    *,
    market_id: str,
    fair_yes_prob: float,
    best_bid_yes: float,
    best_ask_yes: float,
    fillable_size: float,
    confidence: float,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
    blocked_reason: str | None = None,
) -> Opportunity:
    edge = compute_edge(
        fair_yes_prob,
        best_bid_yes,
        best_ask_yes,
        fee_bps,
        slippage_bps,
    )
    side = "buy_yes" if edge["edge_buy_raw_bps"] >= edge["edge_sell_raw_bps"] else "sell_yes"
    return Opportunity(
        market_id=market_id,
        side=side,
        fair_yes_prob=fair_yes_prob,
        best_bid_yes=best_bid_yes,
        best_ask_yes=best_ask_yes,
        edge_buy_bps=round(edge["edge_buy_raw_bps"], 4),
        edge_sell_bps=round(edge["edge_sell_raw_bps"], 4),
        edge_after_costs_bps=round(edge["edge_after_costs_bps"], 4),
        fillable_size=fillable_size,
        confidence=confidence,
        blocked_reason=blocked_reason,
    )

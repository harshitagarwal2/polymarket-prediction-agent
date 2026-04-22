from __future__ import annotations

from dataclasses import dataclass

from adapters.types import OrderAction


@dataclass(frozen=True)
class ExecutableEdge:
    action: OrderAction
    quoted_price: float
    executable_price: float
    fee_drag: float
    slippage_bps: float
    edge: float


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

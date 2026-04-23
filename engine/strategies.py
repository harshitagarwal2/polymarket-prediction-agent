from __future__ import annotations

from dataclasses import dataclass

from adapters.types import OrderAction, OrderIntent
from engine.interfaces import StrategyContext


@dataclass(frozen=True)
class FairValueBandStrategy:
    quantity: float = 1.0
    edge_threshold: float = 0.03
    aggressive: bool = True

    def generate_intents(self, context: StrategyContext) -> list[OrderIntent]:
        fair_value = context.fair_value
        if fair_value is None:
            return []

        trade_quantity = context.metadata.get("trade_quantity")
        quantity = (
            float(trade_quantity) if trade_quantity is not None else self.quantity
        )
        if quantity <= 0:
            return []

        intents: list[OrderIntent] = []
        best_bid = context.book.best_bid
        best_ask = context.book.best_ask

        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            return intents

        if best_ask is not None and fair_value >= best_ask + self.edge_threshold:
            buy_price = (
                best_ask
                if self.aggressive
                else min(fair_value - self.edge_threshold, best_ask)
            )
            intents.append(
                OrderIntent(
                    contract=context.contract,
                    action=OrderAction.BUY,
                    price=round(buy_price, 4),
                    quantity=quantity,
                    post_only=not self.aggressive,
                )
            )

        if (
            best_bid is not None
            and context.position.quantity >= quantity
            and fair_value <= best_bid - self.edge_threshold
        ):
            sell_price = (
                best_bid
                if self.aggressive
                else max(fair_value + self.edge_threshold, best_bid)
            )
            intents.append(
                OrderIntent(
                    contract=context.contract,
                    action=OrderAction.SELL,
                    price=round(sell_price, 4),
                    quantity=quantity,
                    post_only=not self.aggressive,
                    reduce_only=True,
                )
            )

        return intents

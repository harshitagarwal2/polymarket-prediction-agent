from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    FillSnapshot,
    Contract,
    OutcomeSide,
    PriceLevel,
    Venue,
    OrderBookSnapshot,
    BalanceSnapshot,
    PositionSnapshot,
)
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from risk.limits import RiskEngine, RiskLimits


class DemoAdapter:
    venue = Venue.POLYMARKET

    def __init__(self):
        self._contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="demo-token",
            outcome=OutcomeSide.YES,
            title="Demo market",
        )

    def health(self) -> AdapterHealth:
        return AdapterHealth(self.venue, True)

    def get_order_book(self, contract: Contract) -> OrderBookSnapshot:
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.45, quantity=10)],
            asks=[PriceLevel(price=0.50, quantity=10)],
        )

    def list_open_orders(self, contract: Contract | None = None):
        return []

    def list_positions(self, contract: Contract | None = None):
        if contract is None:
            return []
        return [self.get_position(contract)]

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract) -> PositionSnapshot:
        return PositionSnapshot(contract=contract, quantity=0)

    def get_balance(self) -> BalanceSnapshot:
        return BalanceSnapshot(venue=Venue.POLYMARKET, available=100, total=100)

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=self.list_positions(contract),
            open_orders=self.list_open_orders(contract),
            fills=list(self.list_fills(contract)),
        )

    def place_limit_order(self, intent):
        raise RuntimeError("Demo preview should not place orders")

    def cancel_order(self, order_id: str):
        return False

    def cancel_all(self, contract: Contract | None = None):
        return 0

    def close(self):
        return None


if __name__ == "__main__":
    adapter = DemoAdapter()
    contract = Contract(
        venue=Venue.POLYMARKET, symbol="demo-token", outcome=OutcomeSide.YES
    )
    engine = TradingEngine(
        adapter=adapter,
        strategy=FairValueBandStrategy(quantity=3, edge_threshold=0.03),
        risk_engine=RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
        ),
    )
    result = engine.preview_once(contract, fair_value=0.60)
    print("best bid/ask:", result.context.book.best_bid, result.context.book.best_ask)
    print(
        "reconciliation healthy:",
        result.reconciliation_before.healthy if result.reconciliation_before else None,
    )
    print("approved intents:", result.risk.approved)
    print("rejected:", [r.reason for r in result.risk.rejected])

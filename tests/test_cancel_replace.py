from __future__ import annotations

from datetime import datetime, timezone
import unittest

from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderBookSnapshot,
    OrderIntent,
    OrderStatus,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    PriceLevel,
    Venue,
)
from engine.interfaces import NoopStrategy
from engine.runner import TradingEngine
from execution.quote_manager import QuoteManager
from execution.models import OrderProposal
from risk.limits import RiskEngine, RiskLimits


def make_order(
    contract: Contract,
    *,
    order_id: str,
    action: OrderAction,
    price: float,
    quantity: float,
) -> NormalizedOrder:
    now = datetime.now(timezone.utc)
    return NormalizedOrder(
        order_id=order_id,
        contract=contract,
        action=action,
        price=price,
        quantity=quantity,
        remaining_quantity=quantity,
        status=OrderStatus.RESTING,
        created_at=now,
        updated_at=now,
    )


class CancelReplaceAdapter:
    venue = Venue.POLYMARKET

    def __init__(self, open_orders: list[NormalizedOrder] | None = None):
        self.contract = Contract(
            venue=self.venue,
            symbol="token-1",
            outcome=OutcomeSide.YES,
        )
        self._open_orders = list(open_orders or [])
        self.place_calls: list[tuple[float, float]] = []
        self.cancel_calls: list[str] = []

    def health(self):
        return AdapterHealth(self.venue, True)

    def list_markets(self, limit: int = 100):
        return []

    def get_order_book(self, contract: Contract):
        return OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(price=0.49, quantity=10.0)],
            asks=[PriceLevel(price=0.51, quantity=10.0)],
        )

    def list_open_orders(self, contract: Contract | None = None):
        if contract is None:
            return list(self._open_orders)
        return [
            order
            for order in self._open_orders
            if order.contract.market_key == contract.market_key
        ]

    def list_positions(self, contract: Contract | None = None):
        contract = contract or self.contract
        return [PositionSnapshot(contract=contract, quantity=0.0)]

    def list_fills(self, contract: Contract | None = None):
        return []

    def get_position(self, contract: Contract):
        return PositionSnapshot(contract=contract, quantity=0.0)

    def get_balance(self):
        return BalanceSnapshot(venue=self.venue, available=100.0, total=100.0)

    def get_account_snapshot(self, contract: Contract | None = None):
        contract = contract or self.contract
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=[PositionSnapshot(contract=contract, quantity=0.0)],
            open_orders=self.list_open_orders(contract),
            fills=[],
        )

    def place_limit_order(self, intent) -> PlacementResult:
        order_id = f"placed-{len(self.place_calls) + 1}"
        self.place_calls.append((intent.price, intent.quantity))
        self._open_orders.append(
            make_order(
                intent.contract,
                order_id=order_id,
                action=intent.action,
                price=intent.price,
                quantity=intent.quantity,
            )
        )
        return PlacementResult(True, order_id=order_id, status=OrderStatus.RESTING)

    def cancel_order(self, order_id: str):
        self.cancel_calls.append(order_id)
        return True

    def cancel_all(self, contract: Contract | None = None) -> int:
        return 0

    def close(self):
        return None


class CancelReplaceTests(unittest.TestCase):
    def build_engine(self, adapter: CancelReplaceAdapter) -> TradingEngine:
        return TradingEngine(
            adapter=adapter,
            strategy=NoopStrategy(),
            risk_engine=RiskEngine(
                RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
            ),
        )

    def test_sync_quote_places_new_order_through_runner(self):
        adapter = CancelReplaceAdapter()
        engine = self.build_engine(adapter)
        manager = QuoteManager(engine)
        proposal = OrderProposal(
            market_id=adapter.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.51,
            size=2.0,
            tif="GTC",
            rationale="enter quote",
        )

        result = manager.sync_quote(adapter.contract, proposal)

        self.assertEqual(result.action, "place")
        self.assertEqual(adapter.cancel_calls, [])
        self.assertEqual(adapter.place_calls, [(0.51, 2.0)])
        self.assertEqual(result.submitted_order_ids, ("placed-1",))
        self.assertIsNotNone(result.execution)
        if result.execution is None:
            self.fail("expected execution result")
        self.assertEqual(len(result.execution.placements), 1)
        self.assertTrue(result.execution.placements[0].accepted)

    def test_sync_quote_replace_uses_cancel_then_runner_guarded_submit(self):
        existing = make_order(
            Contract(
                venue=Venue.POLYMARKET,
                symbol="token-1",
                outcome=OutcomeSide.YES,
            ),
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.49,
            quantity=1.0,
        )
        adapter = CancelReplaceAdapter(open_orders=[existing])
        engine = self.build_engine(adapter)
        manager = QuoteManager(engine)
        proposal = OrderProposal(
            market_id=adapter.contract.symbol,
            side="buy_yes",
            action="replace",
            price=0.51,
            size=2.0,
            tif="GTC",
            rationale="refresh quote",
        )

        result = manager.sync_quote(adapter.contract, proposal)

        self.assertEqual(result.action, "replace")
        self.assertEqual(result.cancelled_order_ids, ("open-1",))
        self.assertEqual(adapter.cancel_calls, ["open-1"])
        self.assertEqual(adapter.place_calls, [])
        self.assertIsNotNone(result.execution)
        if result.execution is None:
            self.fail("expected blocked execution result")
        self.assertEqual(result.execution.placements, [])
        self.assertEqual(len(engine.pending_cancels(unresolved_only=True)), 1)
        self.assertIsNotNone(
            engine.pending_cancel_submission_guard_reason(adapter.contract)
        )

    def test_sync_quote_cancel_only_tracks_pending_cancel(self):
        existing = make_order(
            Contract(
                venue=Venue.POLYMARKET,
                symbol="token-1",
                outcome=OutcomeSide.YES,
            ),
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.49,
            quantity=1.0,
        )
        adapter = CancelReplaceAdapter(open_orders=[existing])
        engine = self.build_engine(adapter)
        manager = QuoteManager(engine)

        result = manager.sync_quote(adapter.contract, None)

        self.assertEqual(result.action, "cancel")
        self.assertEqual(result.cancelled_order_ids, ("open-1",))
        self.assertEqual(adapter.cancel_calls, ["open-1"])
        self.assertEqual(adapter.place_calls, [])
        self.assertIsNone(result.execution)
        self.assertEqual(len(engine.pending_cancels(unresolved_only=True)), 1)

    def test_sync_quote_defers_when_pending_submission_unresolved(self):
        adapter = CancelReplaceAdapter()
        engine = self.build_engine(adapter)
        manager = QuoteManager(engine)
        engine.track_pending_submission(
            OrderIntent(
                contract=adapter.contract,
                action=OrderAction.BUY,
                price=0.5,
                quantity=1.0,
            ),
            status="needs_recovery",
            reason="ambiguous submission outcome",
        )
        proposal = OrderProposal(
            market_id=adapter.contract.symbol,
            side="buy_yes",
            action="replace",
            price=0.52,
            size=1.0,
            tif="GTC",
            rationale="refresh after ambiguous submit",
        )

        result = manager.sync_quote(adapter.contract, proposal)

        self.assertEqual(result.action, "defer")
        self.assertEqual(adapter.cancel_calls, [])
        self.assertEqual(adapter.place_calls, [])
        self.assertIsNone(result.execution)


if __name__ == "__main__":
    unittest.main()

import unittest

from adapters.types import (
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderIntent,
    OrderStatus,
    OutcomeSide,
    PositionSnapshot,
    Venue,
)
from risk.limits import RiskEngine, RiskLimits


def make_contract() -> Contract:
    return Contract(venue=Venue.KALSHI, symbol="TEST-1", outcome=OutcomeSide.YES)


def make_order(contract: Contract, order_id: str, quantity: float) -> NormalizedOrder:
    return NormalizedOrder(
        order_id=order_id,
        contract=contract,
        action=OrderAction.BUY,
        price=0.45,
        quantity=quantity,
        remaining_quantity=quantity,
        status=OrderStatus.RESTING,
    )


def make_reduce_only_sell_order(
    contract: Contract, order_id: str, quantity: float
) -> NormalizedOrder:
    return NormalizedOrder(
        order_id=order_id,
        contract=contract,
        action=OrderAction.SELL,
        price=0.55,
        quantity=quantity,
        remaining_quantity=quantity,
        status=OrderStatus.RESTING,
        reduce_only=True,
    )


class RiskLimitsTests(unittest.TestCase):
    def test_rejects_market_exposure_over_cap(self):
        contract = make_contract()
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=2, max_global_contracts=10)
        )
        position = PositionSnapshot(contract=contract, quantity=1)
        intents = [
            OrderIntent(
                contract=contract, action=OrderAction.BUY, price=0.45, quantity=2
            )
        ]

        decision = engine.evaluate(intents, position=position, open_orders=[])

        self.assertFalse(decision.approved)
        self.assertTrue(decision.rejected)
        self.assertEqual(
            decision.rejected[0].reason, "per-market exposure cap exceeded"
        )

    def test_rejects_global_exposure_over_cap(self):
        contract = make_contract()
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=3)
        )
        position = PositionSnapshot(contract=contract, quantity=2)
        intents = [
            OrderIntent(
                contract=contract, action=OrderAction.BUY, price=0.45, quantity=2
            )
        ]

        decision = engine.evaluate(intents, position=position, open_orders=[])

        self.assertFalse(decision.approved)
        self.assertEqual(decision.rejected[0].reason, "global exposure cap exceeded")

    def test_approves_valid_intent(self):
        contract = make_contract()
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=10)
        )
        position = PositionSnapshot(contract=contract, quantity=1)
        intent = OrderIntent(
            contract=contract, action=OrderAction.BUY, price=0.45, quantity=1
        )

        decision = engine.evaluate([intent], position=position, open_orders=[])

        self.assertEqual(decision.approved, [intent])
        self.assertFalse(decision.rejected)

    def test_reduce_only_sell_does_not_count_as_new_exposure(self):
        contract = make_contract()
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=2, max_global_contracts=2)
        )
        position = PositionSnapshot(contract=contract, quantity=2)
        intent = OrderIntent(
            contract=contract,
            action=OrderAction.SELL,
            price=0.55,
            quantity=2,
            reduce_only=True,
        )

        decision = engine.evaluate([intent], position=position, open_orders=[])

        self.assertEqual(decision.approved, [intent])
        self.assertFalse(decision.rejected)

    def test_global_exposure_counts_other_positions_and_orders(self):
        contract = make_contract()
        other_contract = Contract(
            venue=Venue.KALSHI,
            symbol="TEST-2",
            outcome=OutcomeSide.YES,
        )
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=3)
        )
        position = PositionSnapshot(contract=contract, quantity=0)
        other_position = PositionSnapshot(contract=other_contract, quantity=2)
        intent = OrderIntent(
            contract=contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=1,
        )

        decision = engine.evaluate(
            [intent],
            position=position,
            positions=[position, other_position],
            open_orders=[make_order(other_contract, "other-open", quantity=1)],
        )

        self.assertFalse(decision.approved)
        self.assertEqual(decision.rejected[0].reason, "global exposure cap exceeded")

    def test_reduce_only_open_sell_order_does_not_count_against_global_exposure(self):
        contract = make_contract()
        other_contract = Contract(
            venue=Venue.KALSHI,
            symbol="TEST-2",
            outcome=OutcomeSide.YES,
        )
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=10, max_global_contracts=3)
        )
        position = PositionSnapshot(contract=contract, quantity=0)
        other_position = PositionSnapshot(contract=other_contract, quantity=2)
        intent = OrderIntent(
            contract=contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=1,
        )

        decision = engine.evaluate(
            [intent],
            position=position,
            positions=[position, other_position],
            open_orders=[
                make_reduce_only_sell_order(other_contract, "other-reduce-only", 1)
            ],
        )

        self.assertEqual(decision.approved, [intent])
        self.assertFalse(decision.rejected)


if __name__ == "__main__":
    unittest.main()

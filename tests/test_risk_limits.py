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
from risk.limits import RiskEngine, RiskLimits, RiskState


def make_contract(
    *, symbol: str = "TEST-1", outcome: OutcomeSide = OutcomeSide.YES
) -> Contract:
    return Contract(venue=Venue.KALSHI, symbol=symbol, outcome=outcome)


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

    def test_evaluate_rejects_entire_batch_when_later_intent_fails(self):
        contract = make_contract()
        engine = RiskEngine(
            RiskLimits(max_contracts_per_market=2, max_global_contracts=10)
        )
        intents = [
            OrderIntent(
                contract=contract,
                action=OrderAction.BUY,
                price=0.45,
                quantity=1,
            ),
            OrderIntent(
                contract=contract,
                action=OrderAction.BUY,
                price=0.45,
                quantity=2,
            ),
        ]

        decision = engine.evaluate(
            intents,
            position=PositionSnapshot(contract=contract, quantity=0),
            open_orders=[],
        )

        self.assertFalse(decision.approved)
        self.assertEqual(len(decision.rejected), 2)
        self.assertIs(decision.rejected[0].intent, intents[0])
        self.assertEqual(
            decision.rejected[0].reason,
            "batched with rejected intent: per-market exposure cap exceeded",
        )
        self.assertIs(decision.rejected[1].intent, intents[1])
        self.assertEqual(
            decision.rejected[1].reason,
            "per-market exposure cap exceeded",
        )

    def test_evaluate_allows_non_atomic_batches_when_disabled(self):
        contract = make_contract()
        engine = RiskEngine(
            RiskLimits(
                max_contracts_per_market=2,
                max_global_contracts=10,
                enforce_atomic_batches=False,
            )
        )
        intents = [
            OrderIntent(
                contract=contract,
                action=OrderAction.BUY,
                price=0.45,
                quantity=1,
            ),
            OrderIntent(
                contract=contract,
                action=OrderAction.BUY,
                price=0.45,
                quantity=2,
            ),
        ]

        decision = engine.evaluate(
            intents,
            position=PositionSnapshot(contract=contract, quantity=0),
            open_orders=[],
        )

        self.assertEqual(decision.approved, [intents[0]])
        self.assertEqual(len(decision.rejected), 1)
        self.assertIs(decision.rejected[0].intent, intents[1])
        self.assertEqual(
            decision.rejected[0].reason,
            "per-market exposure cap exceeded",
        )

    def test_daily_loss_limit_rejects_at_exact_boundary(self):
        contract = make_contract()
        intent = OrderIntent(
            contract=contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=1,
        )
        engine = RiskEngine(
            RiskLimits(
                max_contracts_per_market=10, max_global_contracts=10, max_daily_loss=5.0
            ),
            state=RiskState(daily_realized_pnl=-5.0),
        )

        decision = engine.evaluate(
            [intent],
            position=PositionSnapshot(contract=contract, quantity=0),
            open_orders=[],
        )

        self.assertFalse(decision.approved)
        self.assertEqual(len(decision.rejected), 1)
        self.assertEqual(decision.rejected[0].reason, "daily loss limit reached")

    def test_daily_loss_limit_allows_intents_just_inside_boundary(self):
        contract = make_contract()
        intent = OrderIntent(
            contract=contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=1,
        )
        engine = RiskEngine(
            RiskLimits(
                max_contracts_per_market=10, max_global_contracts=10, max_daily_loss=5.0
            ),
            state=RiskState(daily_realized_pnl=-4.99),
        )

        decision = engine.evaluate(
            [intent],
            position=PositionSnapshot(contract=contract, quantity=0),
            open_orders=[],
        )

        self.assertEqual(decision.approved, [intent])
        self.assertFalse(decision.rejected)

    def test_rejects_event_exposure_over_cap_across_registered_markets(self):
        contract = make_contract(symbol="TEST-1")
        other_contract = make_contract(symbol="TEST-2")
        engine = RiskEngine(
            RiskLimits(
                max_contracts_per_market=10,
                max_global_contracts=10,
                max_contracts_per_event=2,
            )
        )
        engine.register_market_event(contract.market_key, event_key="event-1")
        engine.register_market_event(other_contract.market_key, event_key="event-1")
        intent = OrderIntent(
            contract=contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=2,
        )

        decision = engine.evaluate(
            [intent],
            position=PositionSnapshot(contract=contract, quantity=0),
            positions=[
                PositionSnapshot(contract=contract, quantity=0),
                PositionSnapshot(contract=other_contract, quantity=1),
            ],
            open_orders=[],
        )

        self.assertFalse(decision.approved)
        self.assertEqual(decision.rejected[0].reason, "per-event exposure cap exceeded")

    def test_event_registry_falls_back_to_normalized_composite_key(self):
        contract = make_contract(symbol="TEST-1")
        other_contract = make_contract(symbol="TEST-2")
        engine = RiskEngine(
            RiskLimits(
                max_contracts_per_market=10,
                max_global_contracts=10,
                max_contracts_per_event=2,
            )
        )

        first_key = engine.register_market_event(
            contract.market_key,
            sport="NBA",
            series="NBA Finals",
            game_id="Game 1",
        )
        second_key = engine.register_market_event(
            other_contract.market_key,
            sport="nba",
            series="NBA-Finals",
            game_id="game 1",
        )

        self.assertEqual(first_key, "composite:nba:nba-finals:game-1")
        self.assertEqual(second_key, first_key)
        self.assertNotIn(contract.market_key.lower(), first_key or "")

        intent = OrderIntent(
            contract=contract,
            action=OrderAction.BUY,
            price=0.45,
            quantity=2,
        )
        decision = engine.evaluate(
            [intent],
            position=PositionSnapshot(contract=contract, quantity=0),
            positions=[
                PositionSnapshot(contract=contract, quantity=0),
                PositionSnapshot(contract=other_contract, quantity=1),
            ],
            open_orders=[],
        )

        self.assertFalse(decision.approved)
        self.assertEqual(decision.rejected[0].reason, "per-event exposure cap exceeded")


if __name__ == "__main__":
    unittest.main()

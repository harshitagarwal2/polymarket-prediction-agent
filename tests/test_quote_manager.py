from __future__ import annotations

import math
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
import unittest

from adapters.types import (
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderStatus,
    OutcomeSide,
    Venue,
)
from execution.models import OrderProposal
from execution.quote_manager import QuoteManager


class _UnusedCancelReplaceEngine:
    def execute(self, plan, *, reason: str = "quote refresh", metadata=None):
        raise AssertionError("quote planning test should not execute cancel/replace")


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


class QuoteManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = Contract(
            venue=Venue.POLYMARKET,
            symbol="token-1",
            outcome=OutcomeSide.YES,
        )
        self.engine = SimpleNamespace(
            adapter=SimpleNamespace(list_open_orders=lambda contract: []),
            account_state=SimpleNamespace(open_orders_for=lambda contract: []),
            pending_submissions=lambda contract, unresolved_only=False: [],
        )
        self.manager = QuoteManager(
            cast(Any, self.engine),
            cancel_replace_engine=cast(Any, _UnusedCancelReplaceEngine()),
        )

    def test_plan_quote_places_when_no_working_order_exists(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.51,
            size=2.0,
            tif="GTC",
            rationale="edge_after_costs_bps=210.00",
        )

        plan = self.manager.plan_quote(self.contract, proposal, open_orders=[])

        self.assertEqual(plan.action, "place")
        self.assertEqual(plan.cancel_orders, ())
        self.assertIsNotNone(plan.submit_intent)
        if plan.submit_intent is None:
            self.fail("expected submit intent")
        self.assertEqual(plan.submit_intent.action, OrderAction.BUY)
        self.assertEqual(plan.submit_intent.price, 0.51)
        self.assertEqual(plan.submit_intent.quantity, 2.0)

    def test_plan_quote_uses_engine_tracked_working_orders_when_adapter_lags(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.51,
            size=2.0,
            tif="GTC",
            rationale="edge_after_costs_bps=210.00",
        )
        tracked = make_order(
            self.contract,
            order_id="tracked-1",
            action=OrderAction.BUY,
            price=0.51,
            quantity=2.0,
        )
        self.engine.account_state.open_orders_for = lambda contract: [tracked]
        self.engine.adapter.list_open_orders = lambda contract: []

        plan = self.manager.plan_quote(self.contract, proposal)

        self.assertEqual(plan.action, "keep")
        self.assertEqual(plan.cancel_orders, ())
        self.assertIsNone(plan.submit_intent)

    def test_plan_quote_respects_explicit_empty_open_order_override(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.51,
            size=2.0,
            tif="GTC",
            rationale="edge_after_costs_bps=210.00",
        )
        existing = make_order(
            self.contract,
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )
        self.engine.adapter.list_open_orders = lambda contract: [existing]

        plan = self.manager.plan_quote(self.contract, proposal, open_orders=[])

        self.assertEqual(plan.action, "place")
        self.assertEqual(plan.cancel_orders, ())
        self.assertIsNotNone(plan.submit_intent)

    def test_plan_quote_keeps_matching_working_order(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.5,
            size=1.0,
            tif="GTC",
            rationale="matching quote",
        )
        existing = make_order(
            self.contract,
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )

        plan = self.manager.plan_quote(self.contract, proposal, open_orders=[existing])

        self.assertEqual(plan.action, "keep")
        self.assertEqual(plan.cancel_orders, ())
        self.assertIsNone(plan.submit_intent)

    def test_plan_quote_replaces_unmatched_working_order(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="amend",
            price=0.53,
            size=1.5,
            tif="GTC",
            rationale="tighten quote",
        )
        existing = make_order(
            self.contract,
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )

        plan = self.manager.plan_quote(self.contract, proposal, open_orders=[existing])

        self.assertEqual(plan.action, "replace")
        self.assertEqual([order.order_id for order in plan.cancel_orders], ["open-1"])
        self.assertIsNotNone(plan.submit_intent)
        if plan.submit_intent is None:
            self.fail("expected submit intent")
        self.assertEqual(plan.submit_intent.action, OrderAction.BUY)
        self.assertEqual(plan.submit_intent.price, 0.53)
        self.assertEqual(plan.submit_intent.quantity, 1.5)

    def test_plan_quote_cancels_surplus_duplicate_orders_without_resubmitting(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.5,
            size=1.0,
            tif="GTC",
            rationale="dedupe quote",
        )
        first = make_order(
            self.contract,
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )
        duplicate = make_order(
            self.contract,
            order_id="open-2",
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )

        plan = self.manager.plan_quote(
            self.contract,
            proposal,
            open_orders=[first, duplicate],
        )

        self.assertEqual(plan.action, "cancel")
        self.assertEqual([order.order_id for order in plan.cancel_orders], ["open-2"])
        self.assertIsNone(plan.submit_intent)

    def test_proposal_to_intent_rejects_outcome_mismatch(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_no",
            action="place",
            price=0.49,
            size=1.0,
            tif="GTC",
            rationale="wrong side",
        )

        with self.assertRaisesRegex(ValueError, "contract outcome"):
            self.manager.proposal_to_intent(self.contract, proposal)

    def test_plan_quote_rejects_cancel_for_wrong_market(self):
        proposal = OrderProposal(
            market_id="other-token",
            side="buy_yes",
            action="cancel",
            price=0.0,
            size=0.0,
            tif="GTC",
            rationale="cancel wrong market",
        )
        existing = make_order(
            self.contract,
            order_id="open-1",
            action=OrderAction.BUY,
            price=0.5,
            quantity=1.0,
        )

        with self.assertRaisesRegex(ValueError, "market_id"):
            self.manager.plan_quote(self.contract, proposal, open_orders=[existing])

    def test_proposal_to_intent_rejects_non_finite_price(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=math.nan,
            size=1.0,
            tif="GTC",
            rationale="bad price",
        )

        with self.assertRaisesRegex(ValueError, "price must be finite"):
            self.manager.proposal_to_intent(self.contract, proposal)

    def test_proposal_to_intent_rejects_non_finite_size(self):
        proposal = OrderProposal(
            market_id=self.contract.symbol,
            side="buy_yes",
            action="place",
            price=0.5,
            size=math.inf,
            tif="GTC",
            rationale="bad size",
        )

        with self.assertRaisesRegex(ValueError, "size must be finite"):
            self.manager.proposal_to_intent(self.contract, proposal)


if __name__ == "__main__":
    unittest.main()

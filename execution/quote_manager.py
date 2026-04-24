from __future__ import annotations

import math
from typing import TYPE_CHECKING, Sequence

from adapters.types import (
    Contract,
    NormalizedOrder,
    OrderAction,
    OrderIntent,
    OutcomeSide,
)
from engine.runner import TradingEngine

from execution.models import OrderProposal, QuotePlan


if TYPE_CHECKING:
    from execution.cancel_replace import CancelReplaceResult, CancelReplaceEngine


class QuoteManager:
    def __init__(
        self,
        engine: TradingEngine,
        cancel_replace_engine: "CancelReplaceEngine | None" = None,
        *,
        price_epsilon: float = 1e-9,
        quantity_epsilon: float = 1e-9,
    ) -> None:
        self.engine = engine
        if cancel_replace_engine is None:
            from execution.cancel_replace import CancelReplaceEngine

            cancel_replace_engine = CancelReplaceEngine(engine)
        self.cancel_replace_engine = cancel_replace_engine
        self.price_epsilon = abs(price_epsilon)
        self.quantity_epsilon = abs(quantity_epsilon)

    def working_orders(self, contract: Contract) -> tuple[NormalizedOrder, ...]:
        merged: dict[str, NormalizedOrder] = {}
        for order in self.engine.account_state.open_orders_for(contract):
            merged[order.order_id] = order
        for order in self.engine.adapter.list_open_orders(contract):
            merged[order.order_id] = order
        return tuple(merged.values())

    def proposal_to_intent(
        self,
        contract: Contract,
        proposal: OrderProposal,
    ) -> OrderIntent:
        self._validate_proposal(contract, proposal)
        action = self._proposal_action(contract, proposal)
        return OrderIntent(
            contract=contract,
            action=action,
            price=proposal.price,
            quantity=proposal.size,
            metadata={
                "source": "quote_manager",
                "proposal_market_id": proposal.market_id,
                "proposal_side": proposal.side,
                "proposal_action": proposal.action,
                "tif": proposal.tif,
                "rationale": proposal.rationale,
            },
        )

    def plan_quote(
        self,
        contract: Contract,
        proposal: OrderProposal | None,
        *,
        open_orders: Sequence[NormalizedOrder] | None = None,
    ) -> QuotePlan:
        if proposal is not None:
            self._validate_plan_proposal(contract, proposal)
        existing_orders = (
            self.working_orders(contract) if open_orders is None else tuple(open_orders)
        )
        if self._should_defer_for_pending_submission(contract, existing_orders):
            return QuotePlan(
                contract=contract,
                action="defer",
                existing_orders=existing_orders,
                proposal=proposal,
                rationale="pending submission awaiting authoritative observation",
            )
        normalized_proposal = self._normalize_proposal(proposal)
        if normalized_proposal is None:
            action = "cancel" if existing_orders else "noop"
            return QuotePlan(
                contract=contract,
                action=action,
                existing_orders=existing_orders,
                cancel_orders=existing_orders,
                proposal=None,
                rationale=(
                    "cancel working orders" if existing_orders else "no working orders"
                ),
            )

        intent = self.proposal_to_intent(contract, normalized_proposal)
        matching_orders = [
            order
            for order in existing_orders
            if self._orders_match_intent(order, intent)
        ]
        if matching_orders:
            keep_order = matching_orders[0]
            cancel_orders = tuple(
                order
                for order in existing_orders
                if order.order_id != keep_order.order_id
            )
            return QuotePlan(
                contract=contract,
                action="cancel" if cancel_orders else "keep",
                existing_orders=existing_orders,
                cancel_orders=cancel_orders,
                proposal=normalized_proposal,
                rationale=(
                    "cancel surplus working orders"
                    if cancel_orders
                    else "existing working order already matches proposal"
                ),
            )

        return QuotePlan(
            contract=contract,
            action="replace" if existing_orders else "place",
            existing_orders=existing_orders,
            cancel_orders=existing_orders,
            submit_intent=intent,
            proposal=normalized_proposal,
            rationale=(
                "replace unmatched working order"
                if existing_orders
                else "submit new working order"
            ),
        )

    def sync_quote(
        self,
        contract: Contract,
        proposal: OrderProposal | None,
        *,
        open_orders: Sequence[NormalizedOrder] | None = None,
        reason: str = "quote refresh",
        metadata: dict[str, object] | None = None,
    ) -> "CancelReplaceResult":
        plan = self.plan_quote(contract, proposal, open_orders=open_orders)
        return self.cancel_replace_engine.execute(
            plan,
            reason=reason,
            metadata=metadata,
        )

    def _normalize_proposal(
        self, proposal: OrderProposal | None
    ) -> OrderProposal | None:
        if proposal is None:
            return None
        if proposal.action == "cancel":
            return None
        return proposal

    def _validate_plan_proposal(
        self, contract: Contract, proposal: OrderProposal
    ) -> None:
        if proposal.market_id != contract.symbol:
            raise ValueError(
                "proposal market_id must match contract symbol for execution"
            )
        if proposal.action not in {"place", "replace", "amend", "cancel"}:
            raise ValueError(
                "proposal action must be one of place, replace, amend, or cancel"
            )
        self._proposal_action(contract, proposal)
        if proposal.action == "cancel":
            return
        self._validate_submission_fields(proposal)

    def _validate_proposal(self, contract: Contract, proposal: OrderProposal) -> None:
        self._validate_plan_proposal(contract, proposal)
        if proposal.action == "cancel":
            raise ValueError("cancel proposals cannot be converted into order intents")

    def _validate_submission_fields(self, proposal: OrderProposal) -> None:
        if not math.isfinite(proposal.price):
            raise ValueError("proposal price must be finite")
        if not math.isfinite(proposal.size):
            raise ValueError("proposal size must be finite")
        if proposal.size <= 0.0:
            raise ValueError("proposal size must be positive")

    def _proposal_action(
        self,
        contract: Contract,
        proposal: OrderProposal,
    ) -> OrderAction:
        side_to_action = {
            "buy_yes": (OutcomeSide.YES, OrderAction.BUY),
            "sell_yes": (OutcomeSide.YES, OrderAction.SELL),
            "buy_no": (OutcomeSide.NO, OrderAction.BUY),
            "sell_no": (OutcomeSide.NO, OrderAction.SELL),
        }
        expected = side_to_action.get(proposal.side)
        if expected is None:
            raise ValueError(f"unsupported proposal side: {proposal.side}")
        expected_outcome, action = expected
        if contract.outcome != expected_outcome:
            raise ValueError(
                "proposal side must match the contract outcome for execution"
            )
        return action

    def _orders_match_intent(self, order: NormalizedOrder, intent: OrderIntent) -> bool:
        return (
            order.contract.market_key == intent.contract.market_key
            and order.action == intent.action
            and abs(order.price - intent.price) <= self.price_epsilon
            and abs(order.remaining_quantity - intent.quantity) <= self.quantity_epsilon
            and order.post_only == intent.post_only
            and order.reduce_only == intent.reduce_only
            and order.expiration_ts == intent.expiration_ts
        )

    def _should_defer_for_pending_submission(
        self,
        contract: Contract,
        existing_orders: Sequence[NormalizedOrder],
    ) -> bool:
        return bool(
            not existing_orders
            and self.engine.pending_submissions(contract, unresolved_only=True)
        )

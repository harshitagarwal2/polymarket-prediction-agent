from __future__ import annotations

from dataclasses import dataclass

from adapters.types import Contract, NormalizedOrder, OrderIntent


@dataclass(frozen=True)
class OrderProposal:
    market_id: str
    side: str
    action: str
    price: float
    size: float
    tif: str
    rationale: str


@dataclass(frozen=True)
class QuotePlan:
    contract: Contract
    action: str
    existing_orders: tuple[NormalizedOrder, ...] = ()
    cancel_orders: tuple[NormalizedOrder, ...] = ()
    submit_intent: OrderIntent | None = None
    proposal: OrderProposal | None = None
    rationale: str | None = None

    @property
    def has_submission(self) -> bool:
        return self.submit_intent is not None

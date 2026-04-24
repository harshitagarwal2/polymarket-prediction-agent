from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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
    post_only: bool = False
    reduce_only: bool = False
    expiration_ts: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


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

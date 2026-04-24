from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from adapters.types import PlacementResult
from engine.runner import EngineRunResult, TradingEngine
from engine.safety_state import PendingCancelState

from execution.models import QuotePlan


@dataclass(frozen=True)
class CancelReplaceResult:
    action: str
    cancel_records: tuple[PendingCancelState, ...] = ()
    preview: EngineRunResult | None = None
    execution: EngineRunResult | None = None

    @property
    def cancelled_order_ids(self) -> tuple[str, ...]:
        return tuple(record.order_id for record in self.cancel_records)

    @property
    def placements(self) -> tuple[PlacementResult, ...]:
        if self.execution is None:
            return ()
        return tuple(self.execution.placements)

    @property
    def submitted_order_ids(self) -> tuple[str, ...]:
        return tuple(
            placement.order_id
            for placement in self.placements
            if placement.accepted and placement.order_id is not None
        )


class CancelReplaceEngine:
    def __init__(self, engine: TradingEngine) -> None:
        self.engine = engine

    def execute(
        self,
        plan: QuotePlan,
        *,
        reason: str = "quote refresh",
        metadata: dict[str, Any] | None = None,
    ) -> CancelReplaceResult:
        if plan.action in {"noop", "keep"}:
            return CancelReplaceResult(action=plan.action)

        cancel_records = tuple(
            self.engine.request_cancel_order(order, reason)
            for order in plan.cancel_orders
        )
        preview = None
        execution = None
        if plan.submit_intent is not None:
            merged_metadata = dict(metadata or {})
            merged_metadata.setdefault("execution_shell_action", plan.action)
            merged_metadata.setdefault("execution_shell_reason", plan.rationale)
            preview = self.engine.preview_intents(
                plan.contract,
                [plan.submit_intent],
                metadata=merged_metadata,
            )
            execution = self.engine.run_precomputed(preview, contracts=[plan.contract])
        return CancelReplaceResult(
            action=plan.action,
            cancel_records=cancel_records,
            preview=preview,
            execution=execution,
        )

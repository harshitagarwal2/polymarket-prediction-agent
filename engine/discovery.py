from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Literal, Protocol

from adapters import MarketSummary, OpportunityCandidate
from adapters.base import TradingAdapter
from adapters.types import OrderAction
from engine import OrderLifecycleManager, OrderLifecyclePolicy
from engine.runner import EngineRunResult, TradingEngine
from research.storage import EventJournal


class FairValueProvider(Protocol):
    def fair_value_for(self, market: MarketSummary) -> float | None: ...


@dataclass(frozen=True)
class StaticFairValueProvider:
    fair_values: dict[str, float]

    def fair_value_for(self, market: MarketSummary) -> float | None:
        return self.fair_values.get(market.contract.market_key)


@dataclass(frozen=True)
class OpportunityRanker:
    edge_threshold: float = 0.03
    limit: int = 25

    def rank(
        self,
        markets: list[MarketSummary],
        fair_value_provider: FairValueProvider,
    ) -> list[OpportunityCandidate]:
        candidates: list[OpportunityCandidate] = []
        for market in markets:
            if not market.active:
                continue
            fair_value = fair_value_provider.fair_value_for(market)
            if fair_value is None:
                continue

            if market.best_ask is not None:
                buy_edge = fair_value - market.best_ask
                if buy_edge >= self.edge_threshold:
                    candidates.append(
                        OpportunityCandidate(
                            contract=market.contract,
                            action=OrderAction.BUY,
                            fair_value=fair_value,
                            market_price=market.best_ask,
                            edge=buy_edge,
                            score=buy_edge,
                            rationale=(
                                f"fair_value {fair_value:.4f} exceeds ask {market.best_ask:.4f} by {buy_edge:.4f}"
                            ),
                            raw=market.raw,
                        )
                    )

            if market.best_bid is not None:
                sell_edge = market.best_bid - fair_value
                if sell_edge >= self.edge_threshold:
                    candidates.append(
                        OpportunityCandidate(
                            contract=market.contract,
                            action=OrderAction.SELL,
                            fair_value=fair_value,
                            market_price=market.best_bid,
                            edge=sell_edge,
                            score=sell_edge,
                            rationale=(
                                f"bid {market.best_bid:.4f} exceeds fair_value {fair_value:.4f} by {sell_edge:.4f}"
                            ),
                            raw=market.raw,
                        )
                    )

        return sorted(candidates, key=lambda candidate: candidate.score, reverse=True)[
            : self.limit
        ]


@dataclass
class ScanCycleResult:
    markets: list[MarketSummary]
    candidates: list[OpportunityCandidate]
    selected: OpportunityCandidate | None = None
    execution: EngineRunResult | None = None
    policy_allowed: bool | None = None
    policy_reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reasons: list[str] = field(default_factory=list)


@dataclass
class ExecutionPolicyGate:
    min_top_level_liquidity: float = 1.0
    max_spread: float | None = 0.10
    max_book_age_seconds: float | None = 10.0
    cooldown_seconds: float = 0.0
    block_on_unhealthy_reconciliation: bool = True
    prevent_same_side_duplicate: bool = True
    max_position_quantity_per_contract: float | None = None
    max_open_orders_per_contract: int | None = None
    max_contract_capital_at_risk: float | None = None
    max_open_orders_global: int | None = None
    max_global_open_order_notional: float | None = None
    block_on_contract_partial_fills: bool = True
    max_partial_fills_global: int | None = None
    last_executed_at: dict[str, datetime] = field(default_factory=dict)

    def _candidate_order_notional(
        self,
        candidate: OpportunityCandidate,
        preview: EngineRunResult,
    ) -> float:
        matching_approved = [
            intent
            for intent in preview.risk.approved
            if intent.contract.market_key == candidate.contract.market_key
            and intent.action is candidate.action
        ]
        if matching_approved:
            return sum(intent.notional for intent in matching_approved)

        matching_proposed = [
            intent
            for intent in preview.proposed
            if intent.contract.market_key == candidate.contract.market_key
            and intent.action is candidate.action
        ]
        if matching_proposed:
            return sum(intent.notional for intent in matching_proposed)

        trade_quantity = preview.context.metadata.get("trade_quantity")
        if trade_quantity is not None:
            return float(trade_quantity) * candidate.market_price
        return candidate.market_price

    def evaluate(
        self,
        candidate: OpportunityCandidate,
        preview: EngineRunResult,
    ) -> PolicyDecision:
        reasons: list[str] = []
        book = preview.context.book
        now = datetime.now(timezone.utc)
        candidate_order_notional = self._candidate_order_notional(candidate, preview)

        if self.block_on_unhealthy_reconciliation:
            reconciliation = preview.reconciliation_before
            if reconciliation is not None and not reconciliation.healthy:
                reasons.append("reconciliation not healthy before execution")

        if self.max_book_age_seconds is not None:
            age_seconds = (now - book.observed_at).total_seconds()
            if age_seconds > self.max_book_age_seconds:
                reasons.append(
                    f"market snapshot too old ({age_seconds:.2f}s > {self.max_book_age_seconds:.2f}s)"
                )

        if (
            self.max_spread is not None
            and book.best_bid is not None
            and book.best_ask is not None
        ):
            spread = book.best_ask - book.best_bid
            if spread > self.max_spread:
                reasons.append(
                    f"spread too wide ({spread:.4f} > {self.max_spread:.4f})"
                )

        if candidate.action is OrderAction.BUY:
            top_liquidity = book.asks[0].quantity if book.asks else 0.0
        else:
            top_liquidity = book.bids[0].quantity if book.bids else 0.0
        if top_liquidity < self.min_top_level_liquidity:
            reasons.append(
                f"top-level liquidity too low ({top_liquidity:.4f} < {self.min_top_level_liquidity:.4f})"
            )

        if self.prevent_same_side_duplicate:
            if (
                candidate.action is OrderAction.BUY
                and preview.context.position.quantity > 0
            ):
                reasons.append("existing position already open for candidate contract")
            if any(
                order.contract.market_key == candidate.contract.market_key
                and order.action is candidate.action
                for order in preview.context.open_orders
            ):
                reasons.append(
                    "same-side open order already exists for candidate contract"
                )

        if self.max_position_quantity_per_contract is not None:
            if (
                preview.context.position.quantity
                >= self.max_position_quantity_per_contract
            ):
                reasons.append(
                    "position quantity limit already reached for candidate contract"
                )

        if self.max_open_orders_per_contract is not None:
            if len(preview.context.open_orders) >= self.max_open_orders_per_contract:
                reasons.append("open-order count limit reached for candidate contract")

        if self.max_contract_capital_at_risk is not None:
            current_capital = preview.context.position.quantity * candidate.market_price
            current_capital += sum(
                order.remaining_quantity * order.price
                for order in preview.context.open_orders
            )
            proposed_capital = current_capital + candidate_order_notional
            if proposed_capital > self.max_contract_capital_at_risk:
                reasons.append(
                    f"capital-at-risk limit reached for candidate contract ({proposed_capital:.4f} > {self.max_contract_capital_at_risk:.4f})"
                )

        contract_partial_fill_count = int(
            preview.context.metadata.get("partial_fill_count_contract", 0) or 0
        )
        if self.block_on_contract_partial_fills and contract_partial_fill_count > 0:
            reasons.append("unresolved partial fills exist for candidate contract")

        global_open_order_count = int(
            preview.context.metadata.get("global_open_order_count", 0) or 0
        )
        if self.max_open_orders_global is not None:
            if global_open_order_count >= self.max_open_orders_global:
                reasons.append("global open-order count limit reached")

        global_open_order_notional = float(
            preview.context.metadata.get("global_open_order_notional", 0.0) or 0.0
        )
        if self.max_global_open_order_notional is not None:
            proposed_global_open_order_notional = (
                global_open_order_notional + candidate_order_notional
            )
            if (
                proposed_global_open_order_notional
                > self.max_global_open_order_notional
            ):
                reasons.append(
                    f"global open-order notional limit reached ({proposed_global_open_order_notional:.4f} > {self.max_global_open_order_notional:.4f})"
                )

        global_partial_fill_count = int(
            preview.context.metadata.get("partial_fill_count_global", 0) or 0
        )
        if self.max_partial_fills_global is not None:
            if global_partial_fill_count >= self.max_partial_fills_global:
                reasons.append("global partial-fill limit reached")

        if self.cooldown_seconds > 0:
            previous = self.last_executed_at.get(candidate.contract.market_key)
            if previous is not None:
                elapsed = (now - previous).total_seconds()
                if elapsed < self.cooldown_seconds:
                    reasons.append(
                        f"candidate contract in cooldown ({elapsed:.2f}s < {self.cooldown_seconds:.2f}s)"
                    )

        return PolicyDecision(allowed=not reasons, reasons=reasons)

    def record_execution(self, candidate: OpportunityCandidate) -> None:
        self.last_executed_at[candidate.contract.market_key] = datetime.now(
            timezone.utc
        )


class Sizer(Protocol):
    def size(
        self, candidate: OpportunityCandidate, preview: EngineRunResult
    ) -> float: ...


@dataclass(frozen=True)
class DeterministicSizer:
    base_quantity: float = 1.0
    max_quantity: float = 10.0
    edge_unit: float = 0.03
    liquidity_fraction: float = 0.5
    min_quantity: float = 0.0

    def size(self, candidate: OpportunityCandidate, preview: EngineRunResult) -> float:
        if candidate.action is OrderAction.BUY:
            top_liquidity = (
                preview.context.book.asks[0].quantity
                if preview.context.book.asks
                else 0.0
            )
        else:
            top_liquidity = (
                preview.context.book.bids[0].quantity
                if preview.context.book.bids
                else 0.0
            )
        edge_multiple = max(1.0, candidate.edge / max(self.edge_unit, 1e-9))
        proposed = min(
            self.max_quantity,
            self.base_quantity * edge_multiple,
            top_liquidity * self.liquidity_fraction,
        )
        if proposed < self.min_quantity:
            return 0.0
        return round(proposed, 4)


@dataclass
class AgentOrchestrator:
    adapter: TradingAdapter
    engine: TradingEngine
    fair_value_provider: FairValueProvider
    ranker: OpportunityRanker = field(default_factory=OpportunityRanker)
    policy_gate: ExecutionPolicyGate = field(default_factory=ExecutionPolicyGate)
    sizer: Sizer = field(default_factory=DeterministicSizer)
    journal: EventJournal | None = None

    def _status_payload(self) -> dict[str, object]:
        status = self.engine.status_snapshot()
        pending_cancels = list(status.pending_cancels)
        return {
            "engine_halted": status.halted,
            "engine_halt_reason": status.halt_reason,
            "engine_paused": status.paused,
            "engine_pause_reason": status.pause_reason,
            "heartbeat_required": status.heartbeat_required,
            "heartbeat_active": status.heartbeat_active,
            "heartbeat_running": status.heartbeat_running,
            "heartbeat_healthy_for_trading": status.heartbeat_healthy_for_trading,
            "heartbeat_unhealthy": status.heartbeat_unhealthy,
            "heartbeat_last_success_at": status.heartbeat_last_success_at,
            "heartbeat_consecutive_failures": status.heartbeat_consecutive_failures,
            "heartbeat_last_error": status.heartbeat_last_error,
            "heartbeat_last_id": status.heartbeat_last_id,
            "pending_cancel_count": len(pending_cancels),
            "pending_cancel_operator_attention_required": any(
                item.operator_attention_required for item in pending_cancels
            ),
            "pending_cancel_post_fill_seen": any(
                item.post_cancel_fill_seen for item in pending_cancels
            ),
        }

    def _log_cycle(
        self, cycle: ScanCycleResult, mode: str, cycle_id: str | None = None
    ) -> None:
        if self.journal is None:
            return
        self.journal.append(
            "scan_cycle",
            {
                "cycle_id": cycle_id,
                "mode": mode,
                "market_count": len(cycle.markets),
                "candidate_count": len(cycle.candidates),
                "selected": cycle.selected,
                "execution": cycle.execution,
                "policy_allowed": cycle.policy_allowed,
                "policy_reasons": cycle.policy_reasons,
                **self._status_payload(),
            },
        )

    def log_skip(self, mode: str, reason: str, cycle_id: str | None = None) -> None:
        if self.journal is None:
            return
        self.journal.append(
            "scan_cycle_skipped",
            {
                "cycle_id": cycle_id,
                "mode": mode,
                "reason": reason,
                **self._status_payload(),
            },
        )

    def log_truth_block(
        self, mode: str, issues: list[str], cycle_id: str | None = None
    ) -> None:
        if self.journal is None:
            return
        self.journal.append(
            "scan_cycle_blocked",
            {
                "cycle_id": cycle_id,
                "mode": mode,
                "reason": "incomplete account truth",
                "issues": issues,
                **self._status_payload(),
            },
        )

    def log_lifecycle_actions(self, decisions, cycle_id: str | None = None) -> None:
        if self.journal is None or not decisions:
            return
        self.journal.append(
            "lifecycle_actions",
            {
                "cycle_id": cycle_id,
                "count": len(decisions),
                "decisions": decisions,
                **self._status_payload(),
            },
        )

    def scan(self, market_limit: int = 100) -> ScanCycleResult:
        markets = self.adapter.list_markets(limit=market_limit)
        candidates = self.ranker.rank(markets, self.fair_value_provider)
        selected = candidates[0] if candidates else None
        return ScanCycleResult(
            markets=markets, candidates=candidates, selected=selected
        )

    def preview_top(
        self, market_limit: int = 100, cycle_id: str | None = None
    ) -> ScanCycleResult:
        cycle = self.scan(market_limit=market_limit)
        if cycle.selected is not None:
            preview_metadata = {
                "discovered_edge": cycle.selected.edge,
                "candidate_rationale": cycle.selected.rationale,
                "scanner_action": cycle.selected.action.value,
            }
            cycle.execution = self.engine.preview_once(
                cycle.selected.contract,
                fair_value=cycle.selected.fair_value,
                metadata=preview_metadata,
            )
            trade_quantity = self.sizer.size(cycle.selected, cycle.execution)
            if trade_quantity > 0:
                preview_metadata["trade_quantity"] = trade_quantity
                cycle.execution = self.engine.preview_once(
                    cycle.selected.contract,
                    fair_value=cycle.selected.fair_value,
                    metadata=preview_metadata,
                )
        self._log_cycle(cycle, mode="preview", cycle_id=cycle_id)
        return cycle

    def run_top(
        self, market_limit: int = 100, cycle_id: str | None = None
    ) -> ScanCycleResult:
        cycle = self.scan(market_limit=market_limit)
        if cycle.selected is not None:
            preview_metadata = {
                "discovered_edge": cycle.selected.edge,
                "candidate_rationale": cycle.selected.rationale,
                "scanner_action": cycle.selected.action.value,
            }
            preview = self.engine.preview_once(
                cycle.selected.contract,
                fair_value=cycle.selected.fair_value,
                metadata=preview_metadata,
            )
            trade_quantity = self.sizer.size(cycle.selected, preview)
            if trade_quantity <= 0:
                cycle.execution = preview
                cycle.policy_allowed = False
                cycle.policy_reasons = ["sizer produced zero trade quantity"]
                self._log_cycle(cycle, mode="run", cycle_id=cycle_id)
                return cycle
            preview_metadata["trade_quantity"] = trade_quantity
            preview = self.engine.preview_once(
                cycle.selected.contract,
                fair_value=cycle.selected.fair_value,
                metadata=preview_metadata,
            )
            cycle.execution = preview
            decision = self.policy_gate.evaluate(cycle.selected, preview)
            cycle.policy_allowed = decision.allowed
            cycle.policy_reasons = list(decision.reasons)
            if decision.allowed:
                cycle.execution = self.engine.run_once(
                    cycle.selected.contract,
                    fair_value=cycle.selected.fair_value,
                    metadata=preview_metadata,
                )
                if any(placement.accepted for placement in cycle.execution.placements):
                    self.policy_gate.record_execution(cycle.selected)
        self._log_cycle(cycle, mode="run", cycle_id=cycle_id)
        return cycle


@dataclass(frozen=True)
class PollingLoopConfig:
    mode: Literal["preview", "run"] = "preview"
    market_limit: int = 100
    interval_seconds: float = 5.0
    max_cycles: int | None = None


@dataclass
class PollingAgentLoop:
    orchestrator: AgentOrchestrator
    config: PollingLoopConfig
    sleep_fn: Callable[[float], None] = time.sleep
    lifecycle_manager: OrderLifecycleManager | None = None

    def _start_run_heartbeat(self) -> None:
        if self.config.mode != "run":
            return
        starter = getattr(self.orchestrator.adapter, "start_heartbeat", None)
        if callable(starter):
            starter()
            self.orchestrator.engine.sync_heartbeat_state()

    def _start_run_live_state(self) -> None:
        if self.config.mode != "run":
            return
        starter = getattr(self.orchestrator.adapter, "start_live_user_state", None)
        if callable(starter):
            starter()

    def _start_run_market_state(self) -> None:
        if self.config.mode != "run":
            return
        starter = getattr(self.orchestrator.adapter, "start_live_market_state", None)
        if callable(starter):
            starter()

    def _stop_run_heartbeat(self) -> None:
        stopper = getattr(self.orchestrator.adapter, "stop_heartbeat", None)
        if callable(stopper):
            stopper()
            self.orchestrator.engine.sync_heartbeat_state()

    def _stop_run_live_state(self) -> None:
        stopper = getattr(self.orchestrator.adapter, "stop_live_user_state", None)
        if callable(stopper):
            stopper()

    def _stop_run_market_state(self) -> None:
        stopper = getattr(self.orchestrator.adapter, "stop_live_market_state", None)
        if callable(stopper):
            stopper()

    def _heartbeat_block_result(self, reason: str) -> ScanCycleResult:
        return ScanCycleResult(
            markets=[],
            candidates=[],
            selected=None,
            execution=None,
            policy_allowed=False,
            policy_reasons=[reason],
        )

    def _next_interval_seconds(self) -> float:
        consumer = getattr(
            self.orchestrator.engine, "consume_authoritative_refresh_request", None
        )
        if callable(consumer):
            result = consumer()
            if isinstance(result, tuple) and result and bool(result[0]):
                return 0.0
        return self.config.interval_seconds

    def run(self) -> list[ScanCycleResult]:
        results: list[ScanCycleResult] = []
        cycle_count = 0
        try:
            while (
                self.config.max_cycles is None or cycle_count < self.config.max_cycles
            ):
                cycle_id = uuid.uuid4().hex
                status = self.orchestrator.engine.status_snapshot()
                if status.paused:
                    self._stop_run_market_state()
                    self._stop_run_live_state()
                    self._stop_run_heartbeat()
                    self.orchestrator.log_skip(
                        self.config.mode,
                        status.pause_reason or "paused by operator",
                        cycle_id=cycle_id,
                    )
                    results.append(
                        ScanCycleResult(
                            markets=[],
                            candidates=[],
                            selected=None,
                            execution=None,
                        )
                    )
                    cycle_count += 1
                    if (
                        self.config.max_cycles is None
                        or cycle_count < self.config.max_cycles
                    ):
                        self.sleep_fn(self._next_interval_seconds())
                    continue

                if self.config.mode == "run":
                    self._start_run_market_state()
                    self._start_run_live_state()
                    self._start_run_heartbeat()
                    heartbeat_reason = self.orchestrator.engine.heartbeat_block_reason()
                    if heartbeat_reason is not None:
                        if self.orchestrator.engine.safety_state.heartbeat_unhealthy:
                            self._stop_run_market_state()
                            self.orchestrator.engine.halt(heartbeat_reason)
                            self._stop_run_live_state()
                            self._stop_run_heartbeat()
                        self.orchestrator.log_skip(
                            self.config.mode,
                            heartbeat_reason,
                            cycle_id=cycle_id,
                        )
                        results.append(self._heartbeat_block_result(heartbeat_reason))
                        cycle_count += 1
                        if (
                            self.config.max_cycles is None
                            or cycle_count < self.config.max_cycles
                        ):
                            self.sleep_fn(self._next_interval_seconds())
                        continue
                else:
                    self._stop_run_market_state()
                    self._stop_run_live_state()
                    self._stop_run_heartbeat()

                if self.lifecycle_manager is not None:
                    decisions = self.lifecycle_manager.cancel_stale_orders()
                    if getattr(self.lifecycle_manager, "cancel_handler", None) is None:
                        for decision in decisions:
                            if (
                                decision.action != "cancel"
                                or decision.contract_key is None
                            ):
                                continue
                            self.orchestrator.engine.track_cancel_request(
                                decision.order_id,
                                decision.contract_key,
                                decision.reason,
                            )
                    self.orchestrator.log_lifecycle_actions(
                        decisions, cycle_id=cycle_id
                    )

                account_snapshot = self.orchestrator.adapter.get_account_snapshot(None)
                self.orchestrator.engine.observe_polled_snapshot(account_snapshot)
                if not account_snapshot.complete:
                    self._stop_run_market_state()
                    self._stop_run_live_state()
                    self._stop_run_heartbeat()
                    self.orchestrator.log_truth_block(
                        self.config.mode,
                        account_snapshot.issues,
                        cycle_id=cycle_id,
                    )
                    results.append(
                        ScanCycleResult(
                            markets=[],
                            candidates=[],
                            selected=None,
                            execution=None,
                            policy_allowed=False,
                            policy_reasons=[
                                "incomplete account truth",
                                *account_snapshot.issues,
                            ],
                        )
                    )
                    cycle_count += 1
                    if (
                        self.config.max_cycles is None
                        or cycle_count < self.config.max_cycles
                    ):
                        self.sleep_fn(self._next_interval_seconds())
                    continue

                status = self.orchestrator.engine.status_snapshot()
                if status.halted:
                    self._stop_run_market_state()
                    self._stop_run_live_state()
                    self._stop_run_heartbeat()
                    self.orchestrator.log_skip(
                        self.config.mode,
                        status.halt_reason or "engine halted by safety policy",
                        cycle_id=cycle_id,
                    )
                    results.append(
                        self._heartbeat_block_result(
                            status.halt_reason or "engine halted by safety policy"
                        )
                    )
                    cycle_count += 1
                    if (
                        self.config.max_cycles is None
                        or cycle_count < self.config.max_cycles
                    ):
                        self.sleep_fn(self._next_interval_seconds())
                    continue

                if self.config.mode == "run":
                    result = self.orchestrator.run_top(
                        market_limit=self.config.market_limit,
                        cycle_id=cycle_id,
                    )
                else:
                    result = self.orchestrator.preview_top(
                        market_limit=self.config.market_limit,
                        cycle_id=cycle_id,
                    )
                results.append(result)
                if self.orchestrator.engine.status_snapshot().halted:
                    self._stop_run_market_state()
                    self._stop_run_live_state()
                    self._stop_run_heartbeat()
                cycle_count += 1
                if (
                    self.config.max_cycles is None
                    or cycle_count < self.config.max_cycles
                ):
                    self.sleep_fn(self._next_interval_seconds())
            return results
        finally:
            self._stop_run_market_state()
            self._stop_run_live_state()
            self._stop_run_heartbeat()

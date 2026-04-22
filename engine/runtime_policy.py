from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable, cast

from adapters.polymarket import PolymarketConfig
from contracts.resolution_rules import ContractRuleFreezePolicy
from engine.discovery import (
    DeterministicSizer,
    ExecutionPolicyGate,
    FairValueField,
)
from engine.order_state import OrderLifecyclePolicy
from engine.strategies import FairValueBandStrategy
from execution.planner import PlannerThresholds
from opportunity.ranker import OpportunityRanker, PairOpportunityRanker
from risk.limits import RiskLimits

SCHEMA_VERSION = 1


class RuntimePolicyError(RuntimeError):
    pass


def _ensure_object(value: Any, *, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimePolicyError(f"{context} must be a JSON object")
    return value


def _ensure_known_keys(
    payload: dict[str, Any],
    *,
    context: str,
    allowed_keys: Iterable[str],
) -> None:
    allowed = set(allowed_keys)
    unknown = sorted(key for key in payload if key not in allowed)
    if unknown:
        raise RuntimePolicyError(
            f"{context} contains unknown keys: {', '.join(unknown)}"
        )


def _read_section(
    root: dict[str, Any],
    key: str,
    *,
    allowed_keys: Iterable[str],
) -> dict[str, Any]:
    raw = root.get(key)
    if raw is None:
        return {}
    payload = _ensure_object(raw, context=key)
    _ensure_known_keys(payload, context=key, allowed_keys=allowed_keys)
    return payload


def _read_float(
    payload: dict[str, Any], key: str, default: float, *, context: str
) -> float:
    if key not in payload:
        return default
    raw = payload[key]
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise RuntimePolicyError(f"{context}.{key} must be a number")
    return float(raw)


def _read_optional_float(
    payload: dict[str, Any], key: str, default: float | None, *, context: str
) -> float | None:
    if key not in payload:
        return default
    raw = payload[key]
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise RuntimePolicyError(f"{context}.{key} must be a number or null")
    return float(raw)


def _read_int(payload: dict[str, Any], key: str, default: int, *, context: str) -> int:
    if key not in payload:
        return default
    raw = payload[key]
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise RuntimePolicyError(f"{context}.{key} must be an integer")
    return raw


def _read_optional_int(
    payload: dict[str, Any], key: str, default: int | None, *, context: str
) -> int | None:
    if key not in payload:
        return default
    raw = payload[key]
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise RuntimePolicyError(f"{context}.{key} must be an integer or null")
    return raw


def _read_bool(
    payload: dict[str, Any], key: str, default: bool, *, context: str
) -> bool:
    if key not in payload:
        return default
    raw = payload[key]
    if not isinstance(raw, bool):
        raise RuntimePolicyError(f"{context}.{key} must be a boolean")
    return raw


def _read_string_choice(
    payload: dict[str, Any],
    key: str,
    default: str,
    *,
    context: str,
    choices: tuple[str, ...],
) -> str:
    if key not in payload:
        return default
    raw = payload[key]
    if not isinstance(raw, str):
        raise RuntimePolicyError(f"{context}.{key} must be a string")
    normalized = raw.strip().lower()
    if normalized not in choices:
        raise RuntimePolicyError(
            f"{context}.{key} must be one of: {', '.join(choices)}"
        )
    return normalized


def _read_optional_string_tuple(
    payload: dict[str, Any],
    key: str,
    default: tuple[str, ...] | None,
    *,
    context: str,
) -> tuple[str, ...] | None:
    if key not in payload:
        return default
    raw = payload[key]
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise RuntimePolicyError(f"{context}.{key} must be an array of strings")
    values: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            raise RuntimePolicyError(f"{context}.{key} must be an array of strings")
        stripped = item.strip()
        if stripped:
            values.append(stripped)
    return tuple(values) or None


@dataclass(frozen=True)
class StrategyPolicy:
    base_quantity: float = 1.0
    edge_threshold: float = 0.03

    def build_strategy(self) -> FairValueBandStrategy:
        return FairValueBandStrategy(
            quantity=self.base_quantity,
            edge_threshold=self.edge_threshold,
        )

    def build_sizer(self) -> DeterministicSizer:
        return DeterministicSizer(
            base_quantity=self.base_quantity,
            edge_unit=self.edge_threshold,
        )


@dataclass(frozen=True)
class FairValuePolicy:
    field: FairValueField = "raw"


@dataclass(frozen=True)
class RiskLimitsPolicy:
    max_global_contracts: int = 20
    max_contracts_per_market: int = 5
    max_contracts_per_event: int | None = None
    reserve_contracts_buffer: int = 0
    max_order_notional: float | None = None
    min_price: float = 0.01
    max_price: float = 0.99
    max_daily_loss: float | None = None
    enforce_atomic_batches: bool = True

    def build(self) -> RiskLimits:
        return RiskLimits(
            max_global_contracts=self.max_global_contracts,
            max_contracts_per_market=self.max_contracts_per_market,
            max_contracts_per_event=self.max_contracts_per_event,
            reserve_contracts_buffer=self.reserve_contracts_buffer,
            max_order_notional=self.max_order_notional,
            min_price=self.min_price,
            max_price=self.max_price,
            max_daily_loss=self.max_daily_loss,
            enforce_atomic_batches=self.enforce_atomic_batches,
        )


@dataclass(frozen=True)
class OpportunityRankerPolicy:
    edge_threshold: float = 0.03
    limit: int = 25
    allowed_categories: tuple[str, ...] | None = None
    min_volume: float | None = None
    max_spread: float | None = None
    min_hours_to_expiry: float | None = None
    max_hours_to_expiry: float | None = None
    volume_bonus_cap: float = 0.02
    volume_bonus_saturation: float = 10_000.0
    complement_discount_bonus_weight: float = 0.5
    complement_discount_bonus_cap: float = 0.005
    spread_penalty_weight: float = 0.25
    taker_fee_rate: float = 0.0
    contract_rule_freeze: ContractRuleFreezePolicy = field(
        default_factory=ContractRuleFreezePolicy
    )

    def build(self) -> OpportunityRanker:
        return OpportunityRanker(
            edge_threshold=self.edge_threshold,
            limit=self.limit,
            allowed_categories=self.allowed_categories,
            min_volume=self.min_volume,
            max_spread=self.max_spread,
            min_hours_to_expiry=self.min_hours_to_expiry,
            max_hours_to_expiry=self.max_hours_to_expiry,
            volume_bonus_cap=self.volume_bonus_cap,
            volume_bonus_saturation=self.volume_bonus_saturation,
            complement_discount_bonus_weight=self.complement_discount_bonus_weight,
            complement_discount_bonus_cap=self.complement_discount_bonus_cap,
            spread_penalty_weight=self.spread_penalty_weight,
            taker_fee_rate=self.taker_fee_rate,
            contract_rule_freeze=self.contract_rule_freeze,
        )


@dataclass(frozen=True)
class PairOpportunityRankerPolicy:
    edge_threshold: float = 0.01
    limit: int = 10
    taker_fee_rate: float = 0.0
    allowed_categories: tuple[str, ...] | None = None
    min_volume: float | None = None
    max_spread: float | None = None
    min_hours_to_expiry: float | None = None
    max_hours_to_expiry: float | None = None
    contract_rule_freeze: ContractRuleFreezePolicy = field(
        default_factory=ContractRuleFreezePolicy
    )

    def build(self) -> PairOpportunityRanker:
        return PairOpportunityRanker(
            edge_threshold=self.edge_threshold,
            limit=self.limit,
            taker_fee_rate=self.taker_fee_rate,
            allowed_categories=self.allowed_categories,
            min_volume=self.min_volume,
            max_spread=self.max_spread,
            min_hours_to_expiry=self.min_hours_to_expiry,
            max_hours_to_expiry=self.max_hours_to_expiry,
            contract_rule_freeze=self.contract_rule_freeze,
        )


@dataclass(frozen=True)
class ExecutionPolicyGatePolicy:
    min_top_level_liquidity: float = 1.0
    depth_levels_for_liquidity: int | None = 3
    max_visible_liquidity_consumption: float | None = 1.0
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

    def build(self) -> ExecutionPolicyGate:
        return ExecutionPolicyGate(
            min_top_level_liquidity=self.min_top_level_liquidity,
            depth_levels_for_liquidity=self.depth_levels_for_liquidity,
            max_visible_liquidity_consumption=self.max_visible_liquidity_consumption,
            max_spread=self.max_spread,
            max_book_age_seconds=self.max_book_age_seconds,
            cooldown_seconds=self.cooldown_seconds,
            block_on_unhealthy_reconciliation=self.block_on_unhealthy_reconciliation,
            prevent_same_side_duplicate=self.prevent_same_side_duplicate,
            max_position_quantity_per_contract=self.max_position_quantity_per_contract,
            max_open_orders_per_contract=self.max_open_orders_per_contract,
            max_contract_capital_at_risk=self.max_contract_capital_at_risk,
            max_open_orders_global=self.max_open_orders_global,
            max_global_open_order_notional=self.max_global_open_order_notional,
            block_on_contract_partial_fills=self.block_on_contract_partial_fills,
            max_partial_fills_global=self.max_partial_fills_global,
        )


@dataclass(frozen=True)
class TradingEnginePolicy:
    cancel_retry_interval_seconds: float = 5.0
    cancel_retry_max_attempts: int = 3
    cancel_attention_timeout_seconds: float = 30.0
    overlay_max_age_seconds: float = 30.0
    forced_refresh_debounce_seconds: float = 0.0
    pending_submission_recovery_seconds: float = 5.0
    pending_submission_expiry_seconds: float = 30.0

    def build_kwargs(self) -> dict[str, float | int]:
        return {
            "cancel_retry_interval_seconds": self.cancel_retry_interval_seconds,
            "cancel_retry_max_attempts": self.cancel_retry_max_attempts,
            "cancel_attention_timeout_seconds": self.cancel_attention_timeout_seconds,
            "overlay_max_age_seconds": self.overlay_max_age_seconds,
            "forced_refresh_debounce_seconds": self.forced_refresh_debounce_seconds,
            "pending_submission_recovery_seconds": self.pending_submission_recovery_seconds,
            "pending_submission_expiry_seconds": self.pending_submission_expiry_seconds,
        }


@dataclass(frozen=True)
class ProposalPlannerPolicy:
    min_match_confidence: float = 0.95
    max_source_age_ms: int = 4000
    max_book_dispersion: float = 0.03
    entry_edge_bps: float = 150.0
    exit_edge_bps: float = 50.0
    freeze_minutes_before_start: int = 10
    cooldown_seconds_after_score_change: int = 15

    def build(self) -> PlannerThresholds:
        return PlannerThresholds(
            min_match_confidence=self.min_match_confidence,
            max_source_age_ms=self.max_source_age_ms,
            max_book_dispersion=self.max_book_dispersion,
            entry_edge_bps=self.entry_edge_bps,
            exit_edge_bps=self.exit_edge_bps,
            freeze_minutes_before_start=self.freeze_minutes_before_start,
            cooldown_seconds_after_score_change=self.cooldown_seconds_after_score_change,
        )


@dataclass(frozen=True)
class OrderLifecyclePolicyConfig:
    max_order_age_seconds: float = 30.0

    def build(self) -> OrderLifecyclePolicy:
        return OrderLifecyclePolicy(max_order_age_seconds=self.max_order_age_seconds)


@dataclass(frozen=True)
class PolymarketAdmissionPolicy:
    depth_admission_levels: int | None = 3
    depth_admission_liquidity_fraction: float = 0.5
    depth_admission_max_expected_slippage_bps: float | None = 50.0

    def apply(self, config: PolymarketConfig) -> PolymarketConfig:
        return replace(
            config,
            depth_admission_levels=self.depth_admission_levels,
            depth_admission_liquidity_fraction=self.depth_admission_liquidity_fraction,
            depth_admission_max_expected_slippage_bps=self.depth_admission_max_expected_slippage_bps,
        )


@dataclass(frozen=True)
class VenuePolicy:
    polymarket: PolymarketAdmissionPolicy = PolymarketAdmissionPolicy()


@dataclass(frozen=True)
class RuntimePolicy:
    schema_version: int = SCHEMA_VERSION
    fair_value: FairValuePolicy = FairValuePolicy()
    strategy: StrategyPolicy = StrategyPolicy()
    risk_limits: RiskLimitsPolicy = RiskLimitsPolicy()
    opportunity_ranker: OpportunityRankerPolicy = OpportunityRankerPolicy()
    pair_opportunity_ranker: PairOpportunityRankerPolicy = PairOpportunityRankerPolicy()
    execution_policy_gate: ExecutionPolicyGatePolicy = ExecutionPolicyGatePolicy()
    trading_engine: TradingEnginePolicy = TradingEnginePolicy()
    proposal_planner: ProposalPlannerPolicy = ProposalPlannerPolicy()
    order_lifecycle_policy: OrderLifecyclePolicyConfig = OrderLifecyclePolicyConfig()
    venues: VenuePolicy = VenuePolicy()


def _load_fair_value_policy(root: dict[str, Any]) -> FairValuePolicy:
    key = "fair_value"
    payload = _read_section(root, key, allowed_keys={"field"})
    defaults = FairValuePolicy()
    return FairValuePolicy(
        field=cast(
            FairValueField,
            _read_string_choice(
                payload,
                "field",
                defaults.field,
                context=key,
                choices=("raw", "calibrated"),
            ),
        )
    )


def _load_strategy_policy(root: dict[str, Any]) -> StrategyPolicy:
    key = "strategy"
    payload = _read_section(root, key, allowed_keys={"base_quantity", "edge_threshold"})
    defaults = StrategyPolicy()
    return StrategyPolicy(
        base_quantity=_read_float(
            payload, "base_quantity", defaults.base_quantity, context=key
        ),
        edge_threshold=_read_float(
            payload,
            "edge_threshold",
            defaults.edge_threshold,
            context=key,
        ),
    )


def _load_risk_limits_policy(root: dict[str, Any]) -> RiskLimitsPolicy:
    key = "risk_limits"
    payload = _read_section(
        root,
        key,
        allowed_keys={
            "max_global_contracts",
            "max_contracts_per_market",
            "max_contracts_per_event",
            "reserve_contracts_buffer",
            "max_order_notional",
            "min_price",
            "max_price",
            "max_daily_loss",
            "enforce_atomic_batches",
        },
    )
    defaults = RiskLimitsPolicy()
    return RiskLimitsPolicy(
        max_global_contracts=_read_int(
            payload,
            "max_global_contracts",
            defaults.max_global_contracts,
            context=key,
        ),
        max_contracts_per_market=_read_int(
            payload,
            "max_contracts_per_market",
            defaults.max_contracts_per_market,
            context=key,
        ),
        max_contracts_per_event=_read_optional_int(
            payload,
            "max_contracts_per_event",
            defaults.max_contracts_per_event,
            context=key,
        ),
        reserve_contracts_buffer=_read_int(
            payload,
            "reserve_contracts_buffer",
            defaults.reserve_contracts_buffer,
            context=key,
        ),
        max_order_notional=_read_optional_float(
            payload,
            "max_order_notional",
            defaults.max_order_notional,
            context=key,
        ),
        min_price=_read_float(payload, "min_price", defaults.min_price, context=key),
        max_price=_read_float(payload, "max_price", defaults.max_price, context=key),
        max_daily_loss=_read_optional_float(
            payload,
            "max_daily_loss",
            defaults.max_daily_loss,
            context=key,
        ),
        enforce_atomic_batches=_read_bool(
            payload,
            "enforce_atomic_batches",
            defaults.enforce_atomic_batches,
            context=key,
        ),
    )


def _load_opportunity_ranker_policy(root: dict[str, Any]) -> OpportunityRankerPolicy:
    key = "opportunity_ranker"
    payload = _read_section(
        root,
        key,
        allowed_keys={
            "edge_threshold",
            "limit",
            "allowed_categories",
            "min_volume",
            "max_spread",
            "min_hours_to_expiry",
            "max_hours_to_expiry",
            "volume_bonus_cap",
            "volume_bonus_saturation",
            "complement_discount_bonus_weight",
            "complement_discount_bonus_cap",
            "spread_penalty_weight",
            "taker_fee_rate",
            "contract_rules",
        },
    )
    defaults = OpportunityRankerPolicy()
    return OpportunityRankerPolicy(
        edge_threshold=_read_float(
            payload,
            "edge_threshold",
            defaults.edge_threshold,
            context=key,
        ),
        limit=_read_int(payload, "limit", defaults.limit, context=key),
        allowed_categories=_read_optional_string_tuple(
            payload,
            "allowed_categories",
            defaults.allowed_categories,
            context=key,
        ),
        min_volume=_read_optional_float(
            payload,
            "min_volume",
            defaults.min_volume,
            context=key,
        ),
        max_spread=_read_optional_float(
            payload,
            "max_spread",
            defaults.max_spread,
            context=key,
        ),
        min_hours_to_expiry=_read_optional_float(
            payload,
            "min_hours_to_expiry",
            defaults.min_hours_to_expiry,
            context=key,
        ),
        max_hours_to_expiry=_read_optional_float(
            payload,
            "max_hours_to_expiry",
            defaults.max_hours_to_expiry,
            context=key,
        ),
        volume_bonus_cap=_read_float(
            payload,
            "volume_bonus_cap",
            defaults.volume_bonus_cap,
            context=key,
        ),
        volume_bonus_saturation=_read_float(
            payload,
            "volume_bonus_saturation",
            defaults.volume_bonus_saturation,
            context=key,
        ),
        complement_discount_bonus_weight=_read_float(
            payload,
            "complement_discount_bonus_weight",
            defaults.complement_discount_bonus_weight,
            context=key,
        ),
        complement_discount_bonus_cap=_read_float(
            payload,
            "complement_discount_bonus_cap",
            defaults.complement_discount_bonus_cap,
            context=key,
        ),
        spread_penalty_weight=_read_float(
            payload,
            "spread_penalty_weight",
            defaults.spread_penalty_weight,
            context=key,
        ),
        taker_fee_rate=_read_float(
            payload,
            "taker_fee_rate",
            defaults.taker_fee_rate,
            context=key,
        ),
        contract_rule_freeze=_load_contract_rule_freeze_policy(
            payload,
            key=key,
            defaults=defaults.contract_rule_freeze,
        ),
    )


def _load_pair_opportunity_ranker_policy(
    root: dict[str, Any],
) -> PairOpportunityRankerPolicy:
    key = "pair_opportunity_ranker"
    payload = _read_section(
        root,
        key,
        allowed_keys={
            "edge_threshold",
            "limit",
            "taker_fee_rate",
            "allowed_categories",
            "min_volume",
            "max_spread",
            "min_hours_to_expiry",
            "max_hours_to_expiry",
            "contract_rules",
        },
    )
    defaults = PairOpportunityRankerPolicy()
    return PairOpportunityRankerPolicy(
        edge_threshold=_read_float(
            payload,
            "edge_threshold",
            defaults.edge_threshold,
            context=key,
        ),
        limit=_read_int(payload, "limit", defaults.limit, context=key),
        taker_fee_rate=_read_float(
            payload,
            "taker_fee_rate",
            defaults.taker_fee_rate,
            context=key,
        ),
        allowed_categories=_read_optional_string_tuple(
            payload,
            "allowed_categories",
            defaults.allowed_categories,
            context=key,
        ),
        min_volume=_read_optional_float(
            payload,
            "min_volume",
            defaults.min_volume,
            context=key,
        ),
        max_spread=_read_optional_float(
            payload,
            "max_spread",
            defaults.max_spread,
            context=key,
        ),
        min_hours_to_expiry=_read_optional_float(
            payload,
            "min_hours_to_expiry",
            defaults.min_hours_to_expiry,
            context=key,
        ),
        max_hours_to_expiry=_read_optional_float(
            payload,
            "max_hours_to_expiry",
            defaults.max_hours_to_expiry,
            context=key,
        ),
        contract_rule_freeze=_load_contract_rule_freeze_policy(
            payload,
            key=key,
            defaults=defaults.contract_rule_freeze,
        ),
    )


def _load_contract_rule_freeze_policy(
    payload: dict[str, Any],
    *,
    key: str,
    defaults: ContractRuleFreezePolicy,
) -> ContractRuleFreezePolicy:
    raw = payload.get("contract_rules")
    if raw is None:
        return defaults

    context = f"{key}.contract_rules"
    contract_payload = _ensure_object(raw, context=context)
    _ensure_known_keys(
        contract_payload,
        context=context,
        allowed_keys={
            "freeze_before_expiry_seconds",
            "freeze_when_closed",
            "freeze_when_inactive",
            "freeze_when_not_accepting_orders",
            "freeze_when_order_book_disabled",
        },
    )
    return ContractRuleFreezePolicy(
        freeze_before_expiry_seconds=_read_optional_float(
            contract_payload,
            "freeze_before_expiry_seconds",
            defaults.freeze_before_expiry_seconds,
            context=context,
        ),
        freeze_when_closed=_read_bool(
            contract_payload,
            "freeze_when_closed",
            defaults.freeze_when_closed,
            context=context,
        ),
        freeze_when_inactive=_read_bool(
            contract_payload,
            "freeze_when_inactive",
            defaults.freeze_when_inactive,
            context=context,
        ),
        freeze_when_not_accepting_orders=_read_bool(
            contract_payload,
            "freeze_when_not_accepting_orders",
            defaults.freeze_when_not_accepting_orders,
            context=context,
        ),
        freeze_when_order_book_disabled=_read_bool(
            contract_payload,
            "freeze_when_order_book_disabled",
            defaults.freeze_when_order_book_disabled,
            context=context,
        ),
    )


def _load_execution_policy_gate_policy(
    root: dict[str, Any],
) -> ExecutionPolicyGatePolicy:
    key = "execution_policy_gate"
    payload = _read_section(
        root,
        key,
        allowed_keys={
            "min_top_level_liquidity",
            "depth_levels_for_liquidity",
            "max_visible_liquidity_consumption",
            "max_spread",
            "max_book_age_seconds",
            "cooldown_seconds",
            "block_on_unhealthy_reconciliation",
            "prevent_same_side_duplicate",
            "max_position_quantity_per_contract",
            "max_open_orders_per_contract",
            "max_contract_capital_at_risk",
            "max_open_orders_global",
            "max_global_open_order_notional",
            "block_on_contract_partial_fills",
            "max_partial_fills_global",
        },
    )
    defaults = ExecutionPolicyGatePolicy()
    return ExecutionPolicyGatePolicy(
        min_top_level_liquidity=_read_float(
            payload,
            "min_top_level_liquidity",
            defaults.min_top_level_liquidity,
            context=key,
        ),
        depth_levels_for_liquidity=_read_optional_int(
            payload,
            "depth_levels_for_liquidity",
            defaults.depth_levels_for_liquidity,
            context=key,
        ),
        max_visible_liquidity_consumption=_read_optional_float(
            payload,
            "max_visible_liquidity_consumption",
            defaults.max_visible_liquidity_consumption,
            context=key,
        ),
        max_spread=_read_optional_float(
            payload,
            "max_spread",
            defaults.max_spread,
            context=key,
        ),
        max_book_age_seconds=_read_optional_float(
            payload,
            "max_book_age_seconds",
            defaults.max_book_age_seconds,
            context=key,
        ),
        cooldown_seconds=_read_float(
            payload,
            "cooldown_seconds",
            defaults.cooldown_seconds,
            context=key,
        ),
        block_on_unhealthy_reconciliation=_read_bool(
            payload,
            "block_on_unhealthy_reconciliation",
            defaults.block_on_unhealthy_reconciliation,
            context=key,
        ),
        prevent_same_side_duplicate=_read_bool(
            payload,
            "prevent_same_side_duplicate",
            defaults.prevent_same_side_duplicate,
            context=key,
        ),
        max_position_quantity_per_contract=_read_optional_float(
            payload,
            "max_position_quantity_per_contract",
            defaults.max_position_quantity_per_contract,
            context=key,
        ),
        max_open_orders_per_contract=_read_optional_int(
            payload,
            "max_open_orders_per_contract",
            defaults.max_open_orders_per_contract,
            context=key,
        ),
        max_contract_capital_at_risk=_read_optional_float(
            payload,
            "max_contract_capital_at_risk",
            defaults.max_contract_capital_at_risk,
            context=key,
        ),
        max_open_orders_global=_read_optional_int(
            payload,
            "max_open_orders_global",
            defaults.max_open_orders_global,
            context=key,
        ),
        max_global_open_order_notional=_read_optional_float(
            payload,
            "max_global_open_order_notional",
            defaults.max_global_open_order_notional,
            context=key,
        ),
        block_on_contract_partial_fills=_read_bool(
            payload,
            "block_on_contract_partial_fills",
            defaults.block_on_contract_partial_fills,
            context=key,
        ),
        max_partial_fills_global=_read_optional_int(
            payload,
            "max_partial_fills_global",
            defaults.max_partial_fills_global,
            context=key,
        ),
    )


def _load_trading_engine_policy(root: dict[str, Any]) -> TradingEnginePolicy:
    key = "trading_engine"
    payload = _read_section(
        root,
        key,
        allowed_keys={
            "cancel_retry_interval_seconds",
            "cancel_retry_max_attempts",
            "cancel_attention_timeout_seconds",
            "overlay_max_age_seconds",
            "forced_refresh_debounce_seconds",
            "pending_submission_recovery_seconds",
            "pending_submission_expiry_seconds",
        },
    )
    defaults = TradingEnginePolicy()
    return TradingEnginePolicy(
        cancel_retry_interval_seconds=_read_float(
            payload,
            "cancel_retry_interval_seconds",
            defaults.cancel_retry_interval_seconds,
            context=key,
        ),
        cancel_retry_max_attempts=_read_int(
            payload,
            "cancel_retry_max_attempts",
            defaults.cancel_retry_max_attempts,
            context=key,
        ),
        cancel_attention_timeout_seconds=_read_float(
            payload,
            "cancel_attention_timeout_seconds",
            defaults.cancel_attention_timeout_seconds,
            context=key,
        ),
        overlay_max_age_seconds=_read_float(
            payload,
            "overlay_max_age_seconds",
            defaults.overlay_max_age_seconds,
            context=key,
        ),
        forced_refresh_debounce_seconds=_read_float(
            payload,
            "forced_refresh_debounce_seconds",
            defaults.forced_refresh_debounce_seconds,
            context=key,
        ),
        pending_submission_recovery_seconds=_read_float(
            payload,
            "pending_submission_recovery_seconds",
            defaults.pending_submission_recovery_seconds,
            context=key,
        ),
        pending_submission_expiry_seconds=_read_float(
            payload,
            "pending_submission_expiry_seconds",
            defaults.pending_submission_expiry_seconds,
            context=key,
        ),
    )


def _load_proposal_planner_policy(root: dict[str, Any]) -> ProposalPlannerPolicy:
    key = "proposal_planner"
    payload = _read_section(
        root,
        key,
        allowed_keys={
            "min_match_confidence",
            "max_source_age_ms",
            "max_book_dispersion",
            "entry_edge_bps",
            "exit_edge_bps",
            "freeze_minutes_before_start",
            "cooldown_seconds_after_score_change",
        },
    )
    defaults = ProposalPlannerPolicy()
    return ProposalPlannerPolicy(
        min_match_confidence=_read_float(
            payload,
            "min_match_confidence",
            defaults.min_match_confidence,
            context=key,
        ),
        max_source_age_ms=_read_int(
            payload,
            "max_source_age_ms",
            defaults.max_source_age_ms,
            context=key,
        ),
        max_book_dispersion=_read_float(
            payload,
            "max_book_dispersion",
            defaults.max_book_dispersion,
            context=key,
        ),
        entry_edge_bps=_read_float(
            payload,
            "entry_edge_bps",
            defaults.entry_edge_bps,
            context=key,
        ),
        exit_edge_bps=_read_float(
            payload,
            "exit_edge_bps",
            defaults.exit_edge_bps,
            context=key,
        ),
        freeze_minutes_before_start=_read_int(
            payload,
            "freeze_minutes_before_start",
            defaults.freeze_minutes_before_start,
            context=key,
        ),
        cooldown_seconds_after_score_change=_read_int(
            payload,
            "cooldown_seconds_after_score_change",
            defaults.cooldown_seconds_after_score_change,
            context=key,
        ),
    )


def _load_order_lifecycle_policy(root: dict[str, Any]) -> OrderLifecyclePolicyConfig:
    key = "order_lifecycle_policy"
    payload = _read_section(root, key, allowed_keys={"max_order_age_seconds"})
    defaults = OrderLifecyclePolicyConfig()
    return OrderLifecyclePolicyConfig(
        max_order_age_seconds=_read_float(
            payload,
            "max_order_age_seconds",
            defaults.max_order_age_seconds,
            context=key,
        )
    )


def _load_venue_policy(root: dict[str, Any]) -> VenuePolicy:
    key = "venues"
    payload = _read_section(root, key, allowed_keys={"polymarket"})
    polymarket_raw = payload.get("polymarket")
    defaults = VenuePolicy()
    if polymarket_raw is None:
        return defaults
    polymarket_payload = _ensure_object(polymarket_raw, context=f"{key}.polymarket")
    _ensure_known_keys(
        polymarket_payload,
        context=f"{key}.polymarket",
        allowed_keys={
            "depth_admission_levels",
            "depth_admission_liquidity_fraction",
            "depth_admission_max_expected_slippage_bps",
        },
    )
    polymarket_defaults = defaults.polymarket
    return VenuePolicy(
        polymarket=PolymarketAdmissionPolicy(
            depth_admission_levels=_read_optional_int(
                polymarket_payload,
                "depth_admission_levels",
                polymarket_defaults.depth_admission_levels,
                context=f"{key}.polymarket",
            ),
            depth_admission_liquidity_fraction=_read_float(
                polymarket_payload,
                "depth_admission_liquidity_fraction",
                polymarket_defaults.depth_admission_liquidity_fraction,
                context=f"{key}.polymarket",
            ),
            depth_admission_max_expected_slippage_bps=_read_optional_float(
                polymarket_payload,
                "depth_admission_max_expected_slippage_bps",
                polymarket_defaults.depth_admission_max_expected_slippage_bps,
                context=f"{key}.polymarket",
            ),
        )
    )


def load_runtime_policy(path: str | Path) -> RuntimePolicy:
    payload = json.loads(Path(path).read_text())
    root = _ensure_object(payload, context="runtime policy")
    _ensure_known_keys(
        root,
        context="runtime policy",
        allowed_keys={
            "schema_version",
            "fair_value",
            "strategy",
            "risk_limits",
            "opportunity_ranker",
            "pair_opportunity_ranker",
            "execution_policy_gate",
            "trading_engine",
            "proposal_planner",
            "order_lifecycle_policy",
            "venues",
        },
    )
    version = root.get("schema_version")
    if version != SCHEMA_VERSION:
        raise RuntimePolicyError(
            f"runtime policy schema_version must be {SCHEMA_VERSION}"
        )
    return RuntimePolicy(
        schema_version=SCHEMA_VERSION,
        fair_value=_load_fair_value_policy(root),
        strategy=_load_strategy_policy(root),
        risk_limits=_load_risk_limits_policy(root),
        opportunity_ranker=_load_opportunity_ranker_policy(root),
        pair_opportunity_ranker=_load_pair_opportunity_ranker_policy(root),
        execution_policy_gate=_load_execution_policy_gate_policy(root),
        trading_engine=_load_trading_engine_policy(root),
        proposal_planner=_load_proposal_planner_policy(root),
        order_lifecycle_policy=_load_order_lifecycle_policy(root),
        venues=_load_venue_policy(root),
    )

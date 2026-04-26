from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PolymarketMarketRecord:
    market_id: str
    condition_id: str | None
    token_id_yes: str | None
    token_id_no: str | None
    title: str
    description: str | None
    event_slug: str | None
    market_slug: str | None
    category: str | None
    end_time: str | None
    status: str
    raw_json: dict


@dataclass(frozen=True)
class PolymarketBBORecord:
    market_id: str
    best_bid_yes: float | None
    best_bid_yes_size: float | None
    best_ask_yes: float | None
    best_ask_yes_size: float | None
    midpoint_yes: float | None
    spread_yes: float | None
    book_ts: str
    source_age_ms: int
    raw_hash: str | None


@dataclass(frozen=True)
class SportsbookEventRecord:
    sportsbook_event_id: str
    source: str
    sport: str
    league: str | None
    home_team: str | None
    away_team: str | None
    start_time: str
    raw_json: dict


@dataclass(frozen=True)
class SportsbookOddsRecord:
    sportsbook_event_id: str
    source: str
    market_type: str
    selection: str
    price_decimal: float | None
    implied_prob: float | None
    overround: float | None
    quote_ts: str
    source_age_ms: int
    raw_json: dict
    provider: str | None = None
    source_ts: str | None = None
    capture_ts: str | None = None


@dataclass(frozen=True)
class MarketMappingRecord:
    polymarket_market_id: str
    sportsbook_event_id: str
    sportsbook_market_type: str
    normalized_market_type: str
    match_confidence: float
    resolution_risk: float
    mismatch_reason: str | None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    blocked_reason: str | None = None
    is_active: bool = True


@dataclass(frozen=True)
class FairValueRecord:
    market_id: str
    as_of: str
    fair_yes_prob: float
    lower_prob: float
    upper_prob: float
    book_dispersion: float
    data_age_ms: int
    source_count: int
    model_name: str
    model_version: str
    calibrated_fair_yes_prob: float | None = None


@dataclass(frozen=True)
class OpportunityRecord:
    market_id: str
    as_of: str
    side: str
    fair_yes_prob: float
    best_bid_yes: float
    best_ask_yes: float
    edge_buy_bps: float
    edge_sell_bps: float
    edge_buy_after_costs_bps: float
    edge_sell_after_costs_bps: float
    edge_after_costs_bps: float
    fillable_size: float
    confidence: float
    blocked_reason: str | None
    fair_value_ref: str
    blocked_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class TradeAttributionRecord:
    trade_id: str
    market_id: str
    expected_edge_bps: float | None
    realized_edge_bps: float | None
    slippage_bps: float | None
    pnl: float | None
    model_error: float | None
    stale_data_flag: bool
    mapping_risk: float | None
    notes: dict


@dataclass(frozen=True)
class PolymarketOrderRecord:
    order_id: str
    contract_key: str
    payload: dict


@dataclass(frozen=True)
class PolymarketFillRecord:
    fill_key: str
    order_id: str
    contract_key: str
    payload: dict


@dataclass(frozen=True)
class PolymarketPositionRecord:
    contract_key: str
    payload: dict


@dataclass(frozen=True)
class PolymarketBalanceRecord:
    balance_key: str
    payload: dict


@dataclass(frozen=True)
class ModelRegistryRecord:
    model_name: str
    model_version: str
    created_at: str
    feature_spec: dict
    metrics: dict
    artifact_uri: str


@dataclass(frozen=True)
class RuntimeCycleRecord:
    cycle_id: str
    mode: str
    started_at: str
    selected_market_key: str | None
    policy_allowed: bool | None
    halted: bool
    payload: dict


@dataclass(frozen=True)
class TradeDecisionRecord:
    cycle_id: str
    market_id: str
    contract_key: str | None
    side: str | None
    fair_value: float | None
    market_price: float | None
    score: float | None
    blocked: bool
    blocked_reason: str | None
    blocked_reasons: tuple[str, ...]
    payload: dict


@dataclass(frozen=True)
class ExecutionOrderRecord:
    cycle_id: str
    decision_id: int | None
    order_id: str | None
    contract_key: str | None
    accepted: bool
    status: str
    message: str | None
    payload: dict


@dataclass(frozen=True)
class ExecutionFillRecord:
    fill_key: str
    order_id: str
    contract_key: str | None
    fill_ts: str | None
    price: float | None
    quantity: float | None
    fee: float | None
    snapshot_observed_at: str | None
    snapshot_cohort_id: str | None
    payload: dict

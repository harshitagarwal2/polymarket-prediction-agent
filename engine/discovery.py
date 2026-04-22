from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Protocol

from adapters import MarketSummary, OpportunityCandidate
from adapters.base import TradingAdapter
from adapters.types import (
    Contract,
    OrderAction,
    OrderIntent,
    PlacementResult,
)
from forecasting.fair_value_engine import (
    FairValueField as ForecastFairValueField,
    FairValueManifestEntry as ForecastFairValueManifestEntry,
    FairValueProvider as ForecastFairValueProvider,
    ManifestFairValueProvider as ForecastManifestFairValueProvider,
    StaticFairValueProvider as ForecastStaticFairValueProvider,
)
from opportunity.ranker import (
    OpportunityRanker as RoutedOpportunityRanker,
    PairOpportunityCandidate as RoutedPairOpportunityCandidate,
    PairOpportunityRanker as RoutedPairOpportunityRanker,
)
from engine.contract_rules import ContractRuleFreezePolicy, contract_freeze_reasons
from engine import OrderLifecycleManager, OrderLifecyclePolicy
from engine.runner import EngineRunResult, TradingEngine
from storage.journal import EventJournal
from risk.limits import RiskDecision


class FairValueProvider(Protocol):
    def fair_value_for(self, market: MarketSummary) -> float | None: ...


FairValueField = Literal["raw", "calibrated"]


@dataclass(frozen=True)
class FairValueManifestEntry:
    fair_value: float
    calibrated_fair_value: float | None = None
    generated_at: datetime | None = None
    source: str | None = None
    condition_id: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None

    def selected_fair_value(self, fair_value_field: FairValueField = "raw") -> float:
        if fair_value_field == "calibrated" and self.calibrated_fair_value is not None:
            return self.calibrated_fair_value
        return self.fair_value


@dataclass(frozen=True)
class StaticFairValueProvider:
    fair_values: dict[str, float]

    def fair_value_for(self, market: MarketSummary) -> float | None:
        return self.fair_values.get(market.contract.market_key)


@dataclass(frozen=True)
class ManifestFairValueProvider:
    records: dict[str, FairValueManifestEntry]
    generated_at: datetime | None = None
    source: str | None = None
    max_age_seconds: float | None = None
    fair_value_field: FairValueField = "raw"

    def _market_condition_id(self, market: MarketSummary) -> str | None:
        raw = market.raw
        if not isinstance(raw, dict):
            return None

        token = raw.get("token")
        if isinstance(token, dict):
            value = token.get("condition_id") or token.get("conditionId")
            if value not in (None, ""):
                return str(value)

        payload = raw.get("market") if isinstance(raw.get("market"), dict) else raw
        if isinstance(payload, dict):
            value = payload.get("condition_id") or payload.get("conditionId")
            if value not in (None, ""):
                return str(value)

        return None

    def fair_value_for(self, market: MarketSummary) -> float | None:
        record = self.records.get(market.contract.market_key)
        if record is None:
            return None

        if self.max_age_seconds is not None:
            generated_at = record.generated_at or self.generated_at
            if generated_at is None:
                return None
            age_seconds = (datetime.now(timezone.utc) - generated_at).total_seconds()
            if age_seconds > self.max_age_seconds:
                return None

        if record.condition_id is not None:
            market_condition_id = self._market_condition_id(market)
            if (
                market_condition_id is None
                or market_condition_id != record.condition_id
            ):
                return None

        if record.event_key is not None:
            if market.event_key is None or market.event_key != record.event_key:
                return None

        if record.sport is not None:
            if market.sport is None or market.sport.lower() != record.sport.lower():
                return None

        if record.series is not None:
            if market.series is None or market.series.lower() != record.series.lower():
                return None

        if record.game_id is not None:
            if market.game_id is None or market.game_id != record.game_id:
                return None

        if record.sports_market_type is not None:
            if (
                market.sports_market_type is None
                or market.sports_market_type.lower()
                != record.sports_market_type.lower()
            ):
                return None

        return record.selected_fair_value(self.fair_value_field)


@dataclass(frozen=True)
class PairOpportunityCandidate:
    market_key: str
    yes_contract: Contract
    no_contract: Contract
    yes_price: float
    no_price: float
    gross_cost: float
    total_fee: float
    net_edge: float
    score: float
    rationale: str
    raw: Any | None = None


@dataclass(frozen=True)
class PairOpportunityRanker:
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

    def _normalize_allowed_categories(self) -> set[str] | None:
        if not self.allowed_categories:
            return None
        normalized = {
            category.strip().lower()
            for category in self.allowed_categories
            if category.strip()
        }
        return normalized or None

    def _market_labels(self, market: MarketSummary) -> set[str]:
        labels: set[str] = set()
        for value in (
            market.category,
            market.sport,
            market.series,
            market.event_key,
            market.game_id,
            market.sports_market_type,
        ):
            if value not in (None, ""):
                labels.add(str(value).strip().lower())
        labels.update(tag.strip().lower() for tag in market.tags if tag.strip())
        return labels

    def _market_spread(self, market: MarketSummary) -> float | None:
        if market.best_bid is None or market.best_ask is None:
            return None
        return max(0.0, market.best_ask - market.best_bid)

    def _hours_to_expiry(self, market: MarketSummary, *, now: datetime) -> float | None:
        if market.expires_at is None:
            return None
        expires_at = market.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return (expires_at - now).total_seconds() / 3600.0

    def _market_allowed(
        self,
        market: MarketSummary,
        *,
        allowed_categories: set[str] | None,
        now: datetime,
    ) -> bool:
        if contract_freeze_reasons(
            market,
            policy=self.contract_rule_freeze,
            now=now,
        ):
            return False
        if allowed_categories is not None:
            if not self._market_labels(market).intersection(allowed_categories):
                return False
        if self.min_volume is not None:
            market_volume = float(market.volume or 0.0)
            if market_volume < self.min_volume:
                return False
        spread = self._market_spread(market)
        if (
            self.max_spread is not None
            and spread is not None
            and spread > self.max_spread
        ):
            return False
        hours_to_expiry = self._hours_to_expiry(market, now=now)
        if self.min_hours_to_expiry is not None:
            if hours_to_expiry is None or hours_to_expiry < self.min_hours_to_expiry:
                return False
        if self.max_hours_to_expiry is not None:
            if hours_to_expiry is None or hours_to_expiry > self.max_hours_to_expiry:
                return False
        return True

    def _market_group_key(self, market: MarketSummary) -> str:
        raw = market.raw
        payload = raw
        if isinstance(raw, dict) and isinstance(raw.get("market"), dict):
            payload = raw["market"]
        if isinstance(payload, dict):
            for key in (
                "condition_id",
                "conditionId",
                "market_id",
                "marketId",
                "market",
                "slug",
                "question",
                "title",
            ):
                value = payload.get(key)
                if value not in (None, ""):
                    return str(value)
        if market.event_key:
            return market.event_key
        if market.title:
            return market.title
        return market.contract.symbol

    def _taker_fee(self, price: float) -> float:
        if self.taker_fee_rate <= 0.0:
            return 0.0
        return max(0.0, self.taker_fee_rate * price * (1.0 - price))

    def rank(self, markets: list[MarketSummary]) -> list[PairOpportunityCandidate]:
        now = datetime.now(timezone.utc)
        allowed_categories = self._normalize_allowed_categories()
        grouped: dict[str, dict[str, MarketSummary]] = {}
        for market in markets:
            if not market.active or market.best_ask is None:
                continue
            if not self._market_allowed(
                market, allowed_categories=allowed_categories, now=now
            ):
                continue
            outcome = market.contract.outcome.value
            if outcome not in {"yes", "no"}:
                continue
            grouped.setdefault(self._market_group_key(market), {})[outcome] = market

        candidates: list[PairOpportunityCandidate] = []
        for market_key, pair in grouped.items():
            yes_market = pair.get("yes")
            no_market = pair.get("no")
            if yes_market is None or no_market is None:
                continue
            if yes_market.best_ask is None or no_market.best_ask is None:
                continue
            yes_fee = self._taker_fee(yes_market.best_ask)
            no_fee = self._taker_fee(no_market.best_ask)
            total_fee = yes_fee + no_fee
            gross_cost = yes_market.best_ask + no_market.best_ask
            net_edge = 1.0 - gross_cost - total_fee
            if net_edge < self.edge_threshold:
                continue
            candidates.append(
                PairOpportunityCandidate(
                    market_key=market_key,
                    yes_contract=yes_market.contract,
                    no_contract=no_market.contract,
                    yes_price=yes_market.best_ask,
                    no_price=no_market.best_ask,
                    gross_cost=gross_cost,
                    total_fee=total_fee,
                    net_edge=net_edge,
                    score=round(net_edge, 8),
                    rationale=(
                        f"paired buy cost {gross_cost:.4f} plus fee {total_fee:.4f} leaves net edge {net_edge:.4f}"
                    ),
                    raw={"yes": yes_market.raw, "no": no_market.raw},
                )
            )

        return sorted(candidates, key=lambda candidate: candidate.score, reverse=True)[
            : self.limit
        ]


@dataclass
class PairScanCycleResult:
    markets: list[MarketSummary]
    candidates: list[PairOpportunityCandidate]
    selected: PairOpportunityCandidate | None = None
    execution: EngineRunResult | None = None
    intents: list[OrderIntent] = field(default_factory=list)
    risk: RiskDecision | None = None
    policy_allowed: bool | None = None
    policy_reasons: list[str] = field(default_factory=list)
    placements: list[PlacementResult] = field(default_factory=list)


@dataclass(frozen=True)
class OpportunityRanker:
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

    def _normalize_allowed_categories(self) -> set[str] | None:
        if not self.allowed_categories:
            return None
        normalized = {
            category.strip().lower()
            for category in self.allowed_categories
            if category.strip()
        }
        return normalized or None

    def _market_labels(self, market: MarketSummary) -> set[str]:
        labels: set[str] = set()
        for value in (
            market.category,
            market.sport,
            market.series,
            market.event_key,
            market.game_id,
            market.sports_market_type,
        ):
            if value not in (None, ""):
                labels.add(str(value).strip().lower())
        labels.update(tag.strip().lower() for tag in market.tags if tag.strip())
        return labels

    def _market_spread(self, market: MarketSummary) -> float | None:
        if market.best_bid is None or market.best_ask is None:
            return None
        return max(0.0, market.best_ask - market.best_bid)

    def _hours_to_expiry(self, market: MarketSummary, *, now: datetime) -> float | None:
        if market.expires_at is None:
            return None
        expires_at = market.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return (expires_at - now).total_seconds() / 3600.0

    def _taker_fee(self, price: float) -> float:
        if self.taker_fee_rate <= 0:
            return 0.0
        return max(0.0, self.taker_fee_rate * price * (1.0 - price))

    def _market_group_key(self, market: MarketSummary) -> str:
        raw = market.raw
        payload = raw
        if isinstance(raw, dict) and isinstance(raw.get("market"), dict):
            payload = raw["market"]
        if isinstance(payload, dict):
            for key in (
                "condition_id",
                "conditionId",
                "market_id",
                "marketId",
                "market",
                "slug",
                "question",
                "title",
            ):
                value = payload.get(key)
                if value not in (None, ""):
                    return str(value)
        if market.title:
            return market.title
        if market.contract.title:
            return market.contract.title
        return market.contract.symbol

    def _build_complement_discount_map(
        self, markets: list[MarketSummary]
    ) -> dict[str, float]:
        paired_quotes: dict[str, dict[str, float]] = {}
        for market in markets:
            if market.best_ask is None:
                continue
            outcome = market.contract.outcome.value
            if outcome not in {"yes", "no"}:
                continue
            market_key = self._market_group_key(market)
            paired_quotes.setdefault(market_key, {})[outcome] = market.best_ask

        discounts: dict[str, float] = {}
        for market in markets:
            market_key = self._market_group_key(market)
            quotes = paired_quotes.get(market_key, {})
            if "yes" not in quotes or "no" not in quotes:
                continue
            discounts[market.contract.market_key] = max(
                0.0, 1.0 - (quotes["yes"] + quotes["no"])
            )
        return discounts

    def _market_allowed(
        self,
        market: MarketSummary,
        *,
        allowed_categories: set[str] | None,
        now: datetime,
    ) -> bool:
        if contract_freeze_reasons(
            market,
            policy=self.contract_rule_freeze,
            now=now,
        ):
            return False
        if allowed_categories is not None:
            if not self._market_labels(market).intersection(allowed_categories):
                return False

        if self.min_volume is not None:
            market_volume = float(market.volume or 0.0)
            if market_volume < self.min_volume:
                return False

        spread = self._market_spread(market)
        if (
            self.max_spread is not None
            and spread is not None
            and spread > self.max_spread
        ):
            return False

        hours_to_expiry = self._hours_to_expiry(market, now=now)
        if self.min_hours_to_expiry is not None:
            if hours_to_expiry is None or hours_to_expiry < self.min_hours_to_expiry:
                return False
        if self.max_hours_to_expiry is not None:
            if hours_to_expiry is None or hours_to_expiry > self.max_hours_to_expiry:
                return False

        return True

    def _score_candidate(
        self,
        *,
        edge: float,
        market: MarketSummary,
        spread: float | None,
        complement_discount: float = 0.0,
    ) -> float:
        score = edge
        if market.volume is not None and self.volume_bonus_saturation > 0:
            normalized_volume = (
                min(max(float(market.volume), 0.0), self.volume_bonus_saturation)
                / self.volume_bonus_saturation
            )
            score += normalized_volume * self.volume_bonus_cap
        if spread is not None:
            score -= spread * self.spread_penalty_weight
        if complement_discount > 0:
            score += min(
                complement_discount * self.complement_discount_bonus_weight,
                self.complement_discount_bonus_cap,
            )
        return round(score, 8)

    def _format_rationale(
        self,
        base: str,
        *,
        market: MarketSummary,
        spread: float | None,
        hours_to_expiry: float | None,
        complement_discount: float,
        fee_drag: float,
    ) -> str:
        details: list[str] = []
        if market.category:
            details.append(f"category {market.category}")
        if market.volume is not None:
            details.append(f"volume {float(market.volume):.0f}")
        if spread is not None:
            details.append(f"spread {spread:.4f}")
        if hours_to_expiry is not None:
            details.append(f"expires_in {hours_to_expiry:.1f}h")
        if complement_discount > 0:
            details.append(f"paired_ask_discount {complement_discount:.4f}")
        if fee_drag > 0:
            details.append(f"fee_drag {fee_drag:.4f}")
        if not details:
            return base
        return f"{base}; " + ", ".join(details)

    def rank(
        self,
        markets: list[MarketSummary],
        fair_value_provider: FairValueProvider,
    ) -> list[OpportunityCandidate]:
        now = datetime.now(timezone.utc)
        allowed_categories = self._normalize_allowed_categories()
        complement_discounts = self._build_complement_discount_map(markets)
        candidates: list[OpportunityCandidate] = []
        for market in markets:
            if not market.active:
                continue
            if not self._market_allowed(
                market, allowed_categories=allowed_categories, now=now
            ):
                continue
            fair_value = fair_value_provider.fair_value_for(market)
            if fair_value is None:
                continue

            spread = self._market_spread(market)
            hours_to_expiry = self._hours_to_expiry(market, now=now)
            complement_discount = complement_discounts.get(
                market.contract.market_key, 0.0
            )

            if market.best_ask is not None:
                buy_fee = self._taker_fee(market.best_ask)
                buy_edge = fair_value - market.best_ask - buy_fee
                if buy_edge >= self.edge_threshold:
                    rationale = self._format_rationale(
                        (
                            f"fair_value {fair_value:.4f} exceeds ask {market.best_ask:.4f} net of fee {buy_fee:.4f} by {buy_edge:.4f}"
                        ),
                        market=market,
                        spread=spread,
                        hours_to_expiry=hours_to_expiry,
                        complement_discount=complement_discount,
                        fee_drag=buy_fee,
                    )
                    candidates.append(
                        OpportunityCandidate(
                            contract=market.contract,
                            action=OrderAction.BUY,
                            fair_value=fair_value,
                            market_price=market.best_ask,
                            edge=buy_edge,
                            score=self._score_candidate(
                                edge=buy_edge,
                                market=market,
                                spread=spread,
                                complement_discount=complement_discount,
                            ),
                            rationale=rationale,
                            raw=market.raw,
                        )
                    )

            if market.best_bid is not None:
                sell_fee = self._taker_fee(market.best_bid)
                sell_edge = market.best_bid - fair_value - sell_fee
                if sell_edge >= self.edge_threshold:
                    rationale = self._format_rationale(
                        (
                            f"bid {market.best_bid:.4f} exceeds fair_value {fair_value:.4f} net of fee {sell_fee:.4f} by {sell_edge:.4f}"
                        ),
                        market=market,
                        spread=spread,
                        hours_to_expiry=hours_to_expiry,
                        complement_discount=0.0,
                        fee_drag=sell_fee,
                    )
                    candidates.append(
                        OpportunityCandidate(
                            contract=market.contract,
                            action=OrderAction.SELL,
                            fair_value=fair_value,
                            market_price=market.best_bid,
                            edge=sell_edge,
                            score=self._score_candidate(
                                edge=sell_edge,
                                market=market,
                                spread=spread,
                            ),
                            rationale=rationale,
                            raw=market.raw,
                        )
                    )

        return sorted(candidates, key=lambda candidate: candidate.score, reverse=True)[
            : self.limit
        ]


# Compatibility bridge while engine/ consumers migrate to the first-class
# forecasting/ and opportunity/ packages.
FairValueProvider = ForecastFairValueProvider
FairValueField = ForecastFairValueField
FairValueManifestEntry = ForecastFairValueManifestEntry
StaticFairValueProvider = ForecastStaticFairValueProvider
ManifestFairValueProvider = ForecastManifestFairValueProvider
PairOpportunityCandidate = RoutedPairOpportunityCandidate
PairOpportunityRanker = RoutedPairOpportunityRanker
OpportunityRanker = RoutedOpportunityRanker


@dataclass
class ScanCycleResult:
    markets: list[MarketSummary]
    candidates: list[OpportunityCandidate]
    selected: OpportunityCandidate | None = None
    execution: EngineRunResult | None = None
    policy_allowed: bool | None = None
    policy_reasons: list[str] = field(default_factory=list)
    skipped_candidates: list[dict[str, object]] = field(default_factory=list)
    gate_trace: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reasons: list[str] = field(default_factory=list)


@dataclass
class ExecutionPolicyGate:
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

    def _candidate_order_quantity(
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
            return sum(intent.quantity for intent in matching_approved)

        matching_proposed = [
            intent
            for intent in preview.proposed
            if intent.contract.market_key == candidate.contract.market_key
            and intent.action is candidate.action
        ]
        if matching_proposed:
            return sum(intent.quantity for intent in matching_proposed)

        trade_quantity = preview.context.metadata.get("trade_quantity")
        if trade_quantity is not None:
            return float(trade_quantity)
        return 0.0

    def evaluate(
        self,
        candidate: OpportunityCandidate,
        preview: EngineRunResult,
    ) -> PolicyDecision:
        reasons: list[str] = []
        book = preview.context.book
        now = datetime.now(timezone.utc)
        candidate_order_notional = self._candidate_order_notional(candidate, preview)
        candidate_order_quantity = self._candidate_order_quantity(candidate, preview)

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

        if (
            self.max_visible_liquidity_consumption is not None
            and candidate_order_quantity > 0
        ):
            visible_liquidity = book.cumulative_quantity(
                candidate.action,
                max_levels=self.depth_levels_for_liquidity,
            )
            max_visible_quantity = (
                visible_liquidity * self.max_visible_liquidity_consumption
            )
            if candidate_order_quantity > max_visible_quantity:
                reasons.append(
                    "visible depth too thin "
                    f"({candidate_order_quantity:.4f} > {max_visible_quantity:.4f} "
                    f"across {self.depth_levels_for_liquidity or 'all'} levels)"
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
    depth_levels: int | None = 3

    def size(self, candidate: OpportunityCandidate, preview: EngineRunResult) -> float:
        visible_liquidity = preview.context.book.cumulative_quantity(
            candidate.action,
            max_levels=self.depth_levels,
        )
        edge_multiple = max(1.0, candidate.edge / max(self.edge_unit, 1e-9))
        proposed = min(
            self.max_quantity,
            self.base_quantity * edge_multiple,
            visible_liquidity * self.liquidity_fraction,
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
    pair_ranker: PairOpportunityRanker = field(default_factory=PairOpportunityRanker)
    policy_gate: ExecutionPolicyGate = field(default_factory=ExecutionPolicyGate)
    sizer: Sizer = field(default_factory=DeterministicSizer)
    journal: EventJournal | None = None

    def _runtime_summary(self) -> dict[str, object]:
        status = self.engine.status_snapshot()
        pending_cancels = list(status.pending_cancels)
        pending_submissions = list(status.pending_submissions)
        pending_refresh_requests = list(status.pending_refresh_requests)
        recovery_items = list(status.recovery_items)
        reasons: list[str] = []
        if status.halted and status.halt_reason:
            reasons.append(status.halt_reason)
        if status.paused and status.pause_reason:
            reasons.append(status.pause_reason)
        if status.hold_new_orders and status.hold_reason:
            reasons.append(status.hold_reason)
        if status.overlay_degraded and status.overlay_degraded_reason:
            reasons.append(status.overlay_degraded_reason)
        if pending_submissions:
            reasons.append("pending submissions unresolved")
        if pending_cancels:
            reasons.append("pending cancels unresolved")
        if pending_refresh_requests:
            reasons.append("authoritative refresh queued")
        for item in recovery_items:
            reasons.append(f"{item.item_type}: {item.reason}")

        if status.halted:
            runtime_state = "halted"
        elif status.paused:
            runtime_state = "paused"
        elif status.hold_new_orders:
            runtime_state = "hold_new_orders"
        elif status.overlay_degraded:
            runtime_state = "degraded"
        elif (
            recovery_items
            or pending_submissions
            or pending_cancels
            or pending_refresh_requests
            or status.overlay_delta_suppressed
        ):
            runtime_state = "recovering"
        else:
            runtime_state = "healthy"

        return {
            "state": runtime_state,
            "reasons": reasons,
            "resume_trading_eligible": status.resume_trading_eligible,
            "last_truth_complete": status.last_truth_complete,
            "last_truth_observed_at": status.last_truth_observed_at,
            "pending_cancel_count": len(pending_cancels),
            "pending_submission_count": len(pending_submissions),
            "pending_refresh_count": len(pending_refresh_requests),
            "open_recovery_count": len(recovery_items),
            "overlay_degraded": status.overlay_degraded,
            "overlay_delta_suppressed": status.overlay_delta_suppressed,
            "heartbeat_active": status.heartbeat_active,
            "heartbeat_unhealthy": status.heartbeat_unhealthy,
            "last_action_gate_action": status.last_action_gate_action,
            "last_action_gate_reason": status.last_action_gate_reason,
        }

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
            "pending_submission_count": len(status.pending_submissions),
            "pending_refresh_count": len(status.pending_refresh_requests),
            "open_recovery_count": len(status.recovery_items),
            "resume_trading_eligible": status.resume_trading_eligible,
            "last_truth_complete": status.last_truth_complete,
            "last_truth_observed_at": status.last_truth_observed_at,
            "runtime_summary": self._runtime_summary(),
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
                "selected_market_key": (
                    cycle.selected.contract.market_key
                    if cycle.selected is not None
                    else None
                ),
                "execution": cycle.execution,
                "policy_allowed": cycle.policy_allowed,
                "policy_reasons": cycle.policy_reasons,
                "skipped_candidates": cycle.skipped_candidates,
                "gate_trace": cycle.gate_trace,
                "blocking_gate": self._last_blocking_gate(cycle),
                "cycle_metrics": self._cycle_metrics(cycle),
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
                "reason_category": self._skip_reason_category(reason),
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
                "truth_issue_count": len(issues),
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
                "action_counts": self._lifecycle_action_counts(decisions),
                "affected_contract_keys": sorted(
                    {
                        decision.contract_key
                        for decision in decisions
                        if getattr(decision, "contract_key", None) is not None
                    }
                ),
                **self._status_payload(),
            },
        )

    def _skip_reason_category(self, reason: str) -> str:
        lowered = reason.lower()
        if "pause" in lowered:
            return "pause"
        if "heartbeat" in lowered:
            return "heartbeat"
        if "halt" in lowered:
            return "halt"
        if "truth" in lowered:
            return "truth"
        return "other"

    def _lifecycle_action_counts(self, decisions) -> dict[str, int]:
        counts: dict[str, int] = {}
        for decision in decisions:
            action = getattr(decision, "action", None)
            if action is None:
                continue
            key = str(action)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _cycle_metrics(self, cycle: ScanCycleResult) -> dict[str, object]:
        placements = (
            list(cycle.execution.placements) if cycle.execution is not None else []
        )
        accepted_count = sum(1 for placement in placements if placement.accepted)
        rejected_gate_count = sum(
            1 for entry in cycle.gate_trace if entry.get("allowed") is False
        )
        allowed_gate_count = sum(
            1 for entry in cycle.gate_trace if entry.get("allowed") is True
        )
        return {
            "market_count": len(cycle.markets),
            "candidate_count": len(cycle.candidates),
            "skipped_candidate_count": len(cycle.skipped_candidates),
            "gate_trace_count": len(cycle.gate_trace),
            "allowed_gate_count": allowed_gate_count,
            "rejected_gate_count": rejected_gate_count,
            "selected_market_key": (
                cycle.selected.contract.market_key
                if cycle.selected is not None
                else None
            ),
            "selected_action": (
                cycle.selected.action.value if cycle.selected is not None else None
            ),
            "placement_count": len(placements),
            "accepted_placement_count": accepted_count,
            "rejected_placement_count": len(placements) - accepted_count,
        }

    def scan(self, market_limit: int = 100) -> ScanCycleResult:
        markets = self.adapter.list_markets(limit=market_limit)
        self.engine.risk_engine.register_markets(markets)
        candidates = self.ranker.rank(markets, self.fair_value_provider)
        selected = candidates[0] if candidates else None
        return ScanCycleResult(
            markets=markets, candidates=candidates, selected=selected
        )

    def _candidate_preview_metadata(
        self, candidate: OpportunityCandidate
    ) -> dict[str, object]:
        return {
            "discovered_edge": candidate.edge,
            "candidate_rationale": candidate.rationale,
            "scanner_action": candidate.action.value,
        }

    def _record_skipped_candidate(
        self,
        cycle: ScanCycleResult,
        candidate: OpportunityCandidate,
        reasons: list[str],
        *,
        stage: str,
    ) -> None:
        cycle.skipped_candidates.append(
            {
                "market_key": candidate.contract.market_key,
                "action": candidate.action.value,
                "stage": stage,
                "reasons": list(reasons),
            }
        )

    def _record_gate_trace(
        self,
        cycle: ScanCycleResult,
        candidate: OpportunityCandidate,
        *,
        stage: str,
        allowed: bool,
        reasons: list[str] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        trace_entry: dict[str, object] = {
            "market_key": candidate.contract.market_key,
            "action": candidate.action.value,
            "stage": stage,
            "allowed": allowed,
            "reasons": list(reasons or []),
        }
        if metadata:
            trace_entry["metadata"] = dict(metadata)
        cycle.gate_trace.append(trace_entry)

    def _last_blocking_gate(self, cycle: ScanCycleResult) -> dict[str, object] | None:
        for entry in reversed(cycle.gate_trace):
            if entry.get("allowed") is False:
                return dict(entry)
        return None

    def _execution_rejection_reasons(self, execution: EngineRunResult) -> list[str]:
        return list(
            dict.fromkeys(
                rejection.reason
                for rejection in execution.risk.rejected
                if rejection.reason
            )
        )

    def _select_executable_candidate(
        self, cycle: ScanCycleResult
    ) -> tuple[OpportunityCandidate, dict[str, object]] | None:
        if cycle.selected is None:
            return None

        top_candidate = cycle.selected
        for index, candidate in enumerate(cycle.candidates):
            preview_metadata = self._candidate_preview_metadata(candidate)
            preview = self.engine.preview_once(
                candidate.contract,
                fair_value=candidate.fair_value,
                metadata=preview_metadata,
            )
            trade_quantity = self.sizer.size(candidate, preview)
            if trade_quantity <= 0:
                reasons = ["sizer produced zero trade quantity"]
                self._record_skipped_candidate(
                    cycle,
                    candidate,
                    reasons,
                    stage="sizer",
                )
                self._record_gate_trace(
                    cycle,
                    candidate,
                    stage="sizer",
                    allowed=False,
                    reasons=reasons,
                    metadata={"trade_quantity": trade_quantity},
                )
                if index == 0:
                    cycle.selected = top_candidate
                    cycle.execution = preview
                    cycle.policy_allowed = False
                    cycle.policy_reasons = reasons
                continue

            preview_metadata["trade_quantity"] = trade_quantity
            preview.context.metadata["trade_quantity"] = trade_quantity
            preview = self.engine.preview_context(
                preview.context,
                reconciliation_before=preview.reconciliation_before,
            )
            preview = self.engine.review_precomputed(preview)
            if not preview.risk.approved:
                reasons = self._execution_rejection_reasons(preview)
                if not reasons:
                    reasons = ["engine preview yielded no approved intents"]
                self._record_skipped_candidate(
                    cycle,
                    candidate,
                    reasons,
                    stage="engine_review",
                )
                self._record_gate_trace(
                    cycle,
                    candidate,
                    stage="engine_review",
                    allowed=False,
                    reasons=reasons,
                )
                if index == 0:
                    cycle.selected = top_candidate
                    cycle.execution = preview
                    cycle.policy_allowed = False
                    cycle.policy_reasons = reasons
                continue
            decision = self.policy_gate.evaluate(candidate, preview)
            if not decision.allowed:
                self._record_skipped_candidate(
                    cycle,
                    candidate,
                    list(decision.reasons),
                    stage="policy_gate",
                )
                self._record_gate_trace(
                    cycle,
                    candidate,
                    stage="policy_gate",
                    allowed=False,
                    reasons=list(decision.reasons),
                )
                if index == 0:
                    cycle.selected = top_candidate
                    cycle.execution = preview
                    cycle.policy_allowed = False
                    cycle.policy_reasons = list(decision.reasons)
                continue

            self._record_gate_trace(
                cycle,
                candidate,
                stage="policy_gate",
                allowed=True,
                metadata={"approved_intent_count": len(preview.risk.approved)},
            )

            cycle.selected = candidate
            cycle.execution = preview
            cycle.policy_allowed = True
            cycle.policy_reasons = []
            return candidate, preview_metadata

        return None

    def preview_top(
        self, market_limit: int = 100, cycle_id: str | None = None
    ) -> ScanCycleResult:
        cycle = self.scan(market_limit=market_limit)
        self._select_executable_candidate(cycle)
        self._log_cycle(cycle, mode="preview", cycle_id=cycle_id)
        return cycle

    def run_top(
        self, market_limit: int = 100, cycle_id: str | None = None
    ) -> ScanCycleResult:
        cycle = self.scan(market_limit=market_limit)
        selection = self._select_executable_candidate(cycle)
        if selection is not None and cycle.execution is not None:
            candidate, _preview_metadata = selection
            cycle.execution = self.engine.run_precomputed(cycle.execution)
            if not cycle.execution.risk.approved and cycle.execution.risk.rejected:
                cycle.policy_allowed = False
                cycle.policy_reasons = self._execution_rejection_reasons(
                    cycle.execution
                )
            if any(placement.accepted for placement in cycle.execution.placements):
                self.policy_gate.record_execution(candidate)
            placements = cycle.execution.placements
            accepted_count = sum(1 for placement in placements if placement.accepted)
            self._record_gate_trace(
                cycle,
                candidate,
                stage="placement",
                allowed=accepted_count > 0,
                reasons=list(
                    dict.fromkeys(
                        [
                            placement.message or "placement rejected"
                            for placement in placements
                            if not placement.accepted
                        ]
                    )
                ),
                metadata={
                    "placement_count": len(placements),
                    "accepted_count": accepted_count,
                    "order_ids": [
                        placement.order_id
                        for placement in placements
                        if placement.order_id is not None
                    ],
                },
            )
        self._log_cycle(cycle, mode="run", cycle_id=cycle_id)
        return cycle

    def scan_pairs(self, market_limit: int = 100) -> PairScanCycleResult:
        markets = self.adapter.list_markets(limit=market_limit)
        self.engine.risk_engine.register_markets(markets)
        candidates = self.pair_ranker.rank(markets)
        selected = candidates[0] if candidates else None
        return PairScanCycleResult(
            markets=markets, candidates=candidates, selected=selected
        )

    def _pair_intents(
        self, candidate: PairOpportunityCandidate, *, quantity: float
    ) -> list[OrderIntent]:
        pair_id = uuid.uuid4().hex
        metadata = {
            "pair_id": pair_id,
            "pair_market_key": candidate.market_key,
            "pair_net_edge": candidate.net_edge,
            "pair_rationale": candidate.rationale,
        }
        return [
            OrderIntent(
                contract=candidate.yes_contract,
                action=OrderAction.BUY,
                price=candidate.yes_price,
                quantity=quantity,
                metadata={**metadata, "pair_leg": "yes"},
            ),
            OrderIntent(
                contract=candidate.no_contract,
                action=OrderAction.BUY,
                price=candidate.no_price,
                quantity=quantity,
                metadata={**metadata, "pair_leg": "no"},
            ),
        ]

    def _pair_preview_metadata(
        self, candidate: PairOpportunityCandidate, *, quantity: float
    ) -> dict[str, object]:
        return {
            "pair_market_key": candidate.market_key,
            "pair_net_edge": candidate.net_edge,
            "pair_rationale": candidate.rationale,
            "trade_quantity": quantity,
        }

    def _preview_pair_candidate(
        self, candidate: PairOpportunityCandidate, *, quantity: float
    ) -> EngineRunResult:
        intents = self._pair_intents(candidate, quantity=quantity)
        preview = self.engine.preview_intents(
            candidate.yes_contract,
            intents,
            metadata=self._pair_preview_metadata(candidate, quantity=quantity),
        )
        return self.engine.review_precomputed(preview)

    def preview_best_pair(
        self, market_limit: int = 100, *, quantity: float = 1.0
    ) -> PairScanCycleResult:
        cycle = self.scan_pairs(market_limit=market_limit)
        if cycle.selected is None:
            return cycle
        execution = self._preview_pair_candidate(
            cycle.selected,
            quantity=quantity,
        )
        cycle.execution = execution
        cycle.intents = list(execution.proposed)
        cycle.risk = execution.risk
        reasons = self._execution_rejection_reasons(execution)
        cycle.policy_reasons = reasons
        cycle.policy_allowed = (
            len(execution.risk.approved) == len(execution.proposed) and not reasons
        )
        return cycle

    def run_best_pair(
        self, market_limit: int = 100, *, quantity: float = 1.0
    ) -> PairScanCycleResult:
        cycle = self.preview_best_pair(market_limit=market_limit, quantity=quantity)
        if cycle.selected is None or cycle.execution is None or cycle.risk is None:
            return cycle
        if not cycle.policy_allowed:
            return cycle

        execution = self.engine.run_precomputed(cycle.execution)
        cycle.execution = execution
        cycle.intents = list(execution.proposed)
        cycle.risk = execution.risk
        cycle.placements = execution.placements
        if not execution.risk.approved and execution.risk.rejected:
            cycle.policy_allowed = False
            cycle.policy_reasons = self._execution_rejection_reasons(execution)
            return cycle
        accepted_count = sum(1 for placement in cycle.placements if placement.accepted)
        if accepted_count != len(cycle.placements):
            cycle.policy_reasons = list(
                dict.fromkeys(
                    cycle.policy_reasons
                    + [
                        placement.message or "paired placement failed"
                        for placement in cycle.placements
                        if not placement.accepted
                    ]
                )
            )

        if accepted_count and accepted_count != len(cycle.risk.approved):
            self.engine.halt(
                "paired arbitrage execution left partial exposure; operator intervention required",
                cycle.selected.yes_contract,
            )
        return cycle


@dataclass(frozen=True)
class PollingLoopConfig:
    mode: Literal["preview", "run", "pair-preview", "pair-run"] = "preview"
    market_limit: int = 100
    interval_seconds: float = 5.0
    max_cycles: int | None = None
    quantity: float = 1.0


@dataclass
class PollingAgentLoop:
    orchestrator: AgentOrchestrator
    config: PollingLoopConfig
    sleep_fn: Callable[[float], None] = time.sleep
    lifecycle_manager: OrderLifecycleManager | None = None

    def _start_run_heartbeat(self) -> None:
        if self.config.mode not in {"run", "pair-run"}:
            return
        starter = getattr(self.orchestrator.adapter, "start_heartbeat", None)
        if callable(starter):
            starter()
            self.orchestrator.engine.sync_heartbeat_state()

    def _start_run_live_state(self) -> None:
        if self.config.mode not in {"run", "pair-run"}:
            return
        starter = getattr(self.orchestrator.adapter, "start_live_user_state", None)
        if callable(starter):
            starter()

    def _start_run_market_state(self) -> None:
        if self.config.mode not in {"run", "pair-run"}:
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

    def run(self) -> list[ScanCycleResult | PairScanCycleResult]:
        results: list[ScanCycleResult | PairScanCycleResult] = []
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

                if self.config.mode in {"run", "pair-run"}:
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
                elif self.config.mode == "pair-run":
                    result = self.orchestrator.run_best_pair(
                        market_limit=self.config.market_limit,
                        quantity=self.config.quantity,
                    )
                elif self.config.mode == "pair-preview":
                    result = self.orchestrator.preview_best_pair(
                        market_limit=self.config.market_limit,
                        quantity=self.config.quantity,
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

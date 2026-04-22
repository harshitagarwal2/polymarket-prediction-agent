from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from adapters import MarketSummary, OpportunityCandidate
from adapters.types import Contract, OrderAction
from contracts.ontology import market_group_key, market_hours_to_expiry, market_labels
from contracts.resolution_rules import (
    ContractRuleFreezePolicy,
    contract_freeze_reasons,
)
from forecasting.fair_value_engine import FairValueProvider
from opportunity.models import Opportunity
from opportunity.executable_edge import assess_executable_edge
from opportunity.fillability import estimate_fillability_from_market, market_spread


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
            if not market_labels(market).intersection(allowed_categories):
                return False
        if self.min_volume is not None and float(market.volume or 0.0) < self.min_volume:
            return False
        spread = market_spread(market)
        if (
            self.max_spread is not None
            and spread is not None
            and spread > self.max_spread
        ):
            return False
        hours_to_expiry = market_hours_to_expiry(market, now=now)
        if self.min_hours_to_expiry is not None:
            if hours_to_expiry is None or hours_to_expiry < self.min_hours_to_expiry:
                return False
        if self.max_hours_to_expiry is not None:
            if hours_to_expiry is None or hours_to_expiry > self.max_hours_to_expiry:
                return False
        return True

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
            grouped.setdefault(market_group_key(market), {})[outcome] = market

        candidates: list[PairOpportunityCandidate] = []
        for group_key, pair in grouped.items():
            yes_market = pair.get("yes")
            no_market = pair.get("no")
            if yes_market is None or no_market is None:
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
                    market_key=group_key,
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

    def _taker_fee(self, price: float) -> float:
        if self.taker_fee_rate <= 0:
            return 0.0
        return max(0.0, self.taker_fee_rate * price * (1.0 - price))

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
            paired_quotes.setdefault(market_group_key(market), {})[outcome] = market.best_ask

        discounts: dict[str, float] = {}
        for market in markets:
            group_key = market_group_key(market)
            quotes = paired_quotes.get(group_key, {})
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
            if not market_labels(market).intersection(allowed_categories):
                return False
        if self.min_volume is not None and float(market.volume or 0.0) < self.min_volume:
            return False
        spread = market_spread(market)
        if (
            self.max_spread is not None
            and spread is not None
            and spread > self.max_spread
        ):
            return False
        hours_to_expiry = market_hours_to_expiry(market, now=now)
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
        completion_ratio: float,
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
        if completion_ratio < 1.0:
            details.append(f"fillability {completion_ratio:.2f}")
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

            spread = market_spread(market)
            hours_to_expiry = market_hours_to_expiry(market, now=now)
            complement_discount = complement_discounts.get(
                market.contract.market_key, 0.0
            )

            if market.best_ask is not None:
                buy_assessment = assess_executable_edge(
                    fair_value=fair_value,
                    quoted_price=market.best_ask,
                    action=OrderAction.BUY,
                    fee_rate=self.taker_fee_rate,
                )
                if buy_assessment.edge >= self.edge_threshold:
                    buy_fill = estimate_fillability_from_market(
                        market,
                        action=OrderAction.BUY,
                    )
                    rationale = self._format_rationale(
                        (
                            f"fair_value {fair_value:.4f} exceeds ask {market.best_ask:.4f} net of fee {buy_assessment.fee_drag:.4f} by {buy_assessment.edge:.4f}"
                        ),
                        market=market,
                        spread=spread,
                        hours_to_expiry=hours_to_expiry,
                        complement_discount=complement_discount,
                        fee_drag=buy_assessment.fee_drag,
                        completion_ratio=buy_fill.completion_ratio,
                    )
                    candidates.append(
                        OpportunityCandidate(
                            contract=market.contract,
                            action=OrderAction.BUY,
                            fair_value=fair_value,
                            market_price=market.best_ask,
                            edge=buy_assessment.edge,
                            score=self._score_candidate(
                                edge=buy_assessment.edge,
                                market=market,
                                spread=spread,
                                complement_discount=complement_discount,
                            ),
                            rationale=rationale,
                            raw=market.raw,
                        )
                    )

            if market.best_bid is not None:
                sell_assessment = assess_executable_edge(
                    fair_value=fair_value,
                    quoted_price=market.best_bid,
                    action=OrderAction.SELL,
                    fee_rate=self.taker_fee_rate,
                )
                if sell_assessment.edge >= self.edge_threshold:
                    sell_fill = estimate_fillability_from_market(
                        market,
                        action=OrderAction.SELL,
                    )
                    rationale = self._format_rationale(
                        (
                            f"bid {market.best_bid:.4f} exceeds fair_value {fair_value:.4f} net of fee {sell_assessment.fee_drag:.4f} by {sell_assessment.edge:.4f}"
                        ),
                        market=market,
                        spread=spread,
                        hours_to_expiry=hours_to_expiry,
                        complement_discount=0.0,
                        fee_drag=sell_assessment.fee_drag,
                        completion_ratio=sell_fill.completion_ratio,
                    )
                    candidates.append(
                        OpportunityCandidate(
                            contract=market.contract,
                            action=OrderAction.SELL,
                            fair_value=fair_value,
                            market_price=market.best_bid,
                            edge=sell_assessment.edge,
                            score=self._score_candidate(
                                edge=sell_assessment.edge,
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


def rank_opportunities(opportunities: list[Opportunity]) -> list[Opportunity]:
    def _score(item: Opportunity) -> tuple[float, float, float]:
        dispersion_penalty = 0.0 if item.blocked_reason else 1.0
        return (
            item.edge_after_costs_bps,
            item.fillable_size,
            item.confidence * dispersion_penalty,
        )

    return sorted(opportunities, key=_score, reverse=True)

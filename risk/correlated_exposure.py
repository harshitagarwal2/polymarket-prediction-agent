from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class CorrelatedExposureLimit:
    cluster_key: str
    max_exposure: float


@dataclass(frozen=True)
class CorrelatedMarketNode:
    market_key: str
    cluster_key: str | None = None
    mutually_exclusive_group_key: str | None = None


@dataclass(frozen=True)
class CorrelatedExposureDecision:
    allowed: bool
    cluster_key: str | None
    current_cluster_exposure: float
    projected_cluster_exposure: float
    max_cluster_exposure: float | None
    reason: str | None = None


@dataclass
class CorrelatedExposureGraph:
    market_to_cluster: dict[str, str] = field(default_factory=dict)
    market_to_group: dict[str, str] = field(default_factory=dict)

    def register_market(
        self,
        market_key: str,
        *,
        cluster_key: str | None = None,
        mutually_exclusive_group_key: str | None = None,
    ) -> CorrelatedMarketNode:
        normalized_market_key = str(market_key)
        if cluster_key not in (None, ""):
            self.market_to_cluster[normalized_market_key] = str(cluster_key)
        if mutually_exclusive_group_key not in (None, ""):
            self.market_to_group[normalized_market_key] = str(
                mutually_exclusive_group_key
            )
        return self.snapshot_for(normalized_market_key)

    def snapshot_for(self, market_key: str) -> CorrelatedMarketNode:
        normalized_market_key = str(market_key)
        return CorrelatedMarketNode(
            market_key=normalized_market_key,
            cluster_key=self.market_to_cluster.get(normalized_market_key),
            mutually_exclusive_group_key=self.market_to_group.get(
                normalized_market_key
            ),
        )

    def group_key_for(self, market_key: str) -> str:
        normalized_market_key = str(market_key)
        return self.market_to_group.get(normalized_market_key, f"market:{market_key}")

    def cluster_key_for(self, market_key: str) -> str | None:
        return self.market_to_cluster.get(str(market_key))

    def related_markets(self, market_key: str) -> tuple[str, ...]:
        cluster_key = self.cluster_key_for(market_key)
        if cluster_key is None:
            return ()
        return tuple(
            sorted(
                other_market_key
                for other_market_key, other_cluster_key in self.market_to_cluster.items()
                if other_cluster_key == cluster_key and other_market_key != market_key
            )
        )

    def grouped_cluster_exposure(
        self,
        *,
        cluster_key: str,
        exposure_by_market: dict[str, float],
    ) -> float:
        grouped_exposure: dict[str, float] = {}
        for market_key, exposure in exposure_by_market.items():
            if self.market_to_cluster.get(market_key) != cluster_key:
                continue
            group_key = self.group_key_for(market_key)
            grouped_exposure[group_key] = max(
                grouped_exposure.get(group_key, 0.0),
                max(0.0, float(exposure)),
            )
        return sum(grouped_exposure.values())

    def cluster_exposure_ok(
        self,
        *,
        market_key: str,
        exposure_by_market: dict[str, float],
        proposed_exposure: float,
        limit: CorrelatedExposureLimit,
    ) -> CorrelatedExposureDecision:
        cluster_key = self.cluster_key_for(market_key)
        if cluster_key is None:
            projected = max(0.0, float(proposed_exposure))
            return CorrelatedExposureDecision(
                allowed=projected <= limit.max_exposure,
                cluster_key=None,
                current_cluster_exposure=0.0,
                projected_cluster_exposure=projected,
                max_cluster_exposure=limit.max_exposure,
                reason=(
                    None
                    if projected <= limit.max_exposure
                    else "cluster exposure cap exceeded"
                ),
            )
        current_cluster_exposure = self.grouped_cluster_exposure(
            cluster_key=cluster_key,
            exposure_by_market=exposure_by_market,
        )
        projected_exposures = dict(exposure_by_market)
        projected_exposures[str(market_key)] = max(
            0.0,
            float(projected_exposures.get(str(market_key), 0.0)) + proposed_exposure,
        )
        projected_cluster_exposure = self.grouped_cluster_exposure(
            cluster_key=cluster_key,
            exposure_by_market=projected_exposures,
        )
        allowed = projected_cluster_exposure <= limit.max_exposure
        return CorrelatedExposureDecision(
            allowed=allowed,
            cluster_key=cluster_key,
            current_cluster_exposure=current_cluster_exposure,
            projected_cluster_exposure=projected_cluster_exposure,
            max_cluster_exposure=limit.max_exposure,
            reason=None if allowed else "cluster exposure cap exceeded",
        )


def cluster_exposure_ok(
    current_exposure: float,
    proposed_exposure: float,
    limit: CorrelatedExposureLimit,
) -> bool:
    return current_exposure + proposed_exposure <= limit.max_exposure


def exposure_by_market(
    rows: Iterable[tuple[str, float]],
) -> dict[str, float]:
    aggregated: dict[str, float] = {}
    for market_key, exposure in rows:
        aggregated[str(market_key)] = aggregated.get(str(market_key), 0.0) + float(
            exposure
        )
    return aggregated

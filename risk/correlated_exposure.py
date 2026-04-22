from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CorrelatedExposureLimit:
    cluster_key: str
    max_exposure: float


def cluster_exposure_ok(current_exposure: float, proposed_exposure: float, limit: CorrelatedExposureLimit) -> bool:
    return current_exposure + proposed_exposure <= limit.max_exposure

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FairValueSnapshot:
    market_id: str
    timestamp_ms: int
    fair_yes_prob: float
    lower_prob: float
    upper_prob: float
    book_dispersion: float
    data_age_ms: int
    source_count: int
    model_name: str
    model_version: str

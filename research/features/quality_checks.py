from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QualityCheckResult:
    allowed: bool
    blocked_reasons: tuple[str, ...]


def evaluate_inference_quality(
    *,
    source_age_ms: int | None = None,
    max_source_age_ms: int | None = None,
    bookmaker_count: int | None = None,
    min_bookmaker_count: int | None = None,
    has_polymarket_book: bool = True,
    match_confidence: float | None = None,
    min_match_confidence: float | None = None,
    book_dispersion: float | None = None,
    max_book_dispersion: float | None = None,
) -> QualityCheckResult:
    blocked_reasons: list[str] = []
    if (
        max_source_age_ms is not None
        and source_age_ms is not None
        and source_age_ms > max_source_age_ms
    ):
        blocked_reasons.append("source data stale")
    if (
        min_bookmaker_count is not None
        and bookmaker_count is not None
        and bookmaker_count < min_bookmaker_count
    ):
        blocked_reasons.append("insufficient book coverage")
    if not has_polymarket_book:
        blocked_reasons.append("missing Polymarket book")
    if (
        min_match_confidence is not None
        and match_confidence is not None
        and match_confidence < min_match_confidence
    ):
        blocked_reasons.append("low match confidence")
    if (
        max_book_dispersion is not None
        and book_dispersion is not None
        and book_dispersion > max_book_dispersion
    ):
        blocked_reasons.append("book dispersion exceeds threshold")
    return QualityCheckResult(
        allowed=not blocked_reasons,
        blocked_reasons=tuple(blocked_reasons),
    )

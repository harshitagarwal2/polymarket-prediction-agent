from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from adapters.types import MarketSummary, deserialize_market_summary
from research.calibration import HistogramCalibrator, load_calibration_artifact


DevigMethod = Literal["multiplicative", "power"]
BookAggregation = Literal["independent", "best-line"]


@dataclass(frozen=True)
class SportsbookFairValueRow:
    market_key: str | None
    bookmaker: str
    outcome: str
    captured_at: datetime
    decimal_odds: float
    selection_name: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    sport_key: str | None = None
    condition_id: str | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    source_bookmaker: str | None = None
    source_captured_at: datetime | None = None
    market_match_strategy: str | None = None


@dataclass(frozen=True)
class FairValueManifestBuild:
    generated_at: datetime
    source: str
    max_age_seconds: float | None
    values: dict[str, dict[str, object]]
    skipped_groups: list[dict[str, object]]
    metadata: dict[str, object] | None = None

    def _metadata_payload(self) -> dict[str, object] | None:
        if not isinstance(self.metadata, dict):
            return None

        payload = dict(self.metadata)
        coverage_payload = payload.get("coverage")
        coverage = dict(coverage_payload) if isinstance(coverage_payload, dict) else {}
        coverage["value_count"] = len(self.values)
        coverage["skipped_group_count"] = len(self.skipped_groups)
        payload["coverage"] = coverage
        return payload

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "generated_at": self.generated_at.isoformat().replace("+00:00", "Z"),
            "source": self.source,
            "values": self.values,
        }
        metadata = self._metadata_payload()
        if metadata:
            payload["metadata"] = metadata
        if self.max_age_seconds is not None:
            payload["max_age_seconds"] = self.max_age_seconds
        if self.skipped_groups:
            payload["skipped_groups"] = self.skipped_groups
        return payload


def _serialize_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _resolve_calibration_artifact(
    calibration_artifact: HistogramCalibrator | dict[str, object] | None,
) -> HistogramCalibrator | None:
    if calibration_artifact is None:
        return None
    if isinstance(calibration_artifact, HistogramCalibrator):
        return calibration_artifact
    if isinstance(calibration_artifact, dict):
        return load_calibration_artifact(calibration_artifact)
    raise ValueError("calibration_artifact must be a histogram calibrator or object")


def parse_timestamp(value: object) -> datetime:
    if value in (None, ""):
        raise ValueError("captured_at is required")
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def american_to_decimal(american_odds: float) -> float:
    if american_odds == 0:
        raise ValueError("american odds must not be zero")
    if american_odds > 0:
        return 1.0 + (american_odds / 100.0)
    return 1.0 + (100.0 / abs(american_odds))


def implied_probability_from_odds(
    *, decimal_odds: float | None = None, american_odds: float | None = None
) -> float:
    if decimal_odds is None and american_odds is None:
        raise ValueError("decimal_odds or american_odds is required")
    resolved_decimal = (
        decimal_odds
        if decimal_odds is not None
        else american_to_decimal(american_odds or 0)
    )
    if resolved_decimal <= 1.0:
        raise ValueError("decimal odds must be greater than 1.0")
    probability = 1.0 / resolved_decimal
    if probability <= 0.0 or probability >= 1.0:
        raise ValueError("implied probability must be between 0 and 1")
    return probability


def devig_probabilities(
    implied_probabilities: list[float], method: DevigMethod = "multiplicative"
) -> list[float]:
    if len(implied_probabilities) < 2:
        raise ValueError("at least two implied probabilities are required")
    if any(
        probability <= 0.0 or probability >= 1.0
        for probability in implied_probabilities
    ):
        raise ValueError("implied probabilities must be between 0 and 1")

    total = sum(implied_probabilities)
    if total <= 1.0:
        raise ValueError("overround must be greater than 1.0 for de-vig")

    if method == "multiplicative":
        return [probability / total for probability in implied_probabilities]

    if method == "power":
        low = 1.0
        high = 1.0
        while sum(probability**high for probability in implied_probabilities) > 1.0:
            high *= 2.0
            if high > 1024.0:
                raise ValueError("could not solve power de-vig exponent")
        for _ in range(80):
            mid = (low + high) / 2.0
            powered_sum = sum(probability**mid for probability in implied_probabilities)
            if powered_sum > 1.0:
                low = mid
            else:
                high = mid
        exponent = high
        fair = [probability**exponent for probability in implied_probabilities]
        fair_total = sum(fair)
        return [probability / fair_total for probability in fair]

    raise ValueError(f"unsupported devig method: {method}")


def load_sportsbook_rows(path: str | Path) -> list[SportsbookFairValueRow]:
    payload = json.loads(Path(path).read_text())
    raw_rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(raw_rows, list):
        raise RuntimeError(
            "sportsbook input must be a JSON list or an object with 'rows'"
        )

    return parse_sportsbook_rows(raw_rows)


def parse_sportsbook_rows(
    raw_rows: list[dict[str, Any]],
) -> list[SportsbookFairValueRow]:
    if not isinstance(raw_rows, list):
        raise RuntimeError("sportsbook rows must be a list")

    rows: list[SportsbookFairValueRow] = []
    for item in raw_rows:
        if not isinstance(item, dict):
            raise RuntimeError("sportsbook rows must be JSON objects")
        decimal_odds = item.get("decimal_odds")
        american_odds = item.get("american_odds")
        resolved_decimal = (
            float(decimal_odds)
            if decimal_odds not in (None, "")
            else american_to_decimal(float(american_odds))
            if american_odds not in (None, "")
            else None
        )
        if resolved_decimal is None:
            raise RuntimeError(
                "sportsbook row must include decimal_odds or american_odds"
            )

        rows.append(
            SportsbookFairValueRow(
                market_key=(
                    str(item.get("market_key")).strip()
                    if item.get("market_key") not in (None, "")
                    else None
                ),
                bookmaker=str(item.get("bookmaker") or "").strip(),
                outcome=str(
                    item.get("outcome") or item.get("selection_name") or ""
                ).strip(),
                captured_at=parse_timestamp(item.get("captured_at")),
                decimal_odds=resolved_decimal,
                selection_name=(
                    str(item.get("selection_name"))
                    if item.get("selection_name") not in (None, "")
                    else None
                ),
                home_team=(
                    str(item.get("home_team"))
                    if item.get("home_team") not in (None, "")
                    else None
                ),
                away_team=(
                    str(item.get("away_team"))
                    if item.get("away_team") not in (None, "")
                    else None
                ),
                sport_key=(
                    str(item.get("sport_key"))
                    if item.get("sport_key") not in (None, "")
                    else None
                ),
                condition_id=(
                    str(item.get("condition_id"))
                    if item.get("condition_id") not in (None, "")
                    else None
                ),
                event_key=(
                    str(item.get("event_key"))
                    if item.get("event_key") not in (None, "")
                    else None
                ),
                sport=(
                    str(item.get("sport"))
                    if item.get("sport") not in (None, "")
                    else None
                ),
                series=(
                    str(item.get("series"))
                    if item.get("series") not in (None, "")
                    else None
                ),
                game_id=(
                    str(item.get("game_id"))
                    if item.get("game_id") not in (None, "")
                    else None
                ),
                sports_market_type=(
                    str(item.get("sports_market_type"))
                    if item.get("sports_market_type") not in (None, "")
                    else None
                ),
                source_bookmaker=str(item.get("bookmaker") or "").strip() or None,
                source_captured_at=parse_timestamp(item.get("captured_at")),
            )
        )
    return rows


def load_market_snapshot(path: str | Path) -> list[MarketSummary]:
    payload = json.loads(Path(path).read_text())
    raw_markets = payload.get("markets") if isinstance(payload, dict) else payload
    if not isinstance(raw_markets, list):
        raise RuntimeError(
            "market snapshot must be a JSON list or an object with 'markets'"
        )
    return [
        deserialize_market_summary(item)
        for item in raw_markets
        if isinstance(item, dict)
    ]


def resolve_rows_to_markets(
    rows: list[SportsbookFairValueRow],
    markets: list[MarketSummary],
) -> tuple[list[SportsbookFairValueRow], list[dict[str, object]]]:
    resolved: list[SportsbookFairValueRow] = []
    skipped: list[dict[str, object]] = []
    for row in rows:
        if row.market_key:
            resolved.append(
                SportsbookFairValueRow(
                    market_key=row.market_key,
                    bookmaker=row.bookmaker,
                    outcome=row.outcome,
                    captured_at=row.captured_at,
                    decimal_odds=row.decimal_odds,
                    selection_name=row.selection_name,
                    home_team=row.home_team,
                    away_team=row.away_team,
                    sport_key=row.sport_key,
                    condition_id=row.condition_id,
                    event_key=row.event_key,
                    sport=row.sport,
                    series=row.series,
                    game_id=row.game_id,
                    sports_market_type=row.sports_market_type,
                    source_bookmaker=row.source_bookmaker,
                    source_captured_at=row.source_captured_at,
                    market_match_strategy=row.market_match_strategy
                    or "input_market_key",
                )
            )
            continue

        candidate_markets = []
        normalized_outcome = row.outcome.strip().lower()
        selection_name = (row.selection_name or row.outcome).strip().lower()
        home_team = (row.home_team or "").strip().lower()
        away_team = (row.away_team or "").strip().lower()
        for market in markets:
            title = (market.title or market.contract.title or "").strip().lower()
            if normalized_outcome in {"yes", "no"}:
                if market.contract.outcome.value != normalized_outcome:
                    continue
            else:
                yes_team = None
                if home_team and home_team in title:
                    yes_team = home_team
                elif away_team and away_team in title:
                    yes_team = away_team
                elif selection_name and selection_name in title:
                    yes_team = selection_name
                if yes_team is None:
                    continue
                if (
                    market.contract.outcome.value == "yes"
                    and selection_name != yes_team
                ):
                    continue
                if market.contract.outcome.value == "no" and selection_name == yes_team:
                    continue
            if row.event_key is not None and market.event_key != row.event_key:
                continue
            if row.condition_id is not None:
                market_condition_id = None
                raw = market.raw
                if isinstance(raw, dict):
                    payload = (
                        raw.get("market")
                        if isinstance(raw.get("market"), dict)
                        else raw
                    )
                    if isinstance(payload, dict):
                        market_condition_id = payload.get(
                            "condition_id"
                        ) or payload.get("conditionId")
                if str(market_condition_id) != row.condition_id:
                    continue
            if (
                row.sport is not None
                and (market.sport or "").lower() != row.sport.lower()
            ):
                continue
            if (
                row.series is not None
                and (market.series or "").lower() != row.series.lower()
            ):
                continue
            if (
                row.sports_market_type is not None
                and (market.sports_market_type or "").lower()
                != row.sports_market_type.lower()
            ):
                continue
            if row.game_id is not None and market.game_id != row.game_id:
                continue
            candidate_markets.append(market)

        if len(candidate_markets) != 1:
            skipped.append(
                {
                    "reason": "ambiguous_or_missing_market_match",
                    "outcome": row.outcome,
                    "bookmaker": row.bookmaker,
                    "event_key": row.event_key,
                    "condition_id": row.condition_id,
                    "candidate_count": len(candidate_markets),
                }
            )
            continue

        matched_market = candidate_markets[0]
        matched_condition_id = row.condition_id
        raw = matched_market.raw
        if isinstance(raw, dict):
            payload = raw.get("market") if isinstance(raw.get("market"), dict) else raw
            if isinstance(payload, dict):
                raw_condition_id = payload.get("condition_id") or payload.get(
                    "conditionId"
                )
                if raw_condition_id not in (None, ""):
                    matched_condition_id = str(raw_condition_id)
        resolved.append(
            SportsbookFairValueRow(
                market_key=matched_market.contract.market_key,
                bookmaker=row.bookmaker,
                outcome=matched_market.contract.outcome.value,
                captured_at=row.captured_at,
                decimal_odds=row.decimal_odds,
                selection_name=row.selection_name,
                home_team=row.home_team,
                away_team=row.away_team,
                sport_key=row.sport_key,
                condition_id=matched_condition_id,
                event_key=row.event_key or matched_market.event_key,
                sport=row.sport or matched_market.sport,
                series=row.series or matched_market.series,
                game_id=row.game_id or matched_market.game_id,
                sports_market_type=(
                    row.sports_market_type or matched_market.sports_market_type
                ),
                source_bookmaker=row.source_bookmaker,
                source_captured_at=row.source_captured_at,
                market_match_strategy="market_snapshot",
            )
        )

    return resolved, skipped


def _group_key(row: SportsbookFairValueRow) -> tuple[str, str, str, str, str]:
    identity = row.event_key or row.condition_id or row.game_id
    if identity in (None, ""):
        raise ValueError(
            f"row {row.market_key} must include event_key, condition_id, or game_id"
        )
    return (
        row.bookmaker,
        identity,
        row.sports_market_type or "",
        row.condition_id or "",
        row.captured_at.isoformat(),
    )


def _aggregation_group_key(
    row: SportsbookFairValueRow, aggregation: BookAggregation
) -> tuple[str, str, str, str, str]:
    if aggregation == "independent":
        return _group_key(row)
    identity = row.event_key or row.condition_id or row.game_id
    if identity in (None, ""):
        raise ValueError(
            f"row {row.market_key or row.outcome} must include event_key, condition_id, or game_id"
        )
    return (
        "best-line",
        identity,
        row.sports_market_type or "",
        row.condition_id or "",
        "",
    )


def _aggregate_rows(
    rows: list[SportsbookFairValueRow],
    aggregation: BookAggregation,
) -> list[SportsbookFairValueRow]:
    if aggregation == "independent":
        return rows

    grouped: dict[tuple[str, str, str, str, str], list[SportsbookFairValueRow]] = {}
    for row in rows:
        grouped.setdefault(_aggregation_group_key(row, aggregation), []).append(row)

    aggregated_rows: list[SportsbookFairValueRow] = []
    for group_rows in grouped.values():
        best_by_outcome: dict[str, SportsbookFairValueRow] = {}
        for row in group_rows:
            outcome_key = row.outcome.strip().lower()
            existing = best_by_outcome.get(outcome_key)
            if existing is None or row.decimal_odds > existing.decimal_odds:
                best_by_outcome[outcome_key] = row

        latest_capture = max(row.captured_at for row in best_by_outcome.values())
        for outcome_key, row in best_by_outcome.items():
            aggregated_rows.append(
                SportsbookFairValueRow(
                    market_key=row.market_key,
                    bookmaker="best-line",
                    outcome=outcome_key,
                    captured_at=latest_capture,
                    decimal_odds=row.decimal_odds,
                    condition_id=row.condition_id,
                    event_key=row.event_key,
                    sport=row.sport,
                    series=row.series,
                    game_id=row.game_id,
                    sports_market_type=row.sports_market_type,
                    source_bookmaker=row.source_bookmaker or row.bookmaker,
                    source_captured_at=row.source_captured_at or row.captured_at,
                    market_match_strategy=row.market_match_strategy,
                )
            )

    return aggregated_rows


def build_fair_value_manifest(
    rows: list[SportsbookFairValueRow],
    *,
    method: DevigMethod = "multiplicative",
    source: str | None = None,
    max_age_seconds: float | None = None,
    aggregation: BookAggregation = "independent",
    calibration_artifact: HistogramCalibrator | dict[str, object] | None = None,
) -> FairValueManifestBuild:
    if not rows:
        raise ValueError("sportsbook rows must not be empty")

    calibrator = _resolve_calibration_artifact(calibration_artifact)
    source_rows = list(rows)
    source = source or f"sportsbook-devig:{method}:{aggregation}"
    rows = _aggregate_rows(rows, aggregation)

    grouped: dict[tuple[str, str, str, str, str], list[SportsbookFairValueRow]] = {}
    for row in rows:
        if not row.bookmaker:
            raise ValueError("bookmaker is required")
        if not row.outcome:
            raise ValueError("outcome is required")
        if not row.market_key:
            raise ValueError(
                "market_key is required after sportsbook rows are resolved"
            )
        grouped.setdefault(_group_key(row), []).append(row)

    values: dict[str, dict[str, object]] = {}
    skipped_groups: list[dict[str, object]] = []
    generated_at = max(row.captured_at for row in rows)

    for group, group_rows in grouped.items():
        if len(group_rows) != 2:
            skipped_groups.append(
                {
                    "group": list(group),
                    "reason": "only binary groups are supported",
                    "market_keys": [row.market_key for row in group_rows],
                }
            )
            continue

        market_keys = {row.market_key for row in group_rows}
        outcomes = {row.outcome.strip().lower() for row in group_rows}
        if len(market_keys) != 2 or len(outcomes) != 2:
            skipped_groups.append(
                {
                    "group": list(group),
                    "reason": "group must contain two distinct market keys and outcomes",
                    "market_keys": [row.market_key for row in group_rows],
                }
            )
            continue

        implied = [
            implied_probability_from_odds(decimal_odds=row.decimal_odds)
            for row in group_rows
        ]
        fair_values = devig_probabilities(implied, method)
        for row, fair_value in zip(group_rows, fair_values):
            if row.market_key is None:
                raise ValueError("matched sportsbook row is missing market_key")
            if row.market_key in values:
                skipped_groups.append(
                    {
                        "group": list(group),
                        "reason": "duplicate target market_key across groups",
                        "market_key": row.market_key,
                    }
                )
                continue
            record: dict[str, object] = {
                "fair_value": round(fair_value, 8),
                "generated_at": _serialize_timestamp(row.captured_at),
                "bookmaker": row.bookmaker,
                "source_bookmaker": row.source_bookmaker or row.bookmaker,
                "source_captured_at": _serialize_timestamp(
                    row.source_captured_at or row.captured_at
                ),
                "outcome": row.outcome,
                "match_strategy": row.market_match_strategy or "input_market_key",
            }
            if calibrator is not None:
                record["calibrated_fair_value"] = round(
                    calibrator.apply(fair_value),
                    8,
                )
            if row.condition_id is not None:
                record["condition_id"] = row.condition_id
            if row.event_key is not None:
                record["event_key"] = row.event_key
            if row.sport is not None:
                record["sport"] = row.sport
            if row.series is not None:
                record["series"] = row.series
            if row.game_id is not None:
                record["game_id"] = row.game_id
            if row.sports_market_type is not None:
                record["sports_market_type"] = row.sports_market_type
            record["source"] = source
            values[row.market_key] = record

    capture_times = [row.captured_at for row in source_rows]
    match_strategy_counts: dict[str, int] = {}
    for row in source_rows:
        strategy = row.market_match_strategy or (
            "input_market_key" if row.market_key else "unresolved"
        )
        match_strategy_counts[strategy] = match_strategy_counts.get(strategy, 0) + 1

    source_bookmakers = sorted(
        {
            bookmaker
            for bookmaker in (
                (row.source_bookmaker or row.bookmaker).strip()
                for row in source_rows
                if (row.source_bookmaker or row.bookmaker).strip()
            )
            if bookmaker
        }
    )

    freshness_metadata: dict[str, object] = {
        "captured_at_min": _serialize_timestamp(min(capture_times)),
        "captured_at_max": _serialize_timestamp(max(capture_times)),
    }
    if max_age_seconds is not None:
        freshness_metadata["max_age_seconds"] = max_age_seconds

    metadata: dict[str, object] = {
        "provenance": {
            "devig_method": method,
            "book_aggregation": aggregation,
            "bookmakers": source_bookmakers,
            "bookmaker_count": len(source_bookmakers),
        },
        "freshness": freshness_metadata,
        "coverage": {
            "input_row_count": len(source_rows),
            "value_count": len(values),
            "skipped_group_count": len(skipped_groups),
        },
        "match_quality": {
            "resolved_row_count": len(source_rows),
            "match_strategy_counts": match_strategy_counts,
        },
    }
    if calibrator is not None:
        metadata["calibration"] = {
            "method": "histogram",
            "bin_count": calibrator.bin_count,
            "sample_count": calibrator.sample_count,
            "positive_rate": round(calibrator.positive_rate, 8),
            "applied_field": "fair_value",
        }
    return FairValueManifestBuild(
        generated_at=generated_at,
        source=source,
        max_age_seconds=max_age_seconds,
        values=values,
        skipped_groups=skipped_groups,
        metadata=metadata,
    )

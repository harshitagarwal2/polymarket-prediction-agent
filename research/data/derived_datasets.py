from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from research.attribution import TradeAttribution
from research.data.schemas import (
    InferenceDatasetRow,
    ReplayExecutionLabelRow,
    ResolutionTruthRow,
    TrainingSetRow,
)
from research.replay import ReplayResult
from research.data.storage_paths import build_research_storage_paths
from research.datasets import DatasetRegistry, DatasetSnapshotManifest
from research.features.quality_checks import evaluate_inference_quality
from research.storage import write_jsonl_records
from storage.current_selection import best_mapping_rows


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _int_or_none(value: object) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            return int(value)
        return None
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value)
        return None
    except (TypeError, ValueError):
        return None


def _append_reason(blocked_reasons: list[str], reason: object) -> None:
    if reason in (None, ""):
        return
    normalized = str(reason)
    if normalized not in blocked_reasons:
        blocked_reasons.append(normalized)


def _source_status_reason(
    source_health: dict[str, object], source_name: str
) -> str | None:
    record = source_health.get(source_name)
    if not isinstance(record, dict):
        return None
    status = str(record.get("status") or "").strip().lower()
    if status in {"", "ok", "green"}:
        return None
    return f"source {source_name} unhealthy"


def build_joined_inference_rows(
    *,
    mappings: dict[str, object],
    sportsbook_events: dict[str, object],
    sportsbook_odds: dict[str, object],
    fair_values: dict[str, object],
    bbo_rows: dict[str, object],
    polymarket_markets: dict[str, object],
    source_health: dict[str, object],
    generated_at: datetime,
    max_source_age_ms: int = 60_000,
    min_bookmaker_count: int = 1,
    min_match_confidence: float = 0.6,
    max_book_dispersion: float = 0.1,
) -> list[InferenceDatasetRow]:
    recorded_at = _format_timestamp(generated_at)
    rows: list[InferenceDatasetRow] = []
    odds_rows = [row for row in sportsbook_odds.values() if isinstance(row, dict)]

    for mapping in best_mapping_rows(mappings):
        market_id = str(mapping.get("polymarket_market_id") or "")
        sportsbook_event_id = str(mapping.get("sportsbook_event_id") or "")
        sportsbook_market_type = (
            str(mapping.get("sportsbook_market_type") or "") or None
        )
        matching_odds = [
            row
            for row in odds_rows
            if str(row.get("sportsbook_event_id") or "") == sportsbook_event_id
            and str(row.get("market_type") or "") == (sportsbook_market_type or "")
        ]
        fair_value = fair_values.get(market_id)
        bbo = bbo_rows.get(market_id)
        market = polymarket_markets.get(market_id)
        event = sportsbook_events.get(sportsbook_event_id)

        bookmaker_count = len(
            {
                str(row.get("source") or "")
                for row in matching_odds
                if row.get("source") not in (None, "")
            }
        )
        sportsbook_source_age_ms = max(
            (
                age
                for age in (
                    _int_or_none(row.get("source_age_ms")) for row in matching_odds
                )
                if age is not None
            ),
            default=None,
        )
        polymarket_source_age_ms = (
            _int_or_none(bbo.get("source_age_ms")) if isinstance(bbo, dict) else None
        )
        fair_value_age_ms = (
            _int_or_none(fair_value.get("data_age_ms"))
            if isinstance(fair_value, dict)
            else None
        )
        source_age_ms = max(
            [
                age
                for age in (
                    sportsbook_source_age_ms,
                    polymarket_source_age_ms,
                    fair_value_age_ms,
                )
                if age is not None
            ],
            default=None,
        )
        quality = evaluate_inference_quality(
            source_age_ms=source_age_ms,
            max_source_age_ms=max_source_age_ms,
            bookmaker_count=bookmaker_count,
            min_bookmaker_count=min_bookmaker_count,
            has_polymarket_book=isinstance(bbo, dict),
            match_confidence=_float_or_none(mapping.get("match_confidence")),
            min_match_confidence=min_match_confidence,
            book_dispersion=(
                _float_or_none(fair_value.get("book_dispersion"))
                if isinstance(fair_value, dict)
                else None
            ),
            max_book_dispersion=max_book_dispersion,
        )

        blocked_reasons: list[str] = []
        _append_reason(blocked_reasons, mapping.get("blocked_reason"))
        _append_reason(blocked_reasons, mapping.get("mismatch_reason"))
        if fair_value is None:
            _append_reason(blocked_reasons, "missing fair value")
        for source_name in (
            "polymarket_market_channel",
            "sportsbook_odds",
            "fair_values",
        ):
            _append_reason(
                blocked_reasons,
                _source_status_reason(source_health, source_name),
            )
        for reason in quality.blocked_reasons:
            _append_reason(blocked_reasons, reason)

        condition_id = None
        market_title = None
        if isinstance(market, dict):
            condition_id = str(market.get("condition_id") or "") or None
            market_title = str(market.get("title") or "") or None
            if condition_id is None:
                raw_json = market.get("raw_json")
                if isinstance(raw_json, dict):
                    condition_id = str(raw_json.get("conditionId") or "") or None
            if market_title is None:
                raw_json = market.get("raw_json")
                if isinstance(raw_json, dict):
                    market_title = str(raw_json.get("question") or "") or None

        commence_time = None
        home_team = None
        away_team = None
        if isinstance(event, dict):
            commence_time = (
                str(event.get("start_time") or event.get("commence_time") or "") or None
            )
            home_team = str(event.get("home_team") or "") or None
            away_team = str(event.get("away_team") or "") or None

        row = InferenceDatasetRow(
            record_id=f"{market_id}|{sportsbook_event_id}|{recorded_at}",
            recorded_at=recorded_at,
            market_id=market_id,
            sportsbook_event_id=sportsbook_event_id,
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=str(mapping.get("normalized_market_type") or "")
            or None,
            event_key=str(mapping.get("event_key") or "") or None,
            sport=str(mapping.get("sport") or "") or None,
            series=str(mapping.get("series") or "") or None,
            game_id=str(mapping.get("game_id") or "") or None,
            condition_id=condition_id,
            market_title=market_title,
            home_team=home_team,
            away_team=away_team,
            commence_time=commence_time,
            bookmaker_count=bookmaker_count,
            sportsbook_source_age_ms=sportsbook_source_age_ms,
            polymarket_source_age_ms=polymarket_source_age_ms,
            fair_value_age_ms=fair_value_age_ms,
            source_age_ms=source_age_ms,
            has_polymarket_book=isinstance(bbo, dict),
            fair_yes_prob=(
                _float_or_none(fair_value.get("fair_yes_prob"))
                if isinstance(fair_value, dict)
                else None
            ),
            calibrated_fair_yes_prob=(
                _float_or_none(fair_value.get("calibrated_fair_yes_prob"))
                if isinstance(fair_value, dict)
                else None
            ),
            lower_prob=(
                _float_or_none(fair_value.get("lower_prob"))
                if isinstance(fair_value, dict)
                else None
            ),
            upper_prob=(
                _float_or_none(fair_value.get("upper_prob"))
                if isinstance(fair_value, dict)
                else None
            ),
            book_dispersion=(
                _float_or_none(fair_value.get("book_dispersion"))
                if isinstance(fair_value, dict)
                else None
            ),
            best_bid_yes=(
                _float_or_none(bbo.get("best_bid_yes"))
                if isinstance(bbo, dict)
                else None
            ),
            best_ask_yes=(
                _float_or_none(bbo.get("best_ask_yes"))
                if isinstance(bbo, dict)
                else None
            ),
            midpoint_yes=(
                _float_or_none(bbo.get("midpoint_yes"))
                if isinstance(bbo, dict)
                else None
            ),
            match_confidence=_float_or_none(mapping.get("match_confidence")),
            resolution_risk=_float_or_none(mapping.get("resolution_risk")),
            inference_allowed=not blocked_reasons,
            blocked_reason=blocked_reasons[0] if blocked_reasons else None,
            blocked_reasons=tuple(blocked_reasons),
        )
        rows.append(row)
    return rows


def materialize_inference_dataset(
    *,
    root: str | Path,
    rows: Sequence[InferenceDatasetRow],
    version: str | None = None,
) -> tuple[Path, DatasetSnapshotManifest]:
    paths = build_research_storage_paths(root)
    paths.create_dirs()
    payloads = [row.to_payload() for row in rows]
    registry = DatasetRegistry(paths.root / "datasets")
    manifest = registry.write_rows_snapshot(
        "joined-inference-dataset",
        payloads,
        version=version,
        timestamp_field="recorded_at",
        record_id_field="record_id",
        metadata={
            "processed_path": "processed/inference/joined_inference_dataset.jsonl"
        },
    )
    latest_path = paths.processed_inference_root / "joined_inference_dataset.jsonl"
    write_jsonl_records(latest_path, payloads)
    return latest_path, manifest


def materialize_training_dataset(
    *,
    root: str | Path,
    rows: Sequence[TrainingSetRow],
    version: str | None = None,
) -> tuple[Path, DatasetSnapshotManifest]:
    paths = build_research_storage_paths(root)
    paths.create_dirs()
    payloads = [row.to_payload() for row in rows]
    registry = DatasetRegistry(paths.root / "datasets")
    manifest = registry.write_rows_snapshot(
        "historical-training-dataset",
        payloads,
        version=version,
        timestamp_field="recorded_at",
        record_id_field="record_id",
        metadata={
            "processed_path": "processed/training/historical_training_dataset.jsonl"
        },
    )
    latest_path = paths.processed_training_root / "historical_training_dataset.jsonl"
    write_jsonl_records(latest_path, payloads)
    return latest_path, manifest


def materialize_resolution_truth_dataset(
    *,
    root: str | Path,
    rows: Sequence[ResolutionTruthRow],
    version: str | None = None,
) -> tuple[Path, DatasetSnapshotManifest]:
    paths = build_research_storage_paths(root)
    paths.create_dirs()
    payloads = [row.to_payload() for row in rows]
    registry = DatasetRegistry(paths.root / "datasets")
    manifest = registry.write_rows_snapshot(
        "historical-resolution-truth-dataset",
        payloads,
        version=version,
        timestamp_field="recorded_at",
        record_id_field="record_id",
        metadata={
            "processed_path": "processed/training/historical_resolution_truth_dataset.jsonl"
        },
    )
    latest_path = (
        paths.processed_training_root / "historical_resolution_truth_dataset.jsonl"
    )
    write_jsonl_records(latest_path, payloads)
    return latest_path, manifest


def materialize_replay_execution_label_dataset(
    *,
    root: str | Path,
    rows: Sequence[ReplayExecutionLabelRow],
    version: str | None = None,
) -> tuple[Path, DatasetSnapshotManifest]:
    paths = build_research_storage_paths(root)
    paths.create_dirs()
    payloads = [row.to_payload() for row in rows]
    registry = DatasetRegistry(paths.root / "datasets")
    manifest = registry.write_rows_snapshot(
        "replay-execution-label-dataset",
        payloads,
        version=version,
        timestamp_field="recorded_at",
        record_id_field="record_id",
        metadata={
            "processed_path": "processed/replay/replay_execution_label_dataset.jsonl"
        },
    )
    latest_path = paths.processed_replay_root / "replay_execution_label_dataset.jsonl"
    write_jsonl_records(latest_path, payloads)
    return latest_path, manifest


def build_replay_execution_label_rows(
    case_name: str,
    replay_result: ReplayResult,
    trade_attributions: Sequence[TradeAttribution],
) -> list[ReplayExecutionLabelRow]:
    attribution_by_trade_id = {
        attribution.trade_id: attribution for attribution in trade_attributions
    }
    rows: list[ReplayExecutionLabelRow] = []
    for row_index, trade in enumerate(replay_result.execution_ledger):
        trade_id = f"{trade.order_id}:fill-{trade.fill_step if trade.fill_step is not None else 'na'}:{row_index}"
        attribution = attribution_by_trade_id.get(trade_id)
        metadata = dict(trade.metadata)
        trade_payload = trade.to_payload()
        rows.append(
            ReplayExecutionLabelRow(
                record_id=trade_id,
                recorded_at=None,
                case_name=case_name,
                market_id=trade.contract.market_key,
                order_id=trade.order_id,
                action=trade.action.value,
                filled=trade.filled,
                requested_quantity=trade.requested_quantity,
                filled_quantity=trade.quantity,
                fill_ratio=trade.fill_ratio,
                partial_fill=trade.partial_fill,
                wait_steps=trade.wait_steps,
                resting=trade.resting,
                stale_data_flag=trade.stale_data_flag,
                decision_fair_value=_float_or_none(
                    trade_payload.get("decision_fair_value")
                ),
                decision_reference_price=_float_or_none(
                    trade_payload.get("decision_reference_price")
                ),
                decision_best_bid=_float_or_none(
                    trade_payload.get("decision_best_bid")
                ),
                decision_best_ask=_float_or_none(
                    trade_payload.get("decision_best_ask")
                ),
                decision_midpoint=_float_or_none(
                    trade_payload.get("decision_midpoint")
                ),
                expected_edge_bps=_float_or_none(
                    trade_payload.get("expected_edge_bps")
                ),
                realized_edge_bps=_float_or_none(
                    trade_payload.get("realized_edge_bps")
                ),
                slippage_bps=_float_or_none(trade_payload.get("slippage_bps")),
                visible_quantity=trade.visible_quantity,
                levels_consumed=trade.levels_consumed,
                price_move_bps=trade.price_move_bps,
                mapping_risk=attribution.mapping_risk
                if attribution is not None
                else _mapping_risk(metadata),
                replay_step_index=_int_or_none(metadata.get("replay_step_index")),
                metadata=metadata,
            )
        )
    return rows


def _mapping_risk(metadata: Mapping[str, object]) -> float | None:
    raw = metadata.get("mapping_risk")
    if isinstance(raw, (int, float)):
        return float(raw)
    return None

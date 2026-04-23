from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
import json
from pathlib import Path
import hashlib

from research.data.capture_polymarket import (
    build_polymarket_capture,
    load_polymarket_capture,
)
from research.data.capture_sports_inputs import load_sports_input_capture
from research.data.schemas import PolymarketMarketRecord
from research.data.schemas import SportsInputRow
from research.data.schemas import TrainingSetRow
from research.features.joiners import merge_feature_namespaces
from research.features.sports_features import build_team_strength_features
from research.features.market_features import build_market_microstructure_features
from research.models.elo import extract_elo_training_example
from research.schemas import SportsBenchmarkCase


def build_training_set_rows(cases: list[SportsBenchmarkCase]) -> list[TrainingSetRow]:
    rows: list[TrainingSetRow] = []
    for case in cases:
        example = extract_elo_training_example(case)
        if example is None:
            continue
        rows.append(
            TrainingSetRow(
                home_team=example.home_team,
                away_team=example.away_team,
                label=int(example.home_win),
                record_id=f"benchmark-case|{case.name}",
                metadata={"source_case": case.name},
            )
        )
    return rows


def build_training_set_rows_from_sports_inputs(
    rows: Iterable[SportsInputRow],
    *,
    polymarket_markets: Iterable[PolymarketMarketRecord] | None = None,
) -> list[TrainingSetRow]:
    market_rows = list(polymarket_markets or [])
    training_rows: list[TrainingSetRow] = []
    seen_record_ids: dict[str, int] = {}
    for row in rows:
        if row.home_team in (None, "") or row.away_team in (None, ""):
            continue
        if row.label not in {0, 1}:
            continue
        feature_metadata = build_team_strength_features(
            home_team=str(row.home_team),
            away_team=str(row.away_team),
            selection_name=row.selection_name,
            decimal_odds=row.decimal_odds,
            implied_probability=row.implied_probability,
            captured_at=row.captured_at,
            start_time=row.start_time,
        )
        matched_markets = [
            market for market in market_rows if _matches_market_record(row, market)
        ]
        selected_market = _select_training_market_match(matched_markets)
        market_features = _build_market_feature_metadata(
            row,
            [selected_market] if selected_market is not None else [],
        )
        market_key = next(
            (
                market.market_key
                for market in [selected_market]
                if market is not None and market.market_key not in (None, "")
            ),
            None,
        )
        condition_id = next(
            (
                market.condition_id
                for market in [selected_market]
                if market is not None and market.condition_id not in (None, "")
            ),
            None,
        )
        recorded_at = _format_recorded_at(row.captured_at)
        base_record_id = _build_training_record_id(
            row,
            recorded_at=recorded_at,
            market_key=market_key,
            condition_id=condition_id,
        )
        seen_count = seen_record_ids.get(base_record_id, 0) + 1
        seen_record_ids[base_record_id] = seen_count
        record_id = (
            base_record_id
            if seen_count == 1
            else f"{base_record_id}|dup-{seen_count:02d}"
        )
        training_rows.append(
            TrainingSetRow(
                home_team=str(row.home_team),
                away_team=str(row.away_team),
                label=int(row.label),
                record_id=record_id,
                recorded_at=recorded_at,
                event_key=row.event_key,
                sport=row.sport,
                series=row.series,
                game_id=row.game_id,
                sports_market_type=row.sports_market_type,
                source=row.source,
                market_key=market_key,
                condition_id=condition_id,
                metadata={
                    **feature_metadata,
                    **merge_feature_namespaces(market=market_features),
                    "event_key": row.event_key,
                    "sport": row.sport,
                    "series": row.series,
                    "game_id": row.game_id,
                    "sports_market_type": row.sports_market_type,
                    "source": row.source,
                },
            )
        )
    return training_rows


def _matches_market_record(row: SportsInputRow, market: PolymarketMarketRecord) -> bool:
    if (
        row.sports_market_type not in (None, "")
        and market.sports_market_type not in (None, "")
        and row.sports_market_type != market.sports_market_type
    ):
        return False

    shared_identity = False
    if row.event_key not in (None, "") and market.event_key not in (None, ""):
        shared_identity = True
        if row.event_key != market.event_key:
            return False
    if row.game_id not in (None, "") and market.game_id not in (None, ""):
        shared_identity = True
        if row.game_id != market.game_id:
            return False

    return shared_identity


def _mean(values: Iterable[float | None]) -> float | None:
    resolved = [float(value) for value in values if value is not None]
    if not resolved:
        return None
    return sum(resolved) / len(resolved)


def _sum(values: Iterable[float | None]) -> float | None:
    resolved = [float(value) for value in values if value is not None]
    if not resolved:
        return None
    return sum(resolved)


def _build_market_feature_metadata(
    row: SportsInputRow, market_rows: list[PolymarketMarketRecord]
) -> dict[str, float]:
    if not market_rows:
        return {}

    start_candidates = [
        market.start_time for market in market_rows if market.start_time is not None
    ]
    start_time = min(start_candidates) if start_candidates else row.start_time
    features = build_market_microstructure_features(
        best_bid=_mean(market.best_bid for market in market_rows),
        best_ask=_mean(market.best_ask for market in market_rows),
        volume=_sum(market.volume for market in market_rows),
        best_bid_size=_sum(market.best_bid_size for market in market_rows),
        best_ask_size=_sum(market.best_ask_size for market in market_rows),
        captured_at=row.captured_at,
        start_time=start_time,
    )
    features["market_count"] = float(len(market_rows))
    return features


def _select_training_market_match(
    matched_markets: list[PolymarketMarketRecord],
) -> PolymarketMarketRecord | None:
    if len(matched_markets) != 1:
        return None
    return matched_markets[0]


def _format_recorded_at(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_training_record_id(
    row: SportsInputRow,
    *,
    recorded_at: str,
    market_key: str | None,
    condition_id: str | None,
) -> str:
    identity = (
        row.event_key
        or row.game_id
        or row.source_event_id
        or f"{row.home_team}|{row.away_team}|{row.selection_name}"
    )
    digest = hashlib.sha1(
        json.dumps(
            {
                "bookmaker": row.bookmaker,
                "selection_name": row.selection_name,
                "sports_market_type": row.sports_market_type,
                "source_event_id": row.source_event_id,
                "market_key": market_key,
                "condition_id": condition_id,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:10]
    return "|".join(
        [
            str(row.source),
            str(identity),
            str(recorded_at),
            digest,
        ]
    )


def load_training_set_rows(
    path: str,
    *,
    polymarket_capture_path: str | None = None,
) -> list[TrainingSetRow]:
    capture = load_sports_input_capture(path)
    market_rows: list[PolymarketMarketRecord] = []
    if polymarket_capture_path not in (None, ""):
        market_rows = load_polymarket_capture(polymarket_capture_path).markets
    else:
        payload = json.loads(Path(path).read_text())
        if isinstance(payload, dict) and isinstance(
            payload.get("polymarket_markets"), list
        ):
            market_rows = build_polymarket_capture(
                payload["polymarket_markets"],
                layer=str(payload.get("polymarket_layer") or "training-set"),
                captured_at=capture.captured_at,
            ).markets
    return build_training_set_rows_from_sports_inputs(
        capture.rows,
        polymarket_markets=market_rows,
    )

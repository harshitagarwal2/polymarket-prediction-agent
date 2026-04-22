from __future__ import annotations

from collections.abc import Iterable
import json
from pathlib import Path

from research.data.capture_polymarket import build_polymarket_capture, load_polymarket_capture
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
        market_features = _build_market_feature_metadata(row, market_rows)
        training_rows.append(
            TrainingSetRow(
                home_team=str(row.home_team),
                away_team=str(row.away_team),
                label=int(row.label),
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


def _matches_market_record(
    row: SportsInputRow, market: PolymarketMarketRecord
) -> bool:
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
    matched = [market for market in market_rows if _matches_market_record(row, market)]
    if not matched:
        return {}

    start_candidates = [market.start_time for market in matched if market.start_time is not None]
    start_time = min(start_candidates) if start_candidates else row.start_time
    features = build_market_microstructure_features(
        best_bid=_mean(market.best_bid for market in matched),
        best_ask=_mean(market.best_ask for market in matched),
        volume=_sum(market.volume for market in matched),
        best_bid_size=_sum(market.best_bid_size for market in matched),
        best_ask_size=_sum(market.best_ask_size for market in matched),
        captured_at=row.captured_at,
        start_time=start_time,
    )
    features["market_count"] = float(len(matched))
    return features


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
        if isinstance(payload, dict) and isinstance(payload.get("polymarket_markets"), list):
            market_rows = build_polymarket_capture(
                payload["polymarket_markets"],
                layer=str(payload.get("polymarket_layer") or "training-set"),
                captured_at=capture.captured_at,
            ).markets
    return build_training_set_rows_from_sports_inputs(
        capture.rows,
        polymarket_markets=market_rows,
    )

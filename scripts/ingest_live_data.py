from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from adapters.polymarket import PolymarketAdapter
from adapters.polymarket.gamma_client import fetch_markets
from adapters.polymarket.market_catalog import PolymarketMarketCatalogClient
from adapters.polymarket.normalizer import normalize_bbo_event, normalize_market_row
from adapters.sportsbooks import TheOddsApiClient, normalize_odds_event
from adapters.types import serialize_market_summary
from contracts import (
    MappingManifestBuild,
    map_contract_candidate,
    mapping_blocked_reason,
    semantics_from_market_type,
    validate_mapping_manifest_payload,
)
from contracts.models import ContractMatch
from engine.cli_output import add_quiet_flag, emit_json
from engine.config_loader import load_config_file, nested_config_value
from engine.runtime_metrics import RuntimeMetricsCollector
from engine.runtime_policy import load_runtime_policy
from engine.structured_logging import build_structured_logger, structured_log
from engine.runtime_bootstrap import build_adapter
from storage import (
    BBORepository,
    FairValueRecord,
    FairValueRepository,
    FileBackedCurrentStateStore,
    MappingRepository,
    MarketMappingRecord,
    MarketRepository,
    OpportunityRecord,
    OpportunityRepository,
    ParquetStore,
    PolymarketBBORecord,
    PolymarketMarketRecord,
    RawStore,
    SourceHealthStore,
    SportsbookEventRecord,
    SportsbookEventRepository,
    SportsbookOddsRecord,
    SportsbookOddsRepository,
    best_mapping_rows,
    mapping_priority,
)
from forecasting import FairValueEngine, ForecastCalibrator
from opportunity import opportunity_from_prices, rank_opportunities
from opportunity.models import Opportunity, normalize_blocked_reasons
from risk.freeze_windows import FreezeWindowPolicy, freeze_reasons_for_state
from research.fair_value_manifest import FairValueManifestBuild
from research.data.capture_polymarket import (
    build_polymarket_capture,
    write_polymarket_capture,
)
from research.data.build_training_set import load_training_set_rows
from research.data.capture_sports_inputs import (
    build_sports_input_capture,
    write_sports_input_capture,
)
from research.data.derived_datasets import (
    build_joined_inference_rows,
    materialize_inference_dataset,
    materialize_training_dataset,
)
from research.data.odds_api import (
    fetch_odds_payload,
    load_event_map,
    normalize_odds_events,
)
from research.manifest_validation import validate_manifest_payload
from research.models.book_consensus import load_book_consensus_artifact


SPORT_KEY_BY_LEAGUE = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture normalized offline research inputs."
    )
    parser.add_argument(
        "--layer",
        choices=("gamma", "clob", "data-api", "sports-inputs"),
        required=True,
    )
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--sport-key", default=None)
    parser.add_argument("--event-map-file", default=None)
    parser.add_argument("--regions", default="us")
    parser.add_argument("--markets", default="h2h")
    parser.add_argument(
        "--odds-format", choices=("decimal", "american"), default="decimal"
    )
    parser.add_argument("--bookmakers", default=None)
    parser.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    parser.add_argument("--data-api-path", default="/trades")
    add_quiet_flag(parser)
    return parser


def build_new_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest live data into raw/current/parquet stores."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    pm_markets = subparsers.add_parser("polymarket-markets")
    pm_markets.add_argument("--sport", default=None)
    pm_markets.add_argument("--market-type", default=None)
    pm_markets.add_argument("--limit", type=int, default=100)
    pm_markets.add_argument("--root", default="runtime/data")
    add_quiet_flag(pm_markets)

    pm_bbo = subparsers.add_parser("polymarket-bbo")
    pm_bbo.add_argument("--input", default=None)
    pm_bbo.add_argument("--root", default="runtime/data")
    add_quiet_flag(pm_bbo)

    sb_odds = subparsers.add_parser("sportsbook-odds")
    sb_odds.add_argument("--sport", default=None)
    sb_odds.add_argument("--market", default=None)
    sb_odds.add_argument("--root", default="runtime/data")
    sb_odds.add_argument("--config-file", default=None)
    sb_odds.add_argument("--event-map-file", default=None)
    sb_odds.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    add_quiet_flag(sb_odds)

    mappings = subparsers.add_parser("build-mappings")
    mappings.add_argument("--market", default=None)
    mappings.add_argument("--root", default="runtime/data")
    mappings.add_argument("--config-file", default=None)
    add_quiet_flag(mappings)

    fair_values = subparsers.add_parser("build-fair-values")
    fair_values.add_argument("--root", default="runtime/data")
    fair_values.add_argument("--config-file", default=None)
    fair_values.add_argument("--model-name", default="deterministic_consensus")
    fair_values.add_argument("--model-version", default="v1")
    fair_values.add_argument("--consensus-artifact", default=None)
    fair_values.add_argument("--calibration-artifact", default=None)
    add_quiet_flag(fair_values)

    inference_dataset = subparsers.add_parser("build-inference-dataset")
    inference_dataset.add_argument("--root", default="runtime/data")
    inference_dataset.add_argument("--max-source-age-ms", type=int, default=60_000)
    inference_dataset.add_argument("--min-bookmaker-count", type=int, default=1)
    inference_dataset.add_argument("--min-match-confidence", type=float, default=0.6)
    inference_dataset.add_argument("--max-book-dispersion", type=float, default=0.1)
    add_quiet_flag(inference_dataset)

    training_dataset = subparsers.add_parser("build-training-dataset")
    training_dataset.add_argument("--input", required=True)
    training_dataset.add_argument("--polymarket-input", default=None)
    training_dataset.add_argument("--root", default="runtime/data")
    add_quiet_flag(training_dataset)

    opportunities = subparsers.add_parser("build-opportunities")
    opportunities.add_argument("--root", default="runtime/data")
    opportunities.add_argument("--fee-bps", type=float, default=0.0)
    opportunities.add_argument("--slippage-bps", type=float, default=0.0)
    opportunities.add_argument("--policy-file", default=None)
    add_quiet_flag(opportunities)
    return parser


def _load_live_payload(args) -> object:
    config = load_config_file(args.config_file) if args.config_file else {}
    if args.layer == "sports-inputs":
        sport_key = args.sport_key or nested_config_value(
            config, "capture", "sport_key"
        )
        if sport_key in (None, ""):
            league = nested_config_value(config, "league")
            if isinstance(league, str):
                sport_key = SPORT_KEY_BY_LEAGUE.get(league.strip().lower())
        if not isinstance(sport_key, str) or not sport_key:
            raise RuntimeError(
                "sports-inputs live capture requires --sport-key or a config with league/capture.sport_key"
            )
        event_map = load_event_map(args.event_map_file)
        api_key = os.getenv(args.api_key_env)
        if not api_key:
            raise RuntimeError(
                f"missing required environment variable: {args.api_key_env}"
            )
        payload = fetch_odds_payload(
            sport_key=sport_key,
            api_key=api_key,
            regions=args.regions,
            markets=args.markets,
            odds_format=args.odds_format,
            bookmakers=args.bookmakers,
        )
        return normalize_odds_events(payload, sport_key=sport_key, event_map=event_map)
    adapter = build_adapter("polymarket")
    if args.layer == "gamma":
        return fetch_markets(limit=args.limit)
    if not isinstance(adapter, PolymarketAdapter):
        raise RuntimeError("live capture requires the Polymarket adapter")
    if args.layer == "clob":
        markets = adapter.list_markets(limit=args.limit)
        return [
            {
                "market": serialize_market_summary(market),
                "order_book": adapter.get_order_book(market.contract).raw,
            }
            for market in markets
        ]
    return adapter._fetch_data_api(args.data_api_path, {"limit": args.limit})


def _stores(root: str):
    return {
        "raw": RawStore(Path(root) / "raw"),
        "parquet": ParquetStore(Path(root) / "parquet"),
        "current": FileBackedCurrentStateStore(Path(root) / "current"),
        "markets": MarketRepository(Path(root) / "postgres"),
        "bbo": BBORepository(Path(root) / "postgres"),
        "sb_events": SportsbookEventRepository(Path(root) / "postgres"),
        "sb_odds": SportsbookOddsRepository(Path(root) / "postgres"),
        "mappings": MappingRepository(Path(root) / "postgres"),
        "fair_values": FairValueRepository(Path(root) / "postgres"),
        "opportunities": OpportunityRepository(Path(root) / "postgres"),
        "health": SourceHealthStore(Path(root) / "current" / "source_health.json"),
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _trace_id() -> str:
    return uuid4().hex


def _metrics_collector(root: str) -> RuntimeMetricsCollector:
    return RuntimeMetricsCollector(Path(root) / "current" / "runtime_metrics.json")


def _parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _float_or_default(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sorted_rows(mapping: dict[str, object]) -> list[dict]:
    rows = [row for row in mapping.values() if isinstance(row, dict)]
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True))
    return rows


def _merged_mapping_payload(
    row: dict[str, object],
    **defaults: object,
) -> dict[str, object]:
    raw = row.get("raw_json")
    payload = dict(raw) if isinstance(raw, dict) else {}
    for key, value in defaults.items():
        payload.setdefault(key, value)
    return payload


def _mapping_market_payload(market: dict[str, object]) -> dict[str, object]:
    return _merged_mapping_payload(
        market,
        market_id=market.get("market_id"),
        condition_id=market.get("condition_id"),
        title=market.get("title"),
        question=market.get("title"),
        endDate=market.get("end_time"),
    )


def _mapping_event_payload(event: dict[str, object]) -> dict[str, object]:
    return _merged_mapping_payload(
        event,
        sportsbook_event_id=event.get("sportsbook_event_id"),
        sport=event.get("sport"),
        league=event.get("league"),
        series=event.get("league"),
        home_team=event.get("home_team"),
        away_team=event.get("away_team"),
        start_time=event.get("start_time"),
    )


def _load_optional_config(config_file: str | None) -> dict[str, object]:
    return load_config_file(config_file) if config_file else {}


def _resolve_sport_key(value: str | None, config: dict[str, object]) -> str:
    if isinstance(value, str) and value:
        return value
    configured_sport = nested_config_value(config, "capture", "sport_key")
    if isinstance(configured_sport, str) and configured_sport:
        return configured_sport
    runtime_sport = nested_config_value(config, "runtime", "sport_key")
    if isinstance(runtime_sport, str) and runtime_sport:
        return runtime_sport
    league = nested_config_value(config, "league")
    if isinstance(league, str):
        resolved = SPORT_KEY_BY_LEAGUE.get(league.strip().lower())
        if resolved:
            return resolved
    raise RuntimeError(
        "sportsbook-odds requires --sport or a config with capture.sport_key/runtime.sport_key/league"
    )


def _resolve_sportsbook_market(value: str | None, config: dict[str, object]) -> str:
    if isinstance(value, str) and value:
        return value
    configured_market = nested_config_value(config, "runtime", "sportsbook_market")
    if isinstance(configured_market, str) and configured_market:
        return configured_market
    raise RuntimeError(
        "command requires --market or a config with runtime.sportsbook_market"
    )


def _resolve_event_map_file(value: str | None, config: dict[str, object]) -> str | None:
    if value not in (None, ""):
        return value
    configured = nested_config_value(config, "runtime", "event_map_file")
    return configured if isinstance(configured, str) and configured else None


def _resolve_consensus_artifact(
    value: str | None, config: dict[str, object]
) -> str | None:
    if value not in (None, ""):
        return value
    configured = nested_config_value(config, "runtime", "consensus_artifact")
    return configured if isinstance(configured, str) and configured else None


def _resolve_calibration_artifact(
    value: str | None, config: dict[str, object]
) -> str | None:
    if value not in (None, ""):
        return value
    configured = nested_config_value(config, "runtime", "calibration_artifact")
    return configured if isinstance(configured, str) and configured else None


def _runtime_manifest_path(root: str | Path) -> Path:
    return Path(root) / "current" / "fair_value_manifest.json"


def _runtime_mapping_manifest_path(root: str | Path) -> Path:
    return Path(root) / "current" / "market_mapping_manifest.json"


def _build_runtime_manifest_payload(
    *,
    snapshots: list[dict],
    mappings: dict[str, dict],
    polymarket_markets: dict[str, dict[str, object]],
    generated_at: datetime,
    calibration_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    values: dict[str, dict[str, object]] = {}
    for snapshot in snapshots:
        market_id = str(snapshot.get("market_id") or "")
        if not market_id:
            continue
        mapping = mappings.get(market_id, {})
        market_row = polymarket_markets.get(market_id, {})
        raw_market = market_row.get("raw_json") if isinstance(market_row, dict) else {}
        condition_id = None
        if isinstance(raw_market, dict):
            raw_condition = raw_market.get("conditionId") or raw_market.get(
                "condition_id"
            )
            if raw_condition not in (None, ""):
                condition_id = str(raw_condition)
        values[market_id] = {
            "fair_value": float(snapshot["fair_yes_prob"]),
            "generated_at": str(snapshot["as_of"]),
            "source": "live-current-state",
            "condition_id": condition_id,
            "event_key": mapping.get("event_key"),
            "sport": mapping.get("sport"),
            "series": mapping.get("series"),
            "game_id": mapping.get("game_id"),
            "sports_market_type": mapping.get("normalized_market_type"),
        }
        calibrated_fair_yes_prob = snapshot.get("calibrated_fair_yes_prob")
        if calibrated_fair_yes_prob is not None:
            values[market_id]["calibrated_fair_value"] = float(calibrated_fair_yes_prob)

    provenance: dict[str, object] = {
        "source_table": "runtime/data/current/fair_values.json",
    }
    if snapshots:
        first_snapshot = snapshots[0]
        if first_snapshot.get("model_name") not in (None, ""):
            provenance["model_name"] = str(first_snapshot["model_name"])
        if first_snapshot.get("model_version") not in (None, ""):
            provenance["model_version"] = str(first_snapshot["model_version"])

    metadata: dict[str, object] = {
        "coverage": {"value_count": len(values)},
        "provenance": provenance,
    }
    if calibration_metadata is not None:
        metadata["calibration"] = calibration_metadata

    manifest = FairValueManifestBuild(
        generated_at=generated_at,
        source="live-current-state",
        values=values,
        metadata=metadata,
    )
    payload = manifest.to_payload()
    validate_manifest_payload(payload)
    return payload


def _has_upstream_event_identity(payload: dict[str, object]) -> bool:
    return any(
        payload.get(field) not in (None, "") for field in ("event_key", "game_id")
    )


def _build_fair_value_engine(args) -> FairValueEngine:
    config = _load_optional_config(getattr(args, "config_file", None))
    consensus_artifact = _resolve_consensus_artifact(args.consensus_artifact, config)
    if consensus_artifact in (None, ""):
        return FairValueEngine(
            model_name=args.model_name,
            model_version=args.model_version,
        )
    artifact = load_book_consensus_artifact(consensus_artifact)
    return FairValueEngine(
        model_name=artifact.model,
        model_version=artifact.model_version,
        half_life_seconds=artifact.half_life_seconds,
    )


def _build_fair_value_calibrator(args) -> ForecastCalibrator | None:
    config = _load_optional_config(getattr(args, "config_file", None))
    calibration_artifact = _resolve_calibration_artifact(
        args.calibration_artifact,
        config,
    )
    if calibration_artifact in (None, ""):
        return None
    return ForecastCalibrator.load(calibration_artifact)


def _artifact_configured(args) -> bool:
    config = _load_optional_config(getattr(args, "config_file", None))
    return _resolve_consensus_artifact(args.consensus_artifact, config) not in (
        None,
        "",
    )


def _calibration_configured(args) -> bool:
    config = _load_optional_config(getattr(args, "config_file", None))
    return _resolve_calibration_artifact(args.calibration_artifact, config) not in (
        None,
        "",
    )


def _build_manifest_calibration_metadata(
    calibrator: ForecastCalibrator | None,
) -> dict[str, object] | None:
    if calibrator is None:
        return None
    artifact = calibrator.artifact
    return {
        "method": "histogram",
        "bin_count": artifact.bin_count,
        "sample_count": artifact.sample_count,
        "positive_rate": round(artifact.positive_rate, 8),
        "applied_field": "fair_value",
    }


def _safe_error_kind(exc: Exception) -> str:
    return exc.__class__.__name__


def _best_effort(callback) -> None:
    try:
        callback()
    except Exception:
        return None


def _required_sources(
    source_health: dict[str, object],
) -> tuple[str, ...]:
    return tuple(
        source_name
        for source_name in (
            "polymarket_market_channel",
            "sportsbook_odds",
            "market_mappings",
            "fair_values",
        )
        if source_name in source_health
    )


def _build_opportunity_freeze_policy(policy_file: str | None) -> FreezeWindowPolicy:
    if not policy_file:
        return FreezeWindowPolicy()
    policy = load_runtime_policy(policy_file)
    planner_policy = policy.proposal_planner
    return FreezeWindowPolicy(
        freeze_minutes_before_start=planner_policy.freeze_minutes_before_start,
        freeze_minutes_before_expiry=planner_policy.freeze_minutes_before_expiry,
        freeze_when_source_unhealthy=planner_policy.block_on_unhealthy_source,
    )


def _run_polymarket_markets(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("ingest.polymarket.markets")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    client = PolymarketMarketCatalogClient()
    markets = client.fetch_open_markets()
    if args.sport:
        markets = [
            item
            for item in markets
            if str(item.get("sport") or "").lower() == args.sport.lower()
        ]
    if args.market_type:
        markets = [
            item
            for item in markets
            if args.market_type.lower()
            in str(
                item.get("sports_market_type") or item.get("market_type") or ""
            ).lower()
        ]
    markets = markets[: args.limit]
    rows = [normalize_market_row(item) for item in markets]
    stores["raw"].write(
        "polymarket",
        "market_catalog",
        _utc_now(),
        {"markets": markets},
    )
    for row in rows:
        record = PolymarketMarketRecord(
            market_id=row["market_id"],
            condition_id=row["condition_id"],
            token_id_yes=None,
            token_id_no=None,
            title=row["title"],
            description=row["description"],
            event_slug=row["event_slug"],
            market_slug=row["market_slug"],
            category=row["category"],
            end_time=row["end_time"],
            status=row["status"],
            raw_json=row["raw_json"],
        )
        stores["markets"].upsert(record.market_id, record)
        stores["current"].upsert("polymarket_markets", record.market_id, row)
    stores["parquet"].append_records("polymarket_markets", _utc_now(), rows)
    stores["health"].upsert(
        "polymarket_market_catalog", stale_after_ms=60_000, status="ok"
    )
    structured_log(
        logger,
        action="sync",
        status="ok",
        message="synced market catalog",
        trace_id=trace_id,
        latency_ms=None,
    )
    metrics.record(
        component="ingest.polymarket.markets",
        action="sync",
        status="ok",
        trace_id=trace_id,
        market_count=len(rows),
    )
    emit_json({"market_count": len(rows), "root": args.root}, quiet=args.quiet)
    return 0


def _run_polymarket_bbo(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("ingest.polymarket.bbo")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    if args.input:
        payload = json.loads(Path(args.input).read_text())
        events = payload if isinstance(payload, list) else [payload]
    else:
        events = []
    rows = [normalize_bbo_event(event) for event in events if isinstance(event, dict)]
    for row in rows:
        record = PolymarketBBORecord(
            market_id=row["market_id"],
            best_bid_yes=row["best_bid_yes"],
            best_bid_yes_size=row["best_bid_yes_size"],
            best_ask_yes=row["best_ask_yes"],
            best_ask_yes_size=row["best_ask_yes_size"],
            midpoint_yes=row["midpoint_yes"],
            spread_yes=row["spread_yes"],
            book_ts=row["book_ts"],
            source_age_ms=row["source_age_ms"],
            raw_hash=None,
        )
        stores["bbo"].upsert(record.market_id, record)
        stores["current"].upsert("polymarket_bbo", record.market_id, row)
    for event in events:
        if isinstance(event, dict):
            stores["raw"].write("polymarket", "market_channel", _utc_now(), event)
    stores["parquet"].append_records("polymarket_bbo_history", _utc_now(), rows)
    stores["health"].upsert(
        "polymarket_market_channel", stale_after_ms=4_000, status="ok"
    )
    structured_log(
        logger,
        action="sync",
        status="ok",
        message="normalized bbo events",
        trace_id=trace_id,
    )
    metrics.record(
        component="ingest.polymarket.bbo",
        action="sync",
        status="ok",
        trace_id=trace_id,
        bbo_count=len(rows),
    )
    emit_json({"bbo_count": len(rows), "root": args.root}, quiet=args.quiet)
    return 0


def _run_sportsbook_odds(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("ingest.sportsbook.odds")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    config = _load_optional_config(args.config_file)
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"missing required environment variable: {args.api_key_env}")
    client = TheOddsApiClient(api_key=api_key)
    sport = _resolve_sport_key(args.sport, config)
    market = _resolve_sportsbook_market(args.market, config)
    events = client.fetch_upcoming(sport, market)
    event_map = load_event_map(_resolve_event_map_file(args.event_map_file, config))
    normalized_rows: list[dict] = []
    for event in events:
        event["sport_key"] = sport
        event_identity = event_map.get(str(event.get("id") or ""), {})
        normalized_rows.extend(
            normalize_odds_event(
                event,
                source="theoddsapi",
                market_type=market,
                captured_at=_utc_now(),
            )
        )
        stores["raw"].write("sportsbook", "odds", _utc_now(), event)
        event_payload = dict(event)
        for field in ("event_key", "game_id", "sport", "series"):
            if event_identity.get(field) not in (None, ""):
                event_payload[field] = event_identity[field]
        event_record = SportsbookEventRecord(
            sportsbook_event_id=str(event.get("id") or ""),
            source="theoddsapi",
            sport=sport,
            league=event.get("sport_title"),
            home_team=event.get("home_team"),
            away_team=event.get("away_team"),
            start_time=str(event.get("commence_time") or ""),
            raw_json=event_payload,
        )
        stores["sb_events"].upsert(event_record.sportsbook_event_id, event_record)
        stores["current"].upsert(
            "sportsbook_events", event_record.sportsbook_event_id, event_record.raw_json
        )
    for row in normalized_rows:
        record = SportsbookOddsRecord(
            sportsbook_event_id=row["sportsbook_event_id"],
            source=row["source"],
            market_type=row["market_type"],
            selection=row["selection"],
            price_decimal=row["price_decimal"],
            implied_prob=row["implied_prob"],
            overround=row["overround"],
            quote_ts=row["quote_ts"],
            source_age_ms=row["source_age_ms"],
            raw_json=row["raw_json"],
        )
        stores["sb_odds"].append(record)
        stores["current"].upsert(
            "sportsbook_odds",
            "|".join(
                [
                    record.sportsbook_event_id,
                    record.source,
                    record.market_type,
                    record.selection,
                ]
            ),
            record.__dict__.copy(),
        )
    stores["parquet"].append_records("odds_snapshots", _utc_now(), normalized_rows)
    stores["health"].upsert("sportsbook_odds", stale_after_ms=60_000, status="ok")
    structured_log(
        logger,
        action="sync",
        status="ok",
        message="normalized sportsbook odds",
        trace_id=trace_id,
    )
    metrics.record(
        component="ingest.sportsbook.odds",
        action="sync",
        status="ok",
        trace_id=trace_id,
        event_count=len(events),
        row_count=len(normalized_rows),
    )
    emit_json(
        {
            "event_count": len(events),
            "row_count": len(normalized_rows),
            "root": args.root,
        },
        quiet=args.quiet,
    )
    return 0


def _run_build_mappings(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("ingest.mappings")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    config = _load_optional_config(args.config_file)
    sportsbook_market = _resolve_sportsbook_market(args.market, config)
    markets = _sorted_rows(stores["markets"].read_all())
    events = stores["sb_events"].read_all()
    mappings: list[dict] = []
    current_mappings: dict[str, dict] = {}
    mapping_manifest_values: dict[str, dict[str, object]] = {}
    for market in markets:
        best_match: MarketMappingRecord | None = None
        best_decision = None
        best_blocked_reason = None
        market_payload = _mapping_market_payload(market)
        pm_market_type = str(
            market_payload.get("sportsMarketType")
            or market_payload.get("sports_market_type")
            or sportsbook_market
        )
        for event in events.values():
            event_payload = _mapping_event_payload(event)
            decision = map_contract_candidate(
                market_payload,
                event_payload,
                sportsbook_market_type=sportsbook_market,
                pm_semantics=semantics_from_market_type(
                    pm_market_type,
                    source="polymarket",
                ),
                sb_semantics=semantics_from_market_type(
                    sportsbook_market,
                    source="sportsbook",
                ),
            )
            blocked_reason = decision.blocked_reason
            if blocked_reason is None and not _has_upstream_event_identity(
                event_payload
            ):
                blocked_reason = mapping_blocked_reason(
                    "missing upstream event identity"
                )
            persisted_match_confidence = (
                min(decision.match_confidence, 0.59)
                if blocked_reason is not None
                else decision.match_confidence
            )
            candidate = MarketMappingRecord(
                polymarket_market_id=decision.polymarket_market_id,
                sportsbook_event_id=decision.sportsbook_event_id,
                sportsbook_market_type=decision.sportsbook_market_type,
                normalized_market_type=decision.normalized_market_type,
                match_confidence=persisted_match_confidence,
                resolution_risk=round(
                    max(0.0, 1.0 - persisted_match_confidence),
                    4,
                ),
                mismatch_reason=(blocked_reason.message if blocked_reason else None),
                event_key=decision.event_key,
                sport=decision.sport,
                series=decision.series,
                game_id=decision.game_id,
                blocked_reason=(blocked_reason.message if blocked_reason else None),
                is_active=blocked_reason is None,
            )
            if best_match is None or mapping_priority(
                candidate.__dict__
            ) > mapping_priority(best_match.__dict__):
                best_match = candidate
                best_decision = decision
                best_blocked_reason = blocked_reason
        if best_match is None or best_decision is None:
            continue
        stores["mappings"].append(best_match)
        best_match_payload = best_match.__dict__.copy()
        mappings.append(best_match_payload)
        current_mappings[
            f"{best_match.polymarket_market_id}|{best_match.sportsbook_event_id}"
        ] = best_match_payload
        mapping_manifest_values[best_match.polymarket_market_id] = (
            best_decision.to_payload(
                blocked_reason_override=best_blocked_reason,
                confidence_score_override=best_match.match_confidence,
                is_active=best_match.is_active,
            )
        )
    stores["parquet"].append_records("market_mapping_history", _utc_now(), mappings)
    stores["current"].write_table("market_mappings", current_mappings)
    mapping_manifest_payload = MappingManifestBuild(
        generated_at=_utc_now(),
        source="live-current-state",
        values=mapping_manifest_values,
        metadata={
            "coverage": {
                "active_count": sum(
                    1
                    for value in mapping_manifest_values.values()
                    if bool(value.get("is_active"))
                )
            },
            "provenance": {"source_table": "runtime/data/current/market_mappings.json"},
        },
    ).to_payload()
    validate_mapping_manifest_payload(mapping_manifest_payload)
    _runtime_mapping_manifest_path(args.root).write_text(
        json.dumps(mapping_manifest_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    stores["health"].upsert("market_mappings", stale_after_ms=60_000, status="ok")
    structured_log(
        logger,
        action="build",
        status="ok",
        message="built deterministic mappings",
        trace_id=trace_id,
    )
    metrics.record(
        component="ingest.mappings",
        action="build",
        status="ok",
        trace_id=trace_id,
        mapping_count=len(mappings),
    )
    emit_json({"mapping_count": len(mappings), "root": args.root}, quiet=args.quiet)
    return 0


def _run_build_fair_values(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("forecasting.fair_values")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    try:
        engine = _build_fair_value_engine(args)
        calibrator = _build_fair_value_calibrator(args)
        persisted_history = stores["fair_values"].read_all()
        current_odds_rows = stores["current"].read_table("sportsbook_odds")
        odds_rows = (
            _sorted_rows(current_odds_rows)
            if current_odds_rows
            else _sorted_rows(stores["sb_odds"].read_all())
        )
        odds_by_event_market: dict[tuple[str, str], list[dict]] = {}
        for row in odds_rows:
            event_id = str(row.get("sportsbook_event_id") or "")
            market_type = str(row.get("market_type") or "")
            odds_by_event_market.setdefault((event_id, market_type), []).append(row)

        snapshots: list[dict] = []
        pending_history_updates: dict[str, dict] = {}
        pending_current_updates: dict[str, dict] = {}
        active_mapping_count = 0
        for row in best_mapping_rows(stores["current"].read_table("market_mappings")):
            if not bool(row.get("is_active", True)):
                continue
            if all(row.get(field) in (None, "") for field in ("event_key", "game_id")):
                continue
            active_mapping_count += 1
            odds_for_mapping = odds_by_event_market.get(
                (
                    str(row.get("sportsbook_event_id") or ""),
                    str(row.get("sportsbook_market_type") or ""),
                ),
                [],
            )
            if not odds_for_mapping:
                continue
            snapshot = engine.build(
                ContractMatch(
                    polymarket_market_id=str(row.get("polymarket_market_id") or ""),
                    sportsbook_event_id=str(row.get("sportsbook_event_id") or ""),
                    sportsbook_market_type=str(row.get("sportsbook_market_type") or ""),
                    normalized_market_type=str(row.get("normalized_market_type") or ""),
                    match_confidence=_float_or_default(row.get("match_confidence")),
                    resolution_risk=_float_or_default(row.get("resolution_risk")),
                    mismatch_reason=(
                        str(row.get("mismatch_reason"))
                        if row.get("mismatch_reason") not in (None, "")
                        else None
                    ),
                ),
                odds_for_mapping,
            )
            as_of = datetime.fromtimestamp(
                snapshot.timestamp_ms / 1000.0,
                tz=timezone.utc,
            ).isoformat()
            record = FairValueRecord(
                market_id=snapshot.market_id,
                as_of=as_of,
                fair_yes_prob=snapshot.fair_yes_prob,
                lower_prob=snapshot.lower_prob,
                upper_prob=snapshot.upper_prob,
                book_dispersion=snapshot.book_dispersion,
                data_age_ms=snapshot.data_age_ms,
                source_count=snapshot.source_count,
                model_name=snapshot.model_name,
                model_version=snapshot.model_version,
                calibrated_fair_yes_prob=(
                    calibrator.apply(snapshot.fair_yes_prob)
                    if calibrator is not None
                    else None
                ),
            )
            record_payload = record.__dict__.copy()
            history_key = "|".join(
                [
                    record.market_id,
                    record.as_of,
                    record.model_name,
                    record.model_version,
                ]
            )
            pending_history_updates[history_key] = record_payload
            pending_current_updates[record.market_id] = record_payload
            snapshots.append(record_payload)

        stores["parquet"].append_records("fair_values", _utc_now(), snapshots)
        stores["fair_values"].write_all(
            {
                **persisted_history,
                **pending_history_updates,
            }
        )
        stores["current"].write_table(
            "fair_values",
            pending_current_updates,
        )
        runtime_manifest_payload = _build_runtime_manifest_payload(
            snapshots=snapshots,
            mappings={
                str(row.get("polymarket_market_id") or ""): row
                for row in best_mapping_rows(
                    stores["current"].read_table("market_mappings")
                )
            },
            polymarket_markets={
                str(market_id): row
                for market_id, row in stores["current"]
                .read_table("polymarket_markets")
                .items()
                if isinstance(row, dict)
            },
            generated_at=_utc_now(),
            calibration_metadata=_build_manifest_calibration_metadata(calibrator),
        )
        _runtime_manifest_path(args.root).write_text(
            json.dumps(runtime_manifest_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _best_effort(
            lambda: stores["health"].upsert(
                "fair_values",
                stale_after_ms=60_000,
                status="ok",
                details={
                    "model_name": engine.model_name,
                    "model_version": engine.model_version,
                    "consensus_artifact_configured": _artifact_configured(args),
                    "calibration_artifact_configured": _calibration_configured(args),
                },
            )
        )
        _best_effort(
            lambda: structured_log(
                logger,
                action="build",
                status="ok",
                message="built deterministic fair values",
                trace_id=trace_id,
            )
        )
        _best_effort(
            lambda: metrics.record(
                component="forecasting.fair_values",
                action="build",
                status="ok",
                trace_id=trace_id,
                active_mapping_count=active_mapping_count,
                fair_value_count=len(snapshots),
            )
        )
        _best_effort(
            lambda: emit_json(
                {
                    "active_mapping_count": active_mapping_count,
                    "fair_value_count": len(snapshots),
                    "root": args.root,
                    "model_name": engine.model_name,
                    "model_version": engine.model_version,
                    "calibration_applied": calibrator is not None,
                },
                quiet=args.quiet,
            )
        )
        return 0
    except Exception as exc:
        _best_effort(
            lambda: stores["health"].upsert(
                "fair_values",
                stale_after_ms=60_000,
                status="red",
                details={
                    "error_kind": _safe_error_kind(exc),
                    "consensus_artifact_configured": _artifact_configured(args),
                    "calibration_artifact_configured": _calibration_configured(args),
                },
                success=False,
            )
        )
        _best_effort(
            lambda: structured_log(
                logger,
                action="build",
                status="error",
                message="failed to build deterministic fair values",
                trace_id=trace_id,
            )
        )
        _best_effort(
            lambda: metrics.record(
                component="forecasting.fair_values",
                action="build",
                status="error",
                trace_id=trace_id,
                error_kind=_safe_error_kind(exc),
            )
        )
        raise


def _run_build_inference_dataset(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("research.inference_dataset")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    try:
        current_odds = stores["current"].read_table("sportsbook_odds")
        odds_table = current_odds if current_odds else stores["sb_odds"].read_all()
        rows = build_joined_inference_rows(
            mappings=stores["current"].read_table("market_mappings"),
            sportsbook_events=stores["current"].read_table("sportsbook_events"),
            sportsbook_odds=odds_table,
            fair_values=stores["current"].read_table("fair_values"),
            bbo_rows=stores["bbo"].read_all(),
            polymarket_markets=stores["current"].read_table("polymarket_markets"),
            source_health=stores["current"].read_table("source_health"),
            generated_at=_utc_now(),
            max_source_age_ms=args.max_source_age_ms,
            min_bookmaker_count=args.min_bookmaker_count,
            min_match_confidence=args.min_match_confidence,
            max_book_dispersion=args.max_book_dispersion,
        )
        latest_path, manifest = materialize_inference_dataset(
            root=args.root,
            rows=rows,
        )
        stores["health"].upsert(
            "joined_inference_dataset",
            stale_after_ms=60_000,
            status="ok",
            details={
                "row_count": len(rows),
                "dataset_name": manifest.dataset_name,
                "dataset_version": manifest.version,
            },
        )
        structured_log(
            logger,
            action="build",
            status="ok",
            message="built joined inference dataset",
            trace_id=trace_id,
        )
        metrics.record(
            component="research.inference_dataset",
            action="build",
            status="ok",
            trace_id=trace_id,
            row_count=len(rows),
        )
        emit_json(
            {
                "row_count": len(rows),
                "output": str(latest_path),
                "dataset_version": manifest.version,
            },
            quiet=args.quiet,
        )
        return 0
    except Exception as exc:
        _best_effort(
            lambda: stores["health"].upsert(
                "joined_inference_dataset",
                stale_after_ms=60_000,
                status="red",
                details={"error_kind": _safe_error_kind(exc)},
                success=False,
            )
        )
        raise


def _run_build_training_dataset(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("research.training_dataset")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    try:
        rows = load_training_set_rows(
            args.input,
            polymarket_capture_path=args.polymarket_input,
        )
        latest_path, manifest = materialize_training_dataset(
            root=args.root,
            rows=rows,
        )
        stores["health"].upsert(
            "historical_training_dataset",
            stale_after_ms=60_000,
            status="ok",
            details={
                "row_count": len(rows),
                "dataset_name": manifest.dataset_name,
                "dataset_version": manifest.version,
            },
        )
        structured_log(
            logger,
            action="build",
            status="ok",
            message="built historical training dataset",
            trace_id=trace_id,
        )
        metrics.record(
            component="research.training_dataset",
            action="build",
            status="ok",
            trace_id=trace_id,
            row_count=len(rows),
        )
        emit_json(
            {
                "row_count": len(rows),
                "output": str(latest_path),
                "dataset_version": manifest.version,
            },
            quiet=args.quiet,
        )
        return 0
    except Exception as exc:
        _best_effort(
            lambda: stores["health"].upsert(
                "historical_training_dataset",
                stale_after_ms=60_000,
                status="red",
                details={"error_kind": _safe_error_kind(exc)},
                success=False,
            )
        )
        raise


def _run_build_opportunities(args) -> int:
    trace_id = _trace_id()
    logger = build_structured_logger("opportunity.build")
    metrics = _metrics_collector(args.root)
    stores = _stores(args.root)
    fair_values = {
        str(market_id): row
        for market_id, row in stores["current"].read_table("fair_values").items()
        if isinstance(row, dict)
    }
    bbo_rows = stores["bbo"].read_all()
    mapping_rows = best_mapping_rows(stores["current"].read_table("market_mappings"))
    source_health = stores["current"].read_table("source_health")
    sportsbook_events = stores["current"].read_table("sportsbook_events")
    polymarket_markets = stores["current"].read_table("polymarket_markets")
    required_sources = _required_sources(source_health)
    freeze_policy = _build_opportunity_freeze_policy(args.policy_file)
    opportunities: list[OpportunityRecord] = []
    ranked_snapshots: list[Opportunity] = []
    materialized: list[dict] = []
    current_snapshots: dict[str, dict[str, object]] = {}
    for mapping in mapping_rows:
        market_id = str(mapping.get("polymarket_market_id") or "")
        fair_value = fair_values.get(market_id)
        bbo = bbo_rows.get(market_id)
        blocked_reasons = list(
            normalize_blocked_reasons(
                mapping.get("blocked_reason"),
                mapping.get("mismatch_reason"),
            )
        )
        sportsbook_event = sportsbook_events.get(
            str(mapping.get("sportsbook_event_id") or "")
        )
        market_row = polymarket_markets.get(market_id)
        freeze_reasons = freeze_reasons_for_state(
            policy=freeze_policy,
            now=_utc_now(),
            event_start_time=(
                _parse_datetime(sportsbook_event.get("start_time"))
                if isinstance(sportsbook_event, dict)
                else None
            )
            or (
                _parse_datetime(sportsbook_event.get("commence_time"))
                if isinstance(sportsbook_event, dict)
                else None
            ),
            market_end_time=(
                _parse_datetime(market_row.get("end_time"))
                if isinstance(market_row, dict)
                else None
            ),
            market_active=(
                str(market_row.get("status") or "").strip().lower()
                not in {"closed", "inactive", "resolved", "settled"}
                if isinstance(market_row, dict)
                else None
            ),
            market_resolved=(
                str(market_row.get("status") or "").strip().lower()
                in {"resolved", "settled"}
                if isinstance(market_row, dict)
                else None
            ),
            required_sources=required_sources,
            source_health=source_health,
        )
        blocked_reasons = list(
            normalize_blocked_reasons(blocked_reasons, freeze_reasons)
        )
        if fair_value is None:
            blocked_reasons = list(
                normalize_blocked_reasons(blocked_reasons, "missing fair value")
            )
        if bbo is None:
            blocked_reasons = list(
                normalize_blocked_reasons(blocked_reasons, "missing executable bbo")
            )
        blocked_reason = blocked_reasons[0] if blocked_reasons else None
        confidence = _float_or_default(mapping.get("match_confidence"))

        if fair_value is None or bbo is None:
            snapshot = Opportunity(
                market_id=market_id,
                side="buy_yes",
                fair_yes_prob=(
                    _float_or_default(fair_value.get("fair_yes_prob"))
                    if isinstance(fair_value, dict)
                    else 0.0
                ),
                best_bid_yes=(
                    _float_or_default(bbo.get("best_bid_yes"))
                    if isinstance(bbo, dict)
                    else 0.0
                ),
                best_ask_yes=(
                    _float_or_default(bbo.get("best_ask_yes"))
                    if isinstance(bbo, dict)
                    else 0.0
                ),
                edge_buy_bps=0.0,
                edge_sell_bps=0.0,
                edge_buy_after_costs_bps=0.0,
                edge_sell_after_costs_bps=0.0,
                edge_after_costs_bps=0.0,
                fillable_size=0.0,
                confidence=confidence,
                blocked_reasons=tuple(blocked_reasons),
                blocked_reason=blocked_reason,
            )
            opportunity = OpportunityRecord(
                market_id=snapshot.market_id,
                as_of=_utc_now().isoformat(),
                side=snapshot.side,
                fair_yes_prob=snapshot.fair_yes_prob,
                best_bid_yes=snapshot.best_bid_yes,
                best_ask_yes=snapshot.best_ask_yes,
                edge_buy_bps=snapshot.edge_buy_bps,
                edge_sell_bps=snapshot.edge_sell_bps,
                edge_buy_after_costs_bps=snapshot.edge_buy_after_costs_bps,
                edge_sell_after_costs_bps=snapshot.edge_sell_after_costs_bps,
                edge_after_costs_bps=snapshot.edge_after_costs_bps,
                fillable_size=snapshot.fillable_size,
                confidence=snapshot.confidence,
                blocked_reasons=snapshot.blocked_reasons,
                blocked_reason=snapshot.blocked_reason,
                fair_value_ref=str(fair_value.get("as_of"))
                if fair_value
                else _utc_now().isoformat(),
            )
        else:
            best_bid_yes_size = max(
                _float_or_default(bbo.get("best_bid_yes_size")),
                0.0,
            )
            best_ask_yes_size = max(
                _float_or_default(bbo.get("best_ask_yes_size")),
                0.0,
            )
            snapshot = opportunity_from_prices(
                market_id=market_id,
                fair_yes_prob=float(fair_value["fair_yes_prob"]),
                best_bid_yes=float(bbo["best_bid_yes"]),
                best_ask_yes=float(bbo["best_ask_yes"]),
                fillable_size=min(best_bid_yes_size, best_ask_yes_size),
                buy_yes_fillable_size=best_ask_yes_size,
                sell_yes_fillable_size=best_bid_yes_size,
                confidence=confidence,
                fee_bps=args.fee_bps,
                slippage_bps=args.slippage_bps,
                blocked_reasons=tuple(blocked_reasons),
            )
            opportunity = OpportunityRecord(
                market_id=snapshot.market_id,
                as_of=_utc_now().isoformat(),
                side=snapshot.side,
                fair_yes_prob=snapshot.fair_yes_prob,
                best_bid_yes=snapshot.best_bid_yes,
                best_ask_yes=snapshot.best_ask_yes,
                edge_buy_bps=snapshot.edge_buy_bps,
                edge_sell_bps=snapshot.edge_sell_bps,
                edge_buy_after_costs_bps=snapshot.edge_buy_after_costs_bps,
                edge_sell_after_costs_bps=snapshot.edge_sell_after_costs_bps,
                edge_after_costs_bps=snapshot.edge_after_costs_bps,
                fillable_size=snapshot.fillable_size,
                confidence=snapshot.confidence,
                blocked_reasons=snapshot.blocked_reasons,
                blocked_reason=snapshot.blocked_reason,
                fair_value_ref=str(fair_value["as_of"]),
            )
        stores["opportunities"].append(opportunity)
        payload = opportunity.__dict__.copy()
        current_snapshots[f"{opportunity.market_id}|{opportunity.side}"] = payload
        opportunities.append(opportunity)
        ranked_snapshots.append(snapshot)
        materialized.append(payload)

    ranked = rank_opportunities(ranked_snapshots)
    stores["current"].write_table("opportunities", current_snapshots)
    stores["parquet"].append_records(
        "opportunities",
        _utc_now(),
        materialized,
    )
    stores["health"].upsert("opportunities", stale_after_ms=60_000, status="ok")
    structured_log(
        logger,
        action="build",
        status="ok",
        message="built executable opportunities",
        trace_id=trace_id,
    )
    metrics.record(
        component="opportunity.build",
        action="build",
        status="ok",
        trace_id=trace_id,
        opportunity_count=len(materialized),
        ranked_count=len(ranked),
    )
    emit_json(
        {
            "opportunity_count": len(materialized),
            "ranked_market_ids": [item.market_id for item in ranked[:10]],
            "root": args.root,
        },
        quiet=args.quiet,
    )
    return 0


def _legacy_main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.input is not None:
        payload = json.loads(Path(args.input).read_text())
    else:
        payload = _load_live_payload(args)
    if args.layer == "sports-inputs":
        envelope = build_sports_input_capture(payload, source=args.layer)
        path = write_sports_input_capture(envelope, args.output)
    else:
        envelope = build_polymarket_capture(payload, layer=args.layer)
        path = write_polymarket_capture(envelope, args.output)
    emit_json({"output": str(path), "layer": args.layer}, quiet=args.quiet)
    return 0


def main(argv: list[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    commands = {
        "polymarket-markets",
        "polymarket-bbo",
        "sportsbook-odds",
        "build-mappings",
        "build-fair-values",
        "build-inference-dataset",
        "build-training-dataset",
        "build-opportunities",
    }
    if args_list and args_list[0] in commands:
        args = build_new_parser().parse_args(args_list)
        if args.command == "polymarket-markets":
            return _run_polymarket_markets(args)
        if args.command == "polymarket-bbo":
            return _run_polymarket_bbo(args)
        if args.command == "sportsbook-odds":
            return _run_sportsbook_odds(args)
        if args.command == "build-mappings":
            return _run_build_mappings(args)
        if args.command == "build-fair-values":
            return _run_build_fair_values(args)
        if args.command == "build-inference-dataset":
            return _run_build_inference_dataset(args)
        if args.command == "build-training-dataset":
            return _run_build_training_dataset(args)
        if args.command == "build-opportunities":
            return _run_build_opportunities(args)
        raise RuntimeError(f"unsupported command: {args.command}")
    return _legacy_main(args_list)


if __name__ == "__main__":
    raise SystemExit(main())

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
from forecasting import FairValueEngine
from opportunity import opportunity_from_prices, rank_opportunities
from risk.freeze_windows import FreezeWindowPolicy, freeze_reasons_for_state
from research.data.capture_polymarket import (
    build_polymarket_capture,
    write_polymarket_capture,
)
from research.data.capture_sports_inputs import (
    build_sports_input_capture,
    write_sports_input_capture,
)
from research.data.odds_api import (
    fetch_odds_payload,
    load_event_map,
    normalize_odds_events,
)
from research.features import map_contract_candidate, semantics_from_market_type
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
    sb_odds.add_argument("--sport", required=True)
    sb_odds.add_argument("--market", required=True)
    sb_odds.add_argument("--root", default="runtime/data")
    sb_odds.add_argument("--event-map-file", default=None)
    sb_odds.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    add_quiet_flag(sb_odds)

    mappings = subparsers.add_parser("build-mappings")
    mappings.add_argument("--market", required=True)
    mappings.add_argument("--root", default="runtime/data")
    add_quiet_flag(mappings)

    fair_values = subparsers.add_parser("build-fair-values")
    fair_values.add_argument("--root", default="runtime/data")
    fair_values.add_argument("--model-name", default="deterministic_consensus")
    fair_values.add_argument("--model-version", default="v1")
    fair_values.add_argument("--consensus-artifact", default=None)
    add_quiet_flag(fair_values)

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


def _has_upstream_event_identity(payload: dict[str, object]) -> bool:
    return any(
        payload.get(field) not in (None, "") for field in ("event_key", "game_id")
    )


def _build_fair_value_engine(args) -> FairValueEngine:
    if args.consensus_artifact in (None, ""):
        return FairValueEngine(
            model_name=args.model_name,
            model_version=args.model_version,
        )
    artifact = load_book_consensus_artifact(args.consensus_artifact)
    return FairValueEngine(
        model_name=artifact.model,
        model_version=artifact.model_version,
        half_life_seconds=artifact.half_life_seconds,
    )


def _artifact_configured(args) -> bool:
    return args.consensus_artifact not in (None, "")


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
    api_key = os.getenv(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"missing required environment variable: {args.api_key_env}")
    client = TheOddsApiClient(api_key=api_key)
    events = client.fetch_upcoming(args.sport, args.market)
    event_map = load_event_map(args.event_map_file)
    normalized_rows: list[dict] = []
    for event in events:
        event["sport_key"] = args.sport
        event_identity = event_map.get(str(event.get("id") or ""), {})
        normalized_rows.extend(
            normalize_odds_event(
                event,
                source="theoddsapi",
                market_type=args.market,
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
            sport=args.sport,
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
    markets = _sorted_rows(stores["markets"].read_all())
    events = stores["sb_events"].read_all()
    mappings: list[dict] = []
    current_mappings: dict[str, dict] = {}
    for market in markets:
        best_match: MarketMappingRecord | None = None
        market_payload = _mapping_market_payload(market)
        pm_market_type = str(
            market_payload.get("sportsMarketType")
            or market_payload.get("sports_market_type")
            or args.market
        )
        for event in events.values():
            event_payload = _mapping_event_payload(event)
            decision = map_contract_candidate(
                market_payload,
                event_payload,
                sportsbook_market_type=args.market,
                pm_semantics=semantics_from_market_type(
                    pm_market_type,
                    source="polymarket",
                ),
                sb_semantics=semantics_from_market_type(
                    args.market,
                    source="sportsbook",
                ),
            )
            blocked_reason = decision.blocked_reason
            if blocked_reason is None and not _has_upstream_event_identity(
                event_payload
            ):
                blocked_reason = "missing upstream event identity"
            candidate = MarketMappingRecord(
                polymarket_market_id=decision.polymarket_market_id,
                sportsbook_event_id=decision.sportsbook_event_id,
                sportsbook_market_type=decision.sportsbook_market_type,
                normalized_market_type=decision.normalized_market_type,
                match_confidence=(
                    min(decision.match_confidence, 0.59)
                    if blocked_reason is not None
                    else decision.match_confidence
                ),
                resolution_risk=decision.resolution_risk,
                mismatch_reason=blocked_reason,
                event_key=decision.event_key,
                sport=decision.sport,
                series=decision.series,
                game_id=decision.game_id,
                blocked_reason=blocked_reason,
                is_active=blocked_reason is None,
            )
            if best_match is None or mapping_priority(
                candidate.__dict__
            ) > mapping_priority(best_match.__dict__):
                best_match = candidate
        if best_match is None:
            continue
        stores["mappings"].append(best_match)
        best_match_payload = best_match.__dict__.copy()
        mappings.append(best_match_payload)
        current_mappings[
            f"{best_match.polymarket_market_id}|{best_match.sportsbook_event_id}"
        ] = best_match_payload
    stores["parquet"].append_records("market_mapping_history", _utc_now(), mappings)
    stores["current"].write_table("market_mappings", current_mappings)
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
        _best_effort(
            lambda: stores["health"].upsert(
                "fair_values",
                stale_after_ms=60_000,
                status="ok",
                details={
                    "model_name": engine.model_name,
                    "model_version": engine.model_version,
                    "consensus_artifact_configured": _artifact_configured(args),
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
    materialized: list[dict] = []
    for mapping in mapping_rows:
        market_id = str(mapping.get("polymarket_market_id") or "")
        fair_value = fair_values.get(market_id)
        bbo = bbo_rows.get(market_id)
        blocked_reason = mapping.get("blocked_reason") or mapping.get("mismatch_reason")
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
        if blocked_reason is None and freeze_reasons:
            blocked_reason = freeze_reasons[0]
        if fair_value is None:
            blocked_reason = blocked_reason or "missing fair value"
        if bbo is None:
            blocked_reason = blocked_reason or "missing executable bbo"

        if fair_value is None or bbo is None:
            opportunity = OpportunityRecord(
                market_id=market_id,
                as_of=_utc_now().isoformat(),
                side="buy_yes",
                edge_after_costs_bps=0.0,
                fillable_size=0.0,
                confidence=_float_or_default(mapping.get("match_confidence")),
                blocked_reason=str(blocked_reason),
                fair_value_ref=str(fair_value.get("as_of"))
                if fair_value
                else _utc_now().isoformat(),
            )
        else:
            fillable_size = min(
                float(bbo.get("best_bid_yes_size") or 0.0),
                float(bbo.get("best_ask_yes_size") or 0.0),
            )
            snapshot = opportunity_from_prices(
                market_id=market_id,
                fair_yes_prob=float(fair_value["fair_yes_prob"]),
                best_bid_yes=float(bbo["best_bid_yes"]),
                best_ask_yes=float(bbo["best_ask_yes"]),
                fillable_size=max(fillable_size, 0.0),
                confidence=_float_or_default(mapping.get("match_confidence")),
                fee_bps=args.fee_bps,
                slippage_bps=args.slippage_bps,
                blocked_reason=str(blocked_reason) if blocked_reason else None,
            )
            opportunity = OpportunityRecord(
                market_id=snapshot.market_id,
                as_of=_utc_now().isoformat(),
                side=snapshot.side,
                edge_after_costs_bps=snapshot.edge_after_costs_bps,
                fillable_size=snapshot.fillable_size,
                confidence=snapshot.confidence,
                blocked_reason=snapshot.blocked_reason,
                fair_value_ref=str(fair_value["as_of"]),
            )
        stores["opportunities"].append(opportunity)
        payload = opportunity.__dict__.copy()
        stores["current"].upsert(
            "opportunities",
            f"{opportunity.market_id}|{opportunity.side}",
            payload,
        )
        opportunities.append(opportunity)
        materialized.append(payload)

    ranked = rank_opportunities(
        [
            opportunity_from_prices(
                market_id=row.market_id,
                fair_yes_prob=float(fair_values[row.market_id]["fair_yes_prob"])
                if row.market_id in fair_values
                else 0.0,
                best_bid_yes=float(bbo_rows[row.market_id]["best_bid_yes"])
                if row.market_id in bbo_rows
                and bbo_rows[row.market_id].get("best_bid_yes") is not None
                else 0.0,
                best_ask_yes=float(bbo_rows[row.market_id]["best_ask_yes"])
                if row.market_id in bbo_rows
                and bbo_rows[row.market_id].get("best_ask_yes") is not None
                else 0.0,
                fillable_size=row.fillable_size,
                confidence=row.confidence,
                blocked_reason=row.blocked_reason,
                fee_bps=args.fee_bps,
                slippage_bps=args.slippage_bps,
            )
            for row in opportunities
            if row.market_id in fair_values and row.market_id in bbo_rows
        ]
    )
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
        if args.command == "build-opportunities":
            return _run_build_opportunities(args)
        raise RuntimeError(f"unsupported command: {args.command}")
    return _legacy_main(args_list)


if __name__ == "__main__":
    raise SystemExit(main())

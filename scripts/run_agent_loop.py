from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    Venue,
    deserialize_balance_snapshot,
    deserialize_fill_snapshot,
    deserialize_normalized_order,
    deserialize_position_snapshot,
)
from engine import OrderLifecycleManager, OrderLifecyclePolicy
from engine.cli_output import add_quiet_flag, emit_json
from engine.config_loader import load_config_file, nested_config_value
from engine.discovery import (
    AgentOrchestrator,
    DeterministicSizer,
    ExecutionPolicyGate,
    OpportunityRanker,
    PairOpportunityRanker,
    PollingAgentLoop,
    PollingLoopConfig,
)
from engine.fair_value_loader import (
    FairValueLookup,
    ReloadingFairValueProvider,
    build_fair_value_provider,
)
from engine.runtime_bootstrap import build_adapter, build_current_state_read_adapter
from engine.runtime_bootstrap import parse_comma_separated as _parse_comma_separated
from engine.runtime_metrics import RuntimeMetricsCollector, RuntimeProposalJournal
from engine.runtime_policy import load_runtime_policy
from engine.runner import TradingEngine
from engine.strategies import FairValueBandStrategy
from forecasting.fair_value_engine import ManifestFairValueProvider
from forecasting.fair_value_engine import FairValueManifestEntry
from risk.kill_switch import (
    KillSwitchState,
    build_kill_switch_state,
    format_kill_switch_reason,
)
from risk.limits import RiskEngine, RiskLimits
from storage.current_projection import build_preview_runtime_context
from storage.current_selection import best_mapping_by_market
from storage.journal import EventJournal


def _required_env_vars(venue_name: str) -> list[str]:
    if venue_name == "polymarket":
        return ["POLYMARKET_PRIVATE_KEY"]
    if venue_name == "kalshi":
        return ["KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"]
    raise ValueError(f"unsupported venue: {venue_name}")


def _runtime_requires_postgres_authority(args) -> bool:
    return getattr(args, "mode", None) in {"run", "pair-run"}


def validate_runtime(args) -> None:
    if args.venue in (None, ""):
        raise RuntimeError("venue must be provided")
    if _runtime_requires_postgres_authority(args):
        if getattr(args, "opportunity_root", None) in (None, ""):
            raise RuntimeError(
                "opportunity root must be provided for run and pair-run modes"
            )
    else:
        if args.fair_values_file in (None, ""):
            raise RuntimeError("fair values file must be provided")
        fair_values_path = Path(args.fair_values_file)
        if not fair_values_path.exists():
            raise RuntimeError(f"fair values file not found: {fair_values_path}")

    policy_file = getattr(args, "policy_file", None)
    if policy_file:
        policy_path = Path(policy_file)
        if not policy_path.exists():
            raise RuntimeError(f"policy file not found: {policy_path}")

    missing_env_vars = [
        name for name in _required_env_vars(args.venue) if not os.getenv(name)
    ]
    if missing_env_vars:
        raise RuntimeError(
            "missing required environment variables: " + ", ".join(missing_env_vars)
        )

    if args.venue == "kalshi":
        private_key_path = Path(os.getenv("KALSHI_PRIVATE_KEY_PATH", ""))
        if not private_key_path.exists():
            raise RuntimeError(f"Kalshi private key file not found: {private_key_path}")

    if _runtime_requires_postgres_authority(args):
        build_current_state_read_adapter(args.opportunity_root, require_postgres=True)

    journal_path = Path(args.journal)
    state_path = Path(args.state_file)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the prediction-market polling loop"
    )
    parser.add_argument("--venue", choices=["polymarket", "kalshi"], default=None)
    parser.add_argument("--config-file", default=None)
    parser.add_argument(
        "--mode",
        choices=["preview", "run", "pair-preview", "pair-run"],
        default="preview",
    )
    parser.add_argument("--fair-values-file", default=None)
    parser.add_argument("--policy-file", default=None)
    parser.add_argument("--market-limit", type=int, default=100)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--journal", default="runtime/events.jsonl")
    parser.add_argument("--state-file", default="runtime/safety-state.json")
    parser.add_argument("--quantity", type=float, default=1.0)
    parser.add_argument("--edge-threshold", type=float, default=0.03)
    parser.add_argument("--taker-fee-rate", type=float, default=0.0)
    parser.add_argument("--max-fair-value-age-seconds", type=float, default=None)
    parser.add_argument("--fair-values-reload-seconds", type=float, default=None)
    parser.add_argument("--max-contracts-per-market", type=int, default=10)
    parser.add_argument("--max-global-contracts", type=int, default=20)
    parser.add_argument("--categories", default=None)
    parser.add_argument("--min-volume", type=float, default=None)
    parser.add_argument("--max-spread", type=float, default=None)
    parser.add_argument("--min-hours-to-expiry", type=float, default=None)
    parser.add_argument("--max-hours-to-expiry", type=float, default=None)
    parser.add_argument("--polymarket-live-user-markets", default=None)
    parser.add_argument("--polymarket-user-ws-host", default=None)
    parser.add_argument("--opportunity-root", default=None)
    add_quiet_flag(parser)
    return parser


def _seed_event_exposure_registry(risk_engine: RiskEngine, provider: object) -> None:
    if not isinstance(provider, ManifestFairValueProvider):
        return
    for market_key, record in provider.records.items():
        risk_engine.register_market_event(
            market_key,
            event_key=record.event_key,
            sport=record.sport,
            series=record.series,
            game_id=record.game_id,
        )


def _metrics_collector(args) -> RuntimeMetricsCollector:
    root = (
        Path(args.opportunity_root) / "current"
        if args.opportunity_root
        else Path("runtime/data/current")
    )
    return RuntimeMetricsCollector(root / "runtime_metrics.json")


def _proposal_journal(args) -> RuntimeProposalJournal | None:
    if not args.opportunity_root:
        return None
    root = Path(args.opportunity_root) / "current"
    return RuntimeProposalJournal(root / "preview_order_context.json")


def _parse_runtime_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _required_probability(
    value: object,
    *,
    market_id: str,
    field_name: str,
) -> float:
    if value in (None, ""):
        raise RuntimeError(
            f"projected fair value row for {market_id} is missing {field_name}"
        )
    if not isinstance(value, (int, float, str)) or isinstance(value, bool):
        raise RuntimeError(
            f"projected fair value row for {market_id} has invalid {field_name}"
        )
    try:
        probability = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"projected fair value row for {market_id} has invalid {field_name}"
        ) from exc
    if probability < 0.0 or probability > 1.0:
        raise RuntimeError(
            f"projected fair value row for {market_id} has out-of-range {field_name}"
        )
    return probability


def _optional_probability(
    value: object,
    *,
    market_id: str,
    field_name: str,
) -> float | None:
    if value in (None, ""):
        return None
    return _required_probability(value, market_id=market_id, field_name=field_name)


def _source_health_issue(
    row: dict[str, Any] | None,
    *,
    source_name: str,
    now: datetime,
) -> str | None:
    if row is None:
        return f"{source_name} health row missing"
    status = str(row.get("status") or "")
    if status != "ok":
        return f"{source_name} unhealthy: {status}"
    stale_after_ms = row.get("stale_after_ms")
    last_seen_at = row.get("last_seen_at") or row.get("last_success_at")
    parsed_seen = _parse_runtime_timestamp(last_seen_at)
    if parsed_seen is None:
        return f"{source_name} missing timestamp"
    if isinstance(stale_after_ms, bool) or not isinstance(
        stale_after_ms, (int, float, str)
    ):
        stale_after = 0
    else:
        try:
            stale_after = int(stale_after_ms)
        except (TypeError, ValueError):
            stale_after = 0
    if stale_after > 0 and (now - parsed_seen).total_seconds() * 1000 > stale_after:
        return f"{source_name} stale"
    return None


def _projected_condition_id(row: dict[str, object]) -> str | None:
    raw_market = row.get("raw_json")
    condition_id = row.get("condition_id")
    if condition_id in (None, "") and isinstance(raw_market, dict):
        condition_id = raw_market.get("conditionId") or raw_market.get("condition_id")
    if condition_id in (None, ""):
        return None
    return str(condition_id)


def _projected_token_ids(row: dict[str, object]) -> tuple[str | None, str | None]:
    raw_market = row.get("raw_json")
    token_id_yes = row.get("token_id_yes")
    token_id_no = row.get("token_id_no")
    if isinstance(raw_market, dict):
        token_ids = raw_market.get("tokenIds")
        if isinstance(token_ids, list):
            if token_id_yes in (None, "") and len(token_ids) >= 1:
                token_id_yes = token_ids[0]
            if token_id_no in (None, "") and len(token_ids) >= 2:
                token_id_no = token_ids[1]
    return (
        None if token_id_yes in (None, "") else str(token_id_yes),
        None if token_id_no in (None, "") else str(token_id_no),
    )


def _build_projected_fair_value_provider(
    args,
    *,
    fair_value_field,
) -> ManifestFairValueProvider:
    read_adapter = build_current_state_read_adapter(
        args.opportunity_root,
        require_postgres=True,
    )
    if read_adapter is None:
        raise RuntimeError(
            "runtime projected current-state reads require opportunity root"
        )
    fair_values = read_adapter.read_table("fair_values")
    mappings = best_mapping_by_market(read_adapter.read_table("market_mappings"))
    markets = read_adapter.read_table("polymarket_markets")
    records: dict[str, FairValueManifestEntry] = {}
    generated_at: datetime | None = None

    for market_id, payload in fair_values.items():
        if not isinstance(payload, dict):
            continue
        market_key = str(market_id)
        market_row = markets.get(market_key)
        if not isinstance(market_row, dict):
            continue
        mapping = mappings.get(market_key, {})
        condition_id = _projected_condition_id(market_row)
        token_id_yes, token_id_no = _projected_token_ids(market_row)
        record_generated_at = _parse_runtime_timestamp(payload.get("as_of"))
        if record_generated_at is not None and (
            generated_at is None or record_generated_at > generated_at
        ):
            generated_at = record_generated_at
        fair_yes_prob = _required_probability(
            payload.get("fair_yes_prob"),
            market_id=market_key,
            field_name="fair_yes_prob",
        )
        calibrated_yes_prob = payload.get("calibrated_fair_yes_prob")
        calibrated_yes_value = (
            _required_probability(
                calibrated_yes_prob,
                market_id=market_key,
                field_name="calibrated_fair_yes_prob",
            )
            if fair_value_field == "calibrated"
            else _optional_probability(
                calibrated_yes_prob,
                market_id=market_key,
                field_name="calibrated_fair_yes_prob",
            )
        )

        def _record_for_probability(
            probability: float, calibrated_probability: float | None
        ):
            return FairValueManifestEntry(
                fair_value=probability,
                calibrated_fair_value=calibrated_probability,
                generated_at=record_generated_at,
                source="projected-current-state",
                condition_id=condition_id,
                event_key=(
                    str(mapping.get("event_key"))
                    if mapping.get("event_key") not in (None, "")
                    else None
                ),
                sport=(
                    str(mapping.get("sport"))
                    if mapping.get("sport") not in (None, "")
                    else None
                ),
                series=(
                    str(mapping.get("series"))
                    if mapping.get("series") not in (None, "")
                    else None
                ),
                game_id=(
                    str(mapping.get("game_id"))
                    if mapping.get("game_id") not in (None, "")
                    else None
                ),
                sports_market_type=(
                    str(mapping.get("normalized_market_type"))
                    if mapping.get("normalized_market_type") not in (None, "")
                    else None
                ),
            )

        yes_entry = _record_for_probability(fair_yes_prob, calibrated_yes_value)
        no_entry = _record_for_probability(
            max(0.0, min(1.0, 1.0 - fair_yes_prob)),
            None
            if calibrated_yes_value is None
            else max(0.0, min(1.0, 1.0 - calibrated_yes_value)),
        )

        if market_key:
            records[f"{market_key}:yes"] = yes_entry
            records[f"{market_key}:no"] = no_entry
        if condition_id is not None:
            records[f"{condition_id}:yes"] = yes_entry
            records[f"{condition_id}:no"] = no_entry
        if token_id_yes is not None:
            records[f"{token_id_yes}:yes"] = yes_entry
        if token_id_no is not None:
            records[f"{token_id_no}:no"] = no_entry

    return ManifestFairValueProvider(
        records=records,
        generated_at=generated_at,
        source="projected-current-state",
        max_age_seconds=args.max_fair_value_age_seconds,
        fair_value_field=fair_value_field,
    )


def _build_projected_account_snapshot_provider(args):
    def _provider(contract: Contract | None) -> AccountSnapshot:
        read_adapter = build_current_state_read_adapter(
            args.opportunity_root,
            require_postgres=True,
        )
        if read_adapter is None:
            raise RuntimeError(
                "runtime projected account-truth reads require opportunity root"
            )
        source_health = read_adapter.read_table("source_health")
        account_health = source_health.get("projection_polymarket_user_channel")
        capture_health = source_health.get("polymarket_user_channel")
        issues: list[str] = []
        now = datetime.now(timezone.utc)
        observed_at = now
        if isinstance(account_health, dict):
            observed_at = (
                _parse_runtime_timestamp(
                    account_health.get("last_success_at")
                    or account_health.get("last_seen_at")
                )
                or observed_at
            )
        projection_issue = _source_health_issue(
            cast(dict[str, Any] | None, account_health)
            if isinstance(account_health, dict)
            else None,
            source_name="projection_polymarket_user_channel",
            now=now,
        )
        if projection_issue is not None:
            issues.append(projection_issue)
        capture_issue = _source_health_issue(
            cast(dict[str, Any] | None, capture_health)
            if isinstance(capture_health, dict)
            else None,
            source_name="polymarket_user_channel",
            now=now,
        )
        if capture_issue is not None:
            issues.append(capture_issue)
        if isinstance(capture_health, dict):
            details = capture_health.get("details")
            if isinstance(details, dict):
                if not bool(details.get("account_snapshot", False)):
                    issues.append(
                        "projected account truth missing captured account snapshot"
                    )
                if not bool(details.get("account_snapshot_complete", True)):
                    issues.append("projected account truth incomplete at capture")
                issue_list = details.get("account_snapshot_issues")
                if isinstance(issue_list, list):
                    issues.extend(str(item) for item in issue_list if str(item))

        balance_rows = read_adapter.read_table("polymarket_balance")
        if not balance_rows:
            issues.append("projected account truth missing balance snapshot")
            balance = BalanceSnapshot(venue=Venue.POLYMARKET, available=0.0, total=0.0)
        else:
            balance_payload = next(iter(balance_rows.values()))
            if not isinstance(balance_payload, dict):
                raise RuntimeError("projected account balance row invalid")
            balance = deserialize_balance_snapshot(balance_payload)

        orders = [
            deserialize_normalized_order(payload)
            for payload in read_adapter.read_table("polymarket_orders").values()
            if isinstance(payload, dict)
        ]
        positions = [
            deserialize_position_snapshot(payload)
            for payload in read_adapter.read_table("polymarket_positions").values()
            if isinstance(payload, dict)
        ]
        fills = [
            deserialize_fill_snapshot(payload)
            for payload in read_adapter.read_table("polymarket_fills").values()
            if isinstance(payload, dict)
        ]

        cohort_ids = {
            str(payload.get("snapshot_cohort_id") or "")
            for table_rows in (
                balance_rows.values(),
                read_adapter.read_table("polymarket_orders").values(),
                read_adapter.read_table("polymarket_positions").values(),
                read_adapter.read_table("polymarket_fills").values(),
            )
            for payload in table_rows
            if isinstance(payload, dict)
            and payload.get("snapshot_cohort_id") not in (None, "")
        }
        if len(cohort_ids) > 1:
            issues.append("projected account truth spans multiple snapshot cohorts")

        snapshot_observed_candidates = [
            _parse_runtime_timestamp(payload.get("snapshot_observed_at"))
            for table_rows in (
                balance_rows.values(),
                read_adapter.read_table("polymarket_orders").values(),
                read_adapter.read_table("polymarket_positions").values(),
                read_adapter.read_table("polymarket_fills").values(),
            )
            for payload in table_rows
            if isinstance(payload, dict)
        ]
        snapshot_observed_candidates = [
            candidate
            for candidate in snapshot_observed_candidates
            if candidate is not None
        ]
        if snapshot_observed_candidates:
            unique_observed = {
                candidate.isoformat() for candidate in snapshot_observed_candidates
            }
            if len(unique_observed) > 1:
                issues.append(
                    "projected account truth spans multiple snapshot timestamps"
                )
            observed_at = max(snapshot_observed_candidates)

        if contract is not None:
            orders = [
                order for order in orders if order.contract.symbol == contract.symbol
            ]
            positions = [
                position
                for position in positions
                if position.contract.symbol == contract.symbol
            ]
            fills = [fill for fill in fills if fill.contract.symbol == contract.symbol]

        return AccountSnapshot(
            venue=balance.venue,
            balance=balance,
            positions=positions,
            open_orders=orders,
            fills=fills,
            complete=not issues,
            issues=issues,
            observed_at=observed_at,
        )

    return _provider


def _build_preview_order_proposals(
    args,
    policy,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    read_adapter_kwargs = (
        {"require_postgres": True} if _runtime_requires_postgres_authority(args) else {}
    )
    read_adapter = build_current_state_read_adapter(
        args.opportunity_root,
        **read_adapter_kwargs,
    )
    context = build_preview_runtime_context(
        args.opportunity_root,
        policy=policy,
        read_adapter=read_adapter,
    )
    return list(context.preview_order_proposals), list(context.blocked_preview_orders)


def _build_runtime_kill_switch(args) -> tuple[KillSwitchState, tuple[str, ...]]:
    read_adapter_kwargs = (
        {"require_postgres": True} if _runtime_requires_postgres_authority(args) else {}
    )
    read_adapter = build_current_state_read_adapter(
        args.opportunity_root,
        **read_adapter_kwargs,
    )
    source_health = (
        read_adapter.read_table("source_health") if read_adapter is not None else {}
    )
    state = build_kill_switch_state(source_health=source_health)
    return state, state.reasons()


def _apply_runtime_kill_switch(engine, reason: str) -> None:
    halter = getattr(engine, "halt", None)
    if callable(halter):
        halter(reason)
        return
    safety_state = getattr(engine, "safety_state", None)
    if safety_state is None:
        raise AttributeError("engine must expose halt() or safety_state")
    setattr(safety_state, "halted", True)
    setattr(safety_state, "reason", reason)


def main() -> int:
    args = build_parser().parse_args()
    config = load_config_file(args.config_file) if args.config_file else {}
    configured_venue = config.get("venue")
    if args.venue is None and isinstance(configured_venue, str):
        args.venue = configured_venue
    configured_fair_values_file = nested_config_value(
        config, "runtime", "fair_values_file"
    )
    if args.fair_values_file is None and isinstance(configured_fair_values_file, str):
        args.fair_values_file = configured_fair_values_file
    configured_policy_file = nested_config_value(config, "runtime", "policy_file")
    if args.policy_file is None and isinstance(configured_policy_file, str):
        args.policy_file = configured_policy_file
    configured_opportunity_root = nested_config_value(
        config, "runtime", "opportunity_root"
    )
    if args.opportunity_root is None and isinstance(configured_opportunity_root, str):
        args.opportunity_root = configured_opportunity_root
    configured_interval_seconds = nested_config_value(
        config, "runtime", "interval_seconds"
    )
    if isinstance(configured_interval_seconds, (int, float)):
        args.interval_seconds = float(configured_interval_seconds)
    configured_max_cycles = nested_config_value(config, "runtime", "max_cycles")
    if isinstance(configured_max_cycles, int):
        args.max_cycles = configured_max_cycles
    configured_max_fair_value_age = nested_config_value(
        config, "runtime", "max_fair_value_age_seconds"
    )
    if isinstance(configured_max_fair_value_age, (int, float)):
        args.max_fair_value_age_seconds = float(configured_max_fair_value_age)
    configured_reload_seconds = nested_config_value(
        config, "runtime", "fair_values_reload_seconds"
    )
    if isinstance(configured_reload_seconds, (int, float)):
        args.fair_values_reload_seconds = float(configured_reload_seconds)
    configured_preview_only = nested_config_value(config, "runtime", "preview_only")
    if args.mode == "preview" and isinstance(configured_preview_only, bool):
        args.mode = "preview" if configured_preview_only else "run"
    validate_runtime(args)
    policy = load_runtime_policy(args.policy_file) if args.policy_file else None
    adapter = build_adapter(args.venue, args, policy=policy)
    try:
        fair_value_field = policy.fair_value.field if policy is not None else "raw"

        def loader() -> FairValueLookup:
            if _runtime_requires_postgres_authority(args):
                return cast(
                    FairValueLookup,
                    _build_projected_fair_value_provider(
                        args,
                        fair_value_field=fair_value_field,
                    ),
                )
            return cast(
                FairValueLookup,
                build_fair_value_provider(
                    args.fair_values_file,
                    max_age_seconds=args.max_fair_value_age_seconds,
                    fair_value_field=fair_value_field,
                ),
            )

        provider = loader()
        seeded_provider = provider
        if args.fair_values_reload_seconds is not None:
            provider = ReloadingFairValueProvider(
                loader,
                reload_interval_seconds=args.fair_values_reload_seconds,
            )
        if policy is None:
            categories = _parse_comma_separated(args.categories)
            ranker = OpportunityRanker(
                edge_threshold=args.edge_threshold,
                taker_fee_rate=args.taker_fee_rate,
                allowed_categories=tuple(categories) if categories else None,
                min_volume=args.min_volume,
                max_spread=args.max_spread,
                min_hours_to_expiry=args.min_hours_to_expiry,
                max_hours_to_expiry=args.max_hours_to_expiry,
            )
            pair_ranker = PairOpportunityRanker(
                edge_threshold=args.edge_threshold,
                taker_fee_rate=args.taker_fee_rate,
                allowed_categories=tuple(categories) if categories else None,
                min_volume=args.min_volume,
                max_spread=args.max_spread,
                min_hours_to_expiry=args.min_hours_to_expiry,
                max_hours_to_expiry=args.max_hours_to_expiry,
            )
            strategy = FairValueBandStrategy(
                quantity=args.quantity,
                edge_threshold=args.edge_threshold,
            )
            risk_engine = RiskEngine(
                RiskLimits(
                    max_contracts_per_market=args.max_contracts_per_market,
                    max_global_contracts=args.max_global_contracts,
                )
            )
            policy_gate = ExecutionPolicyGate()
            sizer = DeterministicSizer()
            trading_engine_policy = None
            lifecycle_policy = OrderLifecyclePolicy()
            pair_quantity = args.quantity
        else:
            ranker = cast(OpportunityRanker, policy.opportunity_ranker.build())
            pair_ranker = cast(
                PairOpportunityRanker,
                policy.pair_opportunity_ranker.build(),
            )
            strategy = policy.strategy.build_strategy()
            risk_engine = RiskEngine(policy.risk_limits.build())
            policy_gate = policy.execution_policy_gate.build()
            sizer = policy.strategy.build_sizer()
            trading_engine_policy = policy.trading_engine
            lifecycle_policy = policy.order_lifecycle_policy.build()
            pair_quantity = policy.strategy.base_quantity

        _seed_event_exposure_registry(risk_engine, seeded_provider)
        account_snapshot_provider = (
            _build_projected_account_snapshot_provider(args)
            if _runtime_requires_postgres_authority(args)
            else None
        )
        if trading_engine_policy is None:
            engine = TradingEngine(
                adapter=adapter,
                strategy=strategy,
                risk_engine=risk_engine,
                safety_state_path=args.state_file,
                account_snapshot_provider=account_snapshot_provider,
            )
        else:
            engine = TradingEngine(
                adapter=adapter,
                strategy=strategy,
                risk_engine=risk_engine,
                safety_state_path=args.state_file,
                account_snapshot_provider=account_snapshot_provider,
                cancel_retry_interval_seconds=(
                    trading_engine_policy.cancel_retry_interval_seconds
                ),
                cancel_retry_max_attempts=trading_engine_policy.cancel_retry_max_attempts,
                cancel_attention_timeout_seconds=(
                    trading_engine_policy.cancel_attention_timeout_seconds
                ),
                overlay_max_age_seconds=trading_engine_policy.overlay_max_age_seconds,
                forced_refresh_debounce_seconds=(
                    trading_engine_policy.forced_refresh_debounce_seconds
                ),
                pending_submission_recovery_seconds=(
                    trading_engine_policy.pending_submission_recovery_seconds
                ),
                pending_submission_expiry_seconds=(
                    trading_engine_policy.pending_submission_expiry_seconds
                ),
            )
        kill_switch_state, kill_switch_reasons = _build_runtime_kill_switch(args)
        kill_switch_reason = format_kill_switch_reason(kill_switch_state)
        if kill_switch_reason is not None:
            _apply_runtime_kill_switch(engine, kill_switch_reason)
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=provider,
            ranker=ranker,
            pair_ranker=pair_ranker,
            policy_gate=policy_gate,
            sizer=sizer,
            journal=EventJournal(args.journal),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode=args.mode,
                market_limit=args.market_limit,
                interval_seconds=args.interval_seconds,
                max_cycles=args.max_cycles,
                quantity=pair_quantity,
            ),
            lifecycle_manager=OrderLifecycleManager(
                adapter=adapter,
                policy=lifecycle_policy,
                cancel_handler=getattr(engine, "request_cancel_order", None),
            ),
        )
        results = loop.run()
        metrics = _metrics_collector(args)
        preview_order_proposals, blocked_preview_orders = (
            _build_preview_order_proposals(
                args,
                policy,
            )
        )
        proposal_journal = _proposal_journal(args)
        if proposal_journal is not None:
            proposal_journal.write_preview_snapshot(
                proposals=preview_order_proposals,
                blocked=blocked_preview_orders,
            )
        status_snapshot = getattr(engine, "status_snapshot", None)
        status = status_snapshot() if callable(status_snapshot) else None
        live_state_status_getter = getattr(adapter, "live_state_status", None)
        live_state_status = (
            live_state_status_getter() if callable(live_state_status_getter) else None
        )
        market_state_status_getter = getattr(adapter, "market_state_status", None)
        market_state_status = (
            market_state_status_getter()
            if callable(market_state_status_getter)
            else None
        )
        heartbeat_last_success_at = getattr(status, "heartbeat_last_success_at", None)
        live_fills_last_update_at = getattr(
            live_state_status, "fills_last_update_at", None
        )
        last_live_delta_applied_at = getattr(status, "last_live_delta_applied_at", None)
        last_snapshot_correction_at = getattr(
            status, "last_snapshot_correction_at", None
        )
        overlay_degraded_since = getattr(status, "overlay_degraded_since", None)
        overlay_last_live_event_at = getattr(status, "overlay_last_live_event_at", None)
        overlay_last_confirmed_snapshot_at = getattr(
            status, "overlay_last_confirmed_snapshot_at", None
        )
        overlay_last_recovery_at = getattr(status, "overlay_last_recovery_at", None)
        live_state_last_recovery_at = getattr(
            live_state_status, "last_recovery_at", None
        )
        market_state_last_recovery_at = getattr(
            market_state_status, "last_recovery_at", None
        )
        pending_cancels = list(getattr(status, "pending_cancels", []) or [])

        def _selected_market_key(result) -> str | None:
            selected = getattr(result, "selected", None)
            if selected is None:
                return None
            contract = getattr(selected, "contract", None)
            if contract is not None:
                return getattr(contract, "market_key", None)
            return getattr(selected, "market_key", None)

        emit_json(
            {
                "cycles": len(results),
                "mode": args.mode,
                "last_selected": _selected_market_key(results[-1]) if results else None,
                "engine_halted": engine.safety_state.halted,
                "engine_paused": engine.safety_state.paused,
                "kill_switch_active": bool(kill_switch_reasons),
                "kill_switch_reasons": list(kill_switch_reasons),
                "heartbeat_active": getattr(status, "heartbeat_active", None),
                "heartbeat_running": getattr(status, "heartbeat_running", None),
                "heartbeat_healthy_for_trading": getattr(
                    status, "heartbeat_healthy_for_trading", None
                ),
                "heartbeat_last_success_at": (
                    heartbeat_last_success_at.isoformat()
                    if heartbeat_last_success_at is not None
                    else None
                ),
                "heartbeat_consecutive_failures": getattr(
                    status, "heartbeat_consecutive_failures", None
                ),
                "heartbeat_last_error": getattr(status, "heartbeat_last_error", None),
                "heartbeat_last_id": getattr(status, "heartbeat_last_id", None),
                "pending_cancel_count": len(pending_cancels),
                "pending_cancel_operator_attention_required": any(
                    getattr(item, "operator_attention_required", False)
                    for item in pending_cancels
                ),
                "pending_cancel_post_fill_seen": any(
                    getattr(item, "post_cancel_fill_seen", False)
                    for item in pending_cancels
                ),
                "last_depth_assessment": getattr(status, "last_depth_assessment", None),
                "last_live_delta_applied_at": (
                    last_live_delta_applied_at.isoformat()
                    if last_live_delta_applied_at is not None
                    else None
                ),
                "last_live_delta_source": getattr(
                    status, "last_live_delta_source", None
                ),
                "last_live_delta_order_upserts": getattr(
                    status, "last_live_delta_order_upserts", None
                ),
                "last_live_delta_fill_upserts": getattr(
                    status, "last_live_delta_fill_upserts", None
                ),
                "last_live_delta_terminal_orders": getattr(
                    status, "last_live_delta_terminal_orders", None
                ),
                "last_live_terminal_marker_applied_count": getattr(
                    status, "last_live_terminal_marker_applied_count", None
                ),
                "last_snapshot_correction_at": (
                    last_snapshot_correction_at.isoformat()
                    if last_snapshot_correction_at is not None
                    else None
                ),
                "last_snapshot_correction_order_count": getattr(
                    status, "last_snapshot_correction_order_count", None
                ),
                "last_snapshot_correction_fill_count": getattr(
                    status, "last_snapshot_correction_fill_count", None
                ),
                "last_snapshot_terminal_confirmation_count": getattr(
                    status, "last_snapshot_terminal_confirmation_count", None
                ),
                "last_snapshot_terminal_reversal_count": getattr(
                    status, "last_snapshot_terminal_reversal_count", None
                ),
                "overlay_degraded": getattr(status, "overlay_degraded", None),
                "overlay_degraded_since": (
                    overlay_degraded_since.isoformat()
                    if overlay_degraded_since is not None
                    else None
                ),
                "overlay_degraded_reason": getattr(
                    status, "overlay_degraded_reason", None
                ),
                "overlay_delta_suppressed": getattr(
                    status, "overlay_delta_suppressed", None
                ),
                "overlay_last_live_event_at": (
                    overlay_last_live_event_at.isoformat()
                    if overlay_last_live_event_at is not None
                    else None
                ),
                "overlay_last_confirmed_snapshot_at": (
                    overlay_last_confirmed_snapshot_at.isoformat()
                    if overlay_last_confirmed_snapshot_at is not None
                    else None
                ),
                "overlay_forced_snapshot_count": getattr(
                    status, "overlay_forced_snapshot_count", None
                ),
                "overlay_last_forced_snapshot_scope": getattr(
                    status, "overlay_last_forced_snapshot_scope", None
                ),
                "overlay_last_forced_snapshot_reason": getattr(
                    status, "overlay_last_forced_snapshot_reason", None
                ),
                "overlay_last_recovery_outcome": getattr(
                    status, "overlay_last_recovery_outcome", None
                ),
                "overlay_last_recovery_scope": getattr(
                    status, "overlay_last_recovery_scope", None
                ),
                "overlay_last_recovery_at": (
                    overlay_last_recovery_at.isoformat()
                    if overlay_last_recovery_at is not None
                    else None
                ),
                "overlay_last_suppression_duration_seconds": getattr(
                    status, "overlay_last_suppression_duration_seconds", None
                ),
                "live_state_active": getattr(live_state_status, "active", None),
                "live_state_running": getattr(live_state_status, "running", None),
                "live_state_mode": getattr(live_state_status, "mode", None),
                "live_state_initialized": getattr(
                    live_state_status, "initialized", None
                ),
                "live_state_fresh": getattr(live_state_status, "fresh", None),
                "live_state_degraded_reason": getattr(
                    live_state_status, "degraded_reason", None
                ),
                "live_state_recovery_attempts": getattr(
                    live_state_status, "recovery_attempts", None
                ),
                "live_state_last_recovery_at": (
                    live_state_last_recovery_at.isoformat()
                    if live_state_last_recovery_at is not None
                    else None
                ),
                "live_fills_initialized": getattr(
                    live_state_status, "fills_initialized", None
                ),
                "live_fills_fresh": getattr(live_state_status, "fills_fresh", None),
                "live_fills_last_update_at": (
                    live_fills_last_update_at.isoformat()
                    if live_fills_last_update_at is not None
                    else None
                ),
                "live_cached_fill_count": getattr(
                    live_state_status, "cached_fill_count", None
                ),
                "live_last_fills_source": getattr(
                    live_state_status, "last_fills_source", None
                ),
                "live_last_fills_fallback_reason": getattr(
                    live_state_status, "last_fills_fallback_reason", None
                ),
                "snapshot_open_order_overlay_count": getattr(
                    live_state_status, "snapshot_open_order_overlay_count", None
                ),
                "snapshot_open_order_overlay_source": getattr(
                    live_state_status, "snapshot_open_order_overlay_source", None
                ),
                "snapshot_open_order_overlay_reason": getattr(
                    live_state_status, "snapshot_open_order_overlay_reason", None
                ),
                "snapshot_fill_overlay_count": getattr(
                    live_state_status, "snapshot_fill_overlay_count", None
                ),
                "snapshot_fill_overlay_source": getattr(
                    live_state_status, "snapshot_fill_overlay_source", None
                ),
                "snapshot_fill_overlay_reason": getattr(
                    live_state_status, "snapshot_fill_overlay_reason", None
                ),
                "live_state_last_error": getattr(live_state_status, "last_error", None),
                "live_state_subscribed_markets": list(
                    getattr(live_state_status, "subscribed_markets", ()) or ()
                ),
                "market_state_active": getattr(market_state_status, "active", None),
                "market_state_running": getattr(market_state_status, "running", None),
                "market_state_mode": getattr(market_state_status, "mode", None),
                "market_state_fresh": getattr(market_state_status, "fresh", None),
                "market_state_last_error": getattr(
                    market_state_status, "last_error", None
                ),
                "market_state_degraded_reason": getattr(
                    market_state_status, "degraded_reason", None
                ),
                "market_state_recovery_attempts": getattr(
                    market_state_status, "recovery_attempts", None
                ),
                "market_state_last_recovery_at": (
                    market_state_last_recovery_at.isoformat()
                    if market_state_last_recovery_at is not None
                    else None
                ),
                "market_state_book_overlay_source": getattr(
                    market_state_status, "snapshot_book_overlay_source", None
                ),
                "market_state_book_overlay_reason": getattr(
                    market_state_status, "snapshot_book_overlay_reason", None
                ),
                "market_state_book_overlay_applied": getattr(
                    market_state_status, "snapshot_book_overlay_applied", None
                ),
                "market_state_subscribed_assets": list(
                    getattr(market_state_status, "subscribed_assets", ()) or ()
                ),
                "preview_order_proposal_count": len(preview_order_proposals),
                "preview_order_proposals": preview_order_proposals,
                "preview_order_blocked_count": len(blocked_preview_orders),
                "preview_order_blocked": blocked_preview_orders,
                "shadow_quote_plan": (
                    getattr(results[-1], "shadow_quote_plan", None) if results else None
                ),
            },
            quiet=args.quiet,
        )
        metrics.record(
            component="run_agent_loop",
            action="preview_proposals",
            status="ok",
            trace_id=None,
            cycle_count=len(results),
            preview_order_proposal_count=len(preview_order_proposals),
            preview_order_blocked_count=len(blocked_preview_orders),
        )
        return 0
    finally:
        stop_heartbeat = getattr(adapter, "stop_heartbeat", None)
        if callable(stop_heartbeat):
            stop_heartbeat()
        close = getattr(adapter, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    raise SystemExit(main())

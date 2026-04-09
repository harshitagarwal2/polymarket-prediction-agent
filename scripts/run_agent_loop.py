from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adapters.kalshi import KalshiAdapter, KalshiConfig
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from engine import OrderLifecycleManager, OrderLifecyclePolicy
from engine.discovery import (
    AgentOrchestrator,
    FairValueManifestEntry,
    ManifestFairValueProvider,
    OpportunityRanker,
    PairOpportunityRanker,
    PollingAgentLoop,
    PollingLoopConfig,
    StaticFairValueProvider,
)
from engine.runner import TradingEngine
from engine.safety_store import SafetyStateStore
from engine.strategies import FairValueBandStrategy
from research.storage import EventJournal
from risk.limits import RiskEngine, RiskLimits


def _parse_comma_separated(value: str | None) -> list[str] | None:
    if value in (None, ""):
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


def build_adapter(venue_name: str, args=None):
    if venue_name == "polymarket":
        markets = _parse_comma_separated(
            getattr(args, "polymarket_live_user_markets", None)
            or os.getenv("POLYMARKET_LIVE_USER_MARKETS")
        )
        return PolymarketAdapter(
            PolymarketConfig(
                private_key=os.getenv("POLYMARKET_PRIVATE_KEY"),
                funder=os.getenv("POLYMARKET_FUNDER"),
                account_address=os.getenv("POLYMARKET_ACCOUNT_ADDRESS"),
                user_ws_host=(
                    getattr(args, "polymarket_user_ws_host", None)
                    or os.getenv("POLYMARKET_USER_WS_HOST")
                    or PolymarketConfig.user_ws_host
                ),
                live_user_markets=markets,
            )
        )
    if venue_name == "kalshi":
        return KalshiAdapter(
            KalshiConfig(
                api_key_id=os.getenv("KALSHI_API_KEY_ID"),
                private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH"),
            )
        )
    raise ValueError(f"unsupported venue: {venue_name}")


def load_fair_values(path: str) -> dict[str, float]:
    payload = json.loads(Path(path).read_text())
    return {str(key): float(value) for key, value in payload.items()}


def _parse_fair_value_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def build_fair_value_provider(path: str, *, max_age_seconds: float | None = None):
    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("fair values file must contain a JSON object")

    manifest_values = payload.get("values")
    if isinstance(manifest_values, dict):
        resolved_max_age = max_age_seconds
        if resolved_max_age is None and payload.get("max_age_seconds") not in (
            None,
            "",
        ):
            resolved_max_age = float(payload["max_age_seconds"])

        records: dict[str, FairValueManifestEntry] = {}
        for market_key, item in manifest_values.items():
            if isinstance(item, dict):
                if item.get("fair_value") in (None, ""):
                    raise RuntimeError(
                        f"manifest fair value missing for market key: {market_key}"
                    )
                records[str(market_key)] = FairValueManifestEntry(
                    fair_value=float(item["fair_value"]),
                    generated_at=_parse_fair_value_timestamp(item.get("generated_at")),
                    source=(
                        str(item.get("source"))
                        if item.get("source") not in (None, "")
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
                )
                continue
            records[str(market_key)] = FairValueManifestEntry(fair_value=float(item))

        return ManifestFairValueProvider(
            records=records,
            generated_at=_parse_fair_value_timestamp(payload.get("generated_at")),
            source=(
                str(payload.get("source"))
                if payload.get("source") not in (None, "")
                else None
            ),
            max_age_seconds=resolved_max_age,
        )

    return StaticFairValueProvider(
        {str(key): float(value) for key, value in payload.items()}
    )


class ReloadingFairValueProvider:
    def __init__(
        self,
        loader: Callable[[], object],
        *,
        reload_interval_seconds: float,
    ):
        self.loader = loader
        self.reload_interval_seconds = max(0.0, reload_interval_seconds)
        self._provider = self.loader()
        self._loaded_at = datetime.now(timezone.utc)

    def _refresh_if_due(self) -> None:
        now = datetime.now(timezone.utc)
        age_seconds = (now - self._loaded_at).total_seconds()
        if age_seconds < self.reload_interval_seconds:
            return
        self._provider = self.loader()
        self._loaded_at = now

    def fair_value_for(self, market):
        self._refresh_if_due()
        fair_value_for = getattr(self._provider, "fair_value_for")
        return fair_value_for(market)


def _required_env_vars(venue_name: str) -> list[str]:
    if venue_name == "polymarket":
        return ["POLYMARKET_PRIVATE_KEY"]
    if venue_name == "kalshi":
        return ["KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"]
    raise ValueError(f"unsupported venue: {venue_name}")


def validate_runtime(args) -> None:
    fair_values_path = Path(args.fair_values_file)
    if not fair_values_path.exists():
        raise RuntimeError(f"fair values file not found: {fair_values_path}")

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

    journal_path = Path(args.journal)
    state_path = Path(args.state_file)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.parent.mkdir(parents=True, exist_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the prediction-market polling loop"
    )
    parser.add_argument("--venue", choices=["polymarket", "kalshi"], required=True)
    parser.add_argument(
        "--mode",
        choices=["preview", "run", "pair-preview", "pair-run"],
        default="preview",
    )
    parser.add_argument("--fair-values-file", required=True)
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    validate_runtime(args)
    adapter = build_adapter(args.venue, args)
    try:
        loader = lambda: build_fair_value_provider(
            args.fair_values_file,
            max_age_seconds=args.max_fair_value_age_seconds,
        )
        provider = loader()
        if args.fair_values_reload_seconds is not None:
            provider = ReloadingFairValueProvider(
                loader,
                reload_interval_seconds=args.fair_values_reload_seconds,
            )
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
        engine = TradingEngine(
            adapter=adapter,
            strategy=FairValueBandStrategy(
                quantity=args.quantity, edge_threshold=args.edge_threshold
            ),
            risk_engine=RiskEngine(
                RiskLimits(
                    max_contracts_per_market=args.max_contracts_per_market,
                    max_global_contracts=args.max_global_contracts,
                )
            ),
            safety_state_path=args.state_file,
        )
        orchestrator = AgentOrchestrator(
            adapter=adapter,
            engine=engine,
            fair_value_provider=provider,
            ranker=ranker,
            pair_ranker=pair_ranker,
            journal=EventJournal(args.journal),
        )
        loop = PollingAgentLoop(
            orchestrator=orchestrator,
            config=PollingLoopConfig(
                mode=args.mode,
                market_limit=args.market_limit,
                interval_seconds=args.interval_seconds,
                max_cycles=args.max_cycles,
                quantity=args.quantity,
            ),
            lifecycle_manager=OrderLifecycleManager(
                adapter=adapter,
                policy=OrderLifecyclePolicy(),
                cancel_handler=getattr(engine, "request_cancel_order", None),
            ),
        )
        results = loop.run()
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

        print(
            json.dumps(
                {
                    "cycles": len(results),
                    "mode": args.mode,
                    "last_selected": _selected_market_key(results[-1])
                    if results
                    else None,
                    "engine_halted": engine.safety_state.halted,
                    "engine_paused": engine.safety_state.paused,
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
                    "heartbeat_last_error": getattr(
                        status, "heartbeat_last_error", None
                    ),
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
                    "last_depth_assessment": getattr(
                        status, "last_depth_assessment", None
                    ),
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
                    "live_state_last_error": getattr(
                        live_state_status, "last_error", None
                    ),
                    "live_state_subscribed_markets": list(
                        getattr(live_state_status, "subscribed_markets", ()) or ()
                    ),
                    "market_state_active": getattr(market_state_status, "active", None),
                    "market_state_running": getattr(
                        market_state_status, "running", None
                    ),
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
                },
                indent=2,
            )
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

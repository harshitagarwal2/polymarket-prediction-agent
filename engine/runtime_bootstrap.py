from __future__ import annotations

import os
from pathlib import Path
import json
import subprocess
import shlex
from typing import TYPE_CHECKING, Any

from adapters.base import TradingAdapter
from adapters.kalshi import KalshiAdapter, KalshiConfig
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from storage.postgres.bootstrap import (
    PostgresDsnNotConfiguredError,
    require_postgres_dsn,
    resolve_postgres_dsn,
)
from storage.current_read_adapter import (
    CurrentStateReadAdapter,
    FileCurrentStateReadAdapter,
    ProjectedCurrentStateReadAdapter,
)

if TYPE_CHECKING:
    from engine.runtime_policy import RuntimePolicy


def parse_comma_separated(value: str | None) -> list[str] | None:
    if value in (None, ""):
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


def resolve_polymarket_private_key() -> str | None:
    env_value = os.getenv("POLYMARKET_PRIVATE_KEY")
    if env_value not in (None, ""):
        return env_value
    command = os.getenv("POLYMARKET_PRIVATE_KEY_COMMAND")
    if command not in (None, ""):
        completed = subprocess.run(
            shlex.split(command),
            check=True,
            capture_output=True,
            text=True,
        )
        value = completed.stdout.strip()
        if not value:
            raise RuntimeError("Polymarket private key command produced empty output")
        return value
    file_path = os.getenv("POLYMARKET_PRIVATE_KEY_FILE")
    if file_path in (None, ""):
        return None
    candidate = Path(file_path)
    if not candidate.exists():
        raise RuntimeError(f"Polymarket private key file not found: {candidate}")
    value = candidate.read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"Polymarket private key file is empty: {candidate}")
    return value


def validate_polymarket_live_routing(*, context: str) -> str:
    route_label = os.getenv("POLYMARKET_ROUTE_LABEL")
    if route_label in (None, ""):
        raise RuntimeError(f"{context} requires POLYMARKET_ROUTE_LABEL")
    geo_ack = (os.getenv("POLYMARKET_GEO_COMPLIANCE_ACK") or "").strip().lower()
    if geo_ack not in {"1", "true", "yes"}:
        raise RuntimeError(f"{context} requires POLYMARKET_GEO_COMPLIANCE_ACK=true")
    return route_label


def validate_polymarket_private_order_flow(*, context: str) -> None:
    required = (
        (os.getenv("POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED") or "").strip().lower()
    )
    if required not in {"1", "true", "yes"}:
        return
    host = os.getenv("POLYMARKET_CLOB_HOST") or PolymarketConfig.host
    if host == PolymarketConfig.host:
        raise RuntimeError(
            f"{context} requires a non-default POLYMARKET_CLOB_HOST when POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED=true"
        )


def load_manifest_condition_ids(path: str | None) -> list[str] | None:
    if path in (None, ""):
        return None
    manifest_path = Path(path)
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text())
    if not isinstance(payload, dict):
        return None
    values = payload.get("values")
    if not isinstance(values, dict):
        return None
    condition_ids: list[str] = []
    for value in values.values():
        if not isinstance(value, dict):
            continue
        condition_id = value.get("condition_id")
        if condition_id in (None, ""):
            continue
        normalized = str(condition_id)
        if normalized not in condition_ids:
            condition_ids.append(normalized)
    return condition_ids or None


def load_projected_condition_ids(root: str | Path | None) -> list[str] | None:
    if root in (None, ""):
        return None
    adapter = build_current_state_read_adapter(root, require_postgres=True)
    if adapter is None:
        return None
    fair_values = adapter.read_table("fair_values")
    markets = adapter.read_table("polymarket_markets")
    condition_ids: list[str] = []
    for market_id in fair_values.keys():
        market_row = markets.get(str(market_id))
        if not isinstance(market_row, dict):
            continue
        raw_market = market_row.get("raw_json")
        condition_id = market_row.get("condition_id")
        if condition_id in (None, "") and isinstance(raw_market, dict):
            condition_id = raw_market.get("conditionId") or raw_market.get(
                "condition_id"
            )
        if condition_id in (None, ""):
            continue
        normalized = str(condition_id)
        if normalized not in condition_ids:
            condition_ids.append(normalized)
    return condition_ids or None


def _postgres_root(root: str | Path) -> Path:
    root_path = Path(root)
    return root_path if root_path.name == "postgres" else root_path / "postgres"


def _has_projected_state_authority(root: str | Path) -> bool:
    root_path = Path(root)
    try:
        resolve_postgres_dsn(_postgres_root(root_path))
    except PostgresDsnNotConfiguredError:
        return False
    return True


def build_current_state_read_adapter(
    opportunity_root: str | Path | None,
    *,
    require_postgres: bool = False,
) -> CurrentStateReadAdapter | None:
    if opportunity_root in (None, ""):
        return None
    postgres_root = _postgres_root(opportunity_root)
    if require_postgres:
        require_postgres_dsn(
            postgres_root,
            context="runtime projected current-state reads",
        )
        return ProjectedCurrentStateReadAdapter.from_root(opportunity_root)
    if _has_projected_state_authority(opportunity_root):
        return ProjectedCurrentStateReadAdapter.from_root(opportunity_root)
    return FileCurrentStateReadAdapter.from_opportunity_root(opportunity_root)


def resolve_polymarket_live_user_markets(
    *,
    explicit_markets: str | list[str] | None = None,
    env_markets: str | None = None,
    runtime_mode: str | None = None,
    opportunity_root: str | Path | None = None,
    fair_values_file: str | None = None,
) -> list[str] | None:
    if isinstance(explicit_markets, list):
        resolved: list[str] = []
        for value in explicit_markets:
            resolved.extend(parse_comma_separated(value) or [])
        if resolved:
            return resolved
    else:
        parsed_explicit = parse_comma_separated(explicit_markets)
        if parsed_explicit:
            return parsed_explicit

    parsed_env = parse_comma_separated(env_markets)
    if parsed_env:
        return parsed_env

    if runtime_mode in {"run", "pair-run"} and opportunity_root not in (None, ""):
        return load_projected_condition_ids(opportunity_root)
    return load_manifest_condition_ids(fair_values_file)


def build_adapter(
    venue_name: str,
    args: Any = None,
    *,
    policy: RuntimePolicy | None = None,
) -> TradingAdapter:
    if venue_name == "polymarket":
        markets = resolve_polymarket_live_user_markets(
            explicit_markets=getattr(args, "polymarket_live_user_markets", None),
            env_markets=os.getenv("POLYMARKET_LIVE_USER_MARKETS"),
            runtime_mode=getattr(args, "mode", None),
            opportunity_root=getattr(args, "opportunity_root", None),
            fair_values_file=getattr(args, "fair_values_file", None),
        )
        config = PolymarketConfig(
            host=(os.getenv("POLYMARKET_CLOB_HOST") or PolymarketConfig.host),
            data_api_host=(
                os.getenv("POLYMARKET_DATA_API_HOST") or PolymarketConfig.data_api_host
            ),
            private_key=resolve_polymarket_private_key(),
            funder=os.getenv("POLYMARKET_FUNDER"),
            account_address=os.getenv("POLYMARKET_ACCOUNT_ADDRESS"),
            user_ws_host=(
                getattr(args, "polymarket_user_ws_host", None)
                or os.getenv("POLYMARKET_USER_WS_HOST")
                or PolymarketConfig.user_ws_host
            ),
            live_user_markets=markets,
        )
        if policy is not None:
            config = policy.venues.polymarket.apply(config)
        return PolymarketAdapter(config)
    if venue_name == "kalshi":
        return KalshiAdapter(
            KalshiConfig(
                api_key_id=os.getenv("KALSHI_API_KEY_ID"),
                private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH"),
            )
        )
    raise ValueError(f"unsupported venue: {venue_name}")


__all__ = [
    "build_adapter",
    "build_current_state_read_adapter",
    "load_manifest_condition_ids",
    "load_projected_condition_ids",
    "parse_comma_separated",
    "validate_polymarket_private_order_flow",
    "validate_polymarket_live_routing",
    "resolve_polymarket_private_key",
    "resolve_polymarket_live_user_markets",
]

from __future__ import annotations

import os
from pathlib import Path
import json
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


def _load_manifest_condition_ids(path: str | None) -> list[str] | None:
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


def build_adapter(
    venue_name: str,
    args: Any = None,
    *,
    policy: RuntimePolicy | None = None,
) -> TradingAdapter:
    if venue_name == "polymarket":
        markets = parse_comma_separated(
            getattr(args, "polymarket_live_user_markets", None)
            or os.getenv("POLYMARKET_LIVE_USER_MARKETS")
        )
        if markets is None:
            markets = _load_manifest_condition_ids(
                getattr(args, "fair_values_file", None)
            )
        config = PolymarketConfig(
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
    "parse_comma_separated",
]

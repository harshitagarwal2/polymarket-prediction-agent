from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from adapters.base import TradingAdapter
from adapters.kalshi import KalshiAdapter, KalshiConfig
from adapters.polymarket import PolymarketAdapter, PolymarketConfig

if TYPE_CHECKING:
    from engine.runtime_policy import RuntimePolicy


def parse_comma_separated(value: str | None) -> list[str] | None:
    if value in (None, ""):
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


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


__all__ = ["build_adapter", "parse_comma_separated"]

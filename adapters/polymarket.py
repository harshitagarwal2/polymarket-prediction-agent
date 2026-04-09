from __future__ import annotations

import importlib
import json
import socket
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from adapters import MarketSummary
from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderAction,
    OrderBookExecutionAssessment,
    OrderBookSnapshot,
    OrderIntent,
    OrderStatus,
    OutcomeSide,
    PlacementResult,
    PositionSnapshot,
    PriceLevel,
    Venue,
)


@dataclass
class PolymarketConfig:
    host: str = "https://clob.polymarket.com"
    data_api_host: str = "https://data-api.polymarket.com"
    chain_id: int = 137
    private_key: str | None = None
    funder: str | None = None
    account_address: str | None = None
    signature_type: int = 0
    api_creds_nonce: int | None = None
    request_timeout_seconds: float = 5.0
    retry_max_attempts: int = 3
    retry_backoff_seconds: float = 0.25
    retry_backoff_multiplier: float = 2.0
    retry_max_backoff_seconds: float = 2.0
    heartbeat_interval_seconds: float = 5.0
    heartbeat_max_consecutive_failures: int = 2
    user_ws_host: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    live_state_freshness_seconds: float = 15.0
    live_state_reconnect_backoff_seconds: float = 1.0
    live_state_reconnect_max_backoff_seconds: float = 15.0
    live_state_ping_interval_seconds: float = 10.0
    live_user_markets: list[str] | None = None
    market_ws_host: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    market_state_freshness_seconds: float = 10.0
    live_market_ping_interval_seconds: float = 10.0
    live_market_assets: list[str] | None = None
    depth_admission_levels: int | None = 3
    depth_admission_liquidity_fraction: float = 0.5
    depth_admission_max_expected_slippage_bps: float | None = 50.0


@dataclass(frozen=True)
class HeartbeatStatus:
    supported: bool
    required: bool
    active: bool
    running: bool
    healthy_for_trading: bool
    unhealthy: bool
    last_success_at: datetime | None
    consecutive_failures: int
    last_error: str | None
    last_heartbeat_id: str | None


@dataclass(frozen=True)
class LiveStateStatus:
    supported: bool
    active: bool
    running: bool
    mode: str
    initialized: bool
    fresh: bool
    last_update_at: datetime | None
    last_error: str | None
    subscribed_markets: tuple[str, ...]
    degraded_reason: str | None = None
    degraded_since: datetime | None = None
    recovery_attempts: int = 0
    recovering_since: datetime | None = None
    last_recovery_at: datetime | None = None
    fills_initialized: bool = False
    fills_fresh: bool = False
    fills_last_update_at: datetime | None = None
    cached_fill_count: int = 0
    last_fills_source: str | None = None
    last_fills_fallback_reason: str | None = None
    snapshot_open_order_overlay_count: int = 0
    snapshot_open_order_overlay_source: str | None = None
    snapshot_open_order_overlay_reason: str | None = None
    snapshot_fill_overlay_count: int = 0
    snapshot_fill_overlay_source: str | None = None
    snapshot_fill_overlay_reason: str | None = None


@dataclass(frozen=True)
class LiveUserStateDelta:
    source: str
    observed_at: datetime | None
    open_orders: tuple[NormalizedOrder, ...]
    fills: tuple[FillSnapshot, ...]
    terminal_order_ids: tuple[str, ...]
    reason: str | None = None


@dataclass(frozen=True)
class LiveTerminalOrderMarker:
    order_id: str
    observed_at: datetime
    status: str | None = None


@dataclass(frozen=True)
class MarketStateStatus:
    active: bool
    running: bool
    mode: str
    initialized: bool
    fresh: bool
    last_update_at: datetime | None
    last_error: str | None
    subscribed_assets: tuple[str, ...]
    degraded_reason: str | None = None
    recovery_attempts: int = 0
    last_recovery_at: datetime | None = None
    snapshot_book_overlay_source: str | None = None
    snapshot_book_overlay_reason: str | None = None
    snapshot_book_overlay_applied: bool = False


@dataclass(frozen=True)
class OrderAdmissionDecision:
    action: str
    reason: str | None = None
    scope: str | None = None
    adjusted_quantity: float | None = None
    assessment: dict[str, Any] | None = None


class PolymarketAdapter:
    venue = Venue.POLYMARKET

    def __init__(self, config: PolymarketConfig):
        self.config = config
        self._client: Any | None = None
        self._open_order_first_seen_at: dict[str, datetime] = {}
        self._condition_id_by_token: dict[str, str] = {}
        self._market_state_lock = threading.Lock()
        self._market_state_stop_event = threading.Event()
        self._market_state_thread: threading.Thread | None = None
        self._market_state_active = False
        self._market_state_running = False
        self._market_state_mode = "inactive"
        self._market_state_initialized = False
        self._market_state_last_update_at: datetime | None = None
        self._market_state_last_error: str | None = None
        self._market_state_degraded_reason: str | None = None
        self._market_state_recovery_attempts = 0
        self._market_state_last_recovery_at: datetime | None = None
        self._market_state_assets: tuple[str, ...] = tuple()
        self._market_state_tracked_assets: set[str] = set(
            str(asset) for asset in (self.config.live_market_assets or []) if str(asset)
        )
        self._market_state_books: dict[str, dict[str, Any]] = {}
        self._market_state_last_snapshot_book_overlay_source: str | None = None
        self._market_state_last_snapshot_book_overlay_reason: str | None = None
        self._market_state_last_snapshot_book_overlay_applied = False
        self._live_state_lock = threading.Lock()
        self._live_state_stop_event = threading.Event()
        self._live_state_thread: threading.Thread | None = None
        self._live_state_active = False
        self._live_state_running = False
        self._live_state_mode = "inactive"
        self._live_state_initialized = False
        self._live_state_last_update_at: datetime | None = None
        self._live_state_last_error: str | None = None
        self._live_state_degraded_reason: str | None = None
        self._live_state_degraded_since: datetime | None = None
        self._live_state_recovery_attempts = 0
        self._live_state_recovering_since: datetime | None = None
        self._live_state_last_recovery_at: datetime | None = None
        self._live_state_orders_raw: dict[str, dict[str, Any]] = {}
        self._live_state_fills_raw: dict[str, dict[str, Any]] = {}
        self._live_state_terminal_order_markers: dict[
            str, tuple[datetime, str | None]
        ] = {}
        self._live_state_fill_order: list[str] = []
        self._live_state_fills_initialized = False
        self._live_state_fills_last_update_at: datetime | None = None
        self._live_state_last_fills_source: str | None = None
        self._live_state_last_fills_fallback_reason: str | None = None
        self._live_state_last_snapshot_open_order_overlay_count = 0
        self._live_state_last_snapshot_open_order_overlay_source: str | None = None
        self._live_state_last_snapshot_open_order_overlay_reason: str | None = None
        self._live_state_last_snapshot_fill_overlay_count = 0
        self._live_state_last_snapshot_fill_overlay_source: str | None = None
        self._live_state_last_snapshot_fill_overlay_reason: str | None = None
        self._live_state_markets: tuple[str, ...] = tuple()
        self._heartbeat_lock = threading.Lock()
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_required_for_live_trading = False
        self._heartbeat_id: str | None = None
        self._heartbeat_last_success_at: datetime | None = None
        self._heartbeat_consecutive_failures = 0
        self._heartbeat_last_error: str | None = None
        self._heartbeat_active = False
        self._heartbeat_running = False
        self._heartbeat_unhealthy = False

    def _heartbeat_supported(self) -> bool:
        return bool(self.config.private_key)

    def _live_state_supported(self) -> bool:
        return bool(self.config.private_key)

    def _live_state_fresh_locked(self) -> bool:
        if not self._live_state_initialized or self._live_state_last_update_at is None:
            return False
        age_seconds = (
            datetime.now(timezone.utc) - self._live_state_last_update_at
        ).total_seconds()
        return age_seconds <= max(1.0, self.config.live_state_freshness_seconds)

    def _live_state_fills_fresh_locked(self) -> bool:
        if (
            not self._live_state_fills_initialized
            or self._live_state_fills_last_update_at is None
        ):
            return False
        age_seconds = (
            datetime.now(timezone.utc) - self._live_state_fills_last_update_at
        ).total_seconds()
        return age_seconds <= max(1.0, self.config.live_state_freshness_seconds)

    def _live_state_status_locked(self) -> LiveStateStatus:
        return LiveStateStatus(
            supported=self._live_state_supported(),
            active=self._live_state_active,
            running=self._live_state_running,
            mode=self._live_state_mode,
            initialized=self._live_state_initialized,
            fresh=self._live_state_fresh_locked(),
            last_update_at=self._live_state_last_update_at,
            last_error=self._live_state_last_error,
            subscribed_markets=self._live_state_markets,
            degraded_reason=self._live_state_degraded_reason,
            degraded_since=self._live_state_degraded_since,
            recovery_attempts=self._live_state_recovery_attempts,
            recovering_since=self._live_state_recovering_since,
            last_recovery_at=self._live_state_last_recovery_at,
            fills_initialized=self._live_state_fills_initialized,
            fills_fresh=self._live_state_fills_fresh_locked(),
            fills_last_update_at=self._live_state_fills_last_update_at,
            cached_fill_count=len(self._live_state_fill_order),
            last_fills_source=self._live_state_last_fills_source,
            last_fills_fallback_reason=self._live_state_last_fills_fallback_reason,
            snapshot_open_order_overlay_count=(
                self._live_state_last_snapshot_open_order_overlay_count
            ),
            snapshot_open_order_overlay_source=(
                self._live_state_last_snapshot_open_order_overlay_source
            ),
            snapshot_open_order_overlay_reason=(
                self._live_state_last_snapshot_open_order_overlay_reason
            ),
            snapshot_fill_overlay_count=self._live_state_last_snapshot_fill_overlay_count,
            snapshot_fill_overlay_source=self._live_state_last_snapshot_fill_overlay_source,
            snapshot_fill_overlay_reason=self._live_state_last_snapshot_fill_overlay_reason,
        )

    def live_state_status(self) -> LiveStateStatus:
        with self._live_state_lock:
            return self._live_state_status_locked()

    def _market_state_fresh_locked(self) -> bool:
        if (
            not self._market_state_initialized
            or self._market_state_last_update_at is None
        ):
            return False
        age_seconds = (
            datetime.now(timezone.utc) - self._market_state_last_update_at
        ).total_seconds()
        return age_seconds <= max(1.0, self.config.market_state_freshness_seconds)

    def _market_state_status_locked(self) -> MarketStateStatus:
        return MarketStateStatus(
            active=self._market_state_active,
            running=self._market_state_running,
            mode=self._market_state_mode,
            initialized=self._market_state_initialized,
            fresh=self._market_state_fresh_locked(),
            last_update_at=self._market_state_last_update_at,
            last_error=self._market_state_last_error,
            subscribed_assets=self._market_state_assets,
            degraded_reason=self._market_state_degraded_reason,
            recovery_attempts=self._market_state_recovery_attempts,
            last_recovery_at=self._market_state_last_recovery_at,
            snapshot_book_overlay_source=self._market_state_last_snapshot_book_overlay_source,
            snapshot_book_overlay_reason=self._market_state_last_snapshot_book_overlay_reason,
            snapshot_book_overlay_applied=self._market_state_last_snapshot_book_overlay_applied,
        )

    def market_state_status(self) -> MarketStateStatus:
        with self._market_state_lock:
            return self._market_state_status_locked()

    def _set_market_state_mode_locked(
        self, mode: str, *, reason: str | None = None
    ) -> None:
        self._market_state_mode = mode
        if mode == "healthy":
            self._market_state_degraded_reason = None
            self._market_state_last_error = None
            self._market_state_last_recovery_at = datetime.now(timezone.utc)
        elif mode == "recovering":
            self._market_state_degraded_reason = reason
            self._market_state_recovery_attempts += 1
        elif mode == "degraded":
            self._market_state_degraded_reason = reason
            self._market_state_last_error = reason
        elif mode == "inactive":
            self._market_state_degraded_reason = None

    def mark_live_market_state_degraded(self, reason: str) -> MarketStateStatus:
        with self._market_state_lock:
            self._set_market_state_mode_locked("degraded", reason=reason)
            return self._market_state_status_locked()

    def confirm_live_market_state_recovery(
        self, observed_at: datetime
    ) -> MarketStateStatus:
        with self._market_state_lock:
            self._set_market_state_mode_locked("healthy")
            self._market_state_last_recovery_at = observed_at
            return self._market_state_status_locked()

    def _set_live_state_mode_locked(
        self, mode: str, *, reason: str | None = None
    ) -> None:
        now = datetime.now(timezone.utc)
        self._live_state_mode = mode
        if mode == "healthy":
            self._live_state_degraded_reason = None
            self._live_state_degraded_since = None
            self._live_state_recovering_since = None
            self._live_state_last_recovery_at = now
        elif mode == "recovering":
            self._live_state_recovering_since = now
            self._live_state_degraded_reason = reason
        elif mode == "degraded":
            self._live_state_degraded_reason = reason
            if self._live_state_degraded_since is None:
                self._live_state_degraded_since = now
        elif mode == "inactive":
            self._live_state_degraded_reason = None
            self._live_state_degraded_since = None
            self._live_state_recovering_since = None

    def _mark_live_state_recovering_locked(self, reason: str | None = None) -> None:
        self._live_state_recovery_attempts += 1
        self._set_live_state_mode_locked("recovering", reason=reason)

    def mark_live_state_degraded(self, reason: str) -> LiveStateStatus:
        with self._live_state_lock:
            self._set_live_state_mode_locked("degraded", reason=reason)
            self._live_state_last_error = reason
            return self._live_state_status_locked()

    def confirm_live_state_recovery(self, observed_at: datetime) -> LiveStateStatus:
        with self._live_state_lock:
            if self._live_state_mode == "recovering":
                self._set_live_state_mode_locked("healthy")
                self._live_state_last_recovery_at = observed_at
            return self._live_state_status_locked()

    def _heartbeat_status_locked(self) -> HeartbeatStatus:
        required = self._heartbeat_required_for_live_trading
        healthy_for_trading = (not required) or (
            self._heartbeat_active
            and not self._heartbeat_unhealthy
            and self._heartbeat_last_success_at is not None
        )
        return HeartbeatStatus(
            supported=self._heartbeat_supported(),
            required=required,
            active=self._heartbeat_active,
            running=self._heartbeat_running,
            healthy_for_trading=healthy_for_trading,
            unhealthy=self._heartbeat_unhealthy,
            last_success_at=self._heartbeat_last_success_at,
            consecutive_failures=self._heartbeat_consecutive_failures,
            last_error=self._heartbeat_last_error,
            last_heartbeat_id=self._heartbeat_id,
        )

    def heartbeat_status(self) -> HeartbeatStatus:
        with self._heartbeat_lock:
            return self._heartbeat_status_locked()

    def _record_heartbeat_success(self, response: Any) -> None:
        if not isinstance(response, dict):
            raise RuntimeError("malformed Polymarket heartbeat response")
        heartbeat_id = response.get("heartbeat_id")
        if heartbeat_id in (None, ""):
            raise RuntimeError("Polymarket heartbeat response missing heartbeat_id")
        with self._heartbeat_lock:
            self._heartbeat_id = str(heartbeat_id)
            self._heartbeat_last_success_at = datetime.now(timezone.utc)
            self._heartbeat_consecutive_failures = 0
            self._heartbeat_last_error = None
            self._heartbeat_unhealthy = False

    def _record_heartbeat_failure(self, exc: Exception) -> None:
        with self._heartbeat_lock:
            self._heartbeat_consecutive_failures += 1
            self._heartbeat_last_error = str(exc)
            threshold = max(1, self.config.heartbeat_max_consecutive_failures)
            if self._heartbeat_consecutive_failures >= threshold:
                self._heartbeat_unhealthy = True
                self._heartbeat_active = False
                self._heartbeat_running = False

    def _send_heartbeat_once(self) -> HeartbeatStatus:
        with self._heartbeat_lock:
            heartbeat_id = self._heartbeat_id
        response = self._call_client("post heartbeat", "post_heartbeat", heartbeat_id)
        self._record_heartbeat_success(response)
        return self.heartbeat_status()

    def _heartbeat_loop(self) -> None:
        interval_seconds = max(0.1, min(self.config.heartbeat_interval_seconds, 9.5))
        try:
            while not self._heartbeat_stop_event.wait(interval_seconds):
                try:
                    self._send_heartbeat_once()
                except Exception as exc:
                    self._record_heartbeat_failure(exc)
                    if self.heartbeat_status().unhealthy:
                        break
        finally:
            with self._heartbeat_lock:
                self._heartbeat_running = False
                self._heartbeat_thread = None

    def start_heartbeat(self) -> HeartbeatStatus:
        if not self._heartbeat_supported():
            return self.heartbeat_status()

        with self._heartbeat_lock:
            self._heartbeat_required_for_live_trading = True
            if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
                self._heartbeat_active = True
                return self._heartbeat_status_locked()
            self._heartbeat_stop_event = threading.Event()
            self._heartbeat_active = True
            self._heartbeat_running = False
            self._heartbeat_unhealthy = False
            self._heartbeat_consecutive_failures = 0
            self._heartbeat_last_error = None
            self._heartbeat_last_success_at = None
            self._heartbeat_id = None

        try:
            self._send_heartbeat_once()
        except Exception as exc:
            self._record_heartbeat_failure(exc)

        with self._heartbeat_lock:
            if self._heartbeat_unhealthy:
                return self._heartbeat_status_locked()
            thread = threading.Thread(
                target=self._heartbeat_loop,
                name="polymarket-heartbeat",
                daemon=True,
            )
            self._heartbeat_thread = thread
            self._heartbeat_running = True
            thread.start()
            return self._heartbeat_status_locked()

    def stop_heartbeat(self) -> HeartbeatStatus:
        with self._heartbeat_lock:
            thread = self._heartbeat_thread
            self._heartbeat_required_for_live_trading = False
            self._heartbeat_active = False
            self._heartbeat_running = False
            self._heartbeat_stop_event.set()
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=max(0.1, self.config.request_timeout_seconds))
        with self._heartbeat_lock:
            self._heartbeat_thread = None
            return self._heartbeat_status_locked()

    def _configure_client_timeout(self) -> None:
        try:
            helpers = importlib.import_module("py_clob_client.http_helpers.helpers")
            httpx = importlib.import_module("httpx")
        except ImportError:
            return
        http_client = getattr(helpers, "_http_client", None)
        if http_client is None:
            return
        http_client.timeout = httpx.Timeout(self.config.request_timeout_seconds)

    def _parse_datetime_value(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 1_000_000_000_000:
                timestamp /= 1000.0
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        text = str(value).strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            numeric = None
        if numeric is not None:
            if numeric > 1_000_000_000_000:
                numeric /= 1000.0
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _raw_order_timestamp(
        self, order: dict[str, Any], *keys: str
    ) -> datetime | None:
        for key in keys:
            parsed = self._parse_datetime_value(order.get(key))
            if parsed is not None:
                return parsed
        return None

    def _stable_order_times(
        self, order_id: str, order: dict[str, Any]
    ) -> tuple[datetime, datetime]:
        now = datetime.now(timezone.utc)
        created_at = self._raw_order_timestamp(
            order,
            "created_at",
            "createdAt",
            "created_time",
            "createdTime",
            "placed_at",
            "placedAt",
            "timestamp",
        )
        if created_at is None:
            created_at = self._open_order_first_seen_at.get(order_id)
        if created_at is None:
            created_at = now
        self._open_order_first_seen_at[order_id] = created_at
        updated_at = self._raw_order_timestamp(
            order,
            "updated_at",
            "updatedAt",
            "last_update_time",
            "lastUpdateTime",
            "timestamp",
        )
        if updated_at is None:
            updated_at = now
        return created_at, updated_at

    def _raw_live_event_time(self, payload: dict[str, Any]) -> datetime | None:
        return self._parse_datetime_value(payload.get("__live_event_at"))

    def _live_order_event_time(
        self, order: dict[str, Any], observed_at: datetime
    ) -> datetime:
        return (
            self._raw_order_timestamp(
                order,
                "updated_at",
                "updatedAt",
                "last_update_time",
                "lastUpdateTime",
                "timestamp",
                "created_at",
                "createdAt",
            )
            or observed_at
        )

    def _order_condition_id(self, order: dict[str, Any]) -> str | None:
        for key in (
            "condition_id",
            "conditionId",
            "market",
            "market_id",
            "marketId",
        ):
            value = order.get(key)
            if value not in (None, ""):
                return str(value)
        symbol = order.get("asset_id") or order.get("token_id")
        if symbol not in (None, ""):
            return self._condition_id_by_token.get(str(symbol))
        return None

    def _fill_condition_id(self, trade: dict[str, Any]) -> str | None:
        for key in (
            "condition_id",
            "conditionId",
            "market",
            "market_id",
            "marketId",
        ):
            value = trade.get(key)
            if value not in (None, ""):
                return str(value)
        symbol = (
            trade.get("asset_id")
            or trade.get("assetId")
            or trade.get("token_id")
            or trade.get("tokenId")
        )
        if symbol not in (None, ""):
            return self._condition_id_by_token.get(str(symbol))
        return None

    def _cache_condition_mapping(self, symbol: str, condition_id: str | None) -> None:
        if condition_id in (None, ""):
            return
        self._condition_id_by_token[str(symbol)] = str(condition_id)

    def _live_state_subscription_markets(self) -> tuple[str, ...]:
        configured_markets = {
            str(item) for item in (self.config.live_user_markets or []) if str(item)
        }
        mapped_markets = {
            market for market in self._condition_id_by_token.values() if market
        }
        order_markets = {
            market
            for market in (
                self._order_condition_id(order)
                for order in self._live_state_orders_raw.values()
            )
            if market is not None
        }
        fill_markets = {
            market
            for market in (
                self._fill_condition_id(trade)
                for trade in self._live_state_fills_raw.values()
            )
            if market is not None
        }
        return tuple(
            sorted(configured_markets | mapped_markets | order_markets | fill_markets)
        )

    def _live_state_bootstrap(self) -> tuple[str, ...]:
        observed_at = datetime.now(timezone.utc)
        orders = self._list_open_orders_rest(None)
        self._set_live_orders_cache(
            [dict(order["raw"]) for order in orders],
            observed_at=observed_at,
        )
        try:
            self._refresh_live_fills_cache(observed_at=observed_at)
        except Exception as exc:
            self._record_live_state_error(exc)
        with self._live_state_lock:
            self._live_state_markets = self._live_state_subscription_markets()
            return self._live_state_markets

    def _live_state_bootstrap_if_needed(self) -> None:
        with self._live_state_lock:
            needs_bootstrap = not self._live_state_initialized
        if needs_bootstrap:
            self._live_state_bootstrap()

    def _live_state_read_orders_raw(self) -> list[dict[str, Any]]:
        with self._live_state_lock:
            return [dict(order) for order in self._live_state_orders_raw.values()]

    def _live_state_read_fills_raw(self) -> list[dict[str, Any]]:
        with self._live_state_lock:
            return [
                dict(self._live_state_fills_raw[fill_key])
                for fill_key in self._live_state_fill_order
                if fill_key in self._live_state_fills_raw
            ]

    def _normalize_open_orders(
        self, raw_orders: list[dict[str, Any]], contract: Contract | None = None
    ) -> list[NormalizedOrder]:
        normalized: list[NormalizedOrder] = []
        seen_order_ids: set[str] = set()
        for order in raw_orders:
            order_id_value = (
                order.get("id") or order.get("orderID") or order.get("order_id")
            )
            if order_id_value in (None, ""):
                continue
            order_id = str(order_id_value)
            created_at, updated_at = self._stable_order_times(order_id, order)
            seen_order_ids.add(order_id)
            symbol = (
                order.get("asset_id")
                or order.get("token_id")
                or (contract.symbol if contract else "unknown")
            )
            quantity = self._open_order_quantity(order)
            remaining_quantity = self._open_order_remaining_quantity(order, quantity)
            normalized_contract = Contract(
                venue=self.venue,
                symbol=str(symbol),
                outcome=contract.outcome
                if contract
                else Contract(self.venue, str(symbol)).outcome,
            )
            normalized.append(
                NormalizedOrder(
                    order_id=order_id,
                    contract=normalized_contract,
                    action=OrderAction.BUY
                    if str(order.get("side", "BUY")).upper() == "BUY"
                    else OrderAction.SELL,
                    price=float(order.get("price", 0.0)),
                    quantity=quantity,
                    remaining_quantity=remaining_quantity,
                    status=self._open_order_status(order, quantity, remaining_quantity),
                    created_at=created_at,
                    updated_at=updated_at,
                    post_only=bool(
                        order.get("postOnly", order.get("post_only", False))
                    ),
                    expiration_ts=(
                        int(order["expiration"])
                        if order.get("expiration") not in (None, "")
                        else None
                    ),
                    raw=order,
                )
            )
        self._open_order_first_seen_at = {
            order_id: created_at
            for order_id, created_at in self._open_order_first_seen_at.items()
            if order_id in seen_order_ids
        }
        if contract is None:
            return normalized
        return [
            order for order in normalized if order.contract.symbol == contract.symbol
        ]

    def _retryable_status_code(self, status_code: int | None) -> bool:
        if status_code is None:
            return False
        return status_code in {425, 429} or 500 <= status_code < 600

    def _exception_status_code(self, exc: Exception) -> int | None:
        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int):
            return status_code
        if isinstance(exc, HTTPError):
            return int(exc.code)
        return None

    def _is_retryable_error(self, exc: Exception) -> bool:
        status_code = self._exception_status_code(exc)
        if self._retryable_status_code(status_code):
            return True
        if status_code is not None:
            return False
        if exc.__class__.__name__ == "PolyApiException":
            return True
        if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError, URLError)):
            return True
        return False

    def _call_with_retry(self, operation: str, func, *args, **kwargs):
        attempts = max(1, self.config.retry_max_attempts)
        backoff = max(0.0, self.config.retry_backoff_seconds)
        multiplier = max(1.0, self.config.retry_backoff_multiplier)
        max_backoff = max(backoff, self.config.retry_max_backoff_seconds)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_error = exc
                if attempt >= attempts or not self._is_retryable_error(exc):
                    raise
                time.sleep(min(backoff, max_backoff))
                backoff = min(max_backoff, backoff * multiplier)

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"{operation} failed without returning or raising")

    def _call_client(self, operation: str, method_name: str, *args, **kwargs):
        client = self._ensure_client()
        method = getattr(client, method_name)
        return self._call_with_retry(operation, method, *args, **kwargs)

    def _set_live_orders_cache(
        self,
        raw_orders: list[dict[str, Any]],
        *,
        observed_at: datetime | None = None,
    ) -> None:
        with self._live_state_lock:
            self._live_state_orders_raw = {
                str(order.get("id") or order.get("orderID") or order.get("order_id")): {
                    **dict(order),
                    "__live_event_at": (
                        observed_at or datetime.now(timezone.utc)
                    ).isoformat(),
                }
                for order in raw_orders
                if order.get("id") or order.get("orderID") or order.get("order_id")
            }
            for order_id in self._live_state_orders_raw:
                self._live_state_terminal_order_markers.pop(order_id, None)
            self._live_state_initialized = True
            self._live_state_last_update_at = observed_at or datetime.now(timezone.utc)
            self._live_state_last_error = None
            self._live_state_markets = self._live_state_subscription_markets()

    def _fill_cache_key(self, trade: dict[str, Any]) -> str | None:
        for key in (
            "fill_id",
            "fillId",
            "id",
            "trade_id",
            "tradeId",
            "match_id",
            "matchId",
        ):
            value = trade.get(key)
            if value not in (None, ""):
                return str(value)
        order_id = self._fill_order_id(trade)
        symbol = (
            trade.get("asset_id")
            or trade.get("assetId")
            or trade.get("token_id")
            or trade.get("tokenId")
            or ""
        )
        size = (
            trade.get("size")
            or trade.get("quantity")
            or trade.get("filled_size")
            or trade.get("filledSize")
            or ""
        )
        price = trade.get("price")
        timestamp = self._raw_fill_timestamp(
            trade,
            "timestamp",
            "created_at",
            "createdAt",
            "created_time",
            "createdTime",
        )
        if order_id or symbol or price not in (None, "") or size not in (None, ""):
            return (
                f"{order_id}:{symbol}:{trade.get('side') or ''}:{price or ''}:"
                f"{size}:{timestamp.isoformat() if timestamp is not None else ''}"
            )
        return None

    def _set_live_fills_cache(
        self,
        raw_trades: list[dict[str, Any]],
        *,
        observed_at: datetime | None = None,
    ) -> None:
        fills_raw: dict[str, dict[str, Any]] = {}
        fill_order: list[str] = []
        for trade in raw_trades:
            trade_copy = dict(trade)
            fill_key = self._fill_cache_key(trade_copy)
            if fill_key is None:
                continue
            symbol = (
                trade_copy.get("asset_id")
                or trade_copy.get("assetId")
                or trade_copy.get("token_id")
                or trade_copy.get("tokenId")
            )
            if symbol not in (None, ""):
                self._cache_condition_mapping(
                    str(symbol), self._fill_condition_id(trade_copy)
                )
            if fill_key not in fills_raw:
                fill_order.append(fill_key)
                fills_raw[fill_key] = trade_copy
                continue
            merged = dict(fills_raw[fill_key])
            merged.update(trade_copy)
            fills_raw[fill_key] = merged
        with self._live_state_lock:
            self._live_state_fills_raw = fills_raw
            self._live_state_fill_order = fill_order
            self._live_state_fills_initialized = True
            self._live_state_fills_last_update_at = observed_at or datetime.now(
                timezone.utc
            )
            self._live_state_last_error = None
            self._live_state_markets = self._live_state_subscription_markets()

    def _merge_live_fills_cache(
        self,
        raw_trades: list[dict[str, Any]],
        *,
        observed_at: datetime | None = None,
    ) -> None:
        merged_any = False
        with self._live_state_lock:
            for trade in raw_trades:
                trade_copy = dict(trade)
                fill_key = self._fill_cache_key(trade_copy)
                if fill_key is None:
                    continue
                symbol = (
                    trade_copy.get("asset_id")
                    or trade_copy.get("assetId")
                    or trade_copy.get("token_id")
                    or trade_copy.get("tokenId")
                )
                if symbol not in (None, ""):
                    self._cache_condition_mapping(
                        str(symbol), self._fill_condition_id(trade_copy)
                    )
                merged = dict(self._live_state_fills_raw.get(fill_key, {}))
                merged.update(trade_copy)
                self._live_state_fills_raw[fill_key] = merged
                if fill_key not in self._live_state_fill_order:
                    self._live_state_fill_order.insert(0, fill_key)
                merged_any = True
            if not merged_any:
                return
            self._live_state_fills_initialized = True
            self._live_state_fills_last_update_at = observed_at or datetime.now(
                timezone.utc
            )
            self._live_state_last_error = None
            self._live_state_markets = self._live_state_subscription_markets()

    def _live_state_fill_fallback_reason_locked(self) -> str | None:
        if not self._live_state_active:
            return "live_state_inactive"
        if self._live_state_mode == "recovering":
            return "live_state_recovering"
        if self._live_state_mode == "degraded":
            return "live_state_degraded"
        if not self._live_state_fills_initialized:
            return "fill_cache_cold"
        if not self._live_state_fills_fresh_locked():
            return "fill_cache_stale"
        return None

    def _live_state_order_fallback_reason_locked(self) -> str | None:
        if not self._live_state_active:
            return "live_state_inactive"
        if self._live_state_mode == "recovering":
            return "live_state_recovering"
        if self._live_state_mode == "degraded":
            return "live_state_degraded"
        if not self._live_state_initialized:
            return "order_cache_cold"
        if not self._live_state_fresh_locked():
            return "order_cache_stale"
        return None

    def _record_fill_read_source(
        self,
        *,
        source: str,
        fallback_reason: str | None = None,
    ) -> None:
        with self._live_state_lock:
            self._live_state_last_fills_source = source
            self._live_state_last_fills_fallback_reason = fallback_reason

    def _record_snapshot_fill_overlay(
        self,
        *,
        source: str,
        overlay_count: int,
        reason: str | None = None,
    ) -> None:
        with self._live_state_lock:
            self._live_state_last_snapshot_fill_overlay_source = source
            self._live_state_last_snapshot_fill_overlay_count = overlay_count
            self._live_state_last_snapshot_fill_overlay_reason = reason

    def _record_snapshot_open_order_overlay(
        self,
        *,
        source: str,
        overlay_count: int,
        reason: str | None = None,
    ) -> None:
        with self._live_state_lock:
            self._live_state_last_snapshot_open_order_overlay_source = source
            self._live_state_last_snapshot_open_order_overlay_count = overlay_count
            self._live_state_last_snapshot_open_order_overlay_reason = reason

    def _open_order_overlay_terminal(self, order: NormalizedOrder) -> bool:
        return (
            order.status
            in {
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.REJECTED,
            }
            or order.remaining_quantity <= 0.0
        )

    def _open_order_overlay_candidate(self, order: NormalizedOrder) -> bool:
        return not self._open_order_overlay_terminal(order)

    def _live_order_overlays_size(self, payload: dict[str, Any]) -> bool:
        return any(
            payload.get(key) not in (None, "")
            for key in (
                "original_size",
                "originalSize",
                "initial_size",
                "initialSize",
                "size",
                "quantity",
                "remaining_size",
                "remainingSize",
                "size_left",
                "sizeLeft",
                "unfilled_size",
                "unfilledSize",
                "matched_size",
                "matchedSize",
                "size_matched",
                "filled_size",
                "filledSize",
            )
        )

    def _live_order_overlays_status(self, payload: dict[str, Any]) -> bool:
        return any(
            payload.get(key) not in (None, "")
            for key in ("status", "order_status", "orderStatus", "state")
        ) or self._live_order_overlays_size(payload)

    def _merge_snapshot_open_order(
        self,
        rest_order: NormalizedOrder,
        live_order: NormalizedOrder,
    ) -> NormalizedOrder:
        live_raw = dict(live_order.raw) if isinstance(live_order.raw, dict) else {}
        merged_raw = dict(rest_order.raw) if isinstance(rest_order.raw, dict) else {}
        for key in (
            "price",
            "status",
            "order_status",
            "orderStatus",
            "state",
            "original_size",
            "originalSize",
            "initial_size",
            "initialSize",
            "size",
            "quantity",
            "remaining_size",
            "remainingSize",
            "size_left",
            "sizeLeft",
            "unfilled_size",
            "unfilledSize",
            "matched_size",
            "matchedSize",
            "size_matched",
            "filled_size",
            "filledSize",
            "postOnly",
            "post_only",
            "expiration",
            "updated_at",
            "updatedAt",
            "last_update_time",
            "lastUpdateTime",
        ):
            if key in live_raw and live_raw[key] not in (None, ""):
                merged_raw[key] = live_raw[key]
        quantity = rest_order.quantity
        remaining_quantity = rest_order.remaining_quantity
        status = rest_order.status
        if self._live_order_overlays_size(live_raw):
            quantity = max(
                rest_order.quantity, live_order.quantity, live_order.remaining_quantity
            )
            remaining_quantity = min(max(0.0, live_order.remaining_quantity), quantity)
        if self._live_order_overlays_status(
            live_raw
        ) and not self._open_order_overlay_terminal(live_order):
            status = live_order.status
        post_only = rest_order.post_only
        if (
            live_raw.get("postOnly") is not None
            or live_raw.get("post_only") is not None
        ):
            post_only = live_order.post_only
        expiration_ts = rest_order.expiration_ts
        if live_raw.get("expiration") not in (None, ""):
            expiration_ts = live_order.expiration_ts
        return replace(
            rest_order,
            price=live_order.price
            if live_raw.get("price") not in (None, "")
            else rest_order.price,
            quantity=quantity,
            remaining_quantity=remaining_quantity,
            status=status,
            updated_at=max(rest_order.updated_at, live_order.updated_at),
            post_only=post_only,
            expiration_ts=expiration_ts,
            raw=merged_raw or rest_order.raw,
        )

    def _open_order_overlay_changed(
        self,
        rest_order: NormalizedOrder,
        merged_order: NormalizedOrder,
    ) -> bool:
        return (
            rest_order.price != merged_order.price
            or rest_order.quantity != merged_order.quantity
            or rest_order.remaining_quantity != merged_order.remaining_quantity
            or rest_order.status != merged_order.status
            or rest_order.post_only != merged_order.post_only
            or rest_order.expiration_ts != merged_order.expiration_ts
        )

    def _overlay_live_open_orders_on_snapshot(
        self,
        rest_orders: list[NormalizedOrder],
        contract: Contract | None = None,
    ) -> list[NormalizedOrder]:
        with self._live_state_lock:
            fallback_reason = self._live_state_order_fallback_reason_locked()
            if fallback_reason is not None:
                self._live_state_last_snapshot_open_order_overlay_source = "rest_only"
                self._live_state_last_snapshot_open_order_overlay_count = 0
                self._live_state_last_snapshot_open_order_overlay_reason = (
                    fallback_reason
                )
                return rest_orders
            live_orders = self._normalize_open_orders(
                [dict(order) for order in self._live_state_orders_raw.values()],
                contract,
            )
        self._open_order_first_seen_at = {
            order.order_id: order.created_at for order in (rest_orders + live_orders)
        }

        if not live_orders:
            self._record_snapshot_open_order_overlay(
                source="rest_only",
                overlay_count=0,
                reason="live_order_overlay_empty",
            )
            return rest_orders

        merged = list(rest_orders)
        existing_indexes = {order.order_id: index for index, order in enumerate(merged)}
        overlay: list[NormalizedOrder] = []
        overlay_count = 0
        for live_order in live_orders:
            if not self._open_order_overlay_candidate(live_order):
                continue
            rest_index = existing_indexes.get(live_order.order_id)
            if rest_index is None:
                overlay.append(live_order)
                overlay_count += 1
                continue
            merged_order = self._merge_snapshot_open_order(
                merged[rest_index], live_order
            )
            if not self._open_order_overlay_changed(merged[rest_index], merged_order):
                continue
            merged[rest_index] = merged_order
            overlay_count += 1
        if overlay_count == 0:
            self._record_snapshot_open_order_overlay(
                source="rest_only",
                overlay_count=0,
                reason="live_order_overlay_duplicate",
            )
            return merged

        self._record_snapshot_open_order_overlay(
            source="rest_plus_live_overlay",
            overlay_count=overlay_count,
        )
        return overlay + merged

    def _overlay_live_fills_on_snapshot(
        self,
        rest_fills: list[FillSnapshot],
        contract: Contract | None = None,
    ) -> list[FillSnapshot]:
        with self._live_state_lock:
            fallback_reason = self._live_state_fill_fallback_reason_locked()
            if fallback_reason is not None:
                self._live_state_last_snapshot_fill_overlay_source = "rest_only"
                self._live_state_last_snapshot_fill_overlay_count = 0
                self._live_state_last_snapshot_fill_overlay_reason = fallback_reason
                return rest_fills
            live_trades = [
                dict(self._live_state_fills_raw[fill_key])
                for fill_key in self._live_state_fill_order
                if fill_key in self._live_state_fills_raw
            ]

        live_fills = self._normalize_fills(live_trades, contract)
        if not live_fills:
            self._record_snapshot_fill_overlay(
                source="rest_only",
                overlay_count=0,
                reason="live_fill_overlay_empty",
            )
            return rest_fills

        merged = list(rest_fills)
        existing_fill_keys = {fill.fill_key for fill in rest_fills}
        existing_overlay_signatures = {
            self._fill_overlay_signature(fill) for fill in rest_fills
        }
        overlay: list[FillSnapshot] = []
        for fill in live_fills:
            overlay_signature = self._fill_overlay_signature(fill)
            if (
                fill.fill_key in existing_fill_keys
                or overlay_signature in existing_overlay_signatures
            ):
                continue
            overlay.append(fill)
            existing_fill_keys.add(fill.fill_key)
            existing_overlay_signatures.add(overlay_signature)
        if not overlay:
            self._record_snapshot_fill_overlay(
                source="rest_only",
                overlay_count=0,
                reason="live_fill_overlay_duplicate",
            )
            return merged

        self._record_snapshot_fill_overlay(
            source="rest_plus_live_overlay",
            overlay_count=len(overlay),
        )
        return overlay + merged

    def _fill_overlay_signature(
        self, fill: FillSnapshot
    ) -> tuple[str, str, str, str, str]:
        return (
            fill.order_id,
            fill.contract.symbol,
            fill.action.value,
            f"{fill.price:.8f}",
            f"{fill.quantity:.8f}",
        )

    def _open_order_quantity(self, order: dict[str, Any]) -> float:
        for key in ("original_size", "originalSize", "initial_size", "initialSize"):
            if order.get(key) not in (None, ""):
                return max(0.0, self._parse_quantity(order.get(key)))
        remaining_quantity = None
        for key in (
            "remaining_size",
            "remainingSize",
            "size_left",
            "sizeLeft",
            "unfilled_size",
            "unfilledSize",
        ):
            if order.get(key) not in (None, ""):
                remaining_quantity = max(0.0, self._parse_quantity(order.get(key)))
                break
        matched_quantity = None
        for key in (
            "matched_size",
            "matchedSize",
            "size_matched",
            "filled_size",
            "filledSize",
        ):
            if order.get(key) not in (None, ""):
                matched_quantity = max(0.0, self._parse_quantity(order.get(key)))
                break
        if remaining_quantity is not None and matched_quantity is not None:
            return remaining_quantity + matched_quantity
        if order.get("size") not in (None, ""):
            return max(0.0, self._parse_quantity(order.get("size")))
        if order.get("quantity") not in (None, ""):
            return max(0.0, self._parse_quantity(order.get("quantity")))
        if remaining_quantity is not None:
            return remaining_quantity
        return 0.0

    def _open_order_remaining_quantity(
        self,
        order: dict[str, Any],
        quantity: float,
    ) -> float:
        for key in (
            "remaining_size",
            "remainingSize",
            "size_left",
            "sizeLeft",
            "unfilled_size",
            "unfilledSize",
        ):
            if order.get(key) not in (None, ""):
                return min(max(0.0, self._parse_quantity(order.get(key))), quantity)
        for key in (
            "matched_size",
            "matchedSize",
            "size_matched",
            "filled_size",
            "filledSize",
        ):
            if order.get(key) not in (None, ""):
                return max(0.0, quantity - self._parse_quantity(order.get(key)))
        return max(0.0, quantity)

    def _open_order_status(
        self,
        order: dict[str, Any],
        quantity: float,
        remaining_quantity: float,
    ) -> OrderStatus:
        status_value = (
            order.get("status")
            or order.get("order_status")
            or order.get("orderStatus")
            or order.get("state")
        )
        if status_value is not None:
            normalized = str(status_value).strip().lower()
            if normalized.startswith("order_status_"):
                normalized = normalized.removeprefix("order_status_")
            if normalized in {"live", "resting", "open", "booked", "unmatched"}:
                return OrderStatus.RESTING
            if normalized in {"pending", "queued", "accepted", "processing"}:
                return OrderStatus.PENDING
            if normalized in {
                "partial",
                "partially_filled",
                "partially-filled",
                "partially matched",
                "partially_matched",
                "matched_partially",
            }:
                return OrderStatus.PARTIALLY_FILLED
            if normalized in {"filled", "matched", "complete", "completed"}:
                return OrderStatus.FILLED
            if (
                normalized in {"cancelled", "canceled"}
                or normalized.startswith("canceled_")
                or normalized.startswith("cancelled_")
            ):
                return OrderStatus.CANCELLED
            if normalized in {"rejected", "error", "failed"} or normalized.startswith(
                "rejected_"
            ):
                return OrderStatus.REJECTED
        matched = max(0.0, quantity - remaining_quantity)
        if matched > 0.0:
            if remaining_quantity > 0.0:
                return OrderStatus.PARTIALLY_FILLED
            return OrderStatus.FILLED
        return OrderStatus.RESTING

    def _refresh_live_fills_cache(self, *, observed_at: datetime | None = None) -> None:
        self._set_live_fills_cache(
            self._list_fills_rest_raw(None),
            observed_at=observed_at,
        )

    def live_user_state_delta(
        self, contract: Contract | None = None
    ) -> LiveUserStateDelta | None:
        with self._live_state_lock:
            order_reason = self._live_state_order_fallback_reason_locked()
            fill_reason = self._live_state_fill_fallback_reason_locked()
            if order_reason is not None and fill_reason is not None:
                return None
            order_payloads = [
                dict(order) for order in self._live_state_orders_raw.values()
            ]
            fill_payloads = [
                dict(self._live_state_fills_raw[fill_key])
                for fill_key in self._live_state_fill_order
                if fill_key in self._live_state_fills_raw
            ]
            terminal_order_ids = [
                order_id
                for order_id, (
                    terminal_at,
                    _status,
                ) in self._live_state_terminal_order_markers.items()
                if terminal_at is not None
            ]
            observed_at = self._live_state_last_update_at

        open_orders = (
            tuple(self._normalize_open_orders(order_payloads, contract))
            if order_reason is None
            else tuple()
        )
        fills = (
            tuple(self._normalize_fills(fill_payloads, contract))
            if fill_reason is None
            else tuple()
        )
        return LiveUserStateDelta(
            source="polymarket_live_user_state",
            observed_at=observed_at,
            open_orders=open_orders,
            fills=fills,
            terminal_order_ids=tuple(terminal_order_ids),
            reason=(
                ",".join(
                    part for part in (order_reason, fill_reason) if part is not None
                )
                or None
            ),
        )

    def _record_live_state_error(self, exc: Exception | str) -> None:
        with self._live_state_lock:
            self._live_state_last_error = str(exc)

    def _live_state_auth_payload(self) -> dict[str, str]:
        client = self._ensure_client()
        creds = getattr(client, "creds", None)
        if creds is None:
            raise RuntimeError("Polymarket API credentials are unavailable for user WS")
        return {
            "apiKey": str(creds.api_key),
            "secret": str(creds.api_secret),
            "passphrase": str(creds.api_passphrase),
        }

    def _live_state_subscription_payload(
        self, markets: tuple[str, ...]
    ) -> dict[str, Any]:
        return {
            "type": "user",
            "auth": self._live_state_auth_payload(),
            "markets": list(markets),
        }

    def _market_state_subscription_assets(self) -> tuple[str, ...]:
        configured = {
            str(asset) for asset in (self.config.live_market_assets or []) if str(asset)
        }
        return tuple(sorted(configured | self._market_state_tracked_assets))

    def _market_state_subscription_payload(
        self, assets: tuple[str, ...]
    ) -> dict[str, Any]:
        return {
            "type": "market",
            "assets_ids": list(assets),
            "custom_feature_enabled": True,
            "initial_dump": True,
            "level": 2,
        }

    def _connect_live_market_websocket(self):
        try:
            ws_module = importlib.import_module("websockets.sync.client")
            connect = getattr(ws_module, "connect")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "websockets is required for Polymarket live market state support"
            ) from exc
        return connect(
            self.config.market_ws_host,
            open_timeout=self.config.request_timeout_seconds,
        )

    def _connect_live_user_websocket(self):
        try:
            ws_module = importlib.import_module("websockets.sync.client")
            connect = getattr(ws_module, "connect")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "websockets is required for Polymarket live user state support"
            ) from exc
        return connect(
            self.config.user_ws_host,
            open_timeout=self.config.request_timeout_seconds,
        )

    def _live_state_recv(self, websocket) -> str | None:
        recv = getattr(websocket, "recv")
        try:
            message = recv(timeout=1.0)
        except TypeError:
            message = recv()
        if message is None:
            return None
        if isinstance(message, bytes):
            return message.decode("utf-8")
        return str(message)

    def _live_state_send_ping(self, websocket) -> None:
        ping = getattr(websocket, "ping", None)
        if callable(ping):
            ping()
            return
        send = getattr(websocket, "send", None)
        if callable(send):
            send("PING")

    def _is_terminal_live_order(self, payload: dict[str, Any]) -> bool:
        status = (
            payload.get("status")
            or payload.get("order_status")
            or payload.get("orderStatus")
            or payload.get("state")
        )
        if status is None:
            return False
        normalized = str(status).strip().lower()
        if normalized.startswith("order_status_"):
            normalized = normalized.removeprefix("order_status_")
        return (
            normalized
            in {
                "cancelled",
                "canceled",
                "filled",
                "matched",
                "complete",
                "completed",
                "rejected",
                "failed",
            }
            or normalized.startswith("canceled_")
            or normalized.startswith("cancelled_")
            or normalized.startswith("rejected_")
        )

    def _fill_order_id(self, trade: dict[str, Any]) -> str:
        return str(
            trade.get("taker_order_id")
            or trade.get("takerOrderId")
            or trade.get("order_id")
            or trade.get("orderId")
            or trade.get("maker_order_id")
            or trade.get("makerOrderId")
            or trade.get("id")
            or trade.get("trade_id")
            or trade.get("tradeId")
            or trade.get("match_id")
            or trade.get("matchId")
            or ""
        )

    def _raw_fill_timestamp(self, trade: dict[str, Any], *keys: str) -> datetime | None:
        for key in keys:
            parsed = self._parse_datetime_value(trade.get(key))
            if parsed is not None:
                return parsed
        return None

    def _looks_like_live_fill(self, payload: dict[str, Any]) -> bool:
        if any(
            key in payload
            for key in (
                "fill_id",
                "fillId",
                "trade_id",
                "tradeId",
                "match_id",
                "matchId",
            )
        ):
            return True
        has_order_reference = any(
            key in payload
            for key in (
                "taker_order_id",
                "takerOrderId",
                "maker_order_id",
                "makerOrderId",
                "order_id",
                "orderId",
                "id",
            )
        )
        has_trade_fields = any(
            key in payload
            for key in (
                "price",
                "size",
                "quantity",
                "filled_size",
                "filledSize",
            )
        )
        has_asset_reference = any(
            key in payload
            for key in ("asset_id", "assetId", "token_id", "tokenId", "side")
        )
        return has_order_reference and has_trade_fields and has_asset_reference

    def _iter_live_order_payloads(self, message: Any) -> list[dict[str, Any]]:
        if not isinstance(message, dict):
            return []
        candidates: list[Any] = []
        for key in ("orders", "order"):
            if key in message:
                candidates.append(message[key])
        payload = message.get("payload") or message.get("data")
        if payload is not None:
            candidates.append(payload)
        event_type = str(message.get("event_type") or message.get("type") or "").lower()
        if not candidates and event_type == "order":
            candidates.append(message)

        normalized: list[dict[str, Any]] = []
        for candidate in candidates:
            if isinstance(candidate, dict):
                if any(
                    key in candidate
                    for key in (
                        "id",
                        "orderID",
                        "order_id",
                        "asset_id",
                        "token_id",
                    )
                ):
                    normalized.append(candidate)
            elif isinstance(candidate, list):
                normalized.extend(item for item in candidate if isinstance(item, dict))
        return normalized

    def _iter_live_fill_payloads(self, message: Any) -> list[dict[str, Any]]:
        if not isinstance(message, dict):
            return []
        candidates: list[Any] = []
        for key in ("fills", "fill", "trades", "trade"):
            if key in message:
                candidates.append(message[key])
        payload = message.get("payload") or message.get("data")
        if isinstance(payload, dict):
            for key in ("fills", "fill", "trades", "trade"):
                if key in payload:
                    candidates.append(payload[key])
        if payload is not None:
            candidates.append(payload)
        event_type = str(message.get("event_type") or message.get("type") or "").lower()
        if not candidates and event_type in {"trade", "fill", "match"}:
            candidates.append(message)

        normalized: list[dict[str, Any]] = []
        for candidate in candidates:
            if isinstance(candidate, dict):
                if self._looks_like_live_fill(candidate):
                    normalized.append(candidate)
            elif isinstance(candidate, list):
                normalized.extend(
                    item
                    for item in candidate
                    if isinstance(item, dict) and self._looks_like_live_fill(item)
                )
        return normalized

    def _market_message_asset_ids(self, message: dict[str, Any]) -> list[str]:
        payload = (
            message.get("payload") if isinstance(message.get("payload"), dict) else None
        )
        candidates = [message, payload or {}]
        asset_ids: list[str] = []
        for candidate in candidates:
            for key in ("asset_id", "assetId"):
                value = candidate.get(key)
                if value not in (None, ""):
                    asset_ids.append(str(value))
            for key in ("assets_ids", "asset_ids", "clob_token_ids"):
                values = candidate.get(key)
                if isinstance(values, list):
                    asset_ids.extend(
                        str(value) for value in values if value not in (None, "")
                    )
            price_changes = candidate.get("price_changes")
            if isinstance(price_changes, list):
                for change in price_changes:
                    if not isinstance(change, dict):
                        continue
                    for key in ("asset_id", "assetId"):
                        value = change.get(key)
                        if value not in (None, ""):
                            asset_ids.append(str(value))
        return list(dict.fromkeys(asset_ids))

    def _iter_market_price_changes(
        self, message: dict[str, Any]
    ) -> list[dict[str, Any]]:
        payload = (
            message.get("payload") if isinstance(message.get("payload"), dict) else None
        )
        candidates = [message, payload or {}]
        normalized: list[dict[str, Any]] = []
        for candidate in candidates:
            changes = candidate.get("price_changes")
            if isinstance(changes, list):
                normalized.extend(item for item in changes if isinstance(item, dict))
        if normalized:
            return normalized
        return [message]

    def _market_level_map(self, entries: Any) -> dict[float, float]:
        levels: dict[float, float] = {}
        for entry in entries or []:
            price = None
            size = None
            if isinstance(entry, dict):
                price = entry.get("price")
                size = entry.get("size") or entry.get("quantity") or entry.get("amount")
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                price, size = entry[0], entry[1]
            else:
                price = getattr(entry, "price", None)
                size = getattr(entry, "size", None)
            if price in (None, "") or size in (None, ""):
                continue
            levels[float(price)] = float(size)
        return levels

    def _market_state_snapshot_for_asset(self, asset_id: str) -> dict[str, Any]:
        state = self._market_state_books.get(asset_id, {})
        return {
            "asset_id": asset_id,
            "bids": dict(state.get("bids", {})),
            "asks": dict(state.get("asks", {})),
            "tick_size": state.get("tick_size"),
            "min_order_size": state.get("min_order_size"),
            "tradable": state.get("tradable", True),
            "active": state.get("active", True),
            "last_update_at": state.get("last_update_at"),
        }

    def _update_market_state_asset(
        self,
        asset_id: str,
        updater,
        *,
        observed_at: datetime,
    ) -> bool:
        state = self._market_state_snapshot_for_asset(asset_id)
        changed = updater(state)
        if not changed:
            return False
        state["last_update_at"] = observed_at
        self._market_state_books[asset_id] = state
        return True

    def _top_price(self, levels: dict[float, float], *, reverse: bool) -> float | None:
        if not levels:
            return None
        return max(levels) if reverse else min(levels)

    def _apply_market_state_message(self, payload: Any) -> None:
        messages = payload if isinstance(payload, list) else [payload]
        observed_at = datetime.now(timezone.utc)
        changed = False
        with self._market_state_lock:
            for message in messages:
                if not isinstance(message, dict):
                    continue
                event_type = str(
                    message.get("event_type") or message.get("type") or ""
                ).lower()
                asset_ids = self._market_message_asset_ids(message)
                if event_type == "best_bid_ask":
                    best_bid = message.get("best_bid")
                    best_ask = message.get("best_ask")
                    for asset_id in asset_ids:

                        def updater(state: dict[str, Any]) -> bool:
                            local_changed = False
                            if best_bid not in (None, ""):
                                state.setdefault("bids", {})[float(best_bid)] = (
                                    state.get("bids", {}).get(float(best_bid), 0.0)
                                )
                                local_changed = True
                            if best_ask not in (None, ""):
                                state.setdefault("asks", {})[float(best_ask)] = (
                                    state.get("asks", {}).get(float(best_ask), 0.0)
                                )
                                local_changed = True
                            return local_changed

                        changed = (
                            self._update_market_state_asset(
                                asset_id, updater, observed_at=observed_at
                            )
                            or changed
                        )
                elif event_type == "book":
                    bids = self._market_level_map(message.get("bids"))
                    asks = self._market_level_map(message.get("asks"))
                    for asset_id in asset_ids:

                        def updater(state: dict[str, Any]) -> bool:
                            state["bids"] = bids
                            state["asks"] = asks
                            return True

                        changed = (
                            self._update_market_state_asset(
                                asset_id, updater, observed_at=observed_at
                            )
                            or changed
                        )
                elif event_type == "price_change":
                    for price_change in self._iter_market_price_changes(message):
                        side = str(price_change.get("side") or "").lower()
                        price = price_change.get("price")
                        size = (
                            price_change.get("size")
                            or price_change.get("quantity")
                            or price_change.get("amount")
                        )
                        if price in (None, ""):
                            continue
                        level_price = float(price)
                        level_size = 0.0 if size in (None, "") else float(size)
                        change_asset_ids = self._market_message_asset_ids(price_change)
                        for asset_id in change_asset_ids or asset_ids:

                            def updater(state: dict[str, Any]) -> bool:
                                book_side = "bids" if side in {"buy", "bid"} else "asks"
                                levels = dict(state.get(book_side, {}))
                                if level_size <= 0.0:
                                    levels.pop(level_price, None)
                                else:
                                    levels[level_price] = level_size
                                state[book_side] = levels
                                return True

                            changed = (
                                self._update_market_state_asset(
                                    asset_id, updater, observed_at=observed_at
                                )
                                or changed
                            )
                elif event_type == "tick_size_change":
                    payload = (
                        message.get("payload")
                        if isinstance(message.get("payload"), dict)
                        else {}
                    )
                    tick_size = (
                        message.get("new_tick_size")
                        or message.get("newTickSize")
                        or message.get("tick_size")
                        or message.get("tickSize")
                        or payload.get("new_tick_size")
                        or payload.get("newTickSize")
                        or payload.get("tick_size")
                        or payload.get("tickSize")
                    )
                    if tick_size in (None, ""):
                        continue
                    resolved_tick_size = float(tick_size)
                    for asset_id in asset_ids:

                        def updater(state: dict[str, Any]) -> bool:
                            state["tick_size"] = resolved_tick_size
                            return True

                        changed = (
                            self._update_market_state_asset(
                                asset_id, updater, observed_at=observed_at
                            )
                            or changed
                        )
                elif event_type == "market_resolved":
                    for asset_id in asset_ids:

                        def updater(state: dict[str, Any]) -> bool:
                            state["tradable"] = False
                            state["active"] = False
                            return True

                        changed = (
                            self._update_market_state_asset(
                                asset_id, updater, observed_at=observed_at
                            )
                            or changed
                        )
                elif event_type == "new_market":
                    active = message.get("active")
                    for asset_id in asset_ids:

                        def updater(state: dict[str, Any]) -> bool:
                            if active in (None, ""):
                                return False
                            state["active"] = bool(active)
                            state["tradable"] = bool(active)
                            return True

                        changed = (
                            self._update_market_state_asset(
                                asset_id, updater, observed_at=observed_at
                            )
                            or changed
                        )
            if changed:
                self._market_state_initialized = True
                self._market_state_last_update_at = observed_at
                self._market_state_last_error = None

    def _run_market_state_session(self) -> None:
        assets = self._market_state_subscription_assets()
        if not assets:
            raise RuntimeError("Polymarket live market state requires tracked assets")
        websocket = self._connect_live_market_websocket()
        try:
            send = getattr(websocket, "send")
            send(json.dumps(self._market_state_subscription_payload(assets)))
            next_ping_at = time.monotonic() + max(
                1.0, self.config.live_market_ping_interval_seconds
            )
            while not self._market_state_stop_event.is_set():
                now = time.monotonic()
                if now >= next_ping_at:
                    self._live_state_send_ping(websocket)
                    next_ping_at = now + max(
                        1.0, self.config.live_market_ping_interval_seconds
                    )
                message = self._live_state_recv(websocket)
                if message in (None, "", "PONG"):
                    continue
                parsed = json.loads(message)
                if parsed == "PONG":
                    continue
                self._apply_market_state_message(parsed)
        finally:
            close = getattr(websocket, "close", None)
            if callable(close):
                close()

    def _market_state_loop(self) -> None:
        backoff = max(0.5, self.config.live_state_reconnect_backoff_seconds)
        max_backoff = max(backoff, self.config.live_state_reconnect_max_backoff_seconds)
        try:
            while not self._market_state_stop_event.is_set():
                with self._market_state_lock:
                    self._set_market_state_mode_locked(
                        "recovering", reason="reconnecting live market state"
                    )
                try:
                    self._run_market_state_session()
                    backoff = max(0.5, self.config.live_state_reconnect_backoff_seconds)
                except Exception as exc:
                    with self._market_state_lock:
                        self._set_market_state_mode_locked("degraded", reason=str(exc))
                    if self._market_state_stop_event.wait(backoff):
                        break
                    backoff = min(max_backoff, backoff * 2)
        finally:
            with self._market_state_lock:
                self._market_state_running = False
                self._market_state_thread = None

    def start_live_market_state(self) -> MarketStateStatus:
        desired_assets = self._market_state_subscription_assets()
        with self._market_state_lock:
            running_thread = self._market_state_thread
            running = running_thread is not None and running_thread.is_alive()
            current_assets = self._market_state_assets
        if running and desired_assets and desired_assets != current_assets:
            self.stop_live_market_state()

        with self._market_state_lock:
            if (
                self._market_state_thread is not None
                and self._market_state_thread.is_alive()
            ):
                self._market_state_active = True
                return self._market_state_status_locked()
            self._market_state_stop_event = threading.Event()
            self._market_state_active = True
            self._market_state_running = False
            self._market_state_last_error = None
            self._market_state_assets = desired_assets
            self._set_market_state_mode_locked(
                "recovering", reason="starting live market state"
            )
            if not desired_assets:
                return self._market_state_status_locked()
            thread = threading.Thread(
                target=self._market_state_loop,
                name="polymarket-live-market-state",
                daemon=True,
            )
            self._market_state_thread = thread
            self._market_state_running = True
            thread.start()
            return self._market_state_status_locked()

    def stop_live_market_state(self) -> MarketStateStatus:
        with self._market_state_lock:
            thread = self._market_state_thread
            self._market_state_active = False
            self._market_state_running = False
            self._set_market_state_mode_locked("inactive")
            self._market_state_stop_event.set()
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=max(0.1, self.config.request_timeout_seconds))
        with self._market_state_lock:
            self._market_state_thread = None
            return self._market_state_status_locked()

    def _ensure_market_state_asset(self, asset_id: str) -> None:
        if asset_id in self._market_state_tracked_assets:
            return
        self._market_state_tracked_assets.add(asset_id)
        if self._market_state_active:
            self.start_live_market_state()

    def _apply_live_state_message(self, message: dict[str, Any]) -> None:
        orders = self._iter_live_order_payloads(message)
        fills = self._iter_live_fill_payloads(message)
        if not orders and not fills:
            return
        observed_at = datetime.now(timezone.utc)
        if orders:
            with self._live_state_lock:
                orders_changed = False
                for order in orders:
                    order_id = str(
                        order.get("id")
                        or order.get("orderID")
                        or order.get("order_id")
                        or ""
                    )
                    if not order_id:
                        continue
                    event_at = self._live_order_event_time(order, observed_at)
                    tombstone = self._live_state_terminal_order_markers.get(order_id)
                    if tombstone is not None and event_at <= tombstone[0]:
                        continue
                    existing = dict(self._live_state_orders_raw.get(order_id, {}))
                    existing_event_at = self._raw_live_event_time(existing)
                    if existing_event_at is not None and event_at < existing_event_at:
                        continue
                    existing.update(order)
                    existing["__live_event_at"] = event_at.isoformat()
                    if self._is_terminal_live_order(existing):
                        self._live_state_orders_raw.pop(order_id, None)
                        self._live_state_terminal_order_markers[order_id] = (
                            event_at,
                            str(
                                existing.get("status")
                                or existing.get("order_status")
                                or existing.get("orderStatus")
                                or existing.get("state")
                                or ""
                            )
                            or None,
                        )
                        orders_changed = True
                        continue
                    self._live_state_terminal_order_markers.pop(order_id, None)
                    self._live_state_orders_raw[order_id] = existing
                    orders_changed = True
                if orders_changed:
                    self._live_state_initialized = True
                    self._live_state_last_update_at = observed_at
                    self._live_state_last_error = None
                    self._live_state_markets = self._live_state_subscription_markets()
        if fills:
            self._merge_live_fills_cache(fills, observed_at=observed_at)

    def _run_live_state_session(self) -> None:
        markets = self._live_state_subscription_markets()
        if not markets:
            raise RuntimeError(
                "Polymarket live user stream requires configured or discoverable condition ids"
            )
        try:
            self._refresh_live_fills_cache(observed_at=datetime.now(timezone.utc))
        except Exception as exc:
            self._record_live_state_error(exc)
        websocket = self._connect_live_user_websocket()
        try:
            send = getattr(websocket, "send")
            send(json.dumps(self._live_state_subscription_payload(markets)))
            next_ping_at = time.monotonic() + max(
                1.0, self.config.live_state_ping_interval_seconds
            )
            while not self._live_state_stop_event.is_set():
                now = time.monotonic()
                if now >= next_ping_at:
                    self._live_state_send_ping(websocket)
                    next_ping_at = now + max(
                        1.0, self.config.live_state_ping_interval_seconds
                    )
                message = self._live_state_recv(websocket)
                if message in (None, "", "PONG"):
                    continue
                payload = json.loads(message)
                if payload == "PONG":
                    continue
                self._apply_live_state_message(payload)
        finally:
            close = getattr(websocket, "close", None)
            if callable(close):
                close()

    def start_live_user_state(self) -> LiveStateStatus:
        if not self._live_state_supported():
            return self.live_state_status()

        desired_markets = self._live_state_subscription_markets()
        with self._live_state_lock:
            running_thread = self._live_state_thread
            running = running_thread is not None and running_thread.is_alive()
            current_markets = self._live_state_markets
            current_mode = self._live_state_mode
        if running and (
            (desired_markets and desired_markets != current_markets)
            or current_mode == "degraded"
        ):
            self.stop_live_user_state()

        with self._live_state_lock:
            if (
                self._live_state_thread is not None
                and self._live_state_thread.is_alive()
            ):
                self._live_state_active = True
                return self._live_state_status_locked()
            self._live_state_stop_event = threading.Event()
            self._live_state_active = True
            self._live_state_running = False
            self._live_state_last_error = None
            self._mark_live_state_recovering_locked("starting live user state")

        try:
            self._live_state_bootstrap()
        except Exception as exc:
            self._record_live_state_error(exc)
            with self._live_state_lock:
                self._set_live_state_mode_locked("degraded", reason=str(exc))
                return self._live_state_status_locked()

        with self._live_state_lock:
            if not self._live_state_markets:
                self._live_state_last_error = (
                    "Polymarket live user stream requires condition ids"
                )
                self._set_live_state_mode_locked(
                    "degraded", reason=self._live_state_last_error
                )
                return self._live_state_status_locked()
            thread = threading.Thread(
                target=self._live_state_loop,
                name="polymarket-live-user-state",
                daemon=True,
            )
            self._live_state_thread = thread
            self._live_state_running = True
            thread.start()
            return self._live_state_status_locked()

    def _live_state_loop(self) -> None:
        backoff = max(0.5, self.config.live_state_reconnect_backoff_seconds)
        max_backoff = max(backoff, self.config.live_state_reconnect_max_backoff_seconds)
        try:
            while not self._live_state_stop_event.is_set():
                with self._live_state_lock:
                    self._mark_live_state_recovering_locked(
                        "reconnecting live user state"
                    )
                try:
                    self._run_live_state_session()
                    backoff = max(0.5, self.config.live_state_reconnect_backoff_seconds)
                except Exception as exc:
                    self._record_live_state_error(exc)
                    with self._live_state_lock:
                        self._set_live_state_mode_locked("degraded", reason=str(exc))
                    if self._live_state_stop_event.wait(backoff):
                        break
                    backoff = min(max_backoff, backoff * 2)
        finally:
            with self._live_state_lock:
                self._live_state_running = False
                self._live_state_thread = None

    def stop_live_user_state(self) -> LiveStateStatus:
        with self._live_state_lock:
            thread = self._live_state_thread
            self._live_state_active = False
            self._live_state_running = False
            self._set_live_state_mode_locked("inactive")
            self._live_state_stop_event.set()
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=max(0.1, self.config.request_timeout_seconds))
        with self._live_state_lock:
            self._live_state_thread = None
            return self._live_state_status_locked()

    def _response_message(self, payload: dict[str, Any]) -> str | None:
        for key in ("error", "errorMsg", "error_message", "message", "detail"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    def _response_order_id(self, payload: dict[str, Any]) -> str | None:
        for key in ("orderID", "orderId", "order_id", "id"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    def _market_state_book_fallback_reason_locked(self) -> str | None:
        if not self._market_state_active:
            return "market_state_inactive"
        if self._market_state_mode == "recovering":
            return "market_state_recovering"
        if self._market_state_mode == "degraded":
            return "market_state_degraded"
        if not self._market_state_initialized:
            return "market_state_cold"
        if not self._market_state_fresh_locked():
            return "market_state_stale"
        return None

    def _market_state_levels(
        self, levels: dict[float, float], *, reverse: bool
    ) -> list[PriceLevel]:
        ordered_prices = sorted(levels.keys(), reverse=reverse)
        return [
            PriceLevel(price=price, quantity=levels[price]) for price in ordered_prices
        ]

    def _overlay_market_state_on_order_book(
        self, rest_book: OrderBookSnapshot
    ) -> OrderBookSnapshot:
        asset_id = rest_book.contract.symbol
        with self._market_state_lock:
            fallback_reason = self._market_state_book_fallback_reason_locked()
            if fallback_reason is not None:
                self._market_state_last_snapshot_book_overlay_source = "rest_only"
                self._market_state_last_snapshot_book_overlay_reason = fallback_reason
                self._market_state_last_snapshot_book_overlay_applied = False
                return rest_book
            state = dict(self._market_state_books.get(asset_id, {}))

        if not state:
            with self._market_state_lock:
                self._market_state_last_snapshot_book_overlay_source = "rest_only"
                self._market_state_last_snapshot_book_overlay_reason = (
                    "market_state_asset_missing"
                )
                self._market_state_last_snapshot_book_overlay_applied = False
            return rest_book

        tradable = bool(state.get("tradable", True) and state.get("active", True))
        bids = self._market_state_levels(dict(state.get("bids", {})), reverse=True)
        asks = self._market_state_levels(dict(state.get("asks", {})), reverse=False)
        observed_at = state.get("last_update_at") or rest_book.observed_at
        if not tradable:
            overlay = replace(
                rest_book,
                bids=[],
                asks=[],
                midpoint=None,
                observed_at=observed_at,
                raw={"rest": rest_book.raw, "live_market": state},
            )
            with self._market_state_lock:
                self._market_state_last_snapshot_book_overlay_source = (
                    "rest_plus_live_market"
                )
                self._market_state_last_snapshot_book_overlay_reason = None
                self._market_state_last_snapshot_book_overlay_applied = True
            return overlay

        if not bids and not asks:
            with self._market_state_lock:
                self._market_state_last_snapshot_book_overlay_source = "rest_only"
                self._market_state_last_snapshot_book_overlay_reason = (
                    "market_state_empty"
                )
                self._market_state_last_snapshot_book_overlay_applied = False
            return rest_book

        midpoint = None
        if bids and asks:
            midpoint = (bids[0].price + asks[0].price) / 2
        elif bids:
            midpoint = bids[0].price
        elif asks:
            midpoint = asks[0].price
        overlay = replace(
            rest_book,
            bids=bids or rest_book.bids,
            asks=asks or rest_book.asks,
            midpoint=midpoint if midpoint is not None else rest_book.midpoint,
            observed_at=observed_at,
            raw={"rest": rest_book.raw, "live_market": state},
        )
        with self._market_state_lock:
            self._market_state_last_snapshot_book_overlay_source = (
                "rest_plus_live_market"
            )
            self._market_state_last_snapshot_book_overlay_reason = None
            self._market_state_last_snapshot_book_overlay_applied = True
        return overlay

    def _market_state_order_book(
        self, contract: Contract, state: dict[str, Any]
    ) -> OrderBookSnapshot:
        bids = self._market_state_levels(dict(state.get("bids", {})), reverse=True)
        asks = self._market_state_levels(dict(state.get("asks", {})), reverse=False)
        midpoint = None
        if bids and asks:
            midpoint = (bids[0].price + asks[0].price) / 2
        elif bids:
            midpoint = bids[0].price
        elif asks:
            midpoint = asks[0].price
        return OrderBookSnapshot(
            contract=contract,
            bids=bids,
            asks=asks,
            midpoint=midpoint,
            observed_at=state.get("last_update_at") or datetime.now(timezone.utc),
            raw={"live_market": state},
        )

    def _assess_order_intent_depth(
        self, intent: OrderIntent, state: dict[str, Any]
    ) -> OrderBookExecutionAssessment:
        book = self._market_state_order_book(intent.contract, state)
        visible_quantity = book.cumulative_quantity(
            intent.action,
            limit_price=intent.price,
            max_levels=self.config.depth_admission_levels,
        )
        max_admissible_quantity = (
            visible_quantity * self.config.depth_admission_liquidity_fraction
        )
        estimate = book.estimate_fill(
            intent.action,
            intent.quantity,
            limit_price=intent.price,
            max_levels=self.config.depth_admission_levels,
        )
        reference_price = (
            book.best_ask if intent.action is OrderAction.BUY else book.best_bid
        )
        expected_slippage_bps = estimate.expected_slippage_bps(
            reference_price=reference_price,
            action=intent.action,
        )
        return OrderBookExecutionAssessment(
            action=intent.action,
            requested_quantity=intent.quantity,
            visible_quantity=visible_quantity,
            max_admissible_quantity=max_admissible_quantity,
            expected_slippage_bps=expected_slippage_bps,
            depth_levels_used=estimate.levels_consumed,
            complete_within_visible_depth=estimate.complete,
        )

    def admit_limit_order(self, intent: OrderIntent) -> OrderAdmissionDecision:
        with self._market_state_lock:
            fallback_reason = self._market_state_book_fallback_reason_locked()
            state = dict(self._market_state_books.get(intent.contract.symbol, {}))

        if fallback_reason == "market_state_inactive":
            return OrderAdmissionDecision("allow")
        if fallback_reason in {
            "market_state_recovering",
            "market_state_degraded",
            "market_state_cold",
            "market_state_stale",
        }:
            return OrderAdmissionDecision(
                "refresh_then_retry",
                reason=f"live market overlay unavailable: {fallback_reason}",
                scope=intent.contract.market_key,
            )

        tradable = bool(state.get("tradable", True) and state.get("active", True))
        if not tradable:
            return OrderAdmissionDecision(
                "deny",
                reason="live market state marks market non-tradable",
                scope=intent.contract.market_key,
            )

        assessment = self._assess_order_intent_depth(intent, state)
        assessment_payload = {
            "action": assessment.action.value,
            "requested_quantity": assessment.requested_quantity,
            "visible_quantity": assessment.visible_quantity,
            "max_admissible_quantity": assessment.max_admissible_quantity,
            "expected_slippage_bps": assessment.expected_slippage_bps,
            "depth_levels_used": assessment.depth_levels_used,
            "complete_within_visible_depth": assessment.complete_within_visible_depth,
        }

        bids = dict(state.get("bids", {}))
        asks = dict(state.get("asks", {}))
        best_bid = max(bids) if bids else None
        best_ask = min(asks) if asks else None

        tick_size = state.get("tick_size")
        if tick_size not in (None, ""):
            resolved_tick_size = float(tick_size)
            if (
                intent.price < resolved_tick_size
                or intent.price > 1.0 - resolved_tick_size
            ):
                return OrderAdmissionDecision(
                    "deny",
                    reason=(
                        "price outside venue tick-size bounds "
                        f"({intent.price:.4f} not in [{resolved_tick_size:.4f}, {1.0 - resolved_tick_size:.4f}])"
                    ),
                    scope=intent.contract.market_key,
                    assessment=assessment_payload,
                )
            scaled_price = round(intent.price / resolved_tick_size)
            aligned_price = round(scaled_price * resolved_tick_size, 10)
            if abs(aligned_price - intent.price) > 1e-9:
                return OrderAdmissionDecision(
                    "deny",
                    reason=(
                        "price does not align with live tick size "
                        f"({intent.price:.4f} vs tick {resolved_tick_size:.4f})"
                    ),
                    scope=intent.contract.market_key,
                    assessment=assessment_payload,
                )

        min_order_size = state.get("min_order_size")
        if min_order_size not in (None, "") and intent.quantity < float(min_order_size):
            return OrderAdmissionDecision(
                "deny",
                reason=(
                    "quantity below live minimum order size "
                    f"({intent.quantity:.4f} < {float(min_order_size):.4f})"
                ),
                scope=intent.contract.market_key,
                assessment=assessment_payload,
            )

        if intent.action is OrderAction.BUY and best_ask is None:
            return OrderAdmissionDecision(
                "refresh_then_retry",
                reason="live market best ask unavailable",
                scope=intent.contract.market_key,
                assessment=assessment_payload,
            )
        if intent.action is OrderAction.SELL and best_bid is None:
            return OrderAdmissionDecision(
                "refresh_then_retry",
                reason="live market best bid unavailable",
                scope=intent.contract.market_key,
                assessment=assessment_payload,
            )

        if (
            intent.post_only
            and best_ask is not None
            and intent.action is OrderAction.BUY
        ):
            if intent.price >= best_ask:
                return OrderAdmissionDecision(
                    "deny",
                    reason="post-only buy would cross live best ask",
                    scope=intent.contract.market_key,
                    assessment=assessment_payload,
                )
        if (
            intent.post_only
            and best_bid is not None
            and intent.action is OrderAction.SELL
        ):
            if intent.price <= best_bid:
                return OrderAdmissionDecision(
                    "deny",
                    reason="post-only sell would cross live best bid",
                    scope=intent.contract.market_key,
                    assessment=assessment_payload,
                )

        if not intent.post_only:
            if assessment.visible_quantity <= 0.0:
                return OrderAdmissionDecision(
                    "refresh_then_retry",
                    reason="no visible depth available within limit price",
                    scope=intent.contract.market_key,
                    assessment=assessment_payload,
                )
            if assessment.max_admissible_quantity <= 0.0:
                return OrderAdmissionDecision(
                    "deny",
                    reason="visible depth does not support any admissible size",
                    scope=intent.contract.market_key,
                    assessment=assessment_payload,
                )
            if (
                self.config.depth_admission_max_expected_slippage_bps is not None
                and assessment.expected_slippage_bps is not None
                and assessment.expected_slippage_bps
                > self.config.depth_admission_max_expected_slippage_bps
            ):
                return OrderAdmissionDecision(
                    "shrink_to_size",
                    reason=(
                        "expected slippage exceeds depth admission limit "
                        f"({assessment.expected_slippage_bps:.2f}bps > "
                        f"{self.config.depth_admission_max_expected_slippage_bps:.2f}bps)"
                    ),
                    scope=intent.contract.market_key,
                    adjusted_quantity=round(assessment.max_admissible_quantity, 4),
                    assessment=assessment_payload,
                )
            if assessment.requested_quantity > assessment.max_admissible_quantity:
                return OrderAdmissionDecision(
                    "shrink_to_size",
                    reason=(
                        "requested quantity exceeds configured visible depth envelope "
                        f"({assessment.requested_quantity:.4f} > "
                        f"{assessment.max_admissible_quantity:.4f})"
                    ),
                    scope=intent.contract.market_key,
                    adjusted_quantity=round(assessment.max_admissible_quantity, 4),
                    assessment=assessment_payload,
                )

        return OrderAdmissionDecision("allow", assessment=assessment_payload)

    def _placement_status(self, payload: dict[str, Any]) -> OrderStatus:
        status_value = (
            payload.get("status")
            or payload.get("order_status")
            or payload.get("orderStatus")
            or payload.get("state")
        )
        if status_value is not None:
            normalized = str(status_value).strip().lower()
            if normalized in {"live", "resting", "open", "booked", "unmatched"}:
                return OrderStatus.RESTING
            if normalized in {"pending", "queued", "accepted", "processing"}:
                return OrderStatus.PENDING
            if normalized in {
                "partial",
                "partially_filled",
                "partially-filled",
                "partially matched",
                "partially_matched",
            }:
                return OrderStatus.PARTIALLY_FILLED
            if normalized in {"filled", "matched", "complete", "completed"}:
                return OrderStatus.FILLED
            if normalized in {"cancelled", "canceled"}:
                return OrderStatus.CANCELLED
            if normalized in {"rejected", "error", "failed"}:
                return OrderStatus.REJECTED

        if payload.get("success") is False or payload.get("accepted") is False:
            return OrderStatus.REJECTED

        matched_size = payload.get("matched_size")
        if matched_size is None:
            matched_size = payload.get("size_matched")
        remaining_size = payload.get("remaining_size")
        if remaining_size is None:
            remaining_size = payload.get("size")
        try:
            matched = float(matched_size) if matched_size is not None else 0.0
            remaining = float(remaining_size) if remaining_size is not None else None
        except (TypeError, ValueError):
            matched = 0.0
            remaining = None
        if matched > 0.0:
            if remaining is None or remaining > 0.0:
                return OrderStatus.PARTIALLY_FILLED
            return OrderStatus.FILLED

        if self._response_order_id(payload) is not None:
            return OrderStatus.PENDING
        if payload.get("success") is True or payload.get("accepted") is True:
            return OrderStatus.PENDING
        return OrderStatus.UNKNOWN

    def _placement_result_from_response(self, response: Any) -> PlacementResult:
        if not isinstance(response, dict):
            return PlacementResult(
                False,
                status=OrderStatus.UNKNOWN,
                message="malformed Polymarket order placement response",
                raw=response,
            )

        order_id = self._response_order_id(response)
        status = self._placement_status(response)
        message = self._response_message(response)
        accepted_flag = response.get("accepted")
        if accepted_flag is None:
            accepted_flag = response.get("success")
        if isinstance(accepted_flag, str):
            accepted_flag = accepted_flag.strip().lower() in {
                "1",
                "true",
                "yes",
                "accepted",
                "ok",
            }
        accepted = status not in {OrderStatus.REJECTED, OrderStatus.CANCELLED}
        if accepted_flag is not None:
            accepted = bool(accepted_flag)
        elif order_id is None and status is OrderStatus.UNKNOWN:
            accepted = False
        if accepted and order_id is None and message is None:
            message = "Polymarket accepted placement without an order id"
        return PlacementResult(
            accepted,
            order_id=order_id,
            status=status,
            message=message,
            raw=response,
        )

    def _ensure_client(self):
        if self._client is not None:
            return self._client

        try:
            client_module = importlib.import_module("py_clob_client.client")
            ClobClient = getattr(client_module, "ClobClient")
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "py-clob-client is not installed. Install it with `pip install -e upstreams/py-clob-client` or `pip install py-clob-client`."
            ) from exc

        if self.config.private_key:
            client = ClobClient(
                self.config.host,
                key=self.config.private_key,
                chain_id=self.config.chain_id,
                signature_type=self.config.signature_type,
                funder=self.config.funder,
            )
            client.set_api_creds(
                client.create_or_derive_api_creds(self.config.api_creds_nonce)
            )
        else:
            client = ClobClient(self.config.host)

        self._configure_client_timeout()
        self._client = client
        return client

    def health(self) -> AdapterHealth:
        try:
            self._call_client("health check", "get_ok")
            return AdapterHealth(self.venue, True)
        except Exception as exc:  # pragma: no cover - network dependent
            return AdapterHealth(self.venue, False, str(exc))

    def list_markets(self, limit: int = 100) -> list[MarketSummary]:
        response = self._call_client("list markets", "get_simplified_markets")
        items = response.get("data") if isinstance(response, dict) else response
        if items is None and isinstance(response, dict):
            items = response.get("markets")
        summaries: list[MarketSummary] = []

        def _coerce_json_list(value: Any) -> list[Any] | None:
            if value in (None, ""):
                return None
            if isinstance(value, list):
                return value
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except ValueError:
                    return None
                return parsed if isinstance(parsed, list) else None
            return None

        for item in items or []:
            title = item.get("question") or item.get("title") or item.get("slug")
            category = item.get("category")
            sport = item.get("sport")
            series = item.get("series")
            event_key = (
                item.get("event_key")
                or item.get("eventKey")
                or item.get("event_slug")
                or item.get("eventSlug")
                or item.get("slug")
            )
            game_id = item.get("game_id") or item.get("gameId")
            sports_market_type = item.get("sports_market_type") or item.get(
                "sportsMarketType"
            )
            raw_tags = item.get("tags")
            tags: tuple[str, ...] = ()
            if isinstance(raw_tags, str):
                tags = tuple(tag.strip() for tag in raw_tags.split(",") if tag.strip())
            elif isinstance(raw_tags, list):
                tags = tuple(str(tag).strip() for tag in raw_tags if str(tag).strip())
            active = bool(item.get("active", True))
            volume = (
                item.get("volume") or item.get("volume24hr") or item.get("volume_num")
            )
            expires_raw = item.get("end_date_iso") or item.get("endDate")
            expires_at = None
            if expires_raw:
                try:
                    expires_at = datetime.fromisoformat(
                        str(expires_raw).replace("Z", "+00:00")
                    )
                except ValueError:
                    expires_at = None

            start_time_raw = item.get("gameStartTime") or item.get("start_time")
            start_time = None
            if start_time_raw:
                try:
                    start_time = datetime.fromisoformat(
                        str(start_time_raw).replace("Z", "+00:00")
                    )
                except ValueError:
                    start_time = None

            token_entries = item.get("tokens")
            if not token_entries:
                outcome_names = _coerce_json_list(item.get("outcomes"))
                outcome_prices = _coerce_json_list(
                    item.get("outcomePrices") or item.get("outcome_prices")
                )
                token_ids = _coerce_json_list(
                    item.get("clobTokenIds") or item.get("clob_token_ids")
                )
                if outcome_names and token_ids:
                    token_entries = []
                    for index, token_id in enumerate(token_ids):
                        token_entries.append(
                            {
                                "token_id": token_id,
                                "outcome": (
                                    outcome_names[index]
                                    if index < len(outcome_names)
                                    else None
                                ),
                                "midpoint": (
                                    outcome_prices[index]
                                    if outcome_prices and index < len(outcome_prices)
                                    else None
                                ),
                            }
                        )

            if token_entries:
                for token in token_entries:
                    if not isinstance(token, dict):
                        continue
                    symbol = str(
                        token.get("token_id")
                        or token.get("tokenId")
                        or token.get("asset_id")
                        or token.get("assetId")
                        or ""
                    )
                    if not symbol:
                        continue
                    condition_id = (
                        token.get("condition_id")
                        or token.get("conditionId")
                        or item.get("condition_id")
                        or item.get("conditionId")
                        or item.get("market")
                        or item.get("market_id")
                        or item.get("marketId")
                    )
                    self._cache_condition_mapping(symbol, condition_id)
                    outcome_text = str(
                        token.get("outcome") or token.get("name") or ""
                    ).lower()
                    outcome = (
                        OutcomeSide.YES
                        if outcome_text == "yes"
                        else OutcomeSide.NO
                        if outcome_text == "no"
                        else OutcomeSide.UNKNOWN
                    )
                    best_bid = token.get("best_bid") or token.get("bid")
                    best_ask = token.get("best_ask") or token.get("ask")
                    midpoint = token.get("midpoint")
                    summaries.append(
                        MarketSummary(
                            contract=Contract(
                                venue=self.venue,
                                symbol=symbol,
                                outcome=outcome,
                                title=title,
                            ),
                            title=title,
                            best_bid=float(best_bid) if best_bid is not None else None,
                            best_ask=float(best_ask) if best_ask is not None else None,
                            midpoint=(float(best_bid) + float(best_ask)) / 2
                            if best_bid is not None and best_ask is not None
                            else float(midpoint)
                            if midpoint is not None
                            else None,
                            volume=float(volume) if volume is not None else None,
                            category=category,
                            sport=str(sport) if sport not in (None, "") else None,
                            series=str(series) if series not in (None, "") else None,
                            event_key=(
                                str(event_key) if event_key not in (None, "") else None
                            ),
                            game_id=str(game_id) if game_id not in (None, "") else None,
                            sports_market_type=(
                                str(sports_market_type)
                                if sports_market_type not in (None, "")
                                else None
                            ),
                            start_time=start_time,
                            tags=tags,
                            active=active,
                            expires_at=expires_at,
                            raw={"market": item, "token": token},
                        )
                    )
            else:
                symbol = str(
                    item.get("token_id")
                    or item.get("tokenId")
                    or item.get("asset_id")
                    or item.get("assetId")
                    or item.get("condition_id")
                    or item.get("conditionId")
                    or ""
                )
                if not symbol:
                    continue
                best_bid = item.get("best_bid") or item.get("bid")
                best_ask = item.get("best_ask") or item.get("ask")
                summaries.append(
                    MarketSummary(
                        contract=Contract(venue=self.venue, symbol=symbol, title=title),
                        title=title,
                        best_bid=float(best_bid) if best_bid is not None else None,
                        best_ask=float(best_ask) if best_ask is not None else None,
                        midpoint=(float(best_bid) + float(best_ask)) / 2
                        if best_bid is not None and best_ask is not None
                        else None,
                        volume=float(volume) if volume is not None else None,
                        category=category,
                        sport=str(sport) if sport not in (None, "") else None,
                        series=str(series) if series not in (None, "") else None,
                        event_key=(
                            str(event_key) if event_key not in (None, "") else None
                        ),
                        game_id=str(game_id) if game_id not in (None, "") else None,
                        sports_market_type=(
                            str(sports_market_type)
                            if sports_market_type not in (None, "")
                            else None
                        ),
                        start_time=start_time,
                        tags=tags,
                        active=active,
                        expires_at=expires_at,
                        raw=item,
                    )
                )
        return summaries[:limit]

    def _extract_levels(self, entries: list[Any] | None) -> list[PriceLevel]:
        levels: list[PriceLevel] = []
        for entry in entries or []:
            price = getattr(entry, "price", None)
            size = getattr(entry, "size", None)
            if isinstance(entry, dict):
                price = entry.get("price", price)
                size = entry.get("size", size)
            if price is None or size is None:
                continue
            levels.append(PriceLevel(price=float(price), quantity=float(size)))
        levels.sort(key=lambda level: level.price, reverse=True)
        return levels

    def _account_address(self) -> str | None:
        if self.config.account_address:
            return self.config.account_address
        if self.config.funder:
            return self.config.funder
        try:
            client = self._ensure_client()
            return client.get_address()
        except Exception:
            return None

    def _parse_quantity(self, value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value)
        if not text:
            return 0.0
        if "." in text:
            return float(text)
        return float(text) / 1_000_000.0

    def _fetch_data_api(self, path: str, params: dict[str, Any]) -> Any:
        query = urlencode(
            {key: value for key, value in params.items() if value is not None}
        )
        url = (
            f"{self.config.data_api_host}{path}?{query}"
            if query
            else f"{self.config.data_api_host}{path}"
        )

        def fetch() -> Any:
            with urlopen(url, timeout=self.config.request_timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))

        return self._call_with_retry(f"data api fetch {path}", fetch)

    def get_order_book(self, contract: Contract) -> OrderBookSnapshot:
        self._ensure_market_state_asset(contract.symbol)
        with self._market_state_lock:
            was_recovering = self._market_state_mode == "recovering"
        book = self._call_client("get order book", "get_order_book", contract.symbol)
        bids = self._extract_levels(
            getattr(book, "bids", None) or getattr(book, "buy_orders", None)
        )
        asks = self._extract_levels(
            getattr(book, "asks", None) or getattr(book, "sell_orders", None)
        )
        midpoint = None
        try:
            midpoint_raw = self._call_client(
                "get midpoint", "get_midpoint", contract.symbol
            )
            midpoint = (
                float(midpoint_raw.get("mid", midpoint_raw))
                if isinstance(midpoint_raw, dict)
                else float(midpoint_raw)
            )
        except Exception:
            if bids and asks:
                midpoint = (bids[0].price + asks[0].price) / 2
        rest_book = OrderBookSnapshot(
            contract=contract,
            bids=bids,
            asks=sorted(asks, key=lambda l: l.price),
            midpoint=midpoint,
            raw=book,
        )
        with self._market_state_lock:
            state = self._market_state_snapshot_for_asset(contract.symbol)
            tick_size = getattr(book, "tick_size", None)
            min_order_size = getattr(book, "min_order_size", None)
            if tick_size not in (None, ""):
                state["tick_size"] = float(tick_size)
            if min_order_size not in (None, ""):
                state["min_order_size"] = float(min_order_size)
            state["last_update_at"] = rest_book.observed_at
            self._market_state_books[contract.symbol] = state
            if self._market_state_mode == "recovering":
                self._set_market_state_mode_locked("healthy")
                self._market_state_last_recovery_at = rest_book.observed_at
        if was_recovering:
            with self._market_state_lock:
                self._market_state_last_snapshot_book_overlay_source = "rest_only"
                self._market_state_last_snapshot_book_overlay_reason = (
                    "market_state_recovering"
                )
                self._market_state_last_snapshot_book_overlay_applied = False
            return rest_book
        return self._overlay_market_state_on_order_book(rest_book)

    def _list_open_orders_rest(
        self, contract: Contract | None = None
    ) -> list[dict[str, Any]]:
        if not self.config.private_key:
            return []
        try:
            clob_types = importlib.import_module("py_clob_client.clob_types")
            OpenOrderParams = getattr(clob_types, "OpenOrderParams")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "py-clob-client is required for authenticated order listing."
            ) from exc

        raw_orders = self._call_client(
            "list open orders", "get_orders", OpenOrderParams()
        )
        return [dict(order) for order in (raw_orders or [])]

    def list_open_orders(
        self, contract: Contract | None = None
    ) -> list[NormalizedOrder]:
        with self._live_state_lock:
            use_live_cache = self._live_state_order_fallback_reason_locked() is None
        if use_live_cache:
            return self._normalize_open_orders(
                self._live_state_read_orders_raw(), contract
            )
        return self._normalize_open_orders(
            self._list_open_orders_rest(contract), contract
        )

    def list_positions(
        self, contract: Contract | None = None
    ) -> list[PositionSnapshot]:
        account_address = self._account_address()
        if not account_address:
            return []
        raw_positions = self._fetch_data_api(
            "/positions",
            {
                "user": account_address,
                "sizeThreshold": 0,
                "limit": 500,
            },
        )

        normalized: list[PositionSnapshot] = []
        for position in raw_positions or []:
            symbol = str(position.get("asset") or "")
            outcome_text = str(position.get("outcome") or "").lower()
            normalized_contract = Contract(
                venue=self.venue,
                symbol=symbol,
                outcome=OutcomeSide.YES
                if outcome_text == "yes"
                else OutcomeSide.NO
                if outcome_text == "no"
                else OutcomeSide.UNKNOWN,
                title=position.get("title"),
            )
            snapshot = PositionSnapshot(
                contract=normalized_contract,
                quantity=float(position.get("size", 0.0) or 0.0),
                average_price=float(position.get("avgPrice", 0.0) or 0.0),
                mark_price=float(position.get("curPrice", 0.0) or 0.0),
                raw=position,
            )
            normalized.append(snapshot)
        if contract is None:
            return normalized
        return [
            position
            for position in normalized
            if position.contract.symbol == contract.symbol
        ]

    def _fill_confirmed(self, trade: dict[str, Any]) -> bool:
        status = trade.get("status")
        if status in (None, ""):
            return True
        normalized = str(status).strip().lower()
        return normalized in {
            "trade_status_confirmed",
            "confirmed",
            "filled",
            "matched",
            "complete",
            "completed",
        }

    def _normalize_fill(
        self, trade: dict[str, Any], contract: Contract | None = None
    ) -> FillSnapshot | None:
        if not self._fill_confirmed(trade):
            return None
        symbol_value = (
            trade.get("asset_id")
            or trade.get("assetId")
            or trade.get("token_id")
            or trade.get("tokenId")
            or (contract.symbol if contract else None)
        )
        if symbol_value in (None, ""):
            return None
        outcome_text = str(trade.get("outcome") or "").lower()
        fill_id = (
            str(
                trade.get("fill_id")
                or trade.get("fillId")
                or trade.get("id")
                or trade.get("trade_id")
                or trade.get("tradeId")
                or trade.get("match_id")
                or trade.get("matchId")
                or ""
            )
            or None
        )
        return FillSnapshot(
            order_id=self._fill_order_id(trade) or str(fill_id or ""),
            contract=Contract(
                venue=self.venue,
                symbol=str(symbol_value),
                outcome=OutcomeSide.YES
                if outcome_text == "yes"
                else OutcomeSide.NO
                if outcome_text == "no"
                else OutcomeSide.UNKNOWN,
            ),
            action=OrderAction.BUY
            if str(trade.get("side", "BUY")).upper() == "BUY"
            else OrderAction.SELL,
            price=float(trade.get("price", 0.0) or 0.0),
            quantity=self._parse_quantity(
                trade.get("size")
                or trade.get("quantity")
                or trade.get("filled_size")
                or trade.get("filledSize")
                or 0.0
            ),
            fee=float(
                trade.get("fee_rate_bps")
                or trade.get("feeRateBps")
                or trade.get("fee")
                or 0.0
            ),
            fill_id=fill_id,
            raw=trade,
        )

    def _normalize_fills(
        self, raw_trades: list[dict[str, Any]], contract: Contract | None = None
    ) -> list[FillSnapshot]:
        normalized: list[FillSnapshot] = []
        for trade in raw_trades or []:
            fill = self._normalize_fill(dict(trade), contract)
            if fill is not None:
                normalized.append(fill)
        if contract is None:
            return normalized
        return [fill for fill in normalized if fill.contract.symbol == contract.symbol]

    def _list_fills_rest_raw(
        self, contract: Contract | None = None
    ) -> list[dict[str, Any]]:
        if not self.config.private_key:
            return []
        account_address = self._account_address()
        if not account_address:
            return []
        try:
            clob_types = importlib.import_module("py_clob_client.clob_types")
            TradeParams = getattr(clob_types, "TradeParams")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "py-clob-client is required for authenticated trade history."
            ) from exc

        params = TradeParams(
            maker_address=account_address,
            asset_id=contract.symbol if contract else None,
        )
        raw_trades = self._call_client("list fills", "get_trades", params)
        return [dict(trade) for trade in (raw_trades or []) if isinstance(trade, dict)]

    def _list_fills_rest(
        self,
        contract: Contract | None = None,
        *,
        source: str,
        fallback_reason: str | None = None,
    ) -> list[FillSnapshot]:
        self._record_fill_read_source(
            source=source,
            fallback_reason=fallback_reason,
        )
        return self._normalize_fills(self._list_fills_rest_raw(contract), contract)

    def _snapshot_fills(self, contract: Contract | None = None) -> list[FillSnapshot]:
        rest_fills = self._list_fills_rest(
            contract,
            source="rest_snapshot",
            fallback_reason="snapshot_backstop",
        )
        return self._overlay_live_fills_on_snapshot(rest_fills, contract)

    def _snapshot_open_orders(
        self,
        contract: Contract | None = None,
    ) -> list[NormalizedOrder]:
        rest_orders = self._normalize_open_orders(
            self._list_open_orders_rest(contract),
            contract,
        )
        return self._overlay_live_open_orders_on_snapshot(rest_orders, contract)

    def list_fills(self, contract: Contract | None = None) -> list[FillSnapshot]:
        with self._live_state_lock:
            fallback_reason = self._live_state_fill_fallback_reason_locked()
            use_live_cache = fallback_reason is None
        if use_live_cache:
            self._record_fill_read_source(source="live_cache")
            return self._normalize_fills(self._live_state_read_fills_raw(), contract)
        return self._list_fills_rest(
            contract,
            source="rest",
            fallback_reason=fallback_reason,
        )

    def get_position(self, contract: Contract) -> PositionSnapshot:
        positions = self.list_positions(contract)
        return (
            positions[0]
            if positions
            else PositionSnapshot(contract=contract, quantity=0.0)
        )

    def get_balance(self) -> BalanceSnapshot:
        if not self.config.private_key:
            return BalanceSnapshot(
                venue=self.venue, available=0.0, total=0.0, currency="USDC"
            )
        clob_types = importlib.import_module("py_clob_client.clob_types")
        AssetType = getattr(clob_types, "AssetType")
        BalanceAllowanceParams = getattr(clob_types, "BalanceAllowanceParams")
        collateral = self._call_client(
            "get balance",
            "get_balance_allowance",
            params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
        )

        balance = float(collateral.get("balance", 0.0) or 0.0)
        allowance = float(collateral.get("allowance", balance) or balance)
        return BalanceSnapshot(
            venue=self.venue,
            available=min(balance, allowance),
            total=balance,
            currency="USDC",
            raw=collateral,
        )

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        issues: list[str] = []
        account_address = self._account_address()
        balance = BalanceSnapshot(
            venue=self.venue, available=0.0, total=0.0, currency="USDC"
        )
        positions: list[PositionSnapshot] = []
        open_orders: list[NormalizedOrder] = []
        fills: list[FillSnapshot] = []

        if not self.config.private_key:
            issues.append(
                "Polymarket adapter is running without authenticated account state"
            )
        if not account_address:
            issues.append(
                "Polymarket account address is unavailable for positions/fills recovery"
            )

        if self.config.private_key:
            try:
                balance = self.get_balance()
            except Exception as exc:
                issues.append(f"Polymarket balance truth could not be recovered: {exc}")
            try:
                open_orders = self._snapshot_open_orders(contract)
            except Exception as exc:
                issues.append(
                    f"Polymarket open-order truth could not be recovered: {exc}"
                )

        if account_address:
            try:
                positions = self.list_positions(contract)
            except Exception as exc:
                issues.append(
                    f"Polymarket position truth could not be recovered: {exc}"
                )
            if self.config.private_key:
                try:
                    fills = self._snapshot_fills(contract)
                except Exception as exc:
                    issues.append(
                        f"Polymarket fill truth could not be recovered: {exc}"
                    )

        return AccountSnapshot(
            venue=self.venue,
            balance=balance,
            positions=positions,
            open_orders=open_orders,
            fills=fills,
            complete=not issues,
            issues=issues,
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        if not self.config.private_key:
            return PlacementResult(
                False,
                status=OrderStatus.REJECTED,
                message="Adapter is in read-only mode",
            )

        unsupported_fields: list[str] = []
        if intent.reduce_only and intent.action is not OrderAction.SELL:
            unsupported_fields.append("reduce_only")
        if intent.client_order_id is not None:
            unsupported_fields.append("client_order_id")
        if unsupported_fields:
            return PlacementResult(
                False,
                status=OrderStatus.REJECTED,
                message=(
                    "Polymarket adapter does not support intent semantics: "
                    + ", ".join(unsupported_fields)
                ),
            )

        try:
            clob_types = importlib.import_module("py_clob_client.clob_types")
            constants = importlib.import_module(
                "py_clob_client.order_builder.constants"
            )
            OrderArgs = getattr(clob_types, "OrderArgs")
            OrderType = getattr(clob_types, "OrderType")
            BUY = getattr(constants, "BUY")
            SELL = getattr(constants, "SELL")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "py-clob-client is required for authenticated trading."
            ) from exc

        side = BUY if intent.action is OrderAction.BUY else SELL
        order_type = (
            OrderType.GTD if intent.expiration_ts is not None else OrderType.GTC
        )
        order = OrderArgs(
            token_id=intent.contract.symbol,
            price=float(intent.price),
            size=float(intent.quantity),
            side=side,
            expiration=int(intent.expiration_ts or 0),
        )
        client = self._ensure_client()
        signed = client.create_order(order)
        response = self._call_client(
            "place limit order",
            "post_order",
            signed,
            orderType=order_type,
            post_only=intent.post_only,
        )
        return self._placement_result_from_response(response)

    def cancel_order(self, order_id: str) -> bool:
        if not self.config.private_key:
            return False
        self._call_client("cancel order", "cancel", order_id)
        return True

    def cancel_all(self, contract: Contract | None = None) -> int:
        if not self.config.private_key:
            return 0
        if contract is None:
            self._call_client("cancel all orders", "cancel_all")
            return -1
        count = 0
        for order in self.list_open_orders(contract):
            self._call_client("cancel order", "cancel", order.order_id)
            count += 1
        return count

    def close(self) -> None:
        self.stop_live_market_state()
        self.stop_live_user_state()
        self.stop_heartbeat()
        self._market_state_books.clear()
        self._market_state_tracked_assets.clear()
        self._open_order_first_seen_at.clear()
        self._condition_id_by_token.clear()
        self._live_state_terminal_order_markers.clear()
        self._live_state_fills_raw.clear()
        self._live_state_fill_order.clear()
        self._live_state_fills_initialized = False
        self._live_state_fills_last_update_at = None
        self._live_state_last_fills_source = None
        self._live_state_last_fills_fallback_reason = None
        self._live_state_last_snapshot_open_order_overlay_count = 0
        self._live_state_last_snapshot_open_order_overlay_source = None
        self._live_state_last_snapshot_open_order_overlay_reason = None
        self._live_state_last_snapshot_fill_overlay_count = 0
        self._live_state_last_snapshot_fill_overlay_source = None
        self._live_state_last_snapshot_fill_overlay_reason = None
        self._client = None

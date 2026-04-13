from __future__ import annotations

import importlib
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from . import normalize


def live_state_auth_payload(adapter: Any) -> dict[str, str]:
    client = adapter._ensure_client()
    creds = getattr(client, "creds", None)
    if creds is None:
        raise RuntimeError("Polymarket API credentials are unavailable for user WS")
    return {
        "apiKey": str(creds.api_key),
        "secret": str(creds.api_secret),
        "passphrase": str(creds.api_passphrase),
    }


def live_state_subscription_payload(
    adapter: Any,
    markets: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "type": "user",
        "auth": live_state_auth_payload(adapter),
        "markets": list(markets),
    }


def connect_live_user_websocket(adapter: Any):
    try:
        ws_module = importlib.import_module("websockets.sync.client")
        connect = getattr(ws_module, "connect")
    except ImportError as exc:
        raise RuntimeError(
            "websockets is required for Polymarket live user state support"
        ) from exc
    return connect(
        adapter.config.user_ws_host,
        open_timeout=adapter.config.request_timeout_seconds,
    )


def live_state_recv(websocket) -> str | None:
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


def live_state_send_ping(websocket) -> None:
    ping = getattr(websocket, "ping", None)
    if callable(ping):
        ping()
        return
    send = getattr(websocket, "send", None)
    if callable(send):
        send("PING")


def is_terminal_live_order(payload: dict[str, Any]) -> bool:
    status = (
        payload.get("status")
        or payload.get("order_status")
        or payload.get("orderStatus")
        or payload.get("state")
    )
    if status is None:
        return False
    normalized_status = str(status).strip().lower()
    if normalized_status.startswith("order_status_"):
        normalized_status = normalized_status.removeprefix("order_status_")
    return (
        normalized_status
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
        or normalized_status.startswith("canceled_")
        or normalized_status.startswith("cancelled_")
        or normalized_status.startswith("rejected_")
    )


def looks_like_live_fill(payload: dict[str, Any]) -> bool:
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
        key in payload for key in ("asset_id", "assetId", "token_id", "tokenId", "side")
    )
    return has_order_reference and has_trade_fields and has_asset_reference


def iter_live_order_payloads(message: Any) -> list[dict[str, Any]]:
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

    normalized_payloads: list[dict[str, Any]] = []
    for candidate in candidates:
        if isinstance(candidate, dict):
            if any(
                key in candidate
                for key in ("id", "orderID", "order_id", "asset_id", "token_id")
            ):
                normalized_payloads.append(candidate)
        elif isinstance(candidate, list):
            normalized_payloads.extend(
                item for item in candidate if isinstance(item, dict)
            )
    return normalized_payloads


def iter_live_fill_payloads(message: Any) -> list[dict[str, Any]]:
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

    normalized_payloads: list[dict[str, Any]] = []
    for candidate in candidates:
        if isinstance(candidate, dict):
            if looks_like_live_fill(candidate):
                normalized_payloads.append(candidate)
        elif isinstance(candidate, list):
            normalized_payloads.extend(
                item
                for item in candidate
                if isinstance(item, dict) and looks_like_live_fill(item)
            )
    return normalized_payloads


def apply_live_state_message(adapter: Any, message: dict[str, Any]) -> None:
    orders = iter_live_order_payloads(message)
    fills = iter_live_fill_payloads(message)
    if not orders and not fills:
        return
    observed_at = datetime.now(timezone.utc)
    if orders:
        with adapter._live_state_lock:
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
                event_at = normalize.live_order_event_time(adapter, order, observed_at)
                tombstone = adapter._live_state_terminal_order_markers.get(order_id)
                if tombstone is not None and event_at <= tombstone[0]:
                    continue
                existing = dict(adapter._live_state_orders_raw.get(order_id, {}))
                existing_event_at = normalize.raw_live_event_time(existing)
                if existing_event_at is not None and event_at < existing_event_at:
                    continue
                existing.update(order)
                existing["__live_event_at"] = event_at.isoformat()
                if is_terminal_live_order(existing):
                    adapter._live_state_orders_raw.pop(order_id, None)
                    adapter._live_state_terminal_order_markers[order_id] = (
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
                adapter._live_state_terminal_order_markers.pop(order_id, None)
                adapter._live_state_orders_raw[order_id] = existing
                orders_changed = True
            if orders_changed:
                adapter._live_state_initialized = True
                adapter._live_state_last_update_at = observed_at
                adapter._live_state_last_error = None
                adapter._live_state_markets = adapter._live_state_subscription_markets()
    if fills:
        adapter._merge_live_fills_cache(fills, observed_at=observed_at)


def run_live_state_session(adapter: Any) -> None:
    markets = adapter._live_state_subscription_markets()
    if not markets:
        raise RuntimeError(
            "Polymarket live user stream requires configured or discoverable condition ids"
        )
    try:
        adapter._refresh_live_fills_cache(observed_at=datetime.now(timezone.utc))
    except Exception as exc:
        adapter._record_live_state_error(exc)
    websocket = connect_live_user_websocket(adapter)
    try:
        send = getattr(websocket, "send")
        send(json.dumps(live_state_subscription_payload(adapter, markets)))
        next_ping_at = time.monotonic() + max(
            1.0, adapter.config.live_state_ping_interval_seconds
        )
        while not adapter._live_state_stop_event.is_set():
            now = time.monotonic()
            if now >= next_ping_at:
                live_state_send_ping(websocket)
                next_ping_at = now + max(
                    1.0, adapter.config.live_state_ping_interval_seconds
                )
            message = live_state_recv(websocket)
            if message in (None, "", "PONG"):
                continue
            payload = json.loads(message)
            if payload == "PONG":
                continue
            apply_live_state_message(adapter, payload)
    finally:
        close = getattr(websocket, "close", None)
        if callable(close):
            close()


def start_live_user_state(adapter: Any):
    if not adapter._live_state_supported():
        return adapter.live_state_status()

    desired_markets = adapter._live_state_subscription_markets()
    with adapter._live_state_lock:
        running_thread = adapter._live_state_thread
        running = running_thread is not None and running_thread.is_alive()
        current_markets = adapter._live_state_markets
        current_mode = adapter._live_state_mode
    if running and (
        (desired_markets and desired_markets != current_markets)
        or current_mode == "degraded"
    ):
        adapter.stop_live_user_state()

    with adapter._live_state_lock:
        if (
            adapter._live_state_thread is not None
            and adapter._live_state_thread.is_alive()
        ):
            adapter._live_state_active = True
            return adapter._live_state_status_locked()
        adapter._live_state_stop_event = threading.Event()
        adapter._live_state_active = True
        adapter._live_state_running = False
        adapter._live_state_last_error = None
        adapter._mark_live_state_recovering_locked("starting live user state")

    try:
        adapter._live_state_bootstrap()
    except Exception as exc:
        adapter._record_live_state_error(exc)
        with adapter._live_state_lock:
            adapter._set_live_state_mode_locked("degraded", reason=str(exc))
            return adapter._live_state_status_locked()

    with adapter._live_state_lock:
        if not adapter._live_state_markets:
            adapter._live_state_last_error = (
                "Polymarket live user stream requires condition ids"
            )
            adapter._set_live_state_mode_locked(
                "degraded", reason=adapter._live_state_last_error
            )
            return adapter._live_state_status_locked()
        thread = threading.Thread(
            target=adapter._live_state_loop,
            name="polymarket-live-user-state",
            daemon=True,
        )
        adapter._live_state_thread = thread
        adapter._live_state_running = True
        thread.start()
        return adapter._live_state_status_locked()


def live_state_loop(adapter: Any) -> None:
    backoff = max(0.5, adapter.config.live_state_reconnect_backoff_seconds)
    max_backoff = max(backoff, adapter.config.live_state_reconnect_max_backoff_seconds)
    try:
        while not adapter._live_state_stop_event.is_set():
            with adapter._live_state_lock:
                adapter._mark_live_state_recovering_locked(
                    "reconnecting live user state"
                )
            try:
                run_live_state_session(adapter)
                backoff = max(0.5, adapter.config.live_state_reconnect_backoff_seconds)
            except Exception as exc:
                adapter._record_live_state_error(exc)
                with adapter._live_state_lock:
                    adapter._set_live_state_mode_locked("degraded", reason=str(exc))
                if adapter._live_state_stop_event.wait(backoff):
                    break
                backoff = min(max_backoff, backoff * 2)
    finally:
        with adapter._live_state_lock:
            adapter._live_state_running = False
            adapter._live_state_thread = None


def stop_live_user_state(adapter: Any):
    with adapter._live_state_lock:
        thread = adapter._live_state_thread
        adapter._live_state_active = False
        adapter._live_state_running = False
        adapter._set_live_state_mode_locked("inactive")
        adapter._live_state_stop_event.set()
    if (
        thread is not None
        and thread.is_alive()
        and thread is not threading.current_thread()
    ):
        thread.join(timeout=max(0.1, adapter.config.request_timeout_seconds))
    with adapter._live_state_lock:
        adapter._live_state_thread = None
        return adapter._live_state_status_locked()

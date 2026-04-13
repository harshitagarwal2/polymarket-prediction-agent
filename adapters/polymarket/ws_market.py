from __future__ import annotations

import importlib
import json
import threading
import time
from typing import Any


def market_state_subscription_assets(adapter: Any) -> tuple[str, ...]:
    configured = {
        str(asset) for asset in (adapter.config.live_market_assets or []) if str(asset)
    }
    return tuple(sorted(configured | adapter._market_state_tracked_assets))


def market_state_subscription_payload(
    adapter: Any,
    assets: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "type": "market",
        "assets_ids": list(assets),
        "custom_feature_enabled": True,
        "initial_dump": True,
        "level": 2,
    }


def connect_live_market_websocket(adapter: Any):
    try:
        ws_module = importlib.import_module("websockets.sync.client")
        connect = getattr(ws_module, "connect")
    except ImportError as exc:
        raise RuntimeError(
            "websockets is required for Polymarket live market state support"
        ) from exc
    return connect(
        adapter.config.market_ws_host,
        open_timeout=adapter.config.request_timeout_seconds,
    )


def market_message_asset_ids(message: dict[str, Any]) -> list[str]:
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


def iter_market_price_changes(message: dict[str, Any]) -> list[dict[str, Any]]:
    payload = (
        message.get("payload") if isinstance(message.get("payload"), dict) else None
    )
    candidates = [message, payload or {}]
    normalized_changes: list[dict[str, Any]] = []
    for candidate in candidates:
        changes = candidate.get("price_changes")
        if isinstance(changes, list):
            normalized_changes.extend(
                item for item in changes if isinstance(item, dict)
            )
    if normalized_changes:
        return normalized_changes
    return [message]


def market_level_map(entries: Any) -> dict[float, float]:
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


def run_market_state_session(adapter: Any) -> None:
    assets = adapter._market_state_subscription_assets()
    if not assets:
        raise RuntimeError("Polymarket live market state requires tracked assets")
    websocket = connect_live_market_websocket(adapter)
    try:
        send = getattr(websocket, "send")
        send(json.dumps(market_state_subscription_payload(adapter, assets)))
        next_ping_at = time.monotonic() + max(
            1.0, adapter.config.live_market_ping_interval_seconds
        )
        while not adapter._market_state_stop_event.is_set():
            now = time.monotonic()
            if now >= next_ping_at:
                adapter._live_state_send_ping(websocket)
                next_ping_at = now + max(
                    1.0, adapter.config.live_market_ping_interval_seconds
                )
            message = adapter._live_state_recv(websocket)
            if message in (None, "", "PONG"):
                continue
            parsed = json.loads(message)
            if parsed == "PONG":
                continue
            adapter._apply_market_state_message(parsed)
    finally:
        close = getattr(websocket, "close", None)
        if callable(close):
            close()


def market_state_loop(adapter: Any) -> None:
    backoff = max(0.5, adapter.config.live_state_reconnect_backoff_seconds)
    max_backoff = max(backoff, adapter.config.live_state_reconnect_max_backoff_seconds)
    try:
        while not adapter._market_state_stop_event.is_set():
            with adapter._market_state_lock:
                adapter._set_market_state_mode_locked(
                    "recovering", reason="reconnecting live market state"
                )
            try:
                run_market_state_session(adapter)
                backoff = max(0.5, adapter.config.live_state_reconnect_backoff_seconds)
            except Exception as exc:
                with adapter._market_state_lock:
                    adapter._set_market_state_mode_locked("degraded", reason=str(exc))
                if adapter._market_state_stop_event.wait(backoff):
                    break
                backoff = min(max_backoff, backoff * 2)
    finally:
        with adapter._market_state_lock:
            adapter._market_state_running = False
            adapter._market_state_thread = None


def start_live_market_state(adapter: Any):
    desired_assets = adapter._market_state_subscription_assets()
    with adapter._market_state_lock:
        running_thread = adapter._market_state_thread
        running = running_thread is not None and running_thread.is_alive()
        current_assets = adapter._market_state_assets
    if running and desired_assets and desired_assets != current_assets:
        adapter.stop_live_market_state()

    with adapter._market_state_lock:
        if (
            adapter._market_state_thread is not None
            and adapter._market_state_thread.is_alive()
        ):
            adapter._market_state_active = True
            return adapter._market_state_status_locked()
        adapter._market_state_stop_event = threading.Event()
        adapter._market_state_active = True
        adapter._market_state_running = False
        adapter._market_state_last_error = None
        adapter._market_state_assets = desired_assets
        adapter._set_market_state_mode_locked(
            "recovering", reason="starting live market state"
        )
        if not desired_assets:
            return adapter._market_state_status_locked()
        thread = threading.Thread(
            target=adapter._market_state_loop,
            name="polymarket-live-market-state",
            daemon=True,
        )
        adapter._market_state_thread = thread
        adapter._market_state_running = True
        thread.start()
        return adapter._market_state_status_locked()


def stop_live_market_state(adapter: Any):
    with adapter._market_state_lock:
        thread = adapter._market_state_thread
        adapter._market_state_active = False
        adapter._market_state_running = False
        adapter._set_market_state_mode_locked("inactive")
        adapter._market_state_stop_event.set()
    if (
        thread is not None
        and thread.is_alive()
        and thread is not threading.current_thread()
    ):
        thread.join(timeout=max(0.1, adapter.config.request_timeout_seconds))
    with adapter._market_state_lock:
        adapter._market_state_thread = None
        return adapter._market_state_status_locked()


def ensure_market_state_asset(adapter: Any, asset_id: str) -> None:
    if asset_id in adapter._market_state_tracked_assets:
        return
    adapter._market_state_tracked_assets.add(asset_id)
    if adapter._market_state_active:
        adapter.start_live_market_state()

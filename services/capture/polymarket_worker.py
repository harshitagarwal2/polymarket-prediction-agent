from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
from typing import Any, Sequence

from adapters.polymarket import PolymarketAdapter, PolymarketConfig

from services.capture.polymarket import (
    PolymarketCaptureStores,
    PolymarketMarketSnapshotRequest,
    hydrate_polymarket_market_snapshot,
    persist_polymarket_bbo_rows,
    serialize_account_snapshot,
    persist_polymarket_user_message,
    record_polymarket_capture_failure,
)


def _top_price(levels: dict[float, float], *, reverse: bool) -> float | None:
    if not levels:
        return None
    return max(levels) if reverse else min(levels)


def _price_size(
    levels: dict[float, float], *, reverse: bool
) -> tuple[float | None, float | None]:
    price = _top_price(levels, reverse=reverse)
    if price is None:
        return None, None
    return price, levels.get(price)


@dataclass(frozen=True)
class PolymarketMarketCaptureWorkerConfig:
    root: str
    asset_ids: Sequence[str]
    sport: str | None = None
    market_type: str | None = None
    limit: int = 500
    stale_after_ms: int = 4_000
    hydrate_on_start: bool = True
    hydrate_on_reconnect: bool = True
    max_sessions: int | None = None
    max_messages_per_session: int | None = None
    reconnect_backoff_seconds: float = 1.0
    reconnect_max_backoff_seconds: float = 15.0


@dataclass(frozen=True)
class PolymarketUserCaptureWorkerConfig:
    root: str
    market_ids: Sequence[str]
    stale_after_ms: int = 4_000
    max_sessions: int | None = None
    max_messages_per_session: int | None = None
    reconnect_backoff_seconds: float = 1.0
    reconnect_max_backoff_seconds: float = 15.0


class PolymarketMarketCaptureWorker:
    def __init__(
        self,
        *,
        config: PolymarketMarketCaptureWorkerConfig,
        adapter: Any | None = None,
        stores: PolymarketCaptureStores | None = None,
        sleep_fn=time.sleep,
    ) -> None:
        self.config = config
        self.adapter = adapter or PolymarketAdapter(
            PolymarketConfig(live_market_assets=list(config.asset_ids))
        )
        self.stores = stores or PolymarketCaptureStores.from_root(config.root)
        self.sleep_fn = sleep_fn

    def _hydrate(self, observed_at: datetime) -> dict[str, Any]:
        return hydrate_polymarket_market_snapshot(
            PolymarketMarketSnapshotRequest(
                root=self.config.root,
                sport=self.config.sport,
                market_type=self.config.market_type,
                limit=self.config.limit,
                stale_after_ms=max(self.config.stale_after_ms, 60_000),
            ),
            stores=self.stores,
            observed_at=observed_at,
        )

    def _normalized_rows_for_message(
        self, payload: Any
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        messages = payload if isinstance(payload, list) else [payload]
        self.adapter._apply_market_state_message(payload)
        raw_messages = [message for message in messages if isinstance(message, dict)]
        normalized_rows: list[dict[str, Any]] = []
        for message in raw_messages:
            asset_ids = self.adapter._market_message_asset_ids(message)
            for asset_id in asset_ids:
                snapshot = self.adapter._market_state_snapshot_for_asset(asset_id)
                bids = snapshot.get("bids") or {}
                asks = snapshot.get("asks") or {}
                last_update = snapshot.get("last_update_at")
                if isinstance(last_update, datetime):
                    book_ts = last_update.isoformat()
                elif isinstance(last_update, str) and last_update:
                    book_ts = last_update
                else:
                    book_ts = datetime.now(timezone.utc).isoformat()
                best_bid, best_bid_size = _price_size(bids, reverse=True)
                best_ask, best_ask_size = _price_size(asks, reverse=False)
                if best_bid is None and best_ask is None:
                    continue
                normalized_rows.append(
                    {
                        "market_id": asset_id,
                        "best_bid_yes": best_bid,
                        "best_bid_yes_size": best_bid_size,
                        "best_ask_yes": best_ask,
                        "best_ask_yes_size": best_ask_size,
                        "midpoint_yes": (
                            round((best_bid + best_ask) / 2, 6)
                            if best_bid is not None and best_ask is not None
                            else None
                        ),
                        "spread_yes": (
                            round(best_ask - best_bid, 6)
                            if best_bid is not None and best_ask is not None
                            else None
                        ),
                        "book_ts": book_ts,
                        "source_age_ms": 0,
                    }
                )
        return raw_messages, normalized_rows

    def _run_session(self) -> dict[str, Any]:
        websocket = self.adapter._connect_live_market_websocket()
        send = getattr(websocket, "send")
        send(
            json.dumps(
                self.adapter._market_state_subscription_payload(
                    tuple(self.config.asset_ids)
                )
            )
        )
        processed = 0
        last_result: dict[str, Any] = {"ok": True, "normalized_row_count": 0}
        try:
            while (
                self.config.max_messages_per_session is None
                or processed < self.config.max_messages_per_session
            ):
                message = self.adapter._live_state_recv(websocket)
                if message in (None, "", "PONG"):
                    continue
                payload = json.loads(message)
                if payload == "PONG":
                    continue
                observed_at = datetime.now(timezone.utc)
                raw_messages, normalized_rows = self._normalized_rows_for_message(
                    payload
                )
                checkpoint_value = max(
                    [str(row.get("book_ts") or "") for row in normalized_rows]
                    or [observed_at.isoformat()]
                )
                last_result = persist_polymarket_bbo_rows(
                    raw_messages=raw_messages,
                    normalized_rows=normalized_rows,
                    stores=self.stores,
                    observed_at=observed_at,
                    stale_after_ms=self.config.stale_after_ms,
                    checkpoint_value=checkpoint_value,
                    checkpoint_metadata={"asset_ids": list(self.config.asset_ids)},
                )
                processed += 1
            return last_result
        finally:
            close = getattr(websocket, "close", None)
            if callable(close):
                close()

    def run(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        if self.config.hydrate_on_start:
            results.append(self._hydrate(datetime.now(timezone.utc)))
        session = 0
        backoff = max(0.1, self.config.reconnect_backoff_seconds)
        while self.config.max_sessions is None or session < self.config.max_sessions:
            try:
                results.append(self._run_session())
                backoff = max(0.1, self.config.reconnect_backoff_seconds)
            except Exception as exc:
                observed_at = datetime.now(timezone.utc)
                is_final_session = (
                    self.config.max_sessions is not None
                    and session + 1 >= self.config.max_sessions
                )
                results.append(
                    record_polymarket_capture_failure(
                        self.stores,
                        source_name="polymarket_market_channel",
                        stale_after_ms=self.config.stale_after_ms,
                        error=exc,
                        observed_at=observed_at,
                        checkpoint_name="market_stream",
                        final=is_final_session,
                    )
                )
                if self.config.hydrate_on_reconnect and not is_final_session:
                    results.append(self._hydrate(observed_at))
                if (
                    self.config.max_sessions is None
                    or session + 1 < self.config.max_sessions
                ):
                    self.sleep_fn(backoff)
                    backoff = min(
                        self.config.reconnect_max_backoff_seconds, backoff * 2
                    )
            session += 1
        return results


class PolymarketUserCaptureWorker:
    def __init__(
        self,
        *,
        config: PolymarketUserCaptureWorkerConfig,
        adapter: Any | None = None,
        stores: PolymarketCaptureStores | None = None,
        sleep_fn=time.sleep,
    ) -> None:
        self.config = config
        self.adapter = adapter or PolymarketAdapter(
            PolymarketConfig(live_user_markets=list(config.market_ids))
        )
        self.stores = stores or PolymarketCaptureStores.from_root(config.root)
        self.sleep_fn = sleep_fn

    def _run_session(self) -> dict[str, Any]:
        websocket = self.adapter._connect_live_user_websocket()
        send = getattr(websocket, "send")
        send(
            json.dumps(
                self.adapter._live_state_subscription_payload(
                    tuple(self.config.market_ids)
                )
            )
        )
        processed = 0
        last_result: dict[str, Any] = {"ok": True, "order_count": 0, "fill_count": 0}
        try:
            while (
                self.config.max_messages_per_session is None
                or processed < self.config.max_messages_per_session
            ):
                message = self.adapter._live_state_recv(websocket)
                if message in (None, "", "PONG"):
                    continue
                payload = json.loads(message)
                if payload == "PONG":
                    continue
                observed_at = datetime.now(timezone.utc)
                account_snapshot = self.adapter.get_account_snapshot(None)
                last_result = persist_polymarket_user_message(
                    payload,
                    stores=self.stores,
                    observed_at=observed_at,
                    stale_after_ms=self.config.stale_after_ms,
                    order_payloads=self.adapter._iter_live_order_payloads(payload),
                    fill_payloads=self.adapter._iter_live_fill_payloads(payload),
                    account_snapshot_payload=serialize_account_snapshot(
                        account_snapshot
                    ),
                )
                processed += 1
            return last_result
        finally:
            close = getattr(websocket, "close", None)
            if callable(close):
                close()

    def run(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        session = 0
        backoff = max(0.1, self.config.reconnect_backoff_seconds)
        while self.config.max_sessions is None or session < self.config.max_sessions:
            try:
                results.append(self._run_session())
                backoff = max(0.1, self.config.reconnect_backoff_seconds)
            except Exception as exc:
                observed_at = datetime.now(timezone.utc)
                results.append(
                    record_polymarket_capture_failure(
                        self.stores,
                        source_name="polymarket_user_channel",
                        stale_after_ms=self.config.stale_after_ms,
                        error=exc,
                        observed_at=observed_at,
                        checkpoint_name="user_stream",
                        final=(
                            self.config.max_sessions is not None
                            and session + 1 >= self.config.max_sessions
                        ),
                    )
                )
                if (
                    self.config.max_sessions is None
                    or session + 1 < self.config.max_sessions
                ):
                    self.sleep_fn(backoff)
                    backoff = min(
                        self.config.reconnect_max_backoff_seconds, backoff * 2
                    )
            session += 1
        return results

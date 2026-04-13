from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from adapters.types import Contract
from adapters.types import FillSnapshot
from adapters.types import NormalizedOrder
from adapters.types import OrderAction
from adapters.types import OrderStatus
from adapters.types import OutcomeSide


def parse_datetime_value(value: Any) -> datetime | None:
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


def raw_order_timestamp(
    adapter: Any, order: dict[str, Any], *keys: str
) -> datetime | None:
    for key in keys:
        parsed = parse_datetime_value(order.get(key))
        if parsed is not None:
            return parsed
    return None


def stable_order_times(
    adapter: Any,
    order_id: str,
    order: dict[str, Any],
) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    created_at = raw_order_timestamp(
        adapter,
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
        created_at = adapter._open_order_first_seen_at.get(order_id)
    if created_at is None:
        created_at = now
    adapter._open_order_first_seen_at[order_id] = created_at
    updated_at = raw_order_timestamp(
        adapter,
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


def raw_live_event_time(payload: dict[str, Any]) -> datetime | None:
    return parse_datetime_value(payload.get("__live_event_at"))


def live_order_event_time(
    adapter: Any,
    order: dict[str, Any],
    observed_at: datetime,
) -> datetime:
    return (
        raw_order_timestamp(
            adapter,
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


def order_condition_id(adapter: Any, order: dict[str, Any]) -> str | None:
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
        return adapter._condition_id_by_token.get(str(symbol))
    return None


def fill_condition_id(adapter: Any, trade: dict[str, Any]) -> str | None:
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
        return adapter._condition_id_by_token.get(str(symbol))
    return None


def cache_condition_mapping(
    adapter: Any, symbol: str, condition_id: str | None
) -> None:
    if condition_id in (None, ""):
        return
    adapter._condition_id_by_token[str(symbol)] = str(condition_id)


def live_state_subscription_markets(adapter: Any) -> tuple[str, ...]:
    configured_markets = {
        str(item) for item in (adapter.config.live_user_markets or []) if str(item)
    }
    mapped_markets = {
        market for market in adapter._condition_id_by_token.values() if market
    }
    order_markets = {
        market
        for market in (
            order_condition_id(adapter, order)
            for order in adapter._live_state_orders_raw.values()
        )
        if market is not None
    }
    fill_markets = {
        market
        for market in (
            fill_condition_id(adapter, trade)
            for trade in adapter._live_state_fills_raw.values()
        )
        if market is not None
    }
    return tuple(
        sorted(configured_markets | mapped_markets | order_markets | fill_markets)
    )


def parse_quantity(value: Any) -> float:
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


def open_order_quantity(adapter: Any, order: dict[str, Any]) -> float:
    for key in ("original_size", "originalSize", "initial_size", "initialSize"):
        if order.get(key) not in (None, ""):
            return max(0.0, parse_quantity(order.get(key)))
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
            remaining_quantity = max(0.0, parse_quantity(order.get(key)))
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
            matched_quantity = max(0.0, parse_quantity(order.get(key)))
            break
    if remaining_quantity is not None and matched_quantity is not None:
        return remaining_quantity + matched_quantity
    if order.get("size") not in (None, ""):
        return max(0.0, parse_quantity(order.get("size")))
    if order.get("quantity") not in (None, ""):
        return max(0.0, parse_quantity(order.get("quantity")))
    if remaining_quantity is not None:
        return remaining_quantity
    return 0.0


def open_order_remaining_quantity(
    adapter: Any,
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
            return min(max(0.0, parse_quantity(order.get(key))), quantity)
    for key in (
        "matched_size",
        "matchedSize",
        "size_matched",
        "filled_size",
        "filledSize",
    ):
        if order.get(key) not in (None, ""):
            return max(0.0, quantity - parse_quantity(order.get(key)))
    return max(0.0, quantity)


def open_order_status(
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


def normalize_open_orders(
    adapter: Any,
    raw_orders: list[dict[str, Any]],
    contract: Contract | None = None,
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
        created_at, updated_at = stable_order_times(adapter, order_id, order)
        seen_order_ids.add(order_id)
        symbol = (
            order.get("asset_id")
            or order.get("token_id")
            or (contract.symbol if contract else "unknown")
        )
        quantity = open_order_quantity(adapter, order)
        remaining_quantity = open_order_remaining_quantity(adapter, order, quantity)
        normalized_contract = Contract(
            venue=adapter.venue,
            symbol=str(symbol),
            outcome=contract.outcome
            if contract
            else Contract(adapter.venue, str(symbol)).outcome,
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
                status=open_order_status(order, quantity, remaining_quantity),
                created_at=created_at,
                updated_at=updated_at,
                post_only=bool(order.get("postOnly", order.get("post_only", False))),
                expiration_ts=(
                    int(order["expiration"])
                    if order.get("expiration") not in (None, "")
                    else None
                ),
                raw=order,
            )
        )
    adapter._open_order_first_seen_at = {
        order_id: created_at
        for order_id, created_at in adapter._open_order_first_seen_at.items()
        if order_id in seen_order_ids
    }
    if contract is None:
        return normalized
    return [order for order in normalized if order.contract.symbol == contract.symbol]


def fill_order_id(trade: dict[str, Any]) -> str:
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


def raw_fill_timestamp(
    adapter: Any, trade: dict[str, Any], *keys: str
) -> datetime | None:
    for key in keys:
        parsed = parse_datetime_value(trade.get(key))
        if parsed is not None:
            return parsed
    return None


def fill_cache_key(adapter: Any, trade: dict[str, Any]) -> str | None:
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
    order_id = fill_order_id(trade)
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
    timestamp = raw_fill_timestamp(
        adapter,
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


def fill_confirmed(trade: dict[str, Any]) -> bool:
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


def normalize_fill(
    adapter: Any,
    trade: dict[str, Any],
    contract: Contract | None = None,
) -> FillSnapshot | None:
    if not fill_confirmed(trade):
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
        order_id=fill_order_id(trade) or str(fill_id or ""),
        contract=Contract(
            venue=adapter.venue,
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
        quantity=parse_quantity(
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


def normalize_fills(
    adapter: Any,
    raw_trades: list[dict[str, Any]],
    contract: Contract | None = None,
) -> list[FillSnapshot]:
    normalized: list[FillSnapshot] = []
    for trade in raw_trades or []:
        fill = normalize_fill(adapter, dict(trade), contract)
        if fill is not None:
            normalized.append(fill)
    if contract is None:
        return normalized
    return [fill for fill in normalized if fill.contract.symbol == contract.symbol]

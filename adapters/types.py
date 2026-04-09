from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Venue(str, Enum):
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


class OutcomeSide(str, Enum):
    YES = "yes"
    NO = "no"
    UNKNOWN = "unknown"


class OrderAction(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    RESTING = "resting"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Contract:
    venue: Venue
    symbol: str
    outcome: OutcomeSide = OutcomeSide.UNKNOWN
    title: str | None = None

    @property
    def market_key(self) -> str:
        if self.outcome is OutcomeSide.UNKNOWN:
            return self.symbol
        return f"{self.symbol}:{self.outcome.value}"


@dataclass(frozen=True)
class PriceLevel:
    price: float
    quantity: float


@dataclass(frozen=True)
class OrderBookFillEstimate:
    requested_quantity: float
    filled_quantity: float
    average_price: float | None = None
    worst_price: float | None = None
    levels_consumed: int = 0

    @property
    def unfilled_quantity(self) -> float:
        return max(0.0, self.requested_quantity - self.filled_quantity)

    @property
    def complete(self) -> bool:
        return self.filled_quantity >= self.requested_quantity

    def expected_slippage_bps(
        self, *, reference_price: float | None, action: OrderAction
    ) -> float | None:
        if (
            reference_price is None
            or reference_price <= 0.0
            or self.average_price is None
        ):
            return None
        if action is OrderAction.BUY:
            return max(
                0.0, (self.average_price - reference_price) / reference_price * 10_000
            )
        return max(
            0.0, (reference_price - self.average_price) / reference_price * 10_000
        )


@dataclass(frozen=True)
class OrderBookExecutionAssessment:
    action: OrderAction
    requested_quantity: float
    visible_quantity: float
    max_admissible_quantity: float
    expected_slippage_bps: float | None
    depth_levels_used: int
    complete_within_visible_depth: bool


@dataclass
class OrderBookSnapshot:
    contract: Contract
    bids: list[PriceLevel] = field(default_factory=list)
    asks: list[PriceLevel] = field(default_factory=list)
    midpoint: float | None = None
    last_price: float | None = None
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw: Any | None = None

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].price if self.asks else None

    def levels_for_action(self, action: OrderAction) -> list[PriceLevel]:
        return self.asks if action is OrderAction.BUY else self.bids

    def cumulative_quantity(
        self,
        action: OrderAction,
        *,
        limit_price: float | None = None,
        max_levels: int | None = None,
    ) -> float:
        total = 0.0
        levels_consumed = 0
        for level in self.levels_for_action(action):
            if max_levels is not None and levels_consumed >= max_levels:
                break
            if limit_price is not None:
                if action is OrderAction.BUY and level.price > limit_price:
                    break
                if action is OrderAction.SELL and level.price < limit_price:
                    break
            total += level.quantity
            levels_consumed += 1
        return total

    def estimate_fill(
        self,
        action: OrderAction,
        quantity: float,
        *,
        limit_price: float | None = None,
        max_levels: int | None = None,
    ) -> OrderBookFillEstimate:
        if quantity <= 0:
            return OrderBookFillEstimate(
                requested_quantity=quantity, filled_quantity=0.0
            )

        remaining = quantity
        filled = 0.0
        total_notional = 0.0
        worst_price: float | None = None
        levels_consumed = 0

        for level in self.levels_for_action(action):
            if max_levels is not None and levels_consumed >= max_levels:
                break
            if limit_price is not None:
                if action is OrderAction.BUY and level.price > limit_price:
                    break
                if action is OrderAction.SELL and level.price < limit_price:
                    break
            take_quantity = min(remaining, level.quantity)
            if take_quantity <= 0:
                continue
            filled += take_quantity
            total_notional += take_quantity * level.price
            worst_price = level.price
            remaining -= take_quantity
            levels_consumed += 1
            if remaining <= 0:
                break

        average_price = total_notional / filled if filled > 0 else None
        return OrderBookFillEstimate(
            requested_quantity=quantity,
            filled_quantity=filled,
            average_price=average_price,
            worst_price=worst_price,
            levels_consumed=levels_consumed,
        )


@dataclass(frozen=True)
class OrderIntent:
    contract: Contract
    action: OrderAction
    price: float
    quantity: float
    post_only: bool = False
    reduce_only: bool = False
    expiration_ts: int | None = None
    client_order_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def notional(self) -> float:
        return self.price * self.quantity


@dataclass
class NormalizedOrder:
    order_id: str
    contract: Contract
    action: OrderAction
    price: float
    quantity: float
    remaining_quantity: float
    status: OrderStatus = OrderStatus.UNKNOWN
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    post_only: bool = False
    reduce_only: bool = False
    expiration_ts: int | None = None
    client_order_id: str | None = None
    raw: Any | None = None


@dataclass
class FillSnapshot:
    order_id: str
    contract: Contract
    action: OrderAction
    price: float
    quantity: float
    fee: float = 0.0
    fill_id: str | None = None
    raw: Any | None = None

    @property
    def fill_key(self) -> str:
        if self.fill_id not in (None, ""):
            return str(self.fill_id)
        return f"{self.order_id}:{self.price:.6f}:{self.quantity:.6f}:{self.fee:.6f}"


@dataclass
class PositionSnapshot:
    contract: Contract
    quantity: float = 0.0
    average_price: float | None = None
    mark_price: float | None = None
    raw: Any | None = None


@dataclass
class BalanceSnapshot:
    venue: Venue
    available: float
    total: float | None = None
    currency: str = "USD"
    raw: Any | None = None


@dataclass
class AccountSnapshot:
    venue: Venue
    balance: BalanceSnapshot
    positions: list[PositionSnapshot] = field(default_factory=list)
    open_orders: list[NormalizedOrder] = field(default_factory=list)
    fills: list[FillSnapshot] = field(default_factory=list)
    complete: bool = True
    issues: list[str] = field(default_factory=list)
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PlacementResult:
    accepted: bool
    order_id: str | None = None
    status: OrderStatus = OrderStatus.UNKNOWN
    message: str | None = None
    raw: Any | None = None


@dataclass
class MarketSummary:
    contract: Contract
    title: str | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    midpoint: float | None = None
    volume: float | None = None
    category: str | None = None
    sport: str | None = None
    series: str | None = None
    event_key: str | None = None
    game_id: str | None = None
    sports_market_type: str | None = None
    start_time: datetime | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)
    active: bool = True
    expires_at: datetime | None = None
    raw: Any | None = None


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _deserialize_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def serialize_contract(contract: Contract) -> dict[str, Any]:
    return {
        "venue": contract.venue.value,
        "symbol": contract.symbol,
        "outcome": contract.outcome.value,
        "title": contract.title,
    }


def deserialize_contract(payload: dict[str, Any]) -> Contract:
    return Contract(
        venue=Venue(payload["venue"]),
        symbol=str(payload["symbol"]),
        outcome=OutcomeSide(payload.get("outcome", OutcomeSide.UNKNOWN.value)),
        title=payload.get("title"),
    )


def serialize_market_summary(market: MarketSummary) -> dict[str, Any]:
    return {
        "contract": serialize_contract(market.contract),
        "title": market.title,
        "best_bid": market.best_bid,
        "best_ask": market.best_ask,
        "midpoint": market.midpoint,
        "volume": market.volume,
        "category": market.category,
        "sport": market.sport,
        "series": market.series,
        "event_key": market.event_key,
        "game_id": market.game_id,
        "sports_market_type": market.sports_market_type,
        "start_time": _serialize_datetime(market.start_time),
        "tags": list(market.tags),
        "active": market.active,
        "expires_at": _serialize_datetime(market.expires_at),
        "raw": market.raw,
    }


def deserialize_market_summary(payload: dict[str, Any]) -> MarketSummary:
    return MarketSummary(
        contract=deserialize_contract(payload["contract"]),
        title=payload.get("title"),
        best_bid=(
            float(payload["best_bid"]) if payload.get("best_bid") is not None else None
        ),
        best_ask=(
            float(payload["best_ask"]) if payload.get("best_ask") is not None else None
        ),
        midpoint=(
            float(payload["midpoint"]) if payload.get("midpoint") is not None else None
        ),
        volume=(
            float(payload["volume"]) if payload.get("volume") is not None else None
        ),
        category=payload.get("category"),
        sport=payload.get("sport"),
        series=payload.get("series"),
        event_key=payload.get("event_key"),
        game_id=payload.get("game_id"),
        sports_market_type=payload.get("sports_market_type"),
        start_time=_deserialize_datetime(payload.get("start_time")),
        tags=tuple(str(tag) for tag in payload.get("tags", [])),
        active=bool(payload.get("active", True)),
        expires_at=_deserialize_datetime(payload.get("expires_at")),
        raw=payload.get("raw"),
    )


def serialize_normalized_order(order: NormalizedOrder) -> dict[str, Any]:
    return {
        "order_id": order.order_id,
        "contract": serialize_contract(order.contract),
        "action": order.action.value,
        "price": order.price,
        "quantity": order.quantity,
        "remaining_quantity": order.remaining_quantity,
        "status": order.status.value,
        "created_at": _serialize_datetime(order.created_at),
        "updated_at": _serialize_datetime(order.updated_at),
        "post_only": order.post_only,
        "reduce_only": order.reduce_only,
        "expiration_ts": order.expiration_ts,
        "client_order_id": order.client_order_id,
    }


def deserialize_normalized_order(payload: dict[str, Any]) -> NormalizedOrder:
    return NormalizedOrder(
        order_id=str(payload["order_id"]),
        contract=deserialize_contract(payload["contract"]),
        action=OrderAction(payload["action"]),
        price=float(payload["price"]),
        quantity=float(payload["quantity"]),
        remaining_quantity=float(payload["remaining_quantity"]),
        status=OrderStatus(payload.get("status", OrderStatus.UNKNOWN.value)),
        created_at=_deserialize_datetime(payload.get("created_at"))
        or datetime.now(timezone.utc),
        updated_at=_deserialize_datetime(payload.get("updated_at"))
        or datetime.now(timezone.utc),
        post_only=bool(payload.get("post_only", False)),
        reduce_only=bool(payload.get("reduce_only", False)),
        expiration_ts=payload.get("expiration_ts"),
        client_order_id=payload.get("client_order_id"),
    )


def serialize_fill_snapshot(fill: FillSnapshot) -> dict[str, Any]:
    return {
        "order_id": fill.order_id,
        "contract": serialize_contract(fill.contract),
        "action": fill.action.value,
        "price": fill.price,
        "quantity": fill.quantity,
        "fee": fill.fee,
        "fill_id": fill.fill_id,
    }


def deserialize_fill_snapshot(payload: dict[str, Any]) -> FillSnapshot:
    return FillSnapshot(
        order_id=str(payload["order_id"]),
        contract=deserialize_contract(payload["contract"]),
        action=OrderAction(payload["action"]),
        price=float(payload["price"]),
        quantity=float(payload["quantity"]),
        fee=float(payload.get("fee", 0.0) or 0.0),
        fill_id=payload.get("fill_id"),
    )


def serialize_position_snapshot(position: PositionSnapshot) -> dict[str, Any]:
    return {
        "contract": serialize_contract(position.contract),
        "quantity": position.quantity,
        "average_price": position.average_price,
        "mark_price": position.mark_price,
    }


def deserialize_position_snapshot(payload: dict[str, Any]) -> PositionSnapshot:
    return PositionSnapshot(
        contract=deserialize_contract(payload["contract"]),
        quantity=float(payload.get("quantity", 0.0) or 0.0),
        average_price=(
            float(payload["average_price"])
            if payload.get("average_price") is not None
            else None
        ),
        mark_price=(
            float(payload["mark_price"])
            if payload.get("mark_price") is not None
            else None
        ),
    )


def serialize_balance_snapshot(balance: BalanceSnapshot) -> dict[str, Any]:
    return {
        "venue": balance.venue.value,
        "available": balance.available,
        "total": balance.total,
        "currency": balance.currency,
    }


def deserialize_balance_snapshot(payload: dict[str, Any]) -> BalanceSnapshot:
    return BalanceSnapshot(
        venue=Venue(payload["venue"]),
        available=float(payload["available"]),
        total=(float(payload["total"]) if payload.get("total") is not None else None),
        currency=str(payload.get("currency", "USD")),
    )


@dataclass(frozen=True)
class OpportunityCandidate:
    contract: Contract
    action: OrderAction
    fair_value: float
    market_price: float
    edge: float
    score: float
    rationale: str
    raw: Any | None = None

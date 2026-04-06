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

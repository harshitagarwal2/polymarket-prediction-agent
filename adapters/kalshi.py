from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from adapters import MarketSummary
from adapters.base import AdapterHealth
from adapters.types import (
    AccountSnapshot,
    BalanceSnapshot,
    Contract,
    FillSnapshot,
    NormalizedOrder,
    OrderAction,
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
class KalshiConfig:
    api_key_id: str | None = None
    private_key_path: str | None = None
    api_base: str | None = None
    demo: bool = False
    timeout: float = 10.0
    max_retries: int = 3


class KalshiAdapter:
    venue = Venue.KALSHI

    def __init__(self, config: KalshiConfig):
        self.config = config
        self._client: Any | None = None

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        try:
            pykalshi = importlib.import_module("pykalshi")
            KalshiClient = getattr(pykalshi, "KalshiClient")
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "pykalshi is not installed. Install it with `pip install -e upstreams/pykalshi` or `pip install pykalshi`."
            ) from exc

        client = KalshiClient(
            api_key_id=self.config.api_key_id,
            private_key_path=self.config.private_key_path,
            api_base=self.config.api_base,
            demo=self.config.demo,
            timeout=self.config.timeout,
            max_retries=self.config.max_retries,
        )
        self._client = client
        return client

    def _parse_datetime_value(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    def health(self) -> AdapterHealth:
        try:
            client = self._ensure_client()
            client.get_markets(limit=1)
            return AdapterHealth(self.venue, True)
        except Exception as exc:  # pragma: no cover - network dependent
            return AdapterHealth(self.venue, False, str(exc))

    def list_markets(self, limit: int = 100) -> list[MarketSummary]:
        client = self._ensure_client()
        markets = client.get_markets(limit=limit)
        summaries: list[MarketSummary] = []
        for market in markets:
            ticker = getattr(market, "ticker", None) or getattr(market, "symbol", None)
            if not ticker:
                continue
            title = getattr(market, "title", None) or getattr(market, "question", None)
            volume = getattr(market, "volume_24h", None) or getattr(
                market, "volume", None
            )
            category = getattr(market, "category", None)
            active = bool(
                getattr(market, "status", None) is None
                or str(getattr(market, "status", "")).lower() == "open"
            )
            yes_bid = getattr(market, "yes_bid_dollars", None)
            yes_ask = getattr(market, "yes_ask_dollars", None)
            no_bid = getattr(market, "no_bid_dollars", None)
            no_ask = getattr(market, "no_ask_dollars", None)
            summaries.append(
                MarketSummary(
                    contract=Contract(
                        venue=self.venue,
                        symbol=str(ticker),
                        outcome=OutcomeSide.YES,
                        title=title,
                    ),
                    title=title,
                    best_bid=float(yes_bid) if yes_bid is not None else None,
                    best_ask=float(yes_ask) if yes_ask is not None else None,
                    midpoint=(float(yes_bid) + float(yes_ask)) / 2
                    if yes_bid is not None and yes_ask is not None
                    else None,
                    volume=float(volume) if volume is not None else None,
                    category=str(category) if category is not None else None,
                    active=active,
                    raw=market,
                )
            )
            summaries.append(
                MarketSummary(
                    contract=Contract(
                        venue=self.venue,
                        symbol=str(ticker),
                        outcome=OutcomeSide.NO,
                        title=title,
                    ),
                    title=title,
                    best_bid=float(no_bid) if no_bid is not None else None,
                    best_ask=float(no_ask) if no_ask is not None else None,
                    midpoint=(float(no_bid) + float(no_ask)) / 2
                    if no_bid is not None and no_ask is not None
                    else None,
                    volume=float(volume) if volume is not None else None,
                    category=str(category) if category is not None else None,
                    active=active,
                    raw=market,
                )
            )
        return summaries[:limit]

    def _extract_levels(self, entries: list[Any] | None) -> list[PriceLevel]:
        levels: list[PriceLevel] = []
        for entry in entries or []:
            if isinstance(entry, dict):
                price = (
                    entry.get("price")
                    or entry.get("price_dollars")
                    or entry.get("yes_price_dollars")
                )
                quantity = (
                    entry.get("quantity") or entry.get("count") or entry.get("size")
                )
            else:
                price = getattr(entry, "price", None) or getattr(
                    entry, "price_dollars", None
                )
                quantity = (
                    getattr(entry, "quantity", None)
                    or getattr(entry, "count", None)
                    or getattr(entry, "size", None)
                )
            if price is None or quantity is None:
                continue
            levels.append(PriceLevel(price=float(price), quantity=float(quantity)))
        levels.sort(key=lambda level: level.price, reverse=True)
        return levels

    def get_order_book(self, contract: Contract) -> OrderBookSnapshot:
        client = self._ensure_client()
        market = client.get_market(contract.symbol)
        book = market.get_orderbook()
        if contract.outcome is OutcomeSide.NO:
            bids = self._extract_levels(
                getattr(book, "no", None) or getattr(book, "no_dollars", None)
            )
            asks = []
        else:
            bids = self._extract_levels(
                getattr(book, "yes", None) or getattr(book, "yes_dollars", None)
            )
            asks = []
        midpoint = None
        best_bid = bids[0].price if bids else None
        yes_bid = getattr(market, "yes_bid_dollars", None)
        yes_ask = getattr(market, "yes_ask_dollars", None)
        no_bid = getattr(market, "no_bid_dollars", None)
        no_ask = getattr(market, "no_ask_dollars", None)
        if contract.outcome is OutcomeSide.NO:
            if no_bid is not None and no_ask is not None:
                midpoint = (float(no_bid) + float(no_ask)) / 2
        else:
            if yes_bid is not None and yes_ask is not None:
                midpoint = (float(yes_bid) + float(yes_ask)) / 2
        return OrderBookSnapshot(
            contract=contract,
            bids=bids,
            asks=sorted(asks, key=lambda l: l.price),
            midpoint=midpoint,
            last_price=best_bid,
            raw=book,
        )

    def list_open_orders(
        self, contract: Contract | None = None
    ) -> list[NormalizedOrder]:
        client = self._ensure_client()
        try:
            pykalshi = importlib.import_module("pykalshi")
            KalshiOrderStatus = getattr(pykalshi, "OrderStatus")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pykalshi is required for authenticated order listing."
            ) from exc
        orders = client.portfolio.get_orders(
            status=KalshiOrderStatus.RESTING,
            ticker=contract.symbol if contract else None,
            fetch_all=True,
        )
        normalized: list[NormalizedOrder] = []
        for order in orders:
            side_raw = getattr(order, "side", None)
            action_raw = getattr(order, "action", None)
            side = getattr(side_raw, "value", str(side_raw or "yes")).lower()
            action = getattr(action_raw, "value", str(action_raw or "buy")).lower()
            normalized_contract = Contract(
                self.venue,
                getattr(order, "ticker", contract.symbol if contract else "unknown"),
                OutcomeSide.YES if side == "yes" else OutcomeSide.NO,
            )
            remaining = getattr(order, "remaining_count", None) or getattr(
                order, "remaining_count_fp", 0
            )
            total = getattr(order, "count", None) or getattr(
                order, "count_fp", remaining
            )
            price = getattr(order, "yes_price_dollars", None)
            if normalized_contract.outcome is OutcomeSide.NO:
                price = getattr(order, "no_price_dollars", price)
            normalized.append(
                NormalizedOrder(
                    order_id=str(getattr(order, "order_id", getattr(order, "id", ""))),
                    contract=normalized_contract,
                    action=OrderAction.BUY if action == "buy" else OrderAction.SELL,
                    price=float(price or 0.0),
                    quantity=float(total or 0.0),
                    remaining_quantity=float(remaining or 0.0),
                    status=OrderStatus.RESTING,
                    created_at=self._parse_datetime_value(
                        getattr(order, "created_time", None)
                    )
                    or datetime.now(timezone.utc),
                    updated_at=self._parse_datetime_value(
                        getattr(order, "last_update_time", None)
                    )
                    or self._parse_datetime_value(getattr(order, "created_time", None))
                    or datetime.now(timezone.utc),
                    post_only=bool(getattr(order, "post_only", False)),
                    reduce_only=bool(getattr(order, "reduce_only", False)),
                    expiration_ts=getattr(order, "expiration_ts", None),
                    client_order_id=getattr(order, "client_order_id", None),
                    raw=order,
                )
            )
        return normalized

    def list_positions(
        self, contract: Contract | None = None
    ) -> list[PositionSnapshot]:
        client = self._ensure_client()
        positions = client.portfolio.get_positions(
            ticker=contract.symbol if contract else None,
            fetch_all=True,
        )
        normalized: list[PositionSnapshot] = []
        for position in positions:
            side_raw = getattr(position, "side", None)
            side = getattr(side_raw, "value", str(side_raw or "yes")).lower()
            normalized_contract = Contract(
                venue=self.venue,
                symbol=getattr(
                    position, "ticker", contract.symbol if contract else "unknown"
                ),
                outcome=OutcomeSide.YES if side == "yes" else OutcomeSide.NO,
            )
            normalized.append(
                PositionSnapshot(
                    contract=normalized_contract,
                    quantity=float(getattr(position, "position", 0.0) or 0.0),
                    raw=position,
                )
            )
        return normalized

    def list_fills(self, contract: Contract | None = None) -> list[FillSnapshot]:
        client = self._ensure_client()
        fills = client.portfolio.get_fills(
            ticker=contract.symbol if contract else None,
            fetch_all=True,
        )
        normalized: list[FillSnapshot] = []
        for fill in fills:
            side_raw = getattr(fill, "side", None)
            action_raw = getattr(fill, "action", None)
            side = getattr(side_raw, "value", str(side_raw or "yes")).lower()
            action = getattr(action_raw, "value", str(action_raw or "buy")).lower()
            normalized_contract = Contract(
                venue=self.venue,
                symbol=getattr(
                    fill, "ticker", contract.symbol if contract else "unknown"
                ),
                outcome=OutcomeSide.YES if side == "yes" else OutcomeSide.NO,
            )
            price = getattr(fill, "yes_price_dollars", None)
            if normalized_contract.outcome is OutcomeSide.NO:
                price = getattr(fill, "no_price_dollars", price)
            normalized.append(
                FillSnapshot(
                    order_id=str(getattr(fill, "order_id", getattr(fill, "id", ""))),
                    contract=normalized_contract,
                    action=OrderAction.BUY if action == "buy" else OrderAction.SELL,
                    price=float(price or getattr(fill, "price", 0.0) or 0.0),
                    quantity=float(
                        getattr(fill, "count", getattr(fill, "count_fp", 0.0)) or 0.0
                    ),
                    fee=float(
                        getattr(fill, "fees_paid", getattr(fill, "fee", 0.0)) or 0.0
                    ),
                    fill_id=str(
                        getattr(
                            fill,
                            "fill_id",
                            getattr(fill, "trade_id", getattr(fill, "id", "")),
                        )
                    )
                    or None,
                    raw=fill,
                )
            )
        return normalized

    def get_position(self, contract: Contract) -> PositionSnapshot:
        positions = self.list_positions(contract)
        total = 0.0
        chosen = None
        for position in positions:
            if (
                contract.outcome is OutcomeSide.UNKNOWN
                or contract.market_key == position.contract.market_key
            ):
                quantity = float(position.quantity or 0.0)
                total += quantity
                chosen = position
        return PositionSnapshot(contract=contract, quantity=total, raw=chosen)

    def get_balance(self) -> BalanceSnapshot:
        client = self._ensure_client()
        balance = client.portfolio.get_balance()
        cents = float(getattr(balance, "balance", 0.0) or 0.0)
        return BalanceSnapshot(
            venue=self.venue,
            available=cents / 100.0,
            total=cents / 100.0,
            currency="USD",
            raw=balance,
        )

    def get_account_snapshot(self, contract: Contract | None = None) -> AccountSnapshot:
        return AccountSnapshot(
            venue=self.venue,
            balance=self.get_balance(),
            positions=self.list_positions(contract),
            open_orders=self.list_open_orders(contract),
            fills=self.list_fills(contract),
        )

    def place_limit_order(self, intent: OrderIntent) -> PlacementResult:
        client = self._ensure_client()
        try:
            pykalshi = importlib.import_module("pykalshi")
            Action = getattr(pykalshi, "Action")
            Side = getattr(pykalshi, "Side")
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "pykalshi is required for authenticated trading."
            ) from exc

        if intent.contract.outcome is OutcomeSide.UNKNOWN:
            return PlacementResult(
                False,
                status=OrderStatus.REJECTED,
                message="Kalshi orders require YES/NO outcome",
            )

        side = Side.YES if intent.contract.outcome is OutcomeSide.YES else Side.NO
        action = Action.BUY if intent.action is OrderAction.BUY else Action.SELL
        kwargs: dict[str, Any] = {
            "client_order_id": intent.client_order_id,
            "post_only": intent.post_only,
            "reduce_only": intent.reduce_only,
            "expiration_ts": intent.expiration_ts,
        }
        if side is Side.YES:
            kwargs["yes_price_dollars"] = f"{intent.price:.2f}"
        else:
            kwargs["no_price_dollars"] = f"{intent.price:.2f}"
        order = client.portfolio.place_order(
            intent.contract.symbol,
            action,
            side,
            count_fp=str(intent.quantity),
            **kwargs,
        )
        order_id = getattr(order, "order_id", getattr(order, "id", None))
        return PlacementResult(
            True,
            order_id=str(order_id) if order_id else None,
            status=OrderStatus.RESTING,
            raw=order,
        )

    def cancel_order(self, order_id: str) -> bool:
        client = self._ensure_client()
        client.portfolio.cancel_order(order_id)
        return True

    def cancel_all(self, contract: Contract | None = None) -> int:
        orders = self.list_open_orders(contract)
        for order in orders:
            self.cancel_order(order.order_id)
        return len(orders)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
        self._client = None

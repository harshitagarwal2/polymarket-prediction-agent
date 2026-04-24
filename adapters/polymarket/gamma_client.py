from __future__ import annotations

import json
from typing import Any

from adapters import MarketSummary
from adapters.types import Contract
from adapters.types import OutcomeSide

from . import http_client
from . import normalize


GAMMA_API_HOST = "https://gamma-api.polymarket.com"


def fetch_markets(
    *,
    limit: int = 100,
    host: str = GAMMA_API_HOST,
    timeout_seconds: float = 30.0,
    client=None,
) -> list[dict[str, Any]]:
    payload = http_client.get_json(
        f"{host.rstrip('/')}/markets",
        params={"limit": max(1, int(limit))},
        timeout_seconds=timeout_seconds,
        client=client,
    )
    if not isinstance(payload, list):
        raise RuntimeError("Gamma returned a non-list payload")
    return [item for item in payload if isinstance(item, dict)]


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


def list_markets(adapter: Any, limit: int = 100) -> list[MarketSummary]:
    items = None
    if hasattr(adapter, "_call_client"):
        try:
            response = adapter._call_client("list markets", "get_simplified_markets")
        except Exception:
            response = None
        items = response.get("data") if isinstance(response, dict) else response
        if items is None and isinstance(response, dict):
            items = response.get("markets")
    if items is None:
        timeout_seconds = getattr(
            getattr(adapter, "config", None), "request_timeout_seconds", 30.0
        )
        items = fetch_markets(limit=limit, timeout_seconds=timeout_seconds)
    summaries: list[MarketSummary] = []

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
        volume = item.get("volume") or item.get("volume24hr") or item.get("volume_num")
        expires_at = normalize.parse_datetime_value(
            item.get("end_date_iso") or item.get("endDate")
        )
        start_time = normalize.parse_datetime_value(
            item.get("gameStartTime") or item.get("start_time")
        )

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
                            "outcome": outcome_names[index]
                            if index < len(outcome_names)
                            else None,
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
                normalize.cache_condition_mapping(adapter, symbol, condition_id)
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
                            venue=adapter.venue,
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
                        event_key=str(event_key)
                        if event_key not in (None, "")
                        else None,
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
                    contract=Contract(venue=adapter.venue, symbol=symbol, title=title),
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
                    event_key=str(event_key) if event_key not in (None, "") else None,
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

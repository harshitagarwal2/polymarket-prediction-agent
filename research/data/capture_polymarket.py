from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from adapters.types import Contract, OutcomeSide, Venue, serialize_contract

from research.data.schemas import PolymarketMarketRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_captured_at(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if value in (None, ""):
        return _utc_now()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _parse_optional_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    return _parse_captured_at(value)


@dataclass(frozen=True)
class PolymarketCaptureEnvelope:
    layer: str
    captured_at: datetime = field(default_factory=_utc_now)
    markets: list[PolymarketMarketRecord] = field(default_factory=list)

    def to_payload(self) -> dict[str, object]:
        return {
            "layer": self.layer,
            "captured_at": self.captured_at.isoformat(),
            "markets": [market.to_payload() for market in self.markets],
        }


def _coerce_optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    return float(value)


def _coerce_json_list(value: object) -> list[object] | None:
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


def _record_from_contract_payload(
    market_payload: dict[str, object],
    *,
    layer: str,
    captured_at: datetime,
    raw: dict[str, object],
) -> PolymarketMarketRecord:
    contract_payload = market_payload.get("contract")
    if isinstance(contract_payload, dict):
        contract = contract_payload
        symbol = contract.get("symbol")
        outcome = contract.get("outcome")
        title = contract.get("title") or market_payload.get("title")
        market_key = (
            f"{symbol}:{outcome}"
            if symbol not in (None, "") and outcome not in (None, "")
            else None
        )
    else:
        market_key = (
            str(market_payload.get("market_key"))
            if market_payload.get("market_key") not in (None, "")
            else None
        )
        symbol = None
        outcome = None
        if market_key is not None and ":" in market_key:
            symbol, outcome = market_key.rsplit(":", 1)
        title = market_payload.get("title") or market_payload.get("question")
        contract = (
            serialize_contract(
                Contract(
                    venue=Venue.POLYMARKET,
                    symbol=str(symbol),
                    outcome=OutcomeSide(str(outcome)),
                    title=str(title) if title not in (None, "") else None,
                )
            )
            if symbol not in (None, "") and outcome in {"yes", "no", "unknown"}
            else None
        )

    return PolymarketMarketRecord(
        layer=layer,
        captured_at=captured_at,
        market_key=market_key,
        condition_id=(
            str(market_payload.get("condition_id") or market_payload.get("conditionId"))
            if market_payload.get("condition_id") not in (None, "")
            or market_payload.get("conditionId") not in (None, "")
            else None
        ),
        event_key=(
            str(market_payload.get("event_key") or market_payload.get("eventKey"))
            if market_payload.get("event_key") not in (None, "")
            or market_payload.get("eventKey") not in (None, "")
            else None
        ),
        sport=(
            str(market_payload.get("sport"))
            if market_payload.get("sport") not in (None, "")
            else None
        ),
        series=(
            str(market_payload.get("series"))
            if market_payload.get("series") not in (None, "")
            else None
        ),
        game_id=(
            str(market_payload.get("game_id") or market_payload.get("gameId"))
            if market_payload.get("game_id") not in (None, "")
            or market_payload.get("gameId") not in (None, "")
            else None
        ),
        sports_market_type=(
            str(
                market_payload.get("sports_market_type")
                or market_payload.get("sportsMarketType")
            )
            if market_payload.get("sports_market_type") not in (None, "")
            or market_payload.get("sportsMarketType") not in (None, "")
            else None
        ),
        title=str(title) if title not in (None, "") else None,
        best_bid=_coerce_optional_float(
            market_payload.get("best_bid") or market_payload.get("bestBid")
        ),
        best_ask=_coerce_optional_float(
            market_payload.get("best_ask") or market_payload.get("bestAsk")
        ),
        best_bid_size=_coerce_optional_float(
            market_payload.get("best_bid_size") or market_payload.get("bestBidSize")
        ),
        best_ask_size=_coerce_optional_float(
            market_payload.get("best_ask_size") or market_payload.get("bestAskSize")
        ),
        midpoint=_coerce_optional_float(market_payload.get("midpoint")),
        volume=_coerce_optional_float(market_payload.get("volume")),
        start_time=_parse_optional_datetime(
            market_payload.get("start_time") or market_payload.get("gameStartTime")
        ),
        contract=contract if isinstance(contract, dict) else None,
        raw=raw,
    )


def _markets_from_payload(
    item: dict[str, object],
    *,
    layer: str,
    captured_at: datetime,
) -> list[PolymarketMarketRecord]:
    nested_market = item.get("market")
    market_payload = nested_market if isinstance(nested_market, dict) else item
    if market_payload.get("contract") is not None or market_payload.get(
        "market_key"
    ) not in (None, ""):
        return [
            _record_from_contract_payload(
                market_payload,
                layer=layer,
                captured_at=captured_at,
                raw=dict(item),
            )
        ]

    token_entries = market_payload.get("tokens")
    if not isinstance(token_entries, list):
        outcome_names = _coerce_json_list(market_payload.get("outcomes"))
        outcome_prices = _coerce_json_list(
            market_payload.get("outcomePrices") or market_payload.get("outcome_prices")
        )
        token_ids = _coerce_json_list(
            market_payload.get("clobTokenIds") or market_payload.get("clob_token_ids")
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
                        "midpoint": outcome_prices[index]
                        if outcome_prices and index < len(outcome_prices)
                        else None,
                    }
                )

    records: list[PolymarketMarketRecord] = []
    if isinstance(token_entries, list):
        for token in token_entries:
            if not isinstance(token, dict):
                continue
            symbol = (
                token.get("token_id")
                or token.get("tokenId")
                or token.get("asset_id")
                or token.get("assetId")
            )
            outcome_text = (
                str(token.get("outcome") or token.get("name") or "").strip().lower()
            )
            if symbol in (None, "") or outcome_text not in {"yes", "no"}:
                continue
            derived_payload = {
                **market_payload,
                "market_key": f"{symbol}:{outcome_text}",
                "contract": serialize_contract(
                    Contract(
                        venue=Venue.POLYMARKET,
                        symbol=str(symbol),
                        outcome=OutcomeSide(outcome_text),
                        title=(
                            str(
                                market_payload.get("question")
                                or market_payload.get("title")
                            )
                            if market_payload.get("question") not in (None, "")
                            or market_payload.get("title") not in (None, "")
                            else None
                        ),
                    )
                ),
                "midpoint": token.get("midpoint"),
                "best_bid_size": token.get("best_bid_size")
                or token.get("bestBidSize"),
                "best_ask_size": token.get("best_ask_size")
                or token.get("bestAskSize"),
            }
            records.append(
                _record_from_contract_payload(
                    derived_payload,
                    layer=layer,
                    captured_at=captured_at,
                    raw={"market": dict(market_payload), "token": dict(token)},
                )
            )
    if records:
        return records

    return [
        _record_from_contract_payload(
            market_payload,
            layer=layer,
            captured_at=captured_at,
            raw=dict(item),
        )
    ]


def build_polymarket_capture(
    payload: object,
    *,
    layer: str,
    captured_at: datetime | None = None,
) -> PolymarketCaptureEnvelope:
    resolved_captured_at = captured_at or _utc_now()
    if isinstance(payload, list):
        markets = [
            market
            for item in payload
            if isinstance(item, dict)
            for market in _markets_from_payload(
                item, layer=layer, captured_at=resolved_captured_at
            )
        ]
    elif isinstance(payload, dict):
        markets = _markets_from_payload(
            payload, layer=layer, captured_at=resolved_captured_at
        )
    else:
        markets = []
    return PolymarketCaptureEnvelope(
        layer=layer,
        captured_at=resolved_captured_at,
        markets=markets,
    )


def write_polymarket_capture(
    envelope: PolymarketCaptureEnvelope, output_path: str | Path
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope.to_payload(), indent=2, sort_keys=True))
    return path


def load_polymarket_capture(path: str | Path) -> PolymarketCaptureEnvelope:
    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("Polymarket capture must be a JSON object")
    raw_markets = payload.get("markets") or payload.get("records")
    if not isinstance(raw_markets, list):
        raise RuntimeError("Polymarket capture must contain a markets list")
    markets = [
        market
        for item in raw_markets
        if isinstance(item, dict)
        for market in _markets_from_payload(
            item,
            layer=str(payload.get("layer") or "unknown"),
            captured_at=_parse_captured_at(
                item.get("captured_at") or payload.get("captured_at")
            ),
        )
    ]
    return PolymarketCaptureEnvelope(
        layer=str(payload.get("layer") or "unknown"),
        captured_at=_parse_captured_at(payload.get("captured_at")),
        markets=markets,
    )

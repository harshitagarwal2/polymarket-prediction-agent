from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from research.data.schemas import SportsInputRow


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
class SportsInputCaptureEnvelope:
    source: str
    captured_at: datetime = field(default_factory=_utc_now)
    rows: list[SportsInputRow] = field(default_factory=list)

    def to_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "captured_at": self.captured_at.isoformat(),
            "rows": [row.to_payload() for row in self.rows],
        }


def _coerce_label(item: dict[str, object]) -> int | None:
    raw_label = None
    for field_name in ("label", "outcome_label", "home_win"):
        if field_name in item and item[field_name] not in (None, ""):
            raw_label = item[field_name]
            break
    if raw_label in (None, ""):
        return None
    if isinstance(raw_label, bool):
        return int(raw_label)
    if isinstance(raw_label, (int, float, str)):
        return int(raw_label)
    return None


def _coerce_optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    return float(value)


def _coerce_implied_probability(
    item: dict[str, object], *, decimal_odds: float | None
) -> float | None:
    explicit = _coerce_optional_float(
        item.get("implied_probability") or item.get("impliedProb")
    )
    if explicit is not None:
        return explicit
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    return 1.0 / decimal_odds


def _row_from_payload(
    item: dict[str, object],
    *,
    source: str,
    captured_at: datetime,
) -> SportsInputRow:
    decimal_odds = _coerce_optional_float(
        item.get("decimal_odds") or item.get("price_decimal")
    )
    return SportsInputRow(
        source=source,
        captured_at=captured_at,
        source_event_id=(
            str(item.get("source_event_id"))
            if item.get("source_event_id") not in (None, "")
            else None
        ),
        sport_key=(
            str(item.get("sport_key"))
            if item.get("sport_key") not in (None, "")
            else None
        ),
        bookmaker=(
            str(item.get("bookmaker"))
            if item.get("bookmaker") not in (None, "")
            else None
        ),
        decimal_odds=decimal_odds,
        event_key=(
            str(item.get("event_key"))
            if item.get("event_key") not in (None, "")
            else None
        ),
        sport=(str(item.get("sport")) if item.get("sport") not in (None, "") else None),
        series=(
            str(item.get("series")) if item.get("series") not in (None, "") else None
        ),
        game_id=(
            str(item.get("game_id")) if item.get("game_id") not in (None, "") else None
        ),
        sports_market_type=(
            str(item.get("sports_market_type"))
            if item.get("sports_market_type") not in (None, "")
            else None
        ),
        selection_name=(
            str(item.get("selection_name"))
            if item.get("selection_name") not in (None, "")
            else None
        ),
        outcome=(
            str(item.get("outcome")) if item.get("outcome") not in (None, "") else None
        ),
        home_team=(
            str(item.get("home_team"))
            if item.get("home_team") not in (None, "")
            else None
        ),
        away_team=(
            str(item.get("away_team"))
            if item.get("away_team") not in (None, "")
            else None
        ),
        start_time=_parse_optional_datetime(
            item.get("start_time")
            or item.get("commence_time")
            or item.get("gameStartTime")
        ),
        implied_probability=_coerce_implied_probability(
            item,
            decimal_odds=decimal_odds,
        ),
        label=_coerce_label(item),
        raw=dict(item),
    )


def build_sports_input_capture(
    payload: object,
    *,
    source: str,
    captured_at: datetime | None = None,
) -> SportsInputCaptureEnvelope:
    resolved_captured_at = captured_at or _utc_now()
    if isinstance(payload, list):
        rows = [
            _row_from_payload(item, source=source, captured_at=resolved_captured_at)
            for item in payload
            if isinstance(item, dict)
        ]
    elif isinstance(payload, dict):
        rows = [
            _row_from_payload(payload, source=source, captured_at=resolved_captured_at)
        ]
    else:
        rows = []
    return SportsInputCaptureEnvelope(
        source=source,
        captured_at=resolved_captured_at,
        rows=rows,
    )


def write_sports_input_capture(
    envelope: SportsInputCaptureEnvelope, output_path: str | Path
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope.to_payload(), indent=2, sort_keys=True))
    return path


def load_sports_input_capture(path: str | Path) -> SportsInputCaptureEnvelope:
    payload = json.loads(Path(path).read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("sports input capture must be a JSON object")
    if "rows" in payload:
        raw_rows = payload["rows"]
    else:
        raw_rows = payload.get("records")
    if not isinstance(raw_rows, list):
        raise RuntimeError("sports input capture must contain a rows list")
    rows = [
        _row_from_payload(
            item,
            source=str(payload.get("source") or "sports-inputs"),
            captured_at=_parse_captured_at(
                item.get("captured_at") or payload.get("captured_at")
            ),
        )
        for item in raw_rows
        if isinstance(item, dict)
    ]
    return SportsInputCaptureEnvelope(
        source=str(payload.get("source") or "sports-inputs"),
        captured_at=_parse_captured_at(payload.get("captured_at")),
        rows=rows,
    )

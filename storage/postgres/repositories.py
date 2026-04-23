from __future__ import annotations

from abc import ABC, abstractmethod
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from storage.postgres.bootstrap import (
    apply_migrations,
    connect_postgres,
    resolve_postgres_dsn,
)


_MIGRATED_DSNS: set[str] = set()


def _row_payload(row: Any) -> dict[str, Any]:
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    if isinstance(row, dict):
        return dict(row)
    raise TypeError("row must be a dataclass instance or dict")


def _json_ready(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def _payload_json(payload: dict[str, Any]) -> str:
    return json.dumps(_json_ready(payload), sort_keys=True)


def _decode_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return loaded
    raise TypeError("expected JSON object payload")


def _parse_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _event_timestamp(payload: dict[str, Any]) -> datetime:
    for key in (
        "as_of",
        "book_ts",
        "quote_ts",
        "created_at",
        "updated_at",
        "last_seen_at",
        "start_time",
        "end_time",
    ):
        timestamp = _parse_timestamp(payload.get(key))
        if timestamp is not None:
            return timestamp
    return datetime.now(timezone.utc)


def _captured_at(value: object) -> datetime:
    return _parse_timestamp(value) or datetime.now(timezone.utc)


class _PostgresRepository(ABC):
    table_name = "table"
    source_name = "storage"
    layer_name = "table"

    def __init__(
        self,
        root: str | Path = "runtime/data/postgres",
        *,
        dsn: str | None = None,
    ) -> None:
        self.root = root
        self.dsn = resolve_postgres_dsn(dsn or root)

    def _ensure_schema(self) -> None:
        if self.dsn in _MIGRATED_DSNS:
            return
        apply_migrations(self.dsn)
        _MIGRATED_DSNS.add(self.dsn)

    def _connect(self):
        self._ensure_schema()
        return connect_postgres(self.dsn)

    def _record_raw_event(
        self,
        cursor: Any,
        *,
        entity_type: str,
        entity_key: str | None,
        operation: str,
        payload: dict[str, Any],
        captured_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO raw_capture_events (
              source,
              layer,
              entity_type,
              entity_key,
              operation,
              captured_at,
              payload,
              metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            """,
            (
                self.source_name,
                self.layer_name,
                entity_type,
                entity_key,
                operation,
                captured_at or datetime.now(timezone.utc),
                _payload_json(payload),
                _payload_json(metadata or {}),
            ),
        )

    def _fetch_keyed_payloads(
        self,
        query: str,
        *,
        params: tuple[Any, ...] = (),
    ) -> dict[str, Any]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        return {str(key): _decode_payload(payload) for key, payload in rows}

    def _fetch_indexed_payloads(
        self,
        query: str,
        *,
        params: tuple[Any, ...] = (),
    ) -> dict[str, Any]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
        return {
            str(index): _decode_payload(payload)
            for index, (payload,) in enumerate(rows)
        }

    @abstractmethod
    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        raise NotImplementedError

    def write_all(self, rows: dict[str, Any]) -> None:
        for key, row in rows.items():
            self.upsert(str(key), row)

    def read_current(self) -> dict[str, Any]:
        return self.read_all()

    @abstractmethod
    def read_all(self) -> dict[str, Any]:
        raise NotImplementedError


class MarketRepository(_PostgresRepository):
    table_name = "polymarket_markets"
    source_name = "polymarket"
    layer_name = "market_catalog"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO polymarket_markets (
                      market_id,
                      condition_id,
                      token_id_yes,
                      token_id_no,
                      title,
                      description,
                      event_slug,
                      market_slug,
                      category,
                      end_time,
                      status,
                      raw_json,
                      payload,
                      updated_at
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW()
                    )
                    ON CONFLICT (market_id) DO UPDATE SET
                      condition_id = EXCLUDED.condition_id,
                      token_id_yes = EXCLUDED.token_id_yes,
                      token_id_no = EXCLUDED.token_id_no,
                      title = EXCLUDED.title,
                      description = EXCLUDED.description,
                      event_slug = EXCLUDED.event_slug,
                      market_slug = EXCLUDED.market_slug,
                      category = EXCLUDED.category,
                      end_time = EXCLUDED.end_time,
                      status = EXCLUDED.status,
                      raw_json = EXCLUDED.raw_json,
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    """,
                    (
                        str(key),
                        payload.get("condition_id"),
                        payload.get("token_id_yes"),
                        payload.get("token_id_no"),
                        payload.get("title"),
                        payload.get("description"),
                        payload.get("event_slug"),
                        payload.get("market_slug"),
                        payload.get("category"),
                        _parse_timestamp(payload.get("end_time")),
                        payload.get("status"),
                        _payload_json(payload.get("raw_json") or {}),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key=str(key),
                    operation="upsert",
                    payload=payload,
                    captured_at=datetime.now(timezone.utc),
                )
            connection.commit()
        return payload

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT market_id, payload FROM polymarket_markets ORDER BY market_id"
        )


class BBORepository(_PostgresRepository):
    table_name = "polymarket_bbo"
    source_name = "polymarket"
    layer_name = "market_channel"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO polymarket_bbo (
                      market_id,
                      best_bid_yes,
                      best_bid_yes_size,
                      best_ask_yes,
                      best_ask_yes_size,
                      midpoint_yes,
                      spread_yes,
                      book_ts,
                      source_age_ms,
                      raw_hash,
                      payload,
                      updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (market_id) DO UPDATE SET
                      best_bid_yes = EXCLUDED.best_bid_yes,
                      best_bid_yes_size = EXCLUDED.best_bid_yes_size,
                      best_ask_yes = EXCLUDED.best_ask_yes,
                      best_ask_yes_size = EXCLUDED.best_ask_yes_size,
                      midpoint_yes = EXCLUDED.midpoint_yes,
                      spread_yes = EXCLUDED.spread_yes,
                      book_ts = EXCLUDED.book_ts,
                      source_age_ms = EXCLUDED.source_age_ms,
                      raw_hash = EXCLUDED.raw_hash,
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    """,
                    (
                        str(key),
                        payload.get("best_bid_yes"),
                        payload.get("best_bid_yes_size"),
                        payload.get("best_ask_yes"),
                        payload.get("best_ask_yes_size"),
                        payload.get("midpoint_yes"),
                        payload.get("spread_yes"),
                        _parse_timestamp(payload.get("book_ts")),
                        int(payload.get("source_age_ms") or 0),
                        payload.get("raw_hash"),
                        _payload_json(payload),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO polymarket_book_snapshots (market_id, book_ts, payload)
                    VALUES (%s, %s, %s::jsonb)
                    ON CONFLICT (market_id, book_ts) DO UPDATE SET payload = EXCLUDED.payload
                    """,
                    (
                        str(key),
                        _parse_timestamp(payload.get("book_ts")),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key=str(key),
                    operation="upsert",
                    payload=payload,
                    captured_at=_captured_at(payload.get("book_ts")),
                )
            connection.commit()
        return payload

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT market_id, payload FROM polymarket_bbo ORDER BY market_id"
        )


class SportsbookEventRepository(_PostgresRepository):
    table_name = "sportsbook_events"
    source_name = "sportsbook"
    layer_name = "event_catalog"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO sportsbook_events (
                      sportsbook_event_id,
                      source,
                      sport,
                      league,
                      home_team,
                      away_team,
                      start_time,
                      raw_json,
                      payload,
                      updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                    ON CONFLICT (sportsbook_event_id) DO UPDATE SET
                      source = EXCLUDED.source,
                      sport = EXCLUDED.sport,
                      league = EXCLUDED.league,
                      home_team = EXCLUDED.home_team,
                      away_team = EXCLUDED.away_team,
                      start_time = EXCLUDED.start_time,
                      raw_json = EXCLUDED.raw_json,
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    """,
                    (
                        str(key),
                        payload.get("source"),
                        payload.get("sport"),
                        payload.get("league"),
                        payload.get("home_team"),
                        payload.get("away_team"),
                        _parse_timestamp(payload.get("start_time")),
                        _payload_json(payload.get("raw_json") or {}),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key=str(key),
                    operation="upsert",
                    payload=payload,
                    captured_at=datetime.now(timezone.utc),
                )
            connection.commit()
        return payload

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT sportsbook_event_id, payload FROM sportsbook_events ORDER BY sportsbook_event_id"
        )


class SportsbookOddsRepository(_PostgresRepository):
    table_name = "sportsbook_odds"
    source_name = "sportsbook"
    layer_name = "quote_events"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO sportsbook_odds (
                      sportsbook_event_id,
                      source,
                      market_type,
                      selection,
                      price_decimal,
                      implied_prob,
                      overround,
                      provider,
                      source_ts,
                      capture_ts,
                      quote_ts,
                      source_age_ms,
                      raw_json,
                      payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    ON CONFLICT (
                      sportsbook_event_id,
                      source,
                      market_type,
                      selection,
                      quote_ts
                    ) DO UPDATE SET
                      price_decimal = EXCLUDED.price_decimal,
                      implied_prob = EXCLUDED.implied_prob,
                      overround = EXCLUDED.overround,
                      provider = EXCLUDED.provider,
                      source_ts = EXCLUDED.source_ts,
                      capture_ts = EXCLUDED.capture_ts,
                      source_age_ms = EXCLUDED.source_age_ms,
                      raw_json = EXCLUDED.raw_json,
                      payload = EXCLUDED.payload
                    """,
                    (
                        payload.get("sportsbook_event_id"),
                        payload.get("source"),
                        payload.get("market_type"),
                        payload.get("selection"),
                        payload.get("price_decimal"),
                        payload.get("implied_prob"),
                        payload.get("overround"),
                        payload.get("provider"),
                        _parse_timestamp(payload.get("source_ts")),
                        _parse_timestamp(payload.get("capture_ts")),
                        _parse_timestamp(payload.get("quote_ts")),
                        int(payload.get("source_age_ms") or 0),
                        _payload_json(payload.get("raw_json") or {}),
                        _payload_json(payload),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO sportsbook_odds_current (
                      sportsbook_event_id,
                      source,
                      market_type,
                      selection,
                      quote_ts,
                      payload,
                      updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (sportsbook_event_id, source, market_type, selection) DO UPDATE SET
                      quote_ts = EXCLUDED.quote_ts,
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    WHERE sportsbook_odds_current.quote_ts <= EXCLUDED.quote_ts
                    """,
                    (
                        payload.get("sportsbook_event_id"),
                        payload.get("source"),
                        payload.get("market_type"),
                        payload.get("selection"),
                        _parse_timestamp(payload.get("quote_ts")),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key="|".join(
                        [
                            str(payload.get("sportsbook_event_id") or ""),
                            str(payload.get("source") or ""),
                            str(payload.get("market_type") or ""),
                            str(payload.get("selection") or ""),
                        ]
                    ),
                    operation="append",
                    payload=payload,
                    captured_at=_captured_at(
                        payload.get("capture_ts") or payload.get("quote_ts")
                    ),
                )
            connection.commit()
        return payload

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        del key
        return self.append(row)

    def read_all(self) -> dict[str, Any]:
        return self._fetch_indexed_payloads(
            """
            SELECT payload
            FROM sportsbook_odds
            ORDER BY quote_ts, sportsbook_event_id, source, market_type, selection
            """
        )

    def read_current(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            """
            SELECT
              CONCAT(sportsbook_event_id, '|', source, '|', market_type, '|', selection) AS key,
              payload
            FROM sportsbook_odds_current
            ORDER BY sportsbook_event_id, source, market_type, selection
            """
        )


class MappingRepository(_PostgresRepository):
    table_name = "market_mappings"
    source_name = "projection"
    layer_name = "market_mappings"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO market_mappings (
                      polymarket_market_id,
                      sportsbook_event_id,
                      sportsbook_market_type,
                      normalized_market_type,
                      match_confidence,
                      resolution_risk,
                      mismatch_reason,
                      event_key,
                      sport,
                      series,
                      game_id,
                      blocked_reason,
                      is_active,
                      payload,
                      created_at,
                      updated_at
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW()
                    )
                    RETURNING mapping_id
                    """,
                    (
                        payload.get("polymarket_market_id"),
                        payload.get("sportsbook_event_id"),
                        payload.get("sportsbook_market_type"),
                        payload.get("normalized_market_type"),
                        payload.get("match_confidence"),
                        payload.get("resolution_risk"),
                        payload.get("mismatch_reason"),
                        payload.get("event_key"),
                        payload.get("sport"),
                        payload.get("series"),
                        payload.get("game_id"),
                        payload.get("blocked_reason"),
                        bool(payload.get("is_active", True)),
                        _payload_json(payload),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO market_mappings_current (
                      polymarket_market_id,
                      sportsbook_event_id,
                      payload,
                      updated_at
                    )
                    VALUES (%s, %s, %s::jsonb, NOW())
                    ON CONFLICT (polymarket_market_id, sportsbook_event_id) DO UPDATE SET
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    """,
                    (
                        payload.get("polymarket_market_id"),
                        payload.get("sportsbook_event_id"),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key="|".join(
                        [
                            str(payload.get("polymarket_market_id") or ""),
                            str(payload.get("sportsbook_event_id") or ""),
                        ]
                    ),
                    operation="append",
                    payload=payload,
                    captured_at=datetime.now(timezone.utc),
                )
            connection.commit()
        return payload

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        del key
        return self.append(row)

    def read_all(self) -> dict[str, Any]:
        return self._fetch_indexed_payloads(
            "SELECT payload FROM market_mappings ORDER BY created_at, mapping_id"
        )

    def read_current(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            """
            SELECT CONCAT(polymarket_market_id, '|', sportsbook_event_id) AS key, payload
            FROM market_mappings_current
            ORDER BY polymarket_market_id, sportsbook_event_id
            """
        )


class SourceHealthRepository(_PostgresRepository):
    table_name = "source_health"
    source_name = "projection"
    layer_name = "source_health"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO source_health (
                      source_name,
                      last_seen_at,
                      last_success_at,
                      stale_after_ms,
                      status,
                      details,
                      payload,
                      updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                    ON CONFLICT (source_name) DO UPDATE SET
                      last_seen_at = EXCLUDED.last_seen_at,
                      last_success_at = EXCLUDED.last_success_at,
                      stale_after_ms = EXCLUDED.stale_after_ms,
                      status = EXCLUDED.status,
                      details = EXCLUDED.details,
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    """,
                    (
                        str(key),
                        _parse_timestamp(payload.get("last_seen_at")),
                        _parse_timestamp(payload.get("last_success_at")),
                        int(payload.get("stale_after_ms") or 0),
                        payload.get("status"),
                        _payload_json(payload.get("details") or {}),
                        _payload_json(payload),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO source_health_events (
                      source_name,
                      observed_at,
                      status,
                      success,
                      stale_after_ms,
                      details,
                      payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    """,
                    (
                        str(key),
                        _event_timestamp(payload),
                        payload.get("status"),
                        payload.get("status") == "ok",
                        int(payload.get("stale_after_ms") or 0),
                        _payload_json(payload.get("details") or {}),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key=str(key),
                    operation="upsert",
                    payload=payload,
                    captured_at=_captured_at(payload.get("last_seen_at")),
                )
            connection.commit()
        return payload

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT source_name, payload FROM source_health ORDER BY source_name"
        )


class FairValueRepository(_PostgresRepository):
    table_name = "fair_values"
    source_name = "forecasting"
    layer_name = "fair_values"

    def _history_key(self, payload: dict[str, Any]) -> str:
        return "|".join(
            [
                str(payload.get("market_id") or ""),
                str(payload.get("as_of") or ""),
                str(payload.get("model_name") or ""),
                str(payload.get("model_version") or ""),
            ]
        )

    def _upsert_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                for payload in rows:
                    cursor.execute(
                        """
                        INSERT INTO fair_values (
                          market_id,
                          as_of,
                          fair_yes_prob,
                          calibrated_fair_yes_prob,
                          lower_prob,
                          upper_prob,
                          book_dispersion,
                          data_age_ms,
                          source_count,
                          model_name,
                          model_version,
                          payload
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (market_id, as_of, model_name, model_version) DO UPDATE SET
                          fair_yes_prob = EXCLUDED.fair_yes_prob,
                          calibrated_fair_yes_prob = EXCLUDED.calibrated_fair_yes_prob,
                          lower_prob = EXCLUDED.lower_prob,
                          upper_prob = EXCLUDED.upper_prob,
                          book_dispersion = EXCLUDED.book_dispersion,
                          data_age_ms = EXCLUDED.data_age_ms,
                          source_count = EXCLUDED.source_count,
                          payload = EXCLUDED.payload
                        """,
                        (
                            payload.get("market_id"),
                            _parse_timestamp(payload.get("as_of")),
                            payload.get("fair_yes_prob"),
                            payload.get("calibrated_fair_yes_prob"),
                            payload.get("lower_prob"),
                            payload.get("upper_prob"),
                            payload.get("book_dispersion"),
                            int(payload.get("data_age_ms") or 0),
                            int(payload.get("source_count") or 0),
                            payload.get("model_name"),
                            payload.get("model_version"),
                            _payload_json(payload),
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO fair_values_current (
                          market_id,
                          as_of,
                          model_name,
                          model_version,
                          payload,
                          updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                        ON CONFLICT (market_id) DO UPDATE SET
                          as_of = EXCLUDED.as_of,
                          model_name = EXCLUDED.model_name,
                          model_version = EXCLUDED.model_version,
                          payload = EXCLUDED.payload,
                          updated_at = NOW()
                        WHERE fair_values_current.as_of <= EXCLUDED.as_of
                        """,
                        (
                            payload.get("market_id"),
                            _parse_timestamp(payload.get("as_of")),
                            payload.get("model_name"),
                            payload.get("model_version"),
                            _payload_json(payload),
                        ),
                    )
                    self._record_raw_event(
                        cursor,
                        entity_type=self.table_name,
                        entity_key=self._history_key(payload),
                        operation="upsert",
                        payload=payload,
                        captured_at=_captured_at(payload.get("as_of")),
                    )
            connection.commit()

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        self._upsert_rows([payload])
        return payload

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        self._upsert_rows([payload])
        return payload

    def write_all(self, rows: dict[str, Any]) -> None:
        self._upsert_rows(_row_payload(row) for row in rows.values())

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            """
            SELECT
              CONCAT(market_id, '|', as_of::text, '|', model_name, '|', model_version) AS key,
              payload
            FROM fair_values
            ORDER BY as_of, market_id, model_name, model_version
            """
        )

    def read_current(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT market_id, payload FROM fair_values_current ORDER BY market_id"
        )


class OpportunityRepository(_PostgresRepository):
    table_name = "opportunities"
    source_name = "runtime"
    layer_name = "opportunities"

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO opportunities (
                      market_id,
                      as_of,
                      side,
                      fair_yes_prob,
                      best_bid_yes,
                      best_ask_yes,
                      edge_buy_bps,
                      edge_sell_bps,
                      edge_buy_after_costs_bps,
                      edge_sell_after_costs_bps,
                      edge_after_costs_bps,
                      fillable_size,
                      confidence,
                      blocked_reason,
                      fair_value_ref,
                      blocked_reasons,
                      payload
                    )
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                    )
                    ON CONFLICT (market_id, as_of, side) DO UPDATE SET
                      fair_yes_prob = EXCLUDED.fair_yes_prob,
                      best_bid_yes = EXCLUDED.best_bid_yes,
                      best_ask_yes = EXCLUDED.best_ask_yes,
                      edge_buy_bps = EXCLUDED.edge_buy_bps,
                      edge_sell_bps = EXCLUDED.edge_sell_bps,
                      edge_buy_after_costs_bps = EXCLUDED.edge_buy_after_costs_bps,
                      edge_sell_after_costs_bps = EXCLUDED.edge_sell_after_costs_bps,
                      edge_after_costs_bps = EXCLUDED.edge_after_costs_bps,
                      fillable_size = EXCLUDED.fillable_size,
                      confidence = EXCLUDED.confidence,
                      blocked_reason = EXCLUDED.blocked_reason,
                      fair_value_ref = EXCLUDED.fair_value_ref,
                      blocked_reasons = EXCLUDED.blocked_reasons,
                      payload = EXCLUDED.payload
                    """,
                    (
                        payload.get("market_id"),
                        _parse_timestamp(payload.get("as_of")),
                        payload.get("side"),
                        payload.get("fair_yes_prob"),
                        payload.get("best_bid_yes"),
                        payload.get("best_ask_yes"),
                        payload.get("edge_buy_bps"),
                        payload.get("edge_sell_bps"),
                        payload.get("edge_buy_after_costs_bps"),
                        payload.get("edge_sell_after_costs_bps"),
                        payload.get("edge_after_costs_bps"),
                        payload.get("fillable_size"),
                        payload.get("confidence"),
                        payload.get("blocked_reason"),
                        _parse_timestamp(payload.get("fair_value_ref")),
                        _payload_json(
                            {"blocked_reasons": payload.get("blocked_reasons", [])}
                        ),
                        _payload_json(payload),
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO opportunities_current (
                      market_id,
                      side,
                      as_of,
                      payload,
                      updated_at
                    )
                    VALUES (%s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (market_id, side) DO UPDATE SET
                      as_of = EXCLUDED.as_of,
                      payload = EXCLUDED.payload,
                      updated_at = NOW()
                    WHERE opportunities_current.as_of <= EXCLUDED.as_of
                    """,
                    (
                        payload.get("market_id"),
                        payload.get("side"),
                        _parse_timestamp(payload.get("as_of")),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key="|".join(
                        [
                            str(payload.get("market_id") or ""),
                            str(payload.get("side") or ""),
                        ]
                    ),
                    operation="append",
                    payload=payload,
                    captured_at=_captured_at(payload.get("as_of")),
                )
            connection.commit()
        return payload

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        del key
        return self.append(row)

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            """
            SELECT CONCAT(market_id, '|', as_of::text, '|', side) AS key, payload
            FROM opportunities
            ORDER BY as_of, market_id, side
            """
        )

    def read_current(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT CONCAT(market_id, '|', side) AS key, payload FROM opportunities_current ORDER BY market_id, side"
        )


class TradeAttributionRepository(_PostgresRepository):
    table_name = "trade_attribution"
    source_name = "research"
    layer_name = "trade_attribution"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO trade_attribution (
                      trade_id,
                      market_id,
                      expected_edge_bps,
                      realized_edge_bps,
                      slippage_bps,
                      pnl,
                      model_error,
                      stale_data_flag,
                      mapping_risk,
                      notes,
                      payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    ON CONFLICT (trade_id) DO UPDATE SET
                      market_id = EXCLUDED.market_id,
                      expected_edge_bps = EXCLUDED.expected_edge_bps,
                      realized_edge_bps = EXCLUDED.realized_edge_bps,
                      slippage_bps = EXCLUDED.slippage_bps,
                      pnl = EXCLUDED.pnl,
                      model_error = EXCLUDED.model_error,
                      stale_data_flag = EXCLUDED.stale_data_flag,
                      mapping_risk = EXCLUDED.mapping_risk,
                      notes = EXCLUDED.notes,
                      payload = EXCLUDED.payload
                    """,
                    (
                        str(key),
                        payload.get("market_id"),
                        payload.get("expected_edge_bps"),
                        payload.get("realized_edge_bps"),
                        payload.get("slippage_bps"),
                        payload.get("pnl"),
                        payload.get("model_error"),
                        bool(payload.get("stale_data_flag", False)),
                        payload.get("mapping_risk"),
                        _payload_json(payload.get("notes") or {}),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key=str(key),
                    operation="upsert",
                    payload=payload,
                    captured_at=datetime.now(timezone.utc),
                )
            connection.commit()
        return payload

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            "SELECT trade_id, payload FROM trade_attribution ORDER BY trade_id"
        )


class ModelRegistryRepository(_PostgresRepository):
    table_name = "model_registry"
    source_name = "forecasting"
    layer_name = "model_registry"

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO model_registry (
                      model_name,
                      model_version,
                      created_at,
                      feature_spec,
                      metrics,
                      artifact_uri,
                      payload
                    )
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb)
                    ON CONFLICT (model_name, model_version) DO UPDATE SET
                      created_at = EXCLUDED.created_at,
                      feature_spec = EXCLUDED.feature_spec,
                      metrics = EXCLUDED.metrics,
                      artifact_uri = EXCLUDED.artifact_uri,
                      payload = EXCLUDED.payload
                    """,
                    (
                        payload.get("model_name"),
                        payload.get("model_version"),
                        _parse_timestamp(payload.get("created_at")),
                        _payload_json(payload.get("feature_spec") or {}),
                        _payload_json(payload.get("metrics") or {}),
                        payload.get("artifact_uri"),
                        _payload_json(payload),
                    ),
                )
                self._record_raw_event(
                    cursor,
                    entity_type=self.table_name,
                    entity_key=str(key),
                    operation="upsert",
                    payload=payload,
                    captured_at=_captured_at(payload.get("created_at")),
                )
            connection.commit()
        return payload

    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        key = "|".join(
            [
                str(payload.get("model_name") or ""),
                str(payload.get("model_version") or ""),
            ]
        )
        return self.upsert(key, payload)

    def read_all(self) -> dict[str, Any]:
        return self._fetch_keyed_payloads(
            """
            SELECT CONCAT(model_name, '|', model_version) AS key, payload
            FROM model_registry
            ORDER BY model_name, model_version
            """
        )


def upsert_capture_checkpoint(
    checkpoint_name: str,
    source_name: str,
    checkpoint_value: str | None,
    *,
    checkpoint_ts: str | None = None,
    metadata: dict[str, Any] | None = None,
    root: str | Path = "runtime/data/postgres",
    dsn: str | None = None,
) -> dict[str, Any]:
    payload = {
        "checkpoint_name": checkpoint_name,
        "source_name": source_name,
        "checkpoint_value": checkpoint_value,
        "checkpoint_ts": checkpoint_ts,
        "metadata": metadata or {},
    }
    resolved_dsn = resolve_postgres_dsn(dsn or root)
    if resolved_dsn not in _MIGRATED_DSNS:
        apply_migrations(resolved_dsn)
        _MIGRATED_DSNS.add(resolved_dsn)
    with connect_postgres(resolved_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO capture_checkpoints (
                  checkpoint_name,
                  source_name,
                  checkpoint_value,
                  checkpoint_ts,
                  metadata,
                  updated_at
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (checkpoint_name, source_name) DO UPDATE SET
                  checkpoint_value = EXCLUDED.checkpoint_value,
                  checkpoint_ts = EXCLUDED.checkpoint_ts,
                  metadata = EXCLUDED.metadata,
                  updated_at = NOW()
                """,
                (
                    checkpoint_name,
                    source_name,
                    checkpoint_value,
                    _parse_timestamp(checkpoint_ts),
                    _payload_json(metadata or {}),
                ),
            )
            cursor.execute(
                """
                INSERT INTO raw_capture_events (
                  source,
                  layer,
                  entity_type,
                  entity_key,
                  operation,
                  captured_at,
                  payload,
                  metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    "capture",
                    "checkpoints",
                    "capture_checkpoints",
                    f"{checkpoint_name}|{source_name}",
                    "upsert",
                    _captured_at(checkpoint_ts),
                    _payload_json(payload),
                    _payload_json(metadata or {}),
                ),
            )
        connection.commit()
    return payload


def read_capture_checkpoint(
    checkpoint_name: str,
    source_name: str,
    *,
    root: str | Path = "runtime/data/postgres",
    dsn: str | None = None,
) -> dict[str, Any] | None:
    resolved_dsn = resolve_postgres_dsn(dsn or root)
    if resolved_dsn not in _MIGRATED_DSNS:
        apply_migrations(resolved_dsn)
        _MIGRATED_DSNS.add(resolved_dsn)
    with connect_postgres(resolved_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT checkpoint_value, checkpoint_ts, metadata
                FROM capture_checkpoints
                WHERE checkpoint_name = %s AND source_name = %s
                """,
                (checkpoint_name, source_name),
            )
            row = cursor.fetchone()
    if row is None:
        return None
    checkpoint_value, checkpoint_ts, metadata = row
    return {
        "checkpoint_name": checkpoint_name,
        "source_name": source_name,
        "checkpoint_value": checkpoint_value,
        "checkpoint_ts": (
            checkpoint_ts.astimezone(timezone.utc).isoformat()
            if isinstance(checkpoint_ts, datetime)
            else None
        ),
        "metadata": _decode_payload(metadata) if metadata is not None else {},
    }


def append_raw_capture_event(
    *,
    source: str,
    layer: str,
    entity_type: str,
    operation: str,
    payload: dict[str, Any],
    entity_key: str | None = None,
    captured_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
    root: str | Path = "runtime/data/postgres",
    dsn: str | None = None,
) -> dict[str, Any]:
    resolved_dsn = resolve_postgres_dsn(dsn or root)
    if resolved_dsn not in _MIGRATED_DSNS:
        apply_migrations(resolved_dsn)
        _MIGRATED_DSNS.add(resolved_dsn)
    event_payload = {
        "source": source,
        "layer": layer,
        "entity_type": entity_type,
        "entity_key": entity_key,
        "operation": operation,
        "payload": dict(payload),
        "metadata": dict(metadata or {}),
        "captured_at": (captured_at or datetime.now(timezone.utc)).isoformat(),
    }
    with connect_postgres(resolved_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_capture_events (
                  source,
                  layer,
                  entity_type,
                  entity_key,
                  operation,
                  captured_at,
                  payload,
                  metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                """,
                (
                    source,
                    layer,
                    entity_type,
                    entity_key,
                    operation,
                    captured_at or datetime.now(timezone.utc),
                    _payload_json(payload),
                    _payload_json(metadata or {}),
                ),
            )
        connection.commit()
    return event_payload


def list_raw_capture_events(
    *,
    source: str | None = None,
    layer: str | None = None,
    after_capture_id: int | None = None,
    limit: int = 1000,
    root: str | Path = "runtime/data/postgres",
    dsn: str | None = None,
) -> list[dict[str, Any]]:
    resolved_dsn = resolve_postgres_dsn(dsn or root)
    if resolved_dsn not in _MIGRATED_DSNS:
        apply_migrations(resolved_dsn)
        _MIGRATED_DSNS.add(resolved_dsn)
    filters: list[str] = []
    params: list[Any] = []
    if source not in (None, ""):
        filters.append("source = %s")
        params.append(source)
    if layer not in (None, ""):
        filters.append("layer = %s")
        params.append(layer)
    if after_capture_id is not None:
        filters.append("capture_id > %s")
        params.append(int(after_capture_id))
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(max(1, int(limit)))
    with connect_postgres(resolved_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT capture_id, source, layer, entity_type, entity_key, operation,
                       captured_at, payload, metadata
                FROM raw_capture_events
                {where_clause}
                ORDER BY capture_id ASC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cursor.fetchall()
    return [
        {
            "capture_id": int(capture_id),
            "source": str(row_source),
            "layer": str(row_layer),
            "entity_type": str(entity_type),
            "entity_key": str(entity_key) if entity_key not in (None, "") else None,
            "operation": str(operation),
            "captured_at": captured_at.astimezone(timezone.utc).isoformat(),
            "payload": _decode_payload(payload),
            "metadata": _decode_payload(metadata) if metadata is not None else {},
        }
        for (
            capture_id,
            row_source,
            row_layer,
            entity_type,
            entity_key,
            operation,
            captured_at,
            payload,
            metadata,
        ) in rows
    ]

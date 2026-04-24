from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Protocol

from adapters.sportsbooks import (
    SPORTSGAMEODDS_EVENTS_URL,
    SportsbookJsonFeedClient,
    SportsGameOddsClient,
    TheOddsApiClient,
    normalize_odds_event,
)
from research.data.odds_api import load_event_map
from storage import (
    FileBackedCurrentStateStore,
    ParquetStore,
    RawStore,
    SourceHealthUpdate,
    SourceHealthStore,
    SportsbookEventRecord,
    SportsbookEventRepository,
    SportsbookOddsRecord,
    SportsbookOddsRepository,
    materialize_source_health_state,
    materialize_sportsbook_event_state,
    materialize_sportsbook_quote_state,
    project_source_health_state,
)
from storage.postgres import (
    SourceHealthRepository,
    append_raw_capture_event,
    upsert_capture_checkpoint,
)


def _row_payload(row: Any) -> dict[str, Any]:
    if is_dataclass(row) and not isinstance(row, type):
        return asdict(row)
    if isinstance(row, dict):
        return dict(row)
    raise TypeError("row must be a dataclass instance or dict")


class _JsonKeyedRepository:
    def __init__(self, path: Path) -> None:
        self.path = path

    def upsert(self, key: str, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        existing[str(key)] = payload
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(existing, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return payload

    def read_all(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))


class _JsonAppendRepository(_JsonKeyedRepository):
    def append(self, row: Any) -> dict[str, Any]:
        payload = _row_payload(row)
        existing = self.read_all()
        existing[str(len(existing))] = payload
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(existing, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return payload


def sanitize_capture_error(error: Exception) -> dict[str, str]:
    kind = error.__class__.__name__
    if isinstance(error, FileNotFoundError):
        message = "event map file could not be read"
    elif isinstance(error, PermissionError):
        message = "capture file access denied"
    elif kind in {"HTTPStatusError", "ConnectError", "ReadTimeout", "URLError"}:
        message = "sportsbook fetch failed"
    else:
        message = f"{kind} during sportsbook capture"
    return {"kind": kind, "message": message}


class SportsbookCaptureSource(Protocol):
    provider_name: str

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]: ...

    def event_id(self, event: dict[str, Any]) -> str: ...

    def build_raw_capture_payload(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, Any],
    ) -> dict[str, Any]: ...

    def normalize_event(
        self,
        event: dict[str, Any],
        *,
        market_type: str,
        captured_at: datetime,
    ) -> list[dict[str, Any]]: ...

    def build_event_record(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, Any],
    ) -> SportsbookEventRecord: ...

    def build_capture_metadata(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market: str,
        captured_at: datetime,
    ) -> dict[str, Any]: ...


SUPPORTED_SPORTSBOOK_CAPTURE_PROVIDERS = (
    "theoddsapi",
    "json_feed",
    "sportsgameodds",
)
SPORTSBOOK_CAPTURE_RAW_LAYER = "odds_api"


@dataclass(frozen=True)
class SportsbookCaptureWritePlan:
    persist_normalized_rows: bool = True
    materialize_current_state: bool = True

    @classmethod
    def compatibility_exports(cls) -> "SportsbookCaptureWritePlan":
        return cls(persist_normalized_rows=True, materialize_current_state=True)

    @classmethod
    def raw_ingress_only(cls) -> "SportsbookCaptureWritePlan":
        return cls(persist_normalized_rows=False, materialize_current_state=False)


class KeyedRowRepository(Protocol):
    def upsert(self, key: str, row: Any) -> dict[str, Any]: ...

    def read_all(self) -> dict[str, Any]: ...


class AppendRowRepository(Protocol):
    def append(self, row: Any) -> dict[str, Any]: ...


class TheOddsApiCaptureSource:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        client: TheOddsApiClient | None = None,
    ) -> None:
        self.provider_name = "theoddsapi"
        if client is None:
            if api_key in (None, ""):
                raise ValueError("api_key is required when client is not provided")
            client = TheOddsApiClient(api_key=api_key)
        self._client = client

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]:
        return self._client.fetch_upcoming(sport, market_type)

    def event_id(self, event: dict[str, Any]) -> str:
        return _default_event_id(event)

    def normalize_event(
        self,
        event: dict[str, Any],
        *,
        market_type: str,
        captured_at: datetime,
    ) -> list[dict[str, Any]]:
        return _default_normalize_event(
            event,
            provider_name=self.provider_name,
            market_type=market_type,
            captured_at=captured_at,
        )

    def build_raw_capture_payload(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, Any],
    ) -> dict[str, Any]:
        return _default_build_raw_capture_payload(
            event,
            sport=sport,
            event_identity=event_identity,
        )

    def build_event_record(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, Any],
    ) -> SportsbookEventRecord:
        return _default_build_event_record(
            event,
            provider_name=self.provider_name,
            sport=sport,
            event_identity=event_identity,
        )

    def build_capture_metadata(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market: str,
        captured_at: datetime,
    ) -> dict[str, Any]:
        return _default_build_capture_metadata(
            event,
            provider_name=self.provider_name,
            sport=sport,
            market=market,
            captured_at=captured_at,
        )


def _coalesce_string(event: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = event.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _nested_string(payload: dict[str, Any], *paths: str) -> str | None:
    for path in paths:
        current: Any = payload
        found = True
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                found = False
                break
            current = current[part]
        if found and current not in (None, ""):
            return str(current)
    return None


def _coalesce_decimal_price(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _coalesce_american_price(payload: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    return None


class SportsbookJsonFeedCaptureSource:
    def __init__(
        self,
        *,
        feed_url: str | None = None,
        provider_name: str = "json_feed",
        client: SportsbookJsonFeedClient | None = None,
    ) -> None:
        self.provider_name = provider_name
        resolved_client = client
        if resolved_client is None:
            if feed_url in (None, ""):
                raise ValueError("feed_url is required when client is not provided")
            resolved_client = SportsbookJsonFeedClient(feed_url=feed_url)
        self._client: SportsbookJsonFeedClient = resolved_client

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]:
        return self._client.fetch_upcoming(sport, market_type)

    def event_id(self, event: dict[str, Any]) -> str:
        event_id = _coalesce_string(event, "external_id", "id", "provider_event_id")
        if event_id in (None, ""):
            raise ValueError("json feed event is missing a stable event id")
        return event_id

    def _market_outcomes(
        self,
        event: dict[str, Any],
        *,
        market_type: str,
        home_team: str,
        away_team: str,
    ) -> list[dict[str, float | int | str]]:
        markets = event.get("markets")
        if isinstance(markets, list):
            for market in markets:
                if not isinstance(market, dict):
                    continue
                market_key = market.get("key") or market.get("market_type")
                if market_key not in (None, "") and str(market_key) != market_type:
                    continue
                outcomes = market.get("outcomes") or market.get("prices") or []
                if not isinstance(outcomes, list):
                    continue
                normalized: list[dict[str, float | int | str]] = []
                for outcome in outcomes:
                    if not isinstance(outcome, dict):
                        continue
                    selection = outcome.get("name") or outcome.get("selection")
                    price = _coalesce_decimal_price(
                        outcome,
                        "price",
                        "decimal_price",
                        "decimal_odds",
                    )
                    if price is None:
                        price = _coalesce_american_price(
                            outcome,
                            "american_price",
                            "american_odds",
                        )
                    if selection in (None, "") or price in (None, ""):
                        continue
                    normalized.append({"name": str(selection), "price": price})
                if normalized:
                    return normalized
        fallback: list[dict[str, float | int | str]] = []
        home_price = _coalesce_decimal_price(
            event,
            "home_price",
            "home_odds",
        )
        away_price = _coalesce_decimal_price(
            event,
            "away_price",
            "away_odds",
        )
        if home_price is None:
            home_price = _coalesce_american_price(event, "moneyline_home")
        if away_price is None:
            away_price = _coalesce_american_price(event, "moneyline_away")
        if home_price not in (None, "") and home_team:
            fallback.append({"name": home_team, "price": home_price})
        if away_price not in (None, "") and away_team:
            fallback.append({"name": away_team, "price": away_price})
        return fallback

    def _canonical_event(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
    ) -> dict[str, Any]:
        home_team = _coalesce_string(event, "home_team", "home") or ""
        away_team = _coalesce_string(event, "away_team", "away") or ""
        bookmaker_name = (
            _coalesce_string(
                event,
                "bookmaker",
                "bookmaker_name",
                "source",
                "feed_source",
            )
            or self.provider_name
        )
        last_update = _coalesce_string(
            event,
            "provider_updated_at",
            "last_update",
            "updated_at",
            "source_ts",
            "commence_time",
            "start_time",
        )
        canonical_event = {
            "id": self.event_id(event),
            "sport_key": _coalesce_string(event, "sport_key", "sport") or sport,
            "sport_title": _coalesce_string(
                event,
                "sport_title",
                "league_name",
                "league",
            ),
            "home_team": home_team,
            "away_team": away_team,
            "commence_time": _coalesce_string(
                event,
                "commence_time",
                "start_time",
            ),
            "bookmakers": [
                {
                    "key": bookmaker_name,
                    "title": bookmaker_name,
                    "last_update": last_update,
                    "markets": [
                        {
                            "key": market_type,
                            "outcomes": self._market_outcomes(
                                event,
                                market_type=market_type,
                                home_team=home_team,
                                away_team=away_team,
                            ),
                        }
                    ],
                }
            ],
        }
        provider_event_id = _coalesce_string(event, "provider_event_id", "external_id")
        if provider_event_id not in (None, ""):
            canonical_event["provider_event_id"] = provider_event_id
        return canonical_event

    def normalize_event(
        self,
        event: dict[str, Any],
        *,
        market_type: str,
        captured_at: datetime,
    ) -> list[dict[str, Any]]:
        canonical_event = self._canonical_event(
            event,
            sport=_coalesce_string(event, "sport_key", "sport") or "",
            market_type=market_type,
        )
        return _default_normalize_event(
            canonical_event,
            provider_name=self.provider_name,
            market_type=market_type,
            captured_at=captured_at,
        )

    def build_raw_capture_payload(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, Any],
    ) -> dict[str, Any]:
        canonical_event = self._canonical_event(
            event,
            sport=sport,
            market_type=market_type,
        )
        return _default_build_raw_capture_payload(
            canonical_event,
            sport=sport,
            event_identity=event_identity,
        )

    def build_event_record(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
        event_identity: dict[str, Any],
    ) -> SportsbookEventRecord:
        canonical_event = self._canonical_event(
            event,
            sport=sport,
            market_type=market_type,
        )
        return _default_build_event_record(
            canonical_event,
            provider_name=self.provider_name,
            sport=sport,
            event_identity=event_identity,
        )

    def build_capture_metadata(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market: str,
        captured_at: datetime,
    ) -> dict[str, Any]:
        canonical_event = self._canonical_event(
            event,
            sport=sport,
            market_type=market,
        )
        metadata = _default_build_capture_metadata(
            canonical_event,
            provider_name=self.provider_name,
            sport=sport,
            market=market,
            captured_at=captured_at,
        )
        provider_event_id = _coalesce_string(event, "provider_event_id", "external_id")
        if provider_event_id not in (None, ""):
            metadata["provider_event_id"] = provider_event_id
        return metadata


class SportsGameOddsCaptureSource(SportsbookJsonFeedCaptureSource):
    def __init__(
        self,
        *,
        api_key: str | None = None,
        feed_url: str | None = None,
        client: SportsGameOddsClient | None = None,
    ) -> None:
        resolved_client = client
        if resolved_client is None:
            if api_key in (None, ""):
                raise ValueError("api_key is required when client is not provided")
            if feed_url not in (None, "", SPORTSGAMEODDS_EVENTS_URL):
                raise ValueError(
                    "sportsgameodds feed_url must be the official events endpoint"
                )
            resolved_client = SportsGameOddsClient(
                api_key=api_key,
                feed_url=feed_url or SPORTSGAMEODDS_EVENTS_URL,
            )
        super().__init__(provider_name="sportsgameodds", client=resolved_client)

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]:
        events = self._client.fetch_upcoming("", "")
        return [
            event
            for event in events
            if self._matches_requested_sport(event, sport)
            and self._matches_requested_market(event, market_type)
        ]

    def _matches_requested_sport(self, event: dict[str, Any], sport: str) -> bool:
        if sport in (None, ""):
            return True
        requested_tokens = {str(sport).strip().lower()}
        if "_" in str(sport):
            requested_tokens.update(
                part for part in str(sport).strip().lower().split("_") if part
            )
        observed_tokens = {
            token
            for token in (
                _coalesce_string(event, "sport_key", "sport", "sportKey"),
                _coalesce_string(event, "league_name", "leagueName", "leagueID"),
            )
            if token not in (None, "")
            for token in {str(token).strip().lower()}
        }
        return bool(requested_tokens & observed_tokens)

    def _matches_requested_market(
        self, event: dict[str, Any], market_type: str
    ) -> bool:
        if market_type in (None, ""):
            return True
        requested = str(market_type).strip().lower()
        aliases = {requested}
        if requested == "h2h":
            aliases.add("moneyline")
        odds = event.get("odds")
        if not isinstance(odds, dict):
            return False
        for odd_key, odd_payload in odds.items():
            candidates = {str(odd_key).strip().lower()}
            if isinstance(odd_payload, dict):
                for value in (
                    odd_payload.get("market_type"),
                    odd_payload.get("market"),
                    odd_payload.get("betType"),
                ):
                    if value not in (None, ""):
                        candidates.add(str(value).strip().lower())
            if aliases & candidates:
                return True
        return False

    def _selection_name(
        self,
        odd_key: str,
        odd_payload: dict[str, Any],
        *,
        home_team: str,
        away_team: str,
    ) -> str | None:
        direct = _coalesce_string(odd_payload, "selection", "name", "side")
        if direct not in (None, ""):
            return direct
        normalized_key = odd_key.strip().lower()
        if normalized_key.endswith("home"):
            return home_team
        if normalized_key.endswith("away"):
            return away_team
        return None

    def _extract_bookmaker_price(
        self, bookmaker_payload: dict[str, Any]
    ) -> float | int | None:
        odds_payload = bookmaker_payload.get("odds")
        if isinstance(odds_payload, dict):
            price = _coalesce_decimal_price(
                odds_payload,
                "decimal",
                "decimal_price",
                "decimal_odds",
                "price",
            )
            if price is None:
                price = _coalesce_american_price(
                    odds_payload,
                    "american",
                    "american_price",
                    "american_odds",
                )
            if price is not None:
                return price
        price = _coalesce_decimal_price(
            bookmaker_payload,
            "decimal",
            "decimal_price",
            "decimal_odds",
            "price",
        )
        if price is None:
            price = _coalesce_american_price(
                bookmaker_payload,
                "american",
                "american_price",
                "american_odds",
            )
        return price

    def _canonical_bookmakers(
        self,
        event: dict[str, Any],
        *,
        market_type: str,
        home_team: str,
        away_team: str,
        last_update: str | None,
    ) -> list[dict[str, Any]]:
        bookmaker_rows: dict[str, dict[str, Any]] = {}
        odds = event.get("odds")
        if not isinstance(odds, dict):
            return []
        for odd_key, odd_payload in odds.items():
            if not isinstance(odd_payload, dict):
                continue
            candidates = {str(odd_key).strip().lower()}
            for value in (
                odd_payload.get("market_type"),
                odd_payload.get("market"),
                odd_payload.get("betType"),
            ):
                if value not in (None, ""):
                    candidates.add(str(value).strip().lower())
            if market_type == "h2h":
                candidates.add("moneyline")
            if market_type not in candidates and not (
                {"h2h", "moneyline"} & candidates and market_type == "h2h"
            ):
                continue
            selection = self._selection_name(
                str(odd_key), odd_payload, home_team=home_team, away_team=away_team
            )
            if selection in (None, ""):
                continue
            by_bookmaker = odd_payload.get("byBookmaker")
            if not isinstance(by_bookmaker, dict):
                continue
            for bookmaker_id, bookmaker_payload in by_bookmaker.items():
                if not isinstance(bookmaker_payload, dict):
                    continue
                price = self._extract_bookmaker_price(bookmaker_payload)
                if price in (None, ""):
                    continue
                bookmaker_name = str(bookmaker_id)
                entry = bookmaker_rows.setdefault(
                    bookmaker_name,
                    {
                        "key": bookmaker_name,
                        "title": bookmaker_name,
                        "last_update": _coalesce_string(
                            bookmaker_payload,
                            "updated_at",
                            "updatedAt",
                            "last_update",
                        )
                        or last_update,
                        "markets": [{"key": market_type, "outcomes": []}],
                    },
                )
                entry["markets"][0]["outcomes"].append(
                    {"name": str(selection), "price": price}
                )
        return [
            entry
            for entry in bookmaker_rows.values()
            if entry["markets"][0]["outcomes"]
        ]

    def event_id(self, event: dict[str, Any]) -> str:
        event_id = _coalesce_string(event, "eventID", "external_id", "id")
        if event_id in (None, ""):
            raise ValueError("sportsgameodds event is missing a stable event id")
        return event_id

    def _canonical_event(
        self,
        event: dict[str, Any],
        *,
        sport: str,
        market_type: str,
    ) -> dict[str, Any]:
        home_team = (
            _coalesce_string(event, "homeTeamName", "home", "home_team")
            or _nested_string(event, "teams.home.team.name")
            or ""
        )
        away_team = (
            _coalesce_string(event, "awayTeamName", "away", "away_team")
            or _nested_string(event, "teams.away.team.name")
            or ""
        )
        odds = event.get("odds")
        last_update = _coalesce_string(
            event,
            "updatedAt",
            "startsAt",
            "startTime",
            "start_time",
        ) or _nested_string(event, "status.startsAt")
        bookmakers = self._canonical_bookmakers(
            event,
            market_type=market_type,
            home_team=home_team,
            away_team=away_team,
            last_update=last_update,
        )
        canonical_event = {
            "id": self.event_id(event),
            "sport_key": _coalesce_string(event, "sport_key", "sport", "sportKey")
            or sport,
            "sport_title": _coalesce_string(
                event, "league_name", "leagueName", "leagueID"
            ),
            "home_team": home_team,
            "away_team": away_team,
            "commence_time": _coalesce_string(
                event, "startTime", "startsAt", "start_time"
            )
            or _nested_string(event, "status.startsAt"),
            "bookmakers": bookmakers,
        }
        canonical_event["provider_event_id"] = self.event_id(event)
        return canonical_event


@dataclass(frozen=True)
class SportsbookCaptureRequest:
    root: str
    sport: str
    market: str
    event_map_file: str | None = None
    stale_after_ms: int = 60_000


@dataclass(frozen=True)
class SportsbookCaptureStores:
    raw: RawStore
    parquet: ParquetStore
    current: FileBackedCurrentStateStore
    sportsbook_events: KeyedRowRepository
    sportsbook_odds: AppendRowRepository
    current_health: SourceHealthStore
    postgres_health: KeyedRowRepository

    @classmethod
    def from_root(
        cls,
        root: str | Path,
        *,
        require_postgres: bool = False,
    ) -> SportsbookCaptureStores:
        root_path = Path(root)
        postgres_root = root_path / "postgres"
        try:
            sportsbook_events = SportsbookEventRepository(postgres_root)
            sportsbook_odds = SportsbookOddsRepository(postgres_root)
            postgres_health = SourceHealthRepository(postgres_root)
        except RuntimeError as exc:
            if "Could not resolve a Postgres DSN" not in str(exc):
                raise
            if require_postgres:
                raise
            sportsbook_events = _JsonKeyedRepository(
                postgres_root / "sportsbook_events.json"
            )
            sportsbook_odds = _JsonAppendRepository(
                postgres_root / "sportsbook_odds.json"
            )
            postgres_health = _JsonKeyedRepository(postgres_root / "source_health.json")
        return cls(
            raw=RawStore(root_path / "raw"),
            parquet=ParquetStore(root_path / "parquet"),
            current=FileBackedCurrentStateStore(root_path / "current"),
            sportsbook_events=sportsbook_events,
            sportsbook_odds=sportsbook_odds,
            current_health=SourceHealthStore(
                root_path / "current" / "source_health.json"
            ),
            postgres_health=postgres_health,
        )


def _dsn_optional_failure(exc: RuntimeError) -> bool:
    message = str(exc)
    return "Postgres DSN" in message or "Could not resolve a Postgres DSN" in message


def _default_event_id(event: dict[str, Any]) -> str:
    return str(event.get("id") or "")


def _resolve_write_plan(
    *,
    write_plan: SportsbookCaptureWritePlan | None,
    materialize_current: bool | None,
) -> SportsbookCaptureWritePlan:
    if write_plan is not None:
        return write_plan
    if materialize_current is None:
        return SportsbookCaptureWritePlan.compatibility_exports()
    return SportsbookCaptureWritePlan(materialize_current_state=materialize_current)


def _parse_iso_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _event_provenance_fields(event_payload: dict[str, Any]) -> dict[str, str]:
    source_timestamps = [
        timestamp
        for bookmaker in event_payload.get("bookmakers", [])
        if isinstance(bookmaker, dict)
        for timestamp in [_parse_iso_timestamp(bookmaker.get("last_update"))]
        if timestamp is not None
    ]
    if not source_timestamps:
        return {}
    return {
        "source_ts_min": min(source_timestamps).isoformat(),
        "source_ts_max": max(source_timestamps).isoformat(),
    }


def _default_normalize_event(
    event: dict[str, Any],
    *,
    provider_name: str,
    market_type: str,
    captured_at: datetime,
) -> list[dict[str, Any]]:
    return normalize_odds_event(
        event,
        source=provider_name,
        market_type=market_type,
        captured_at=captured_at,
    )


def _default_build_event_record(
    event: dict[str, Any],
    *,
    provider_name: str,
    sport: str,
    event_identity: dict[str, Any],
) -> SportsbookEventRecord:
    enriched_event = _default_build_raw_capture_payload(
        event,
        sport=sport,
        event_identity=event_identity,
    )
    return SportsbookEventRecord(
        sportsbook_event_id=_default_event_id(event),
        source=provider_name,
        sport=sport,
        league=(
            str(event.get("sport_title"))
            if event.get("sport_title") not in (None, "")
            else None
        ),
        home_team=(
            str(event.get("home_team"))
            if event.get("home_team") not in (None, "")
            else None
        ),
        away_team=(
            str(event.get("away_team"))
            if event.get("away_team") not in (None, "")
            else None
        ),
        start_time=str(event.get("commence_time") or ""),
        raw_json=enriched_event,
    )


def _default_build_raw_capture_payload(
    event: dict[str, Any],
    *,
    sport: str,
    event_identity: dict[str, Any],
) -> dict[str, Any]:
    enriched_event = dict(event)
    if enriched_event.get("sport_key") in (None, ""):
        enriched_event["sport_key"] = sport
    for field in ("event_key", "game_id", "sport", "series"):
        if event_identity.get(field) not in (None, ""):
            enriched_event[field] = event_identity[field]
    return enriched_event


def _default_build_capture_metadata(
    event: dict[str, Any],
    *,
    provider_name: str,
    sport: str,
    market: str,
    captured_at: datetime,
) -> dict[str, Any]:
    return {
        "provider": provider_name,
        "sport": sport,
        "market": market,
        "capture_ts": captured_at.isoformat(),
        **_event_provenance_fields(event),
    }


def _safe_append_raw_capture_event(
    stores: SportsbookCaptureStores,
    *,
    source_name: str,
    layer: str,
    entity_type: str,
    payload: dict[str, Any],
    entity_key: str | None,
    captured_at: datetime,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return append_raw_capture_event(
            source=source_name,
            layer=layer,
            entity_type=entity_type,
            entity_key=entity_key,
            operation="append",
            payload=payload,
            captured_at=captured_at,
            metadata=metadata,
            root=stores.current.root.parent / "postgres",
        )
    except RuntimeError as exc:
        if not _dsn_optional_failure(exc):
            raise
        stores.raw.write(source_name, layer, captured_at, payload)
        return {
            "source": source_name,
            "layer": layer,
            "entity_type": entity_type,
            "entity_key": entity_key,
            "operation": "append",
            "payload": dict(payload),
            "metadata": dict(metadata or {}),
            "captured_at": captured_at.isoformat(),
        }


def _safe_upsert_checkpoint(
    stores: SportsbookCaptureStores,
    *,
    checkpoint_name: str,
    source_name: str,
    checkpoint_value: str | None,
    checkpoint_ts: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return upsert_capture_checkpoint(
            checkpoint_name,
            source_name,
            checkpoint_value,
            checkpoint_ts=checkpoint_ts,
            metadata=metadata,
            root=stores.current.root.parent / "postgres",
        )
    except RuntimeError as exc:
        if not _dsn_optional_failure(exc):
            raise
        return {
            "checkpoint_name": checkpoint_name,
            "source_name": source_name,
            "checkpoint_value": checkpoint_value,
            "checkpoint_ts": checkpoint_ts,
            "metadata": dict(metadata or {}),
        }


def _write_source_health(
    stores: SportsbookCaptureStores,
    *,
    source_name: str,
    stale_after_ms: int,
    status: str,
    details: dict[str, Any],
    success: bool,
    observed_at: datetime,
    materialize_current: bool = True,
) -> dict[str, Any]:
    update = SourceHealthUpdate(
        source_name=source_name,
        stale_after_ms=stale_after_ms,
        status=status,
        details=details,
        success=success,
        observed_at=observed_at,
    )
    if materialize_current:
        projected = materialize_source_health_state(stores.current_health, [update])
        record = projected[source_name]
    else:
        read_all = getattr(stores.postgres_health, "read_all", None)
        existing_rows = read_all() if callable(read_all) else {}
        existing = (
            existing_rows.get(source_name, {})
            if isinstance(existing_rows, dict)
            else {}
        )
        projected = project_source_health_state(
            [update], existing={source_name: existing}
        )
        record = projected[source_name]
    stores.postgres_health.upsert(source_name, record)
    return dict(record)


def record_sportsbook_capture_failure(
    stores: SportsbookCaptureStores,
    request: SportsbookCaptureRequest,
    source: SportsbookCaptureSource,
    *,
    error: Exception,
    observed_at: datetime | None = None,
    materialize_current: bool | None = None,
    write_plan: SportsbookCaptureWritePlan | None = None,
) -> dict[str, Any]:
    capture_time = observed_at or datetime.now(timezone.utc)
    sanitized_error = sanitize_capture_error(error)
    resolved_write_plan = _resolve_write_plan(
        write_plan=write_plan,
        materialize_current=materialize_current,
    )
    health = _write_source_health(
        stores,
        source_name="sportsbook_odds",
        stale_after_ms=request.stale_after_ms,
        status="red",
        details={
            "provider": source.provider_name,
            "sport": request.sport,
            "market": request.market,
            "error_kind": sanitized_error["kind"],
            "error_message": sanitized_error["message"],
        },
        success=False,
        observed_at=capture_time,
        materialize_current=resolved_write_plan.materialize_current_state,
    )
    return {
        "ok": False,
        "error_kind": sanitized_error["kind"],
        "error_message": sanitized_error["message"],
        "provider": source.provider_name,
        "sport": request.sport,
        "market": request.market,
        "root": request.root,
        "source_health": health,
    }


def capture_sportsbook_odds_once(
    request: SportsbookCaptureRequest,
    *,
    source: SportsbookCaptureSource,
    stores: SportsbookCaptureStores | None = None,
    observed_at: datetime | None = None,
    materialize_current: bool | None = None,
    write_plan: SportsbookCaptureWritePlan | None = None,
) -> dict[str, object]:
    resolved_stores = stores or SportsbookCaptureStores.from_root(request.root)
    capture_time = observed_at or datetime.now(timezone.utc)
    resolved_write_plan = _resolve_write_plan(
        write_plan=write_plan,
        materialize_current=materialize_current,
    )
    events = source.fetch_upcoming(request.sport, request.market)
    event_map = load_event_map(request.event_map_file)

    current_event_rows: list[SportsbookEventRecord] = []
    current_quote_rows: list[SportsbookOddsRecord] = []
    normalized_rows: list[dict[str, Any]] = []

    for event in events:
        event_payload = dict(event)
        if event_payload.get("sport_key") in (None, ""):
            event_payload["sport_key"] = request.sport
        event_id = source.event_id(event_payload)
        event_identity = event_map.get(event_id, {})
        capture_payload = source.build_raw_capture_payload(
            event_payload,
            sport=request.sport,
            market_type=request.market,
            event_identity=event_identity,
        )
        normalized_rows.extend(
            source.normalize_event(
                event_payload,
                market_type=request.market,
                captured_at=capture_time,
            )
        )
        resolved_stores.raw.write(
            "sportsbook",
            "odds",
            capture_time,
            capture_payload,
        )

        _safe_append_raw_capture_event(
            resolved_stores,
            source_name="sportsbook",
            layer=SPORTSBOOK_CAPTURE_RAW_LAYER,
            entity_type="sportsbook_odds_envelope",
            entity_key=event_id or None,
            payload=capture_payload,
            captured_at=capture_time,
            metadata=source.build_capture_metadata(
                event_payload,
                sport=request.sport,
                market=request.market,
                captured_at=capture_time,
            ),
        )
        if resolved_write_plan.persist_normalized_rows:
            event_record = source.build_event_record(
                event_payload,
                sport=request.sport,
                market_type=request.market,
                event_identity=event_identity,
            )
            resolved_stores.sportsbook_events.upsert(
                event_record.sportsbook_event_id,
                event_record,
            )
            if resolved_write_plan.materialize_current_state:
                current_event_rows.append(event_record)

    for row in normalized_rows:
        record = SportsbookOddsRecord(
            sportsbook_event_id=str(row["sportsbook_event_id"]),
            source=str(row["source"]),
            market_type=str(row["market_type"]),
            selection=str(row["selection"]),
            price_decimal=(
                float(row["price_decimal"])
                if row.get("price_decimal") not in (None, "")
                else None
            ),
            implied_prob=(
                float(row["implied_prob"])
                if row.get("implied_prob") not in (None, "")
                else None
            ),
            overround=(
                float(row["overround"])
                if row.get("overround") not in (None, "")
                else None
            ),
            quote_ts=str(row["quote_ts"]),
            source_age_ms=int(row["source_age_ms"]),
            raw_json=dict(row["raw_json"]),
            provider=(
                str(row.get("provider"))
                if row.get("provider") not in (None, "")
                else source.provider_name
            ),
            source_ts=(
                str(row.get("source_ts"))
                if row.get("source_ts") not in (None, "")
                else None
            ),
            capture_ts=(
                str(row.get("capture_ts"))
                if row.get("capture_ts") not in (None, "")
                else None
            ),
        )
        if resolved_write_plan.persist_normalized_rows:
            resolved_stores.sportsbook_odds.append(record)
            if resolved_write_plan.materialize_current_state:
                current_quote_rows.append(record)

    latest_source_ts = max(
        [
            str(row.get("source_ts") or "")
            for row in normalized_rows
            if row.get("source_ts")
        ],
        default=capture_time.isoformat(),
    )
    checkpoint = _safe_upsert_checkpoint(
        resolved_stores,
        checkpoint_name="sportsbook_odds",
        source_name=source.provider_name,
        checkpoint_value=latest_source_ts,
        checkpoint_ts=latest_source_ts,
        metadata={
            "provider": source.provider_name,
            "sport": request.sport,
            "market": request.market,
            "event_count": len(events),
            "row_count": len(normalized_rows),
        },
    )

    if resolved_write_plan.materialize_current_state:
        materialize_sportsbook_event_state(resolved_stores.current, current_event_rows)
        materialize_sportsbook_quote_state(resolved_stores.current, current_quote_rows)
    if resolved_write_plan.persist_normalized_rows:
        resolved_stores.parquet.append_records(
            "odds_snapshots", capture_time, normalized_rows
        )

    health = _write_source_health(
        resolved_stores,
        source_name="sportsbook_odds",
        stale_after_ms=request.stale_after_ms,
        status="ok",
        details={
            "provider": source.provider_name,
            "sport": request.sport,
            "market": request.market,
            "event_count": len(events),
            "row_count": len(normalized_rows),
            "checkpoint": checkpoint,
        },
        success=True,
        observed_at=capture_time,
        materialize_current=resolved_write_plan.materialize_current_state,
    )

    return {
        "ok": True,
        "provider": source.provider_name,
        "sport": request.sport,
        "market": request.market,
        "event_count": len(events),
        "row_count": len(normalized_rows),
        "root": request.root,
        "checkpoint": checkpoint,
        "source_health": health,
    }

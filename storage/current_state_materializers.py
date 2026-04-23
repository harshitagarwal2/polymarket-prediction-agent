from __future__ import annotations

from typing import Any, Iterable

from .current_state import FileBackedCurrentStateStore
from .current_state_projectors import (  # pyright: ignore[reportMissingImports]
    project_polymarket_bbo_state,
    project_polymarket_market_state,
    project_source_health_state,
    project_sportsbook_event_state,
    project_sportsbook_quote_state,
)
from .source_health import SourceHealthStore


def _materialize_table(
    store: FileBackedCurrentStateStore,
    *,
    table: str,
    projected: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    store.write_table(table, projected)
    return projected


def materialize_polymarket_market_state(
    store: FileBackedCurrentStateStore,
    rows: Iterable[Any],
) -> dict[str, dict[str, Any]]:
    return _materialize_table(
        store,
        table="polymarket_markets",
        projected=project_polymarket_market_state(rows),
    )


def materialize_polymarket_bbo_state(
    store: FileBackedCurrentStateStore,
    rows: Iterable[Any],
) -> dict[str, dict[str, Any]]:
    return _materialize_table(
        store,
        table="polymarket_bbo",
        projected=project_polymarket_bbo_state(rows),
    )


def materialize_sportsbook_event_state(
    store: FileBackedCurrentStateStore,
    rows: Iterable[Any],
) -> dict[str, dict[str, Any]]:
    return _materialize_table(
        store,
        table="sportsbook_events",
        projected=project_sportsbook_event_state(rows),
    )


def materialize_sportsbook_quote_state(
    store: FileBackedCurrentStateStore,
    rows: Iterable[Any],
) -> dict[str, dict[str, Any]]:
    return _materialize_table(
        store,
        table="sportsbook_odds",
        projected=project_sportsbook_quote_state(rows),
    )


def materialize_source_health_state(
    store: SourceHealthStore,
    rows: Iterable[Any],
) -> dict[str, dict[str, Any]]:
    projected = project_source_health_state(rows, existing=store.read_all())
    store.write_all(projected)
    return projected


__all__ = [
    "materialize_polymarket_bbo_state",
    "materialize_polymarket_market_state",
    "materialize_source_health_state",
    "materialize_sportsbook_event_state",
    "materialize_sportsbook_quote_state",
]

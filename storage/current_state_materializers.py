from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .current_state import FileBackedCurrentStateStore
from .current_state_projectors import (  # pyright: ignore[reportMissingImports]
    CAPTURE_OWNED_SOURCE_HEALTH_NAMES,
    merge_source_health_state,
    project_polymarket_bbo_state,
    project_polymarket_market_state,
    project_source_health_state,
    project_sportsbook_event_state,
    project_sportsbook_quote_state,
)
from .postgres.bootstrap import (  # pyright: ignore[reportMissingImports]
    PostgresDsnNotConfiguredError,
    resolve_postgres_dsn,
)
from .source_health import SourceHealthStore


def _projected_authority_enabled(runtime_root: Path) -> bool:
    try:
        resolve_postgres_dsn(runtime_root / "postgres")
    except PostgresDsnNotConfiguredError:
        return False
    return True


def _current_store_projected_authority_enabled(
    store: FileBackedCurrentStateStore,
) -> bool:
    return _projected_authority_enabled(store.root.parent)


def _source_health_store_projected_authority_enabled(store: SourceHealthStore) -> bool:
    return _projected_authority_enabled(store.path.parent.parent)


def _source_name(row: Any) -> str:
    if isinstance(row, dict):
        return str(row.get("source_name") or "")
    value = getattr(row, "source_name", "")
    return str(value or "")


def _materialize_table(
    store: FileBackedCurrentStateStore,
    *,
    table: str,
    projected: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if _current_store_projected_authority_enabled(store):
        return projected
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
    row_list = tuple(rows)
    if _source_health_store_projected_authority_enabled(store):
        projected = project_source_health_state(row_list, existing=store.read_all())
        builder_rows = tuple(
            row
            for row in row_list
            if _source_name(row) not in CAPTURE_OWNED_SOURCE_HEALTH_NAMES
        )
        if builder_rows:
            store.write_all(
                project_source_health_state(builder_rows, existing=store.read_all())
            )
        return projected
    else:
        projected = project_source_health_state(row_list, existing=store.read_all())
    store.write_all(projected)
    return projected


def materialize_capture_owned_source_health_state(
    store: SourceHealthStore,
    rows: Iterable[Any],
) -> dict[str, dict[str, Any]]:
    projected = merge_source_health_state(
        rows,
        existing=store.read_all(),
        owned_source_names=CAPTURE_OWNED_SOURCE_HEALTH_NAMES,
    )
    store.write_all(projected)
    return projected


__all__ = [
    "materialize_capture_owned_source_health_state",
    "materialize_polymarket_bbo_state",
    "materialize_polymarket_market_state",
    "materialize_source_health_state",
    "materialize_sportsbook_event_state",
    "materialize_sportsbook_quote_state",
]

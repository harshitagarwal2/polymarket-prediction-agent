from __future__ import annotations

import argparse
import importlib
import os
from pathlib import Path
from typing import Iterable


DEFAULT_DSN_ENV_VARS = (
    "PREDICTION_MARKET_POSTGRES_DSN",
    "POSTGRES_DSN",
    "DATABASE_URL",
)


def _marker_directory(root: str | Path) -> Path:
    root_path = Path(root)
    return root_path if root_path.name == "postgres" else root_path / "postgres"


def _looks_like_dsn(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized.startswith("postgresql://") or normalized.startswith(
        "postgres://"
    )


def _dsn_from_env() -> str | None:
    for key in DEFAULT_DSN_ENV_VARS:
        value = os.getenv(key)
        if value:
            return value
    return None


def resolve_postgres_dsn(value: str | Path | None = None) -> str:
    if value is None:
        env_dsn = _dsn_from_env()
        if env_dsn:
            return env_dsn
        raise RuntimeError(
            "Postgres DSN not configured. Set PREDICTION_MARKET_POSTGRES_DSN, "
            "POSTGRES_DSN, or DATABASE_URL, or pass a postgresql:// DSN directly."
        )

    if isinstance(value, Path):
        candidate_path = value
    else:
        candidate_text = str(value)
        if _looks_like_dsn(candidate_text):
            return candidate_text
        candidate_path = Path(candidate_text)

    if candidate_path.is_file():
        candidate = candidate_path.read_text(encoding="utf-8").strip()
        if candidate:
            if not _looks_like_dsn(candidate):
                raise RuntimeError(
                    f"DSN file {candidate_path} did not contain a postgres DSN"
                )
            return candidate

    if candidate_path.exists() and candidate_path.is_dir():
        for filename in ("postgres.dsn", ".postgres.dsn", "database_url.txt"):
            marker_path = candidate_path / filename
            if marker_path.exists():
                candidate = marker_path.read_text(encoding="utf-8").strip()
                if candidate:
                    if not _looks_like_dsn(candidate):
                        raise RuntimeError(
                            f"DSN file {marker_path} did not contain a postgres DSN"
                        )
                    return candidate

    env_dsn = _dsn_from_env()
    if env_dsn:
        return env_dsn
    raise RuntimeError(
        f"Could not resolve a Postgres DSN from {candidate_path}. "
        "Pass a postgresql:// DSN, or provide a postgres.dsn/.postgres.dsn marker file."
    )


def connect_postgres(dsn: str):
    try:
        psycopg = importlib.import_module("psycopg")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is required for Postgres storage. Install the optional 'postgres' extra."
        ) from exc
    return psycopg.connect(dsn)


def write_dsn_marker(root: str | Path, dsn: str) -> Path:
    marker_dir = _marker_directory(root)
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker_path = marker_dir / "postgres.dsn"
    marker_path.write_text(dsn.strip(), encoding="utf-8")
    return marker_path


def _migration_files() -> list[Path]:
    migrations_dir = Path(__file__).with_name("migrations")
    return sorted(migrations_dir.glob("*.sql"))


def apply_migrations(dsn: str) -> list[str]:
    applied: list[str] = []
    with connect_postgres(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                  filename TEXT PRIMARY KEY,
                  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            for path in _migration_files():
                filename = path.name
                cursor.execute(
                    "SELECT 1 FROM schema_migrations WHERE filename = %s",
                    (filename,),
                )
                if cursor.fetchone() is not None:
                    continue
                cursor.execute(path.read_text(encoding="utf-8"))
                cursor.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (filename,),
                )
                applied.append(filename)
        connection.commit()
    return applied


def bootstrap_postgres(dsn: str, *, root: str | Path | None = None) -> list[str]:
    applied = apply_migrations(dsn)
    if root is not None:
        write_dsn_marker(root, dsn)
    return applied


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Apply storage/postgres SQL migrations to a local Postgres DSN."
    )
    parser.add_argument(
        "--dsn",
        help="Postgres DSN. Falls back to PREDICTION_MARKET_POSTGRES_DSN / POSTGRES_DSN / DATABASE_URL.",
    )
    parser.add_argument(
        "--root",
        help="Directory or file containing postgres.dsn/.postgres.dsn when --dsn is omitted.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    candidate = args.dsn if args.dsn else args.root
    dsn = resolve_postgres_dsn(candidate)
    marker_path = write_dsn_marker(args.root, dsn) if args.root else None
    applied = bootstrap_postgres(dsn)
    if applied:
        print("applied migrations:")
        for filename in applied:
            print(f"- {filename}")
    else:
        print("migrations already up to date")
    if marker_path is not None:
        print(f"dsn marker: {marker_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

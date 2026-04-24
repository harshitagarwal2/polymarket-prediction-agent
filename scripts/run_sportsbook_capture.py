from __future__ import annotations

import argparse
import os
from pathlib import Path

from engine.cli_output import add_quiet_flag, emit_json
from engine.config_loader import load_config_file, nested_config_value
from services.capture import (
    SportsbookCaptureStores,
    SportsbookCaptureWorker,
    SportsbookCaptureWorkerConfig,
    SportsGameOddsCaptureSource,
    SportsbookJsonFeedCaptureSource,
    SUPPORTED_SPORTSBOOK_CAPTURE_PROVIDERS,
    TheOddsApiCaptureSource,
    sanitize_capture_error,
)


SPORT_KEY_BY_LEAGUE = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
}


def _load_optional_config(config_file: str | None) -> dict[str, object]:
    return load_config_file(config_file) if config_file else {}


def _resolve_sport_key(value: str | None, config: dict[str, object]) -> str:
    if isinstance(value, str) and value:
        return value
    configured_sport = nested_config_value(config, "capture", "sport_key")
    if isinstance(configured_sport, str) and configured_sport:
        return configured_sport
    runtime_sport = nested_config_value(config, "runtime", "sport_key")
    if isinstance(runtime_sport, str) and runtime_sport:
        return runtime_sport
    league = nested_config_value(config, "league")
    if isinstance(league, str):
        resolved = SPORT_KEY_BY_LEAGUE.get(league.strip().lower())
        if resolved:
            return resolved
    raise RuntimeError(
        "run-sportsbook-capture requires --sport or a config with capture.sport_key/runtime.sport_key/league"
    )


def _resolve_sportsbook_market(value: str | None, config: dict[str, object]) -> str:
    if isinstance(value, str) and value:
        return value
    configured_market = nested_config_value(config, "runtime", "sportsbook_market")
    if isinstance(configured_market, str) and configured_market:
        return configured_market
    raise RuntimeError(
        "run-sportsbook-capture requires --market or a config with runtime.sportsbook_market"
    )


def _resolve_event_map_file(value: str | None, config: dict[str, object]) -> str | None:
    if value not in (None, ""):
        return value
    configured = nested_config_value(config, "runtime", "event_map_file")
    return configured if isinstance(configured, str) and configured else None


def _resolve_provider(value: str | None, config: dict[str, object]) -> str:
    if isinstance(value, str) and value:
        return value
    configured = nested_config_value(config, "capture", "provider")
    if isinstance(configured, str) and configured:
        return configured
    return "theoddsapi"


def _resolve_provider_url(value: str | None, config: dict[str, object]) -> str | None:
    if value not in (None, ""):
        return value
    configured = nested_config_value(config, "capture", "provider_url")
    return configured if isinstance(configured, str) and configured else None


def _resolve_api_key_env(args, provider: str, config: dict[str, object]) -> str | None:
    if args.api_key_env not in (None, ""):
        return args.api_key_env
    configured = nested_config_value(config, "capture", "api_key_env")
    if isinstance(configured, str) and configured:
        return configured
    if provider == "sportsgameodds":
        return "SPORTSGAMEODDS_API_KEY"
    if provider == "theoddsapi":
        return "THE_ODDS_API_KEY"
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Continuously capture sportsbook odds into runtime and postgres-backed stores."
    )
    parser.add_argument("--sport", default=None)
    parser.add_argument("--market", default=None)
    parser.add_argument("--root", default="runtime/data")
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--event-map-file", default=None)
    parser.add_argument(
        "--provider",
        default=None,
        choices=SUPPORTED_SPORTSBOOK_CAPTURE_PROVIDERS,
    )
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument("--provider-url", default=None)
    parser.add_argument("--refresh-interval-seconds", type=float, default=60.0)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--stale-after-ms", type=int, default=60_000)
    add_quiet_flag(parser)
    return parser


def _build_source(
    args,
    config: dict[str, object],
) -> (
    TheOddsApiCaptureSource
    | SportsbookJsonFeedCaptureSource
    | SportsGameOddsCaptureSource
):
    provider = _resolve_provider(args.provider, config)
    provider_url = _resolve_provider_url(args.provider_url, config)
    api_key_env = _resolve_api_key_env(args, provider, config)

    if provider == "theoddsapi":
        if api_key_env in (None, ""):
            raise RuntimeError(
                "run-sportsbook-capture requires an API key env for theoddsapi"
            )
        api_key = os.getenv(api_key_env)
        if not api_key:
            raise RuntimeError(f"missing required environment variable: {api_key_env}")
        return TheOddsApiCaptureSource(api_key=api_key)
    if provider == "json_feed":
        if provider_url in (None, ""):
            raise RuntimeError(
                "run-sportsbook-capture requires --provider-url for provider json_feed"
            )
        return SportsbookJsonFeedCaptureSource(feed_url=provider_url)
    if provider == "sportsgameodds":
        if api_key_env in (None, ""):
            raise RuntimeError(
                "run-sportsbook-capture requires an API key env for provider sportsgameodds"
            )
        api_key = os.getenv(api_key_env)
        if not api_key:
            raise RuntimeError(f"missing required environment variable: {api_key_env}")
        return SportsGameOddsCaptureSource(api_key=api_key, feed_url=provider_url)
    raise RuntimeError(f"unsupported sportsbook provider: {provider}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _load_optional_config(args.config_file)
    source = _build_source(args, config)

    try:
        stores = SportsbookCaptureStores.from_root(args.root, require_postgres=True)
    except RuntimeError as exc:
        if "Could not resolve a Postgres DSN" not in str(exc):
            raise
        sanitized_error = sanitize_capture_error(exc)
        emit_json(
            {
                "ok": False,
                "error_kind": sanitized_error["kind"],
                "error_message": "Postgres worker storage is not configured",
                "root": args.root,
            },
            quiet=args.quiet,
        )
        return 1

    worker = SportsbookCaptureWorker(
        source=source,
        stores=stores,
        config=SportsbookCaptureWorkerConfig(
            root=args.root,
            sport=_resolve_sport_key(args.sport, config),
            market=_resolve_sportsbook_market(args.market, config),
            event_map_file=_resolve_event_map_file(args.event_map_file, config),
            refresh_interval_seconds=args.refresh_interval_seconds,
            max_cycles=args.max_cycles,
            stale_after_ms=args.stale_after_ms,
        ),
    )
    results = worker.run()
    payload = results[-1] if results else {"ok": False, "root": args.root}
    emit_json(payload, quiet=args.quiet)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

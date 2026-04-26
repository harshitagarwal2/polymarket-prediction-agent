from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from adapters.types import serialize_market_summary
from engine.cli_output import add_quiet_flag, emit_json
from engine.runtime_bootstrap import build_adapter
from research.fair_values import (
    build_fair_value_manifest,
    parse_sportsbook_rows,
    resolve_rows_to_markets,
)
from research.data.odds_api import (
    fetch_odds_payload,
    load_event_map,
    normalize_odds_events,
)


def _atomic_write_json(path: str | Path, payload: object) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        suffix=".json",
        dir=target.parent,
        delete=False,
        encoding="utf-8",
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        temp_path = Path(handle.name)
    temp_path.replace(target)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Continuously refresh sportsbook-derived fair values for Polymarket sports trading."
    )
    parser.add_argument("--sport-key", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--status-file", default="runtime/fair_values_refresh_status.json"
    )
    parser.add_argument("--event-map-file", default=None)
    parser.add_argument("--regions", default="us")
    parser.add_argument("--markets", default="h2h")
    parser.add_argument(
        "--odds-format", choices=("decimal", "american"), default="decimal"
    )
    parser.add_argument("--bookmakers", default=None)
    parser.add_argument(
        "--book-aggregation", choices=("independent", "best-line"), default="best-line"
    )
    parser.add_argument(
        "--devig-method", choices=("multiplicative", "power"), default="multiplicative"
    )
    parser.add_argument("--max-age-seconds", type=float, default=900)
    parser.add_argument("--markets-limit", type=int, default=200)
    parser.add_argument("--refresh-interval-seconds", type=float, default=60.0)
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    parser.add_argument("--max-fair-value-delta", type=float, default=0.35)
    add_quiet_flag(parser)
    return parser


def _load_previous_fair_values(path: str | Path) -> dict[str, float]:
    target = Path(path)
    if not target.exists():
        return {}
    raw = target.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    values = payload.get("values")
    if not isinstance(values, dict):
        return {}
    result: dict[str, float] = {}
    for market_id, value in values.items():
        if not isinstance(value, dict):
            continue
        fair_value = value.get("fair_value")
        if isinstance(fair_value, (int, float)) and not isinstance(fair_value, bool):
            result[str(market_id)] = float(fair_value)
    return result


def _validate_fair_value_velocity(
    previous_values: dict[str, float],
    current_values: dict[str, dict[str, object]],
    *,
    max_delta: float,
) -> dict[str, float]:
    if max_delta <= 0:
        return {}
    violations: dict[str, float] = {}
    for market_id, payload in current_values.items():
        previous = previous_values.get(market_id)
        if previous is None:
            continue
        fair_value = payload.get("fair_value")
        if not isinstance(fair_value, (int, float)) or isinstance(fair_value, bool):
            continue
        delta = abs(float(fair_value) - previous)
        if delta > max_delta:
            violations[market_id] = round(delta, 6)
    if violations:
        violation_text = ", ".join(
            f"{market_id}:{delta:.4f}"
            for market_id, delta in sorted(violations.items())
        )
        raise RuntimeError(
            "fair value velocity check failed; implausible jumps detected: "
            + violation_text
        )
    return violations


def _run_refresh_cycle_impl(args) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    previous_values = _load_previous_fair_values(args.output)
    event_map = load_event_map(args.event_map_file)
    adapter = build_adapter("polymarket")
    markets = adapter.list_markets(limit=args.markets_limit)
    payload = fetch_odds_payload(
        sport_key=args.sport_key,
        api_key=os.getenv(args.api_key_env, ""),
        regions=args.regions,
        markets=args.markets,
        odds_format=args.odds_format,
        bookmakers=args.bookmakers,
    )
    rows = parse_sportsbook_rows(
        normalize_odds_events(payload, sport_key=args.sport_key, event_map=event_map)
    )
    resolved_rows, skipped_rows = resolve_rows_to_markets(rows, markets)
    manifest = build_fair_value_manifest(
        resolved_rows,
        method=args.devig_method,
        max_age_seconds=args.max_age_seconds,
        aggregation=args.book_aggregation,
    )
    if skipped_rows:
        skipped_groups = list(manifest.skipped_groups or [])
        skipped_groups.extend(skipped_rows)
        manifest = replace(manifest, skipped_groups=skipped_groups)
    velocity_violations = _validate_fair_value_velocity(
        previous_values,
        manifest.values or {},
        max_delta=args.max_fair_value_delta,
    )
    _atomic_write_json(args.output, manifest.to_payload())
    status_payload = {
        "ok": True,
        "last_success_at": started_at.isoformat(),
        "sport_key": args.sport_key,
        "event_count": len(payload),
        "market_count": len(markets),
        "row_count": len(rows),
        "resolved_row_count": len(resolved_rows),
        "skipped_group_count": len(manifest.skipped_groups or []),
        "velocity_violation_count": len(velocity_violations),
        "output": str(Path(args.output)),
        "book_aggregation": args.book_aggregation,
        "devig_method": args.devig_method,
        "max_fair_value_delta": args.max_fair_value_delta,
    }
    _atomic_write_json(args.status_file, status_payload)
    return status_payload


def main() -> None:
    args = build_parser().parse_args()
    last_success: dict[str, object] | None = None
    cycle = 0
    while cycle < args.max_cycles:
        try:
            status_payload = _run_refresh_cycle_impl(args)
            last_success = status_payload
            emit_json(status_payload, quiet=args.quiet)
        except Exception as exc:
            failure_payload = {
                "ok": False,
                "error": str(exc),
                "last_success_at": (
                    last_success.get("last_success_at")
                    if last_success is not None
                    else None
                ),
                "output": str(Path(args.output)),
            }
            _atomic_write_json(args.status_file, failure_payload)
            emit_json(failure_payload, quiet=args.quiet)
        cycle += 1
        if cycle < args.max_cycles:
            time.sleep(max(0.0, args.refresh_interval_seconds))


if __name__ == "__main__":
    main()

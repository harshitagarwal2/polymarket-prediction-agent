from __future__ import annotations

import argparse
import os

from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from engine.cli_output import add_quiet_flag, emit_json
from engine.runtime_bootstrap import (
    resolve_polymarket_live_user_markets,
    resolve_polymarket_private_key,
    validate_polymarket_live_routing,
)
from services.capture.polymarket import (
    PolymarketCaptureStores,
    sanitize_polymarket_capture_error,
)
from services.capture import (
    PolymarketMarketCaptureWorker,
    PolymarketMarketCaptureWorkerConfig,
    PolymarketUserCaptureWorker,
    PolymarketUserCaptureWorkerConfig,
)


def _split_csv(values: list[str] | None) -> list[str]:
    resolved: list[str] = []
    for value in values or []:
        resolved.extend(item.strip() for item in value.split(",") if item.strip())
    return resolved


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run dedicated Polymarket capture workers against Postgres-backed storage."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    market = subparsers.add_parser("market")
    market.add_argument("--root", default="runtime/data")
    market.add_argument("--asset-id", action="append", default=[])
    market.add_argument("--sport", default=None)
    market.add_argument("--market-type", default=None)
    market.add_argument("--limit", type=int, default=500)
    market.add_argument("--stale-after-ms", type=int, default=4_000)
    market.add_argument("--max-sessions", type=int, default=None)
    market.add_argument("--max-messages-per-session", type=int, default=None)
    add_quiet_flag(market)

    user = subparsers.add_parser("user")
    user.add_argument("--root", default="runtime/data")
    user.add_argument("--market-id", action="append", default=[])
    user.add_argument("--stale-after-ms", type=int, default=4_000)
    user.add_argument("--max-sessions", type=int, default=None)
    user.add_argument("--max-messages-per-session", type=int, default=None)
    add_quiet_flag(user)
    return parser


def _build_user_adapter(market_ids: list[str]) -> PolymarketAdapter:
    return PolymarketAdapter(
        PolymarketConfig(
            host=os.getenv("POLYMARKET_CLOB_HOST") or PolymarketConfig.host,
            data_api_host=(
                os.getenv("POLYMARKET_DATA_API_HOST") or PolymarketConfig.data_api_host
            ),
            private_key=resolve_polymarket_private_key(),
            funder=os.getenv("POLYMARKET_FUNDER"),
            account_address=os.getenv("POLYMARKET_ACCOUNT_ADDRESS"),
            live_user_markets=market_ids,
        )
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        stores = PolymarketCaptureStores.from_root(args.root)
    except RuntimeError as exc:
        sanitized = sanitize_polymarket_capture_error(exc)
        emit_json(
            {
                "ok": False,
                "error_kind": sanitized["kind"],
                "error_message": "Postgres worker storage is not configured",
                "root": args.root,
            },
            quiet=args.quiet,
        )
        return 1
    if args.command == "market":
        validate_polymarket_live_routing(context="run-polymarket-capture market")
        if resolve_polymarket_private_key() in (None, ""):
            raise RuntimeError(
                "run-polymarket-capture market requires POLYMARKET_PRIVATE_KEY or POLYMARKET_PRIVATE_KEY_FILE"
            )
        asset_ids = _split_csv(args.asset_id)
        if not asset_ids:
            raise RuntimeError(
                "run-polymarket-capture market requires at least one --asset-id"
            )
        worker = PolymarketMarketCaptureWorker(
            stores=stores,
            config=PolymarketMarketCaptureWorkerConfig(
                root=args.root,
                asset_ids=asset_ids,
                sport=args.sport,
                market_type=args.market_type,
                limit=args.limit,
                stale_after_ms=args.stale_after_ms,
                max_sessions=args.max_sessions,
                max_messages_per_session=args.max_messages_per_session,
            ),
        )
        results = worker.run()
        payload = results[-1] if results else {"ok": False, "root": args.root}
        emit_json(payload, quiet=args.quiet)
        return 0 if payload.get("ok") else 1
    market_ids = resolve_polymarket_live_user_markets(
        explicit_markets=args.market_id,
        env_markets=os.getenv("POLYMARKET_LIVE_USER_MARKETS"),
        runtime_mode="run",
        opportunity_root=args.root,
    )
    if not market_ids:
        raise RuntimeError(
            "run-polymarket-capture user requires at least one --market-id, POLYMARKET_LIVE_USER_MARKETS, or projected fair-value coverage"
        )
    validate_polymarket_live_routing(context="run-polymarket-capture user")
    if resolve_polymarket_private_key() in (None, ""):
        raise RuntimeError(
            "run-polymarket-capture user requires POLYMARKET_PRIVATE_KEY or POLYMARKET_PRIVATE_KEY_FILE"
        )
    worker = PolymarketUserCaptureWorker(
        stores=stores,
        adapter=_build_user_adapter(market_ids),
        config=PolymarketUserCaptureWorkerConfig(
            root=args.root,
            market_ids=market_ids,
            stale_after_ms=args.stale_after_ms,
            max_sessions=args.max_sessions,
            max_messages_per_session=args.max_messages_per_session,
        ),
    )
    results = worker.run()
    payload = results[-1] if results else {"ok": False, "root": args.root}
    emit_json(payload, quiet=args.quiet)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

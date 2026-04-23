from __future__ import annotations

import argparse

from engine.cli_output import add_quiet_flag, emit_json
from services.projection import CurrentProjectionWorker, CurrentProjectionWorkerConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project raw Postgres capture events into current-state tables and compatibility snapshots."
    )
    parser.add_argument("--root", default="runtime/data")
    parser.add_argument("--refresh-interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--max-events-per-lane", type=int, default=1000)
    add_quiet_flag(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    worker = CurrentProjectionWorker(
        config=CurrentProjectionWorkerConfig(
            root=args.root,
            refresh_interval_seconds=args.refresh_interval_seconds,
            max_cycles=args.max_cycles,
            max_events_per_lane=args.max_events_per_lane,
        )
    )
    try:
        results = worker.run()
    except RuntimeError as exc:
        emit_json(
            {
                "ok": False,
                "error_kind": exc.__class__.__name__,
                "error_message": str(exc),
                "root": args.root,
            },
            quiet=args.quiet,
        )
        return 1
    payload = results[-1] if results else {"ok": False, "root": args.root}
    emit_json(payload, quiet=args.quiet)
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

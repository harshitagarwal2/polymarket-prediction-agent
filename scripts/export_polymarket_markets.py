from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adapters.types import serialize_market_summary
from scripts.run_agent_loop import build_adapter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export a normalized Polymarket market snapshot for offline matching."
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=200)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    adapter = build_adapter("polymarket")
    markets = adapter.list_markets(limit=args.limit)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps([serialize_market_summary(market) for market in markets], indent=2)
    )
    print(
        json.dumps(
            {"output": str(output_path), "market_count": len(markets)},
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

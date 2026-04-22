from __future__ import annotations

import argparse
import json

from storage.journal import (
    read_jsonl_events,
    summarize_recent_runtime,
    summarize_scan_cycle_events,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize runtime event journal")
    parser.add_argument("--journal", default="runtime/events.jsonl")
    args = parser.parse_args()
    events = read_jsonl_events(args.journal)
    summary = {
        "aggregate": summarize_scan_cycle_events(events),
        "recent_runtime": summarize_recent_runtime(events),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

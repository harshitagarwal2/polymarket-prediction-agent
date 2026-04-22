from __future__ import annotations

import json
from argparse import ArgumentParser
from pathlib import Path
from typing import Any


def add_quiet_flag(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress stdout output and only write requested artifacts.",
    )


def emit_json(payload: Any, *, quiet: bool = False, **json_kwargs: Any) -> None:
    if quiet:
        return
    print(json.dumps(payload, indent=2, sort_keys=True, **json_kwargs))


def emit_lines(*lines: str | Path, quiet: bool = False) -> None:
    if quiet:
        return
    for line in lines:
        print(line)


__all__ = ["add_quiet_flag", "emit_json", "emit_lines"]

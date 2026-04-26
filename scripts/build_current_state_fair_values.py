from __future__ import annotations

import sys

from scripts import ingest_live_data


def main(argv: list[str] | None = None) -> int:
    return ingest_live_data.main(
        [
            "build-fair-values",
            "--require-postgres-authority",
            *(argv or sys.argv[1:]),
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())

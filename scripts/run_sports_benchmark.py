from __future__ import annotations

from research.benchmark_cli import main as _main


def main() -> int:
    result = _main()
    return 0 if result is None else result


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

from scripts import operator_cli
from storage.postgres import (
    ExecutionOrderRepository,
    PolymarketFillRepository,
    RuntimeCycleRepository,
    bootstrap_postgres,
    require_postgres_dsn,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic tax audit export smoke verification."
    )
    parser.add_argument("--root", default="runtime/data")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir) / "runtime-data"
        dsn = require_postgres_dsn(None, context="tax audit smoke")
        last_error: Exception | None = None
        for _ in range(10):
            try:
                bootstrap_postgres(dsn, root=root)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                time.sleep(1)
        if last_error is not None:
            raise last_error
        postgres_root = root / "postgres"
        RuntimeCycleRepository(postgres_root).upsert(
            "smoke-cycle-1",
            {
                "cycle_id": "smoke-cycle-1",
                "mode": "tax_audit_smoke",
                "started_at": "2026-04-24T21:00:00+00:00",
                "selected_market_key": "asset-1:yes",
                "policy_allowed": True,
                "halted": False,
                "payload": {"source": "smoke"},
            },
        )
        ExecutionOrderRepository(postgres_root).append(
            {
                "cycle_id": "smoke-cycle-1",
                "decision_id": None,
                "order_id": "order-1",
                "contract_key": "asset-1:yes",
                "accepted": True,
                "status": "accepted",
                "message": None,
                "payload": {"source": "smoke"},
            }
        )
        PolymarketFillRepository(postgres_root).replace_all(
            {
                "fill-1": {
                    "fill_id": "fill-1",
                    "order_id": "order-1",
                    "contract": {
                        "venue": "polymarket",
                        "symbol": "asset-1",
                        "outcome": "yes",
                    },
                    "action": "buy",
                    "price": 0.45,
                    "quantity": 1.0,
                    "fee": 0.01,
                    "snapshot_observed_at": "2026-04-24T21:00:00+00:00",
                    "snapshot_cohort_id": "cohort-1",
                }
            }
        )
        output = root / "tax_audit.csv"
        operator_cli.cmd_export_tax_audit(
            argparse.Namespace(
                opportunity_root=str(root),
                output=str(output),
                require_postgres=False,
                quiet=True,
            )
        )
        text = output.read_text(encoding="utf-8")
        if "fill-1" not in text:
            raise RuntimeError("tax audit smoke did not export the ledger-backed fill row")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

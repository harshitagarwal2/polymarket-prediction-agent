from __future__ import annotations

import argparse
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import operator_cli


class TaxAuditExportTests(unittest.TestCase):
    def test_export_tax_audit_writes_csv_from_execution_fill_ledger(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "runtime-data"
            output = root / "tax_audit.csv"
            stdout = io.StringIO()

            class _FakeExecutionFillRepository:
                def __init__(self, *_args, **_kwargs):
                    pass

                def read_all(self):
                    return {
                        "fill-1": {
                            "fill_key": "fill-1",
                            "order_id": "order-1",
                            "contract_key": "asset-1:yes",
                            "price": 0.45,
                            "quantity": 1.0,
                            "fee": 0.01,
                            "fill_ts": "2026-04-24T21:00:00+00:00",
                            "snapshot_observed_at": "2026-04-24T21:00:00+00:00",
                            "snapshot_cohort_id": "cohort-1",
                            "payload": {"action": "buy"},
                        }
                    }

            with (
                patch.object(operator_cli, "_preflight_execution_ledger"),
                patch.object(
                    operator_cli,
                    "sync_execution_fills_from_projected_state",
                    return_value=1,
                ),
                patch.object(
                    operator_cli,
                    "ExecutionFillRepository",
                    _FakeExecutionFillRepository,
                ),
                patch("sys.stdout", stdout),
            ):
                result = operator_cli.cmd_export_tax_audit(
                    argparse.Namespace(
                        opportunity_root=str(root),
                        output=str(output),
                        require_postgres=False,
                        quiet=False,
                    )
                )

            payload = json.loads(stdout.getvalue())
            csv_text = output.read_text(encoding="utf-8")
            self.assertEqual(result, 0)
            self.assertEqual(payload["row_count"], 1)
            self.assertEqual(payload["synced_fill_count"], 1)
            self.assertIn(
                "fill_id,order_id,venue,symbol,outcome,action,price,quantity,fee,fill_ts,snapshot_observed_at,snapshot_cohort_id",
                csv_text,
            )
            self.assertIn(
                "fill-1,order-1,polymarket,asset-1,yes,buy,0.45,1.0,0.01,2026-04-24T21:00:00+00:00,2026-04-24T21:00:00+00:00,cohort-1",
                csv_text,
            )


if __name__ == "__main__":
    unittest.main()

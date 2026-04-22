from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.runtime_metrics import RuntimeMetricsCollector


class RuntimeMetricsCollectorTests(unittest.TestCase):
    def test_record_persists_metric_summary_and_recent_event(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            collector = RuntimeMetricsCollector(Path(temp_dir) / "runtime_metrics.json")
            event = collector.record(
                component="ingest.polymarket.bbo",
                action="sync",
                status="ok",
                trace_id="trace-123",
                latency_ms=12.5,
                bbo_count=4,
            )
            snapshot = collector.snapshot()

        metric = snapshot["metrics"]["ingest.polymarket.bbo:sync"]
        self.assertEqual(metric["count"], 1)
        self.assertEqual(metric["ok_count"], 1)
        self.assertEqual(metric["last_trace_id"], "trace-123")
        self.assertEqual(metric["last_latency_ms"], 12.5)
        self.assertEqual(event["component"], "ingest.polymarket.bbo")
        self.assertEqual(event["action"], "sync")
        self.assertEqual(event["status"], "ok")
        self.assertEqual(event["trace_id"], "trace-123")
        self.assertEqual(event["latency_ms"], 12.5)
        self.assertEqual(event["bbo_count"], 4)

from __future__ import annotations

import json
import unittest

from engine.structured_logging import REQUIRED_FIELDS, JsonFormatter


class StructuredLoggingTests(unittest.TestCase):
    def test_json_formatter_emits_required_fields(self):
        formatter = JsonFormatter()
        record = __import__("logging").LogRecord(
            name="ingest.test",
            level=20,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.trace_id = "trace-1"
        record.component = "ingest.test"
        record.action = "sync"
        record.market_id = "pm-1"
        record.event_id = "evt-1"
        record.status = "ok"
        record.latency_ms = 12.5
        payload = json.loads(formatter.format(record))
        for field in REQUIRED_FIELDS:
            self.assertIn(field, payload)
        self.assertEqual(payload["trace_id"], "trace-1")

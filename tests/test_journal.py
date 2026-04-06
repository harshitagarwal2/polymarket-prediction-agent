from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from engine.accounting import AccountTruthSummary, compare_truth_summaries
from research.storage import (
    EventJournal,
    read_jsonl_events,
    summarize_recent_runtime,
    summarize_scan_cycle_events,
)


class JournalTests(unittest.TestCase):
    def test_append_writes_jsonl_event(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            journal = EventJournal(path)

            journal.append("scan_cycle", {"market_count": 3, "candidate_count": 1})

            self.assertTrue(path.exists())
            lines = path.read_text().strip().splitlines()
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload["event_type"], "scan_cycle")
            self.assertEqual(payload["payload"]["market_count"], 3)

    def test_event_journal_assigns_event_id_and_preserves_cycle_id(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            journal = EventJournal(path)

            journal.append("scan_cycle", {"cycle_id": "cycle-123", "market_count": 1})

            events = read_jsonl_events(path)
            self.assertEqual(events[0]["payload"]["cycle_id"], "cycle-123")
            self.assertTrue(events[0]["event_id"])

    def test_summarize_scan_cycle_events(self):
        events = [
            {
                "event_type": "scan_cycle",
                "payload": {
                    "candidate_count": 2,
                    "policy_allowed": True,
                    "engine_halted": False,
                    "engine_paused": False,
                },
            },
            {
                "event_type": "scan_cycle",
                "payload": {
                    "candidate_count": 1,
                    "policy_allowed": False,
                    "engine_halted": True,
                    "engine_paused": False,
                },
            },
            {"event_type": "scan_cycle_skipped", "payload": {"reason": "paused"}},
        ]

        summary = summarize_scan_cycle_events(events)

        self.assertEqual(summary["scan_cycles"], 2)
        self.assertEqual(summary["skipped_cycles"], 1)
        self.assertEqual(summary["policy_allowed_cycles"], 1)
        self.assertEqual(summary["policy_rejected_cycles"], 1)
        self.assertEqual(summary["total_candidates_seen"], 3)

    def test_compare_truth_summaries_reports_drift(self):
        previous = AccountTruthSummary(
            complete=True,
            issues=[],
            open_orders=1,
            positions=1,
            fills=1,
            partial_fills=0,
            balance_available=100.0,
            balance_total=100.0,
            open_order_notional=1.0,
            reserved_buy_notional=1.0,
            marked_position_notional=0.5,
            observed_at=None,
        )
        current = AccountTruthSummary(
            complete=False,
            issues=["truth degraded"],
            open_orders=2,
            positions=1,
            fills=2,
            partial_fills=1,
            balance_available=95.0,
            balance_total=95.0,
            open_order_notional=2.0,
            reserved_buy_notional=2.0,
            marked_position_notional=0.75,
            observed_at=None,
        )

        drift = compare_truth_summaries(previous, current)

        self.assertTrue(drift.changed)
        self.assertEqual(drift.open_orders_delta, 1)
        self.assertEqual(drift.partial_fills_delta, 1)
        self.assertEqual(drift.balance_available_delta, -5.0)

    def test_summarize_recent_runtime(self):
        events = [
            {
                "ts": "2026-04-06T00:00:00+00:00",
                "event_type": "scan_cycle",
                "payload": {
                    "mode": "run",
                    "selected": {"contract": {"symbol": "token-1", "outcome": "yes"}},
                    "policy_allowed": False,
                    "policy_reasons": ["thin liquidity"],
                    "execution": {
                        "placements": [
                            {"accepted": True, "order_id": "abc-123"},
                            {"accepted": False, "order_id": None},
                        ]
                    },
                },
            },
            {
                "ts": "2026-04-06T00:01:00+00:00",
                "event_type": "operator_pause",
                "payload": {"reason": "manual maintenance"},
            },
            {
                "ts": "2026-04-06T00:02:00+00:00",
                "event_type": "scan_cycle_blocked",
                "payload": {"mode": "run", "issues": ["truth incomplete"]},
            },
        ]

        summary = summarize_recent_runtime(events)

        self.assertEqual(summary["last_event_ts"], "2026-04-06T00:02:00+00:00")
        self.assertEqual(summary["last_selected_market_key"], "token-1:yes")
        self.assertEqual(summary["last_policy_allowed"], False)
        self.assertEqual(summary["last_operator_action"], "operator_pause")
        self.assertEqual(summary["last_truth_block_issues"], ["truth incomplete"])
        self.assertEqual(summary["last_execution_attempt_mode"], "run")
        self.assertEqual(summary["last_execution_selected_market_key"], "token-1:yes")
        self.assertEqual(summary["last_execution_placement_count"], 2)
        self.assertEqual(summary["last_execution_accepted_placement_count"], 1)
        self.assertEqual(summary["last_execution_order_ids"], ["abc-123"])

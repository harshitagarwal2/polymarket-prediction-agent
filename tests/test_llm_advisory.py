from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from forecasting.contracts import load_contract_evidence
from llm import (
    build_llm_advisory_artifact,
    load_llm_advisory_artifact,
    load_llm_advisory_contract_rows,
    render_llm_advisory_markdown,
    write_llm_advisory_artifacts,
)
from llm.advisory_context import build_preview_runtime_context
from scripts import run_agent_loop


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "llm_advisory"
LLM_INPUT_PATH = FIXTURE_DIR / "llm_input.json"
RUNTIME_ROOT = FIXTURE_DIR / "runtime"


class LLMAdvisoryTests(unittest.TestCase):
    def test_preview_runtime_context_matches_run_agent_loop_preview_builder(self):
        expected_proposals, expected_blocked = (
            run_agent_loop._build_preview_order_proposals(
                SimpleNamespace(opportunity_root=str(RUNTIME_ROOT)),
                None,
            )
        )

        context = build_preview_runtime_context(RUNTIME_ROOT)

        self.assertEqual(context.preview_order_proposals, tuple(expected_proposals))
        self.assertEqual(context.blocked_preview_orders, tuple(expected_blocked))

    def test_build_llm_advisory_round_trips_and_is_contract_evidence_compatible(self):
        rows = load_llm_advisory_contract_rows(LLM_INPUT_PATH)
        context = build_preview_runtime_context(RUNTIME_ROOT)
        expected_proposal_payload = dict(context.preview_order_proposals[0])
        expected_blocked_payload = dict(context.blocked_preview_orders[0])
        artifact = build_llm_advisory_artifact(
            rows,
            preview_order_proposals=context.preview_order_proposals,
            blocked_preview_orders=context.blocked_preview_orders,
            runtime_health={"state": "recovering", "open_recovery_count": 1},
            provider_name="offline-review",
            provider_model="fixture-v1",
            prompt_version="prompt-1",
            generated_at=datetime(2026, 4, 22, tzinfo=timezone.utc),
        )

        self.assertEqual(artifact.evidence_summary.summary, rows[0].summary)
        self.assertEqual(len(artifact.preview_order_proposals), 1)
        self.assertEqual(len(artifact.blocked_preview_orders), 1)
        artifact_payload = artifact.to_payload()
        proposal_payloads = artifact_payload["preview_order_proposals"]
        self.assertIsInstance(proposal_payloads, list)
        if not isinstance(proposal_payloads, list):
            self.fail("expected preview_order_proposals list")
        first_proposal_payload = proposal_payloads[0]
        self.assertIsInstance(first_proposal_payload, dict)
        if not isinstance(first_proposal_payload, dict):
            self.fail("expected proposal payload dict")
        self.assertEqual(
            first_proposal_payload["edge_buy_after_costs_bps"],
            expected_proposal_payload["edge_buy_after_costs_bps"],
        )
        blocked_payloads = artifact_payload["blocked_preview_orders"]
        self.assertIsInstance(blocked_payloads, list)
        if not isinstance(blocked_payloads, list):
            self.fail("expected blocked_preview_orders list")
        first_blocked_payload = blocked_payloads[0]
        self.assertIsInstance(first_blocked_payload, dict)
        if not isinstance(first_blocked_payload, dict):
            self.fail("expected blocked payload dict")
        self.assertEqual(
            first_blocked_payload["blocked_reasons"],
            expected_blocked_payload["blocked_reasons"],
        )

        second_row = next(
            row for row in artifact.contracts if row.contract_id == "contract-pm-2"
        )
        self.assertIn("elevated_ambiguity_score", second_row.ambiguity_flags)
        second_preview_context = second_row.preview_context
        self.assertIsNotNone(second_preview_context)
        if second_preview_context is None:
            self.fail("expected preview context for contract-pm-2")
        self.assertEqual(second_preview_context["blocked_reason"], "missing mapping")
        self.assertEqual(
            second_preview_context["blocked_reasons"],
            expected_blocked_payload["blocked_reasons"],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "llm_advisory.json"
            write_llm_advisory_artifacts(artifact, output_path)
            loaded_artifact = load_llm_advisory_artifact(output_path)
            evidence_by_contract = load_contract_evidence(output_path)

        self.assertEqual(loaded_artifact.provider_name, "offline-review")
        loaded_payload = loaded_artifact.to_payload()
        loaded_proposal_payloads = loaded_payload["preview_order_proposals"]
        self.assertIsInstance(loaded_proposal_payloads, list)
        if not isinstance(loaded_proposal_payloads, list):
            self.fail("expected loaded preview_order_proposals list")
        loaded_first_proposal_payload = loaded_proposal_payloads[0]
        self.assertIsInstance(loaded_first_proposal_payload, dict)
        if not isinstance(loaded_first_proposal_payload, dict):
            self.fail("expected loaded proposal payload dict")
        self.assertEqual(
            loaded_first_proposal_payload["edge_buy_after_costs_bps"],
            expected_proposal_payload["edge_buy_after_costs_bps"],
        )
        loaded_blocked_payloads = loaded_payload["blocked_preview_orders"]
        self.assertIsInstance(loaded_blocked_payloads, list)
        if not isinstance(loaded_blocked_payloads, list):
            self.fail("expected loaded blocked_preview_orders list")
        loaded_first_blocked_payload = loaded_blocked_payloads[0]
        self.assertIsInstance(loaded_first_blocked_payload, dict)
        if not isinstance(loaded_first_blocked_payload, dict):
            self.fail("expected loaded blocked payload dict")
        self.assertEqual(
            loaded_first_blocked_payload["blocked_reasons"],
            expected_blocked_payload["blocked_reasons"],
        )
        self.assertEqual(
            sorted(evidence_by_contract), ["contract-pm-1", "contract-pm-2"]
        )
        self.assertAlmostEqual(
            evidence_by_contract["contract-pm-1"].llm_probability or 0.0,
            0.62,
        )

    def test_preview_runtime_context_preserves_zero_snapshot_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "runtime" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.0,
                            "best_bid_yes": 0.0,
                            "best_ask_yes": 0.0,
                            "edge_buy_bps": 125.0,
                            "edge_sell_bps": -125.0,
                            "edge_buy_after_costs_bps": 0.0,
                            "edge_sell_after_costs_bps": 0.0,
                            "edge_after_costs_bps": 0.0,
                            "fillable_size": 0.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "best_bid_yes_size": 9.0,
                            "best_ask_yes_size": 6.0,
                            "source_age_ms": 500,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "start_time": "2026-04-30T19:00:00Z",
                        }
                    }
                )
            )
            (runtime_root / "polymarket_markets.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "status": "open",
                            "end_time": "2026-04-30T22:00:00Z",
                        }
                    }
                )
            )

            context = build_preview_runtime_context(Path(temp_dir) / "runtime")

        self.assertEqual(context.preview_order_proposals, ())
        self.assertEqual(len(context.blocked_preview_orders), 1)
        blocked_payload = context.blocked_preview_orders[0]
        self.assertEqual(blocked_payload["fair_yes_prob"], 0.0)
        self.assertEqual(blocked_payload["best_bid_yes"], 0.0)
        self.assertEqual(blocked_payload["best_ask_yes"], 0.0)
        self.assertEqual(blocked_payload["edge_buy_after_costs_bps"], 0.0)
        self.assertEqual(blocked_payload["fillable_size"], 0.0)
        blocked_reasons = blocked_payload["blocked_reasons"]
        self.assertIsInstance(blocked_reasons, list)
        if not isinstance(blocked_reasons, list):
            self.fail("expected blocked_reasons list")
        self.assertIn("insufficient visible depth", blocked_reasons)

    def test_preview_runtime_context_uses_commence_time_for_freeze_window(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "runtime" / "current"
            runtime_root.mkdir(parents=True)
            (runtime_root / "opportunities.json").write_text(
                json.dumps(
                    {
                        "pm-1|buy_yes": {
                            "market_id": "pm-1",
                            "side": "buy_yes",
                            "fair_yes_prob": 0.6,
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "edge_buy_bps": 1300.0,
                            "edge_sell_bps": -1500.0,
                            "edge_buy_after_costs_bps": 1285.0,
                            "edge_sell_after_costs_bps": -1515.0,
                            "edge_after_costs_bps": 220.0,
                            "fillable_size": 5.0,
                            "confidence": 0.99,
                            "blocked_reasons": [],
                            "blocked_reason": None,
                        }
                    }
                )
            )
            (runtime_root / "fair_values.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "fair_yes_prob": 0.6,
                            "book_dispersion": 0.01,
                            "data_age_ms": 1000,
                        }
                    }
                )
            )
            (runtime_root / "polymarket_bbo.json").write_text(
                json.dumps(
                    {
                        "pm-1": {
                            "market_id": "pm-1",
                            "best_bid_yes": 0.45,
                            "best_ask_yes": 0.47,
                            "best_bid_yes_size": 9.0,
                            "best_ask_yes_size": 5.0,
                            "source_age_ms": 500,
                        }
                    }
                )
            )
            (runtime_root / "market_mappings.json").write_text(
                json.dumps(
                    {
                        "pm-1|sb-1": {
                            "polymarket_market_id": "pm-1",
                            "sportsbook_event_id": "sb-1",
                        }
                    }
                )
            )
            (runtime_root / "sportsbook_events.json").write_text(
                json.dumps(
                    {
                        "sb-1": {
                            "sportsbook_event_id": "sb-1",
                            "commence_time": (
                                datetime.now(timezone.utc) + timedelta(minutes=5)
                            ).isoformat(),
                        }
                    }
                )
            )

            context = build_preview_runtime_context(Path(temp_dir) / "runtime")

        self.assertEqual(context.preview_order_proposals, ())
        self.assertEqual(len(context.blocked_preview_orders), 1)
        blocked_payload = context.blocked_preview_orders[0]
        blocked_reasons = blocked_payload["blocked_reasons"]
        self.assertIsInstance(blocked_reasons, list)
        if not isinstance(blocked_reasons, list):
            self.fail("expected blocked_reasons list")
        self.assertIn("market within pre-start freeze window", blocked_reasons)

    def test_load_llm_advisory_artifact_rejects_unsupported_schema_version(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 2,
                    "generated_at": "2026-04-22T00:00:00Z",
                    "source": "operator_cli",
                    "provider_name": "offline",
                    "evidence_summary": {
                        "summary": "No evidence available.",
                        "citations": [],
                        "key_points": [],
                    },
                    "operator_memo": "Operator memo\nProposals: 0",
                    "contracts": [],
                    "preview_order_proposals": [],
                    "blocked_preview_orders": [],
                },
                handle,
            )
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                "unsupported llm advisory schema_version",
            ):
                load_llm_advisory_artifact(handle.name)

    def test_load_llm_advisory_artifact_rejects_non_object_preview_rows(self):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "schema_version": 1,
                    "generated_at": "2026-04-22T00:00:00Z",
                    "source": "operator_cli",
                    "provider_name": "offline",
                    "evidence_summary": {
                        "summary": "No evidence available.",
                        "citations": [],
                        "key_points": [],
                    },
                    "operator_memo": "Operator memo\nProposals: 0",
                    "contracts": [],
                    "preview_order_proposals": ["bad-row"],
                    "blocked_preview_orders": [],
                },
                handle,
            )
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                "preview_order_proposals\\[0\\] must be an object",
            ):
                load_llm_advisory_artifact(handle.name)

    def test_load_llm_advisory_contract_rows_rejects_invalid_nested_contract_payload(
        self,
    ):
        with tempfile.NamedTemporaryFile("w+", suffix=".json") as handle:
            json.dump(
                {
                    "contracts": [
                        {
                            "contract_id": "bad-contract",
                            "llm_contract": {
                                "includes_overtime": True,
                                "void_on_postponement": True,
                                "requires_player_to_start": None,
                                "resolution_source": "league",
                                "ambiguity_score": 1.5,
                            },
                        }
                    ]
                },
                handle,
            )
            handle.flush()

            with self.assertRaisesRegex(
                ValueError,
                "ambiguity_score must be between 0 and 1",
            ):
                load_llm_advisory_contract_rows(handle.name)

    def test_render_llm_advisory_markdown_sanitizes_untrusted_text(self):
        artifact = build_llm_advisory_artifact(
            [
                {
                    "contract_id": "contract-danger",
                    "question": "Question\u001b[31m <b>red</b>",
                    "summary": "Summary <script>alert(1)</script>",
                    "citations": ["cite\u001b[0m", "<unsafe>"],
                }
            ],
            runtime_health={"state": "healthy\u001b[31m", "tag": "<unsafe>"},
        )

        rendered = render_llm_advisory_markdown(artifact)

        self.assertNotIn("\u001b", rendered)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", rendered)
        self.assertIn("&lt;b&gt;red&lt;/b&gt;", rendered)
        self.assertIn("&lt;unsafe&gt;", rendered)

    def test_preview_runtime_context_matches_missing_bbo_blocked_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir) / "runtime"
            shutil.copytree(RUNTIME_ROOT, temp_root)
            current_root = temp_root / "current"
            (current_root / "polymarket_bbo.json").write_text(json.dumps({}))

            expected_proposals, expected_blocked = (
                run_agent_loop._build_preview_order_proposals(
                    SimpleNamespace(opportunity_root=str(temp_root)),
                    None,
                )
            )

            context = build_preview_runtime_context(temp_root)

        self.assertEqual(context.preview_order_proposals, tuple(expected_proposals))
        self.assertEqual(context.blocked_preview_orders, tuple(expected_blocked))


if __name__ == "__main__":
    unittest.main()

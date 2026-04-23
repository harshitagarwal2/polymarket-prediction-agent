from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from contracts import parse_llm_contract_payload
from execution.models import OrderProposal
from forecasting import (
    build_feature_row,
    fit_linear_feature_model,
    predict_rows,
    training_rows_from_labeled_features,
)
from llm import build_operator_memo, summarize_evidence
from research.attribution.pnl_attribution import (
    attribute_trade,
    persist_trade_attribution,
)
from research.replay.exchange_sim import (
    apply_wait_time_slippage,
    cancel_effective_after_steps,
    simulate_fillable_quantity,
    snapshot_is_stale,
)


class MlAndLlmScaffoldsTests(unittest.TestCase):
    def test_ml_train_and_infer_round_trip(self):
        features = [
            build_feature_row(
                fair_value={
                    "fair_yes_prob": 0.62,
                    "book_dispersion": 0.01,
                    "data_age_ms": 1000,
                    "source_count": 2,
                    "as_of": "2026-04-22T00:00:00Z",
                },
                opportunity={
                    "edge_after_costs_bps": 180,
                    "fillable_size": 3,
                    "confidence": 0.98,
                },
                bbo={"best_bid_yes": 0.45, "best_ask_yes": 0.47},
                mapping={"match_confidence": 0.98, "resolution_risk": 0.02},
            ),
            build_feature_row(
                fair_value={
                    "fair_yes_prob": 0.49,
                    "book_dispersion": 0.03,
                    "data_age_ms": 7000,
                    "source_count": 1,
                    "as_of": "2026-04-22T00:00:00Z",
                },
                opportunity={
                    "edge_after_costs_bps": 20,
                    "fillable_size": 1,
                    "confidence": 0.60,
                },
                bbo={"best_bid_yes": 0.48, "best_ask_yes": 0.52},
                mapping={"match_confidence": 0.60, "resolution_risk": 0.30},
            ),
        ]
        rows = training_rows_from_labeled_features(features, [1, 0])
        artifact = fit_linear_feature_model(rows)
        predictions = predict_rows(artifact, features)
        self.assertEqual(len(predictions), 2)
        self.assertGreater(predictions[0], predictions[1])

    def test_llm_contract_parser_validates_schema(self):
        parsed = parse_llm_contract_payload(
            {
                "includes_overtime": True,
                "void_on_postponement": True,
                "requires_player_to_start": False,
                "resolution_source": "league official result",
                "ambiguity_score": 0.12,
            }
        )
        self.assertTrue(parsed.includes_overtime)
        self.assertEqual(parsed.ambiguity_score, 0.12)

    def test_llm_contract_parser_rejects_non_boolean_flags(self):
        with self.assertRaisesRegex(
            ValueError,
            "includes_overtime must be true or false",
        ):
            parse_llm_contract_payload(
                {
                    "includes_overtime": "false",
                    "void_on_postponement": True,
                    "requires_player_to_start": False,
                    "ambiguity_score": 0.12,
                }
            )

    def test_operator_memo_and_evidence_summary_are_deterministic(self):
        memo = summarize_evidence(
            ["Line moved after injury report", "Depth still thin"],
            citations=["note-1"],
        )
        rendered = build_operator_memo(
            [
                OrderProposal(
                    market_id="pm-1",
                    side="buy_yes",
                    action="place",
                    price=0.47,
                    size=5.0,
                    tif="GTC",
                    rationale="edge_after_costs_bps=220.00",
                )
            ],
            blocked_reasons=["missing mapping"],
        )
        self.assertEqual(memo.summary, "Line moved after injury report")
        self.assertIn("Proposals: 1", rendered)
        self.assertIn("missing mapping", rendered)

    def test_replay_and_attribution_helpers_capture_basic_effects(self):
        attribution = attribute_trade(
            trade_id="t-1",
            market_id="pm-1",
            expected_edge_bps=200.0,
            realized_edge_bps=150.0,
            pnl=12.5,
            mapping_risk=0.1,
        )
        self.assertEqual(attribution.slippage_bps, -50.0)
        self.assertEqual(
            simulate_fillable_quantity(10.0, 6.0, max_fill_ratio_per_step=0.5), 3.0
        )
        self.assertTrue(
            cancel_effective_after_steps(
                5,
                cancel_requested_step=3,
                cancel_latency_steps=2,
            )
        )
        self.assertTrue(
            snapshot_is_stale(current_step=5, snapshot_step=1, stale_after_steps=3)
        )
        self.assertGreater(
            apply_wait_time_slippage(
                price=0.5,
                wait_steps=2,
                price_move_bps_per_step=25.0,
                is_buy=True,
            ),
            0.5,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            record = persist_trade_attribution(
                attribution,
                root=Path(temp_dir),
            )
            self.assertEqual(record.trade_id, "t-1")

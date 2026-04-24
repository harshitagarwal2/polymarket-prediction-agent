from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from types import SimpleNamespace

from scripts import run_agent_loop
from storage import (
    ProjectedCurrentStateReadAdapter,
)
from storage.current_projection import (
    PreviewRuntimeContext,
    build_preview_runtime_context,
    load_current_state_tables,
)


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "llm_advisory"
RUNTIME_ROOT = FIXTURE_DIR / "runtime"


@dataclass(frozen=True)
class _StubProjectedRepository:
    payload: dict[str, object]

    def read_all(self) -> dict[str, object]:
        return self.payload


def _as_payload_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    raise TypeError("expected payload dict")


def _projected_adapter_from_payloads(
    payloads: Mapping[str, object],
) -> ProjectedCurrentStateReadAdapter:
    return ProjectedCurrentStateReadAdapter(
        opportunities=_StubProjectedRepository(
            _as_payload_dict(payloads["opportunities"])
        ),
        mappings=_StubProjectedRepository(
            _as_payload_dict(payloads["market_mappings"])
        ),
        fair_values=_StubProjectedRepository(_as_payload_dict(payloads["fair_values"])),
        bbo_rows=_StubProjectedRepository(_as_payload_dict(payloads["polymarket_bbo"])),
        sportsbook_events=_StubProjectedRepository(
            _as_payload_dict(payloads["sportsbook_events"])
        ),
        source_health=_StubProjectedRepository(
            _as_payload_dict(payloads["source_health"])
        ),
        polymarket_markets=_StubProjectedRepository(
            _as_payload_dict(payloads["polymarket_markets"])
        ),
    )


class CurrentProjectionTests(unittest.TestCase):
    def test_load_current_state_tables_matches_projected_adapter_payload_shape(self):
        payloads = {
            "opportunities": {
                "market-1|buy_yes": {
                    "market_id": "market-1",
                    "as_of": "2024-01-01T00:00:00+00:00",
                    "side": "buy_yes",
                    "fair_yes_prob": 0.55,
                    "best_bid_yes": 0.5,
                    "best_ask_yes": 0.52,
                    "edge_buy_bps": 300.0,
                    "edge_sell_bps": -200.0,
                    "edge_buy_after_costs_bps": 250.0,
                    "edge_sell_after_costs_bps": -240.0,
                    "edge_after_costs_bps": 250.0,
                    "fillable_size": 50.0,
                    "confidence": 0.9,
                    "blocked_reason": None,
                    "blocked_reasons": [],
                    "fair_value_ref": "market-1|2024-01-01T00:00:00+00:00|model|v1",
                }
            },
            "market_mappings": {
                "market-1|event-1": {
                    "polymarket_market_id": "market-1",
                    "sportsbook_event_id": "event-1",
                    "sportsbook_market_type": "h2h",
                    "normalized_market_type": "moneyline",
                    "match_confidence": 0.99,
                    "resolution_risk": 0.01,
                    "mismatch_reason": None,
                    "blocked_reason": None,
                    "is_active": True,
                }
            },
            "fair_values": {
                "market-1": {
                    "market_id": "market-1",
                    "as_of": "2024-01-01T00:00:00+00:00",
                    "fair_yes_prob": 0.55,
                    "lower_prob": 0.53,
                    "upper_prob": 0.57,
                    "book_dispersion": 0.02,
                    "data_age_ms": 100,
                    "source_count": 3,
                    "model_name": "model",
                    "model_version": "v1",
                }
            },
            "polymarket_bbo": {
                "market-1": {
                    "market_id": "market-1",
                    "best_bid_yes": 0.5,
                    "best_bid_yes_size": 40.0,
                    "best_ask_yes": 0.52,
                    "best_ask_yes_size": 35.0,
                    "midpoint_yes": 0.51,
                    "spread_yes": 0.02,
                    "book_ts": "2024-01-01T00:00:00+00:00",
                    "source_age_ms": 80,
                    "raw_hash": None,
                }
            },
            "sportsbook_events": {
                "event-1": {
                    "sportsbook_event_id": "event-1",
                    "source": "draftkings",
                    "sport": "basketball",
                    "league": "nba",
                    "home_team": "A",
                    "away_team": "B",
                    "start_time": "2024-01-01T01:00:00+00:00",
                    "raw_json": {},
                }
            },
            "source_health": {
                "fair_values": {
                    "source_name": "fair_values",
                    "status": "fresh",
                    "last_seen_at": "2024-01-01T00:00:00+00:00",
                    "last_success_at": "2024-01-01T00:00:00+00:00",
                    "stale_after_ms": 60000,
                    "details": {},
                },
                "market_mappings": {
                    "source_name": "market_mappings",
                    "status": "fresh",
                    "last_seen_at": "2024-01-01T00:00:00+00:00",
                    "last_success_at": "2024-01-01T00:00:00+00:00",
                    "stale_after_ms": 60000,
                    "details": {},
                },
            },
            "polymarket_markets": {
                "market-1": {
                    "market_id": "market-1",
                    "condition_id": "condition-1",
                    "token_id_yes": "yes",
                    "token_id_no": "no",
                    "title": "Market 1",
                    "description": None,
                    "event_slug": "event-1",
                    "market_slug": "market-1",
                    "category": "sports",
                    "end_time": "2024-01-01T02:00:00+00:00",
                    "status": "open",
                    "raw_json": {},
                }
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "runtime"
            current_root = runtime_root / "current"
            current_root.mkdir(parents=True, exist_ok=True)
            for table_name, payload in payloads.items():
                (current_root / f"{table_name}.json").write_text(
                    json.dumps(payload),
                    encoding="utf-8",
                )

            postgres_root = Path(temp_dir) / "postgres"
            file_tables = load_current_state_tables(runtime_root)
            projected_tables = load_current_state_tables(
                None,
                read_adapter=_projected_adapter_from_payloads(payloads),
            )

        self.assertEqual(file_tables, projected_tables)

    def test_build_preview_runtime_context_matches_projected_adapter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            current_root = RUNTIME_ROOT / "current"
            projected_payloads = {
                table_name: json.loads(
                    (current_root / f"{table_name}.json").read_text(encoding="utf-8")
                )
                for table_name in (
                    "opportunities",
                    "market_mappings",
                    "fair_values",
                    "polymarket_bbo",
                    "sportsbook_events",
                    "source_health",
                    "polymarket_markets",
                )
            }

            file_context = build_preview_runtime_context(RUNTIME_ROOT)
            projected_context = build_preview_runtime_context(
                None,
                read_adapter=_projected_adapter_from_payloads(projected_payloads),
            )

        self.assertEqual(file_context, projected_context)

    def test_load_current_state_tables_returns_empty_tables_when_root_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            tables = load_current_state_tables(Path(temp_dir) / "runtime")

        self.assertEqual(tables.opportunities, {})
        self.assertEqual(tables.mappings, {})
        self.assertEqual(tables.fair_values, {})
        self.assertEqual(tables.bbo_rows, {})
        self.assertEqual(tables.sportsbook_events, {})
        self.assertEqual(tables.source_health, {})
        self.assertEqual(tables.polymarket_markets, {})

    def test_build_preview_runtime_context_matches_run_agent_loop_preview_builder(self):
        expected_proposals, expected_blocked = (
            run_agent_loop._build_preview_order_proposals(
                SimpleNamespace(opportunity_root=str(RUNTIME_ROOT)),
                None,
            )
        )

        context = build_preview_runtime_context(RUNTIME_ROOT)

        self.assertIsInstance(context, PreviewRuntimeContext)
        self.assertEqual(context.preview_order_proposals, tuple(expected_proposals))
        self.assertEqual(context.blocked_preview_orders, tuple(expected_blocked))


if __name__ == "__main__":
    unittest.main()

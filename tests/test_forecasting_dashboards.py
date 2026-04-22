from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from forecasting.contracts import build_contract_consistency_surface
from scripts import render_model_vs_market_dashboard


class ForecastingDashboardTests(unittest.TestCase):
    def test_contract_consistency_surface_falls_back_without_llm_evidence(self):
        surface = build_contract_consistency_surface(
            'contract-1',
            model_probability=0.62,
            market_probability=0.48,
            evidence=None,
        )

        self.assertEqual(surface.mode, 'deterministic_fallback')
        self.assertEqual(surface.status, 'fallback')
        self.assertAlmostEqual(surface.model_vs_market_gap or 0.0, 0.14)
        self.assertIn('deterministic model-vs-market fallback', ' '.join(surface.notes))

    def test_render_model_vs_market_dashboard_writes_artifacts_with_optional_evidence(self):
        rows_payload = {
            'rows': [
                {
                    'contract_id': 'election-yes',
                    'model_probability': 0.62,
                    'market_probability': 0.55,
                    'outcome_label': 1,
                    'domain': 'politics',
                    'segment': 'election',
                },
                {
                    'contract_id': 'election-no',
                    'model_probability': 0.38,
                    'market_probability': 0.45,
                    'outcome_label': 0,
                    'domain': 'politics',
                    'segment': 'election',
                },
            ]
        }
        evidence_payload = {
            'rows': [
                {
                    'contract_id': 'election-yes',
                    'llm_probability': 0.6,
                    'summary': 'candidate momentum remains stable',
                    'citations': ['memo-1'],
                }
            ]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / 'rows.json'
            evidence_path = Path(temp_dir) / 'evidence.json'
            output_dir = Path(temp_dir) / 'out'
            input_path.write_text(json.dumps(rows_payload))
            evidence_path.write_text(json.dumps(evidence_payload))

            stdout = io.StringIO()
            with (
                patch(
                    'sys.argv',
                    [
                        'render_model_vs_market_dashboard.py',
                        '--input',
                        str(input_path),
                        '--output-dir',
                        str(output_dir),
                        '--llm-contract-evidence',
                        str(evidence_path),
                        '--quiet',
                    ],
                ),
                patch('sys.stdout', stdout),
            ):
                render_model_vs_market_dashboard.main()

            self.assertEqual(stdout.getvalue(), '')
            json_payload = json.loads(
                (output_dir / 'model_vs_market_dashboard.json').read_text()
            )
            markdown = (output_dir / 'model_vs_market_dashboard.md').read_text()

        self.assertEqual(json_payload['summary']['contract_count'], 2)
        self.assertTrue(json_payload['summary']['calibration_comparison']['available'])
        self.assertEqual(json_payload['summary']['consistency']['fallback_count'], 1)
        self.assertIn('Calibration comparison', markdown)
        self.assertIn('deterministic_fallback/fallback', markdown)

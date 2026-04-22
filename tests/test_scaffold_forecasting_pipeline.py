from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import scaffold_forecasting_pipeline


class ForecastingPipelineScaffoldTests(unittest.TestCase):
    def test_scaffold_forecasting_pipeline_supports_non_sports_domain(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / 'politics_pipeline.json'
            stdout = io.StringIO()
            with (
                patch(
                    'sys.argv',
                    [
                        'scaffold_forecasting_pipeline.py',
                        '--domain',
                        'politics',
                        '--output',
                        str(output_path),
                        '--quiet',
                    ],
                ),
                patch('sys.stdout', stdout),
            ):
                scaffold_forecasting_pipeline.main()

            self.assertEqual(stdout.getvalue(), '')
            payload = json.loads(output_path.read_text())

        self.assertEqual(payload['domain'], 'politics')
        self.assertTrue(payload['deterministic_fallback'])
        self.assertTrue(payload['llm_contract_evidence_optional'])
        self.assertEqual(payload['stages'][0]['name'], 'capture')
        self.assertEqual(payload['stages'][-1]['name'], 'evaluate-and-publish')
        self.assertIn(
            'runtime/forecasting/politics/model_vs_market_dashboard.json',
            payload['artifacts'].values(),
        )
        self.assertTrue(
            any('domain-agnostic' in note for note in payload['notes'])
        )

from __future__ import annotations

import argparse

from engine.cli_output import add_quiet_flag, emit_json
from forecasting.contracts import load_contract_evidence
from forecasting.dashboards import (
    build_model_vs_market_dashboard,
    load_model_vs_market_rows,
    write_model_vs_market_dashboard,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Render model-vs-market dashboard artifacts from contract-level forecast rows.'
    )
    parser.add_argument('--input', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--title', default='Model vs Market Dashboard')
    parser.add_argument('--llm-contract-evidence', default=None)
    parser.add_argument('--calibration-bin-count', type=int, default=5)
    add_quiet_flag(parser)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    rows = load_model_vs_market_rows(args.input)
    evidence_by_contract = (
        load_contract_evidence(args.llm_contract_evidence)
        if args.llm_contract_evidence is not None
        else {}
    )
    dashboard = build_model_vs_market_dashboard(
        rows,
        title=args.title,
        evidence_by_contract=evidence_by_contract,
        calibration_bin_count=args.calibration_bin_count,
    )
    json_path, markdown_path = write_model_vs_market_dashboard(
        dashboard, args.output_dir
    )
    consistency = dashboard.summary.get('consistency', {})
    calibration = dashboard.summary.get('calibration_comparison', {})
    emit_json(
        {
            'json_output': str(json_path),
            'markdown_output': str(markdown_path),
            'contract_count': dashboard.summary['contract_count'],
            'fallback_count': consistency.get('fallback_count', 0),
            'warn_count': consistency.get('warn_count', 0),
            'calibration_available': calibration.get('available', False),
        },
        quiet=args.quiet,
    )


if __name__ == '__main__':
    main()

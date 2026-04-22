from __future__ import annotations

import argparse

from engine.cli_output import add_quiet_flag, emit_json
from forecasting.pipeline import build_pipeline_scaffold, write_pipeline_scaffold


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Write a domain-agnostic forecasting pipeline scaffold.'
    )
    parser.add_argument('--domain', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument(
        '--no-llm-contract-evidence',
        action='store_true',
        help='Disable optional LLM contract evidence surfaces in the scaffold.',
    )
    parser.add_argument(
        '--no-deterministic-fallback',
        action='store_true',
        help='Disable deterministic fallback notes in the scaffold output.',
    )
    add_quiet_flag(parser)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    scaffold = build_pipeline_scaffold(
        args.domain,
        llm_contract_evidence_optional=not args.no_llm_contract_evidence,
        deterministic_fallback=not args.no_deterministic_fallback,
    )
    output_path = write_pipeline_scaffold(scaffold, args.output)
    emit_json(
        {
            'output': str(output_path),
            'domain': scaffold.domain,
            'stage_count': len(scaffold.stages),
            'deterministic_fallback': scaffold.deterministic_fallback,
            'llm_contract_evidence_optional': scaffold.llm_contract_evidence_optional,
        },
        quiet=args.quiet,
    )


if __name__ == '__main__':
    main()

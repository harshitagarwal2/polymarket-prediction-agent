from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ForecastingPipelineStage:
    name: str
    description: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    notes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            'name': self.name,
            'description': self.description,
            'inputs': list(self.inputs),
            'outputs': list(self.outputs),
        }
        if self.notes:
            payload['notes'] = list(self.notes)
        return payload


@dataclass(frozen=True)
class ForecastingPipelineScaffold:
    domain: str
    deterministic_fallback: bool
    llm_contract_evidence_optional: bool
    base_dir: str
    artifacts: dict[str, str]
    stages: tuple[ForecastingPipelineStage, ...]
    notes: tuple[str, ...]

    def to_payload(self) -> dict[str, object]:
        return {
            'domain': self.domain,
            'deterministic_fallback': self.deterministic_fallback,
            'llm_contract_evidence_optional': self.llm_contract_evidence_optional,
            'base_dir': self.base_dir,
            'artifacts': self.artifacts,
            'stages': [stage.to_payload() for stage in self.stages],
            'notes': list(self.notes),
        }


def _slugify(value: str) -> str:
    slug = ''.join(character.lower() if character.isalnum() else '-' for character in value)
    while '--' in slug:
        slug = slug.replace('--', '-')
    return slug.strip('-') or 'forecasting-domain'


def build_pipeline_scaffold(
    domain: str,
    *,
    llm_contract_evidence_optional: bool = True,
    deterministic_fallback: bool = True,
) -> ForecastingPipelineScaffold:
    resolved_domain = domain.strip()
    if not resolved_domain:
        raise ValueError('domain must not be empty')
    slug = _slugify(resolved_domain)
    base_dir = f'runtime/forecasting/{slug}'
    artifacts = {
        'normalized_contracts': f'{base_dir}/normalized_contracts.json',
        'training_dataset': f'{base_dir}/training_dataset.json',
        'forecast_output': f'{base_dir}/forecast_output.json',
        'dashboard_json': f'{base_dir}/model_vs_market_dashboard.json',
        'dashboard_markdown': f'{base_dir}/model_vs_market_dashboard.md',
    }
    if llm_contract_evidence_optional:
        artifacts['llm_contract_evidence'] = f'{base_dir}/llm_contract_evidence.json'
    stages = (
        ForecastingPipelineStage(
            name='capture',
            description='Capture raw contract metadata and market probabilities for the target domain.',
            inputs=('external feeds', 'market metadata'),
            outputs=(artifacts['normalized_contracts'],),
            notes=(
                'Domain is intentionally generic so non-sports contracts can share the same scaffold.',
            ),
        ),
        ForecastingPipelineStage(
            name='train-or-load-model',
            description='Build a deterministic baseline model artifact or reuse an existing one.',
            inputs=(artifacts['normalized_contracts'],),
            outputs=(artifacts['training_dataset'],),
            notes=('Keep deterministic baselines available even when optional LLM evidence is enabled.',),
        ),
        ForecastingPipelineStage(
            name='forecast',
            description='Generate contract-level model probabilities and compare them to market probabilities.',
            inputs=(artifacts['training_dataset'], artifacts['normalized_contracts']),
            outputs=(artifacts['forecast_output'],),
            notes=(
                'Forecast outputs should stay consumable by model-vs-market dashboards and calibration checks.',
            ),
        ),
        ForecastingPipelineStage(
            name='evaluate-and-publish',
            description='Write dashboard artifacts and consistency surfaces for operator review.',
            inputs=tuple(artifacts.values()),
            outputs=(artifacts['dashboard_json'], artifacts['dashboard_markdown']),
            notes=(
                'Dashboards remain valid without LLM evidence when deterministic fallback is enabled.',
            ),
        ),
    )
    notes = [
        'This scaffold is domain-agnostic and suitable for politics, macro, crypto, or custom event contracts.',
    ]
    if llm_contract_evidence_optional:
        notes.append(
            'LLM contract evidence is optional; missing evidence should leave deterministic scoring as the source of truth.'
        )
    if deterministic_fallback:
        notes.append(
            'Deterministic fallback remains active so forecast generation and dashboards do not depend on LLM availability.'
        )
    return ForecastingPipelineScaffold(
        domain=resolved_domain,
        deterministic_fallback=deterministic_fallback,
        llm_contract_evidence_optional=llm_contract_evidence_optional,
        base_dir=base_dir,
        artifacts=artifacts,
        stages=stages,
        notes=tuple(notes),
    )


def write_pipeline_scaffold(
    scaffold: ForecastingPipelineScaffold,
    output_path: str | Path,
) -> Path:
    resolved_output_path = Path(output_path)
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_output_path.write_text(
        json.dumps(scaffold.to_payload(), indent=2, sort_keys=True)
    )
    return resolved_output_path

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


def _coerce_optional_probability(value: object, *, field_name: str) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{field_name} must be numeric")
    probability = float(value)
    if not math.isfinite(probability):
        raise ValueError(f"{field_name} must be finite")
    if probability < 0.0 or probability > 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return probability


def _coerce_optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip() or None


def _coerce_string_tuple(value: object) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError("string list fields must be sequences")
    return tuple(str(item).strip() for item in value if str(item).strip())


@dataclass(frozen=True)
class ContractEvidence:
    contract_id: str
    question: str | None = None
    llm_probability: float | None = None
    llm_confidence: float | None = None
    summary: str | None = None
    citations: tuple[str, ...] = ()
    deterministic_probability: float | None = None
    consistency_notes: tuple[str, ...] = ()

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> 'ContractEvidence':
        contract_id = _coerce_optional_text(payload.get('contract_id'))
        if contract_id is None:
            raise ValueError('contract evidence requires contract_id')
        return cls(
            contract_id=contract_id,
            question=_coerce_optional_text(payload.get('question')),
            llm_probability=_coerce_optional_probability(
                payload.get('llm_probability'), field_name='llm_probability'
            ),
            llm_confidence=_coerce_optional_probability(
                payload.get('llm_confidence'), field_name='llm_confidence'
            ),
            summary=_coerce_optional_text(payload.get('summary')),
            citations=_coerce_string_tuple(payload.get('citations')),
            deterministic_probability=_coerce_optional_probability(
                payload.get('deterministic_probability'),
                field_name='deterministic_probability',
            ),
            consistency_notes=_coerce_string_tuple(payload.get('consistency_notes')),
        )

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {'contract_id': self.contract_id}
        if self.question is not None:
            payload['question'] = self.question
        if self.llm_probability is not None:
            payload['llm_probability'] = self.llm_probability
        if self.llm_confidence is not None:
            payload['llm_confidence'] = self.llm_confidence
        if self.summary is not None:
            payload['summary'] = self.summary
        if self.citations:
            payload['citations'] = list(self.citations)
        if self.deterministic_probability is not None:
            payload['deterministic_probability'] = self.deterministic_probability
        if self.consistency_notes:
            payload['consistency_notes'] = list(self.consistency_notes)
        return payload


@dataclass(frozen=True)
class ContractConsistencySurface:
    contract_id: str
    mode: str
    status: str
    model_probability: float | None
    market_probability: float | None
    llm_probability: float | None
    deterministic_probability: float | None
    model_vs_market_gap: float | None
    llm_vs_model_gap: float | None
    notes: tuple[str, ...]
    citations: tuple[str, ...]

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            'contract_id': self.contract_id,
            'mode': self.mode,
            'status': self.status,
            'notes': list(self.notes),
            'citations': list(self.citations),
        }
        for key, value in (
            ('model_probability', self.model_probability),
            ('market_probability', self.market_probability),
            ('llm_probability', self.llm_probability),
            ('deterministic_probability', self.deterministic_probability),
            ('model_vs_market_gap', self.model_vs_market_gap),
            ('llm_vs_model_gap', self.llm_vs_model_gap),
        ):
            if value is not None:
                payload[key] = value
        return payload


def _normalize_evidence_payload(payload: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(payload, list):
        if not all(isinstance(item, Mapping) for item in payload):
            raise ValueError('contract evidence rows must be objects')
        return tuple(payload)
    if not isinstance(payload, Mapping):
        raise ValueError('contract evidence payload must be an object or list')
    for key in ('evidence', 'contracts', 'rows'):
        nested = payload.get(key)
        if nested is not None:
            return _normalize_evidence_payload(nested)
    if 'contract_id' in payload:
        return (payload,)
    rows: list[Mapping[str, object]] = []
    for contract_id, value in payload.items():
        if isinstance(value, Mapping):
            row = dict(value)
            row.setdefault('contract_id', str(contract_id))
            rows.append(row)
    if rows:
        return tuple(rows)
    raise ValueError('contract evidence payload must contain contract rows')


def load_contract_evidence(
    source: str | Path | Mapping[str, object] | Sequence[Mapping[str, object]],
) -> dict[str, ContractEvidence]:
    payload: object = source
    if isinstance(source, (str, Path)):
        payload = json.loads(Path(source).read_text())
    rows = _normalize_evidence_payload(payload)
    evidence_by_contract: dict[str, ContractEvidence] = {}
    for row in rows:
        evidence = ContractEvidence.from_payload(row)
        evidence_by_contract[evidence.contract_id] = evidence
    return evidence_by_contract


def build_contract_consistency_surface(
    contract_id: str,
    *,
    model_probability: float | None,
    market_probability: float | None,
    evidence: ContractEvidence | None,
    model_market_gap_warn_threshold: float = 0.15,
    llm_model_gap_warn_threshold: float = 0.20,
) -> ContractConsistencySurface:
    notes: list[str] = []
    mode = 'llm_evidence'
    status = 'ok'
    model_vs_market_gap = None
    if model_probability is not None and market_probability is not None:
        model_vs_market_gap = model_probability - market_probability
        if abs(model_vs_market_gap) > model_market_gap_warn_threshold:
            notes.append(
                'model and market probabilities differ beyond the review threshold'
            )
            status = 'warn'

    llm_probability = None
    deterministic_probability = None
    llm_vs_model_gap = None
    citations: tuple[str, ...] = ()
    if evidence is None:
        mode = 'deterministic_fallback'
        status = 'fallback'
        notes.append(
            'no llm contract evidence provided; using deterministic model-vs-market fallback'
        )
    else:
        llm_probability = evidence.llm_probability
        deterministic_probability = evidence.deterministic_probability
        citations = evidence.citations
        notes.extend(evidence.consistency_notes)
        if llm_probability is None:
            mode = 'deterministic_fallback'
            status = 'fallback'
            notes.append(
                'llm contract evidence is missing probability output; keeping deterministic fallback active'
            )
        elif model_probability is not None:
            llm_vs_model_gap = llm_probability - model_probability
            if abs(llm_vs_model_gap) > llm_model_gap_warn_threshold:
                notes.append(
                    'llm probability differs materially from the model probability'
                )
                status = 'warn'
        if evidence.summary:
            notes.append(f'llm summary: {evidence.summary}')
        if not citations:
            notes.append('llm evidence has no citations')
            if status == 'ok':
                status = 'warn'

    return ContractConsistencySurface(
        contract_id=contract_id,
        mode=mode,
        status=status,
        model_probability=model_probability,
        market_probability=market_probability,
        llm_probability=llm_probability,
        deterministic_probability=deterministic_probability,
        model_vs_market_gap=model_vs_market_gap,
        llm_vs_model_gap=llm_vs_model_gap,
        notes=tuple(notes),
        citations=citations,
    )

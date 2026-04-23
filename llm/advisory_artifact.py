from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from contracts import ParsedLLMContract, parse_llm_contract_payload
from execution.models import OrderProposal
from llm.evidence_summarizer import EvidenceMemo, summarize_evidence
from llm.operator_memo import build_operator_memo


LLM_ADVISORY_SCHEMA_VERSION = 1


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: object) -> datetime:
    if value in (None, ""):
        raise ValueError("generated_at is required")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_optional_text(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _coerce_text(value: object, *, field_name: str) -> str:
    text = _coerce_optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required")
    return text


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


def _coerce_float(value: object, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{field_name} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _coerce_string_tuple(value: object, *, field_name: str) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field_name} must be a sequence")
    return tuple(
        text for item in value if (text := _coerce_optional_text(item)) is not None
    )


def _load_payload(
    source: str | Path | Mapping[str, object] | Sequence[Mapping[str, object]],
) -> object:
    if isinstance(source, (str, Path)):
        return json.loads(Path(source).read_text())
    return source


def _normalize_contract_rows(payload: object) -> tuple[Mapping[str, object], ...]:
    if isinstance(payload, list):
        if not all(isinstance(item, Mapping) for item in payload):
            raise ValueError("contract advisory rows must be objects")
        return tuple(payload)
    if not isinstance(payload, Mapping):
        raise ValueError("contract advisory payload must be an object or list")
    for key in ("contracts", "rows", "evidence"):
        nested = payload.get(key)
        if nested is not None:
            return _normalize_contract_rows(nested)
    if "contract_id" in payload:
        return (payload,)
    rows: list[Mapping[str, object]] = []
    for contract_id, value in payload.items():
        if not isinstance(value, Mapping):
            continue
        row = dict(value)
        row.setdefault("contract_id", str(contract_id))
        rows.append(row)
    if rows:
        return tuple(rows)
    raise ValueError("contract advisory payload must contain contract rows")


def _dedupe_strings(values: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _sanitize_rendered_text(value: object) -> str:
    text = str(value)
    sanitized = "".join(
        character
        for character in text
        if character in ("\n", "\t") or ord(character) >= 32
    )
    sanitized = (
        sanitized.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\\", "\\\\")
    )
    for character in (
        "`",
        "*",
        "[",
        "]",
        "|",
    ):
        sanitized = sanitized.replace(character, f"\\{character}")
    return sanitized


def _serialize_evidence_summary(summary: EvidenceMemo) -> dict[str, object]:
    return {
        "summary": summary.summary,
        "citations": list(summary.citations),
        "key_points": list(summary.key_points),
    }


def _deserialize_evidence_summary(payload: object) -> EvidenceMemo:
    if not isinstance(payload, Mapping):
        raise ValueError("evidence_summary must be an object")
    return EvidenceMemo(
        summary=_coerce_text(
            payload.get("summary"), field_name="evidence_summary.summary"
        ),
        citations=_coerce_string_tuple(
            payload.get("citations"), field_name="evidence_summary.citations"
        ),
        key_points=_coerce_string_tuple(
            payload.get("key_points"), field_name="evidence_summary.key_points"
        ),
    )


def _derived_ambiguity_flags(contract: ParsedLLMContract | None) -> tuple[str, ...]:
    if contract is None:
        return ()
    flags: list[str] = []
    if contract.ambiguity_score >= 0.25:
        flags.append("elevated_ambiguity_score")
    if contract.requires_player_to_start is None:
        flags.append("player_start_rule_unspecified")
    if contract.resolution_source is None:
        flags.append("missing_resolution_source")
    return tuple(flags)


@dataclass(frozen=True)
class BlockedPreviewOrder:
    market_id: str
    side: str
    blocked_reason: str
    payload: dict[str, object] | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "BlockedPreviewOrder":
        market_id = _coerce_text(payload.get("market_id"), field_name="market_id")
        side = _coerce_text(payload.get("side"), field_name="side")
        blocked_reason = _coerce_text(
            payload.get("blocked_reason"), field_name="blocked_reason"
        )
        normalized_payload = dict(payload)
        normalized_payload.update(
            {
                "market_id": market_id,
                "side": side,
                "blocked_reason": blocked_reason,
            }
        )
        return cls(
            market_id=market_id,
            side=side,
            blocked_reason=blocked_reason,
            payload=normalized_payload,
        )

    def to_payload(self) -> dict[str, object]:
        payload = dict(self.payload) if self.payload is not None else {}
        payload.setdefault("market_id", self.market_id)
        payload.setdefault("side", self.side)
        payload.setdefault("blocked_reason", self.blocked_reason)
        return payload


@dataclass(frozen=True)
class PreviewOrderProposal:
    market_id: str
    side: str
    action: str
    price: float
    size: float
    tif: str
    rationale: str
    payload: dict[str, object] | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "PreviewOrderProposal":
        proposal = _proposal_from_payload(payload)
        normalized_payload = dict(payload)
        normalized_payload.update(
            {
                "market_id": proposal.market_id,
                "side": proposal.side,
                "action": proposal.action,
                "price": proposal.price,
                "size": proposal.size,
                "tif": proposal.tif,
                "rationale": proposal.rationale,
            }
        )
        return cls(
            market_id=proposal.market_id,
            side=proposal.side,
            action=proposal.action,
            price=proposal.price,
            size=proposal.size,
            tif=proposal.tif,
            rationale=proposal.rationale,
            payload=normalized_payload,
        )

    def to_payload(self) -> dict[str, object]:
        payload = dict(self.payload) if self.payload is not None else {}
        payload.setdefault("market_id", self.market_id)
        payload.setdefault("side", self.side)
        payload.setdefault("action", self.action)
        payload.setdefault("price", self.price)
        payload.setdefault("size", self.size)
        payload.setdefault("tif", self.tif)
        payload.setdefault("rationale", self.rationale)
        return payload

    def to_order_proposal(self) -> OrderProposal:
        return OrderProposal(
            market_id=self.market_id,
            side=self.side,
            action=self.action,
            price=self.price,
            size=self.size,
            tif=self.tif,
            rationale=self.rationale,
        )


@dataclass(frozen=True)
class LLMAdvisoryContractRow:
    contract_id: str
    market_id: str | None = None
    question: str | None = None
    llm_probability: float | None = None
    llm_confidence: float | None = None
    summary: str | None = None
    citations: tuple[str, ...] = ()
    deterministic_probability: float | None = None
    consistency_notes: tuple[str, ...] = ()
    llm_contract: ParsedLLMContract | None = None
    ambiguity_flags: tuple[str, ...] = ()
    preview_context: dict[str, object] | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "LLMAdvisoryContractRow":
        llm_contract_payload = payload.get("llm_contract")
        llm_contract = (
            parse_llm_contract_payload(dict(llm_contract_payload))
            if isinstance(llm_contract_payload, Mapping)
            else None
        )
        ambiguity_flags = _coerce_string_tuple(
            payload.get("ambiguity_flags"), field_name="ambiguity_flags"
        )
        if not ambiguity_flags:
            ambiguity_flags = _derived_ambiguity_flags(llm_contract)
        preview_context_payload = payload.get("preview_context")
        preview_context = (
            dict(preview_context_payload)
            if isinstance(preview_context_payload, Mapping)
            else None
        )
        return cls(
            contract_id=_coerce_text(
                payload.get("contract_id"), field_name="contract_id"
            ),
            market_id=_coerce_optional_text(payload.get("market_id")),
            question=_coerce_optional_text(payload.get("question")),
            llm_probability=_coerce_optional_probability(
                payload.get("llm_probability"), field_name="llm_probability"
            ),
            llm_confidence=_coerce_optional_probability(
                payload.get("llm_confidence"), field_name="llm_confidence"
            ),
            summary=_coerce_optional_text(payload.get("summary")),
            citations=_coerce_string_tuple(
                payload.get("citations"), field_name="citations"
            ),
            deterministic_probability=_coerce_optional_probability(
                payload.get("deterministic_probability"),
                field_name="deterministic_probability",
            ),
            consistency_notes=_coerce_string_tuple(
                payload.get("consistency_notes"), field_name="consistency_notes"
            ),
            llm_contract=llm_contract,
            ambiguity_flags=ambiguity_flags,
            preview_context=preview_context,
        )

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {"contract_id": self.contract_id}
        if self.market_id is not None:
            payload["market_id"] = self.market_id
        if self.question is not None:
            payload["question"] = self.question
        if self.llm_probability is not None:
            payload["llm_probability"] = self.llm_probability
        if self.llm_confidence is not None:
            payload["llm_confidence"] = self.llm_confidence
        if self.summary is not None:
            payload["summary"] = self.summary
        if self.citations:
            payload["citations"] = list(self.citations)
        if self.deterministic_probability is not None:
            payload["deterministic_probability"] = self.deterministic_probability
        if self.consistency_notes:
            payload["consistency_notes"] = list(self.consistency_notes)
        if self.llm_contract is not None:
            payload["llm_contract"] = self.llm_contract.to_payload()
        if self.ambiguity_flags:
            payload["ambiguity_flags"] = list(self.ambiguity_flags)
        if self.preview_context is not None:
            payload["preview_context"] = dict(self.preview_context)
        return payload


@dataclass(frozen=True)
class LLMAdvisoryArtifact:
    generated_at: datetime
    evidence_summary: EvidenceMemo
    operator_memo: str
    contracts: tuple[LLMAdvisoryContractRow, ...] = ()
    preview_order_proposals: tuple[PreviewOrderProposal, ...] = ()
    blocked_preview_orders: tuple[BlockedPreviewOrder, ...] = ()
    schema_version: int = LLM_ADVISORY_SCHEMA_VERSION
    source: str = "operator_cli"
    provider_name: str = "offline"
    provider_model: str | None = None
    prompt_version: str | None = None
    runtime_health: dict[str, object] | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat().replace("+00:00", "Z"),
            "source": self.source,
            "provider_name": self.provider_name,
            "evidence_summary": _serialize_evidence_summary(self.evidence_summary),
            "operator_memo": self.operator_memo,
            "contract_count": len(self.contracts),
            "preview_order_proposal_count": len(self.preview_order_proposals),
            "blocked_preview_order_count": len(self.blocked_preview_orders),
            "contracts": [row.to_payload() for row in self.contracts],
            "preview_order_proposals": [
                proposal.to_payload() for proposal in self.preview_order_proposals
            ],
            "blocked_preview_orders": [
                row.to_payload() for row in self.blocked_preview_orders
            ],
        }
        if self.provider_model is not None:
            payload["provider_model"] = self.provider_model
        if self.prompt_version is not None:
            payload["prompt_version"] = self.prompt_version
        if self.runtime_health is not None:
            payload["runtime_health"] = dict(self.runtime_health)
        return payload


def _proposal_from_payload(payload: Mapping[str, object]) -> OrderProposal:
    return OrderProposal(
        market_id=_coerce_text(payload.get("market_id"), field_name="market_id"),
        side=_coerce_text(payload.get("side"), field_name="side"),
        action=_coerce_text(payload.get("action"), field_name="action"),
        price=_coerce_float(payload.get("price"), field_name="price"),
        size=_coerce_float(payload.get("size"), field_name="size"),
        tif=_coerce_text(payload.get("tif"), field_name="tif"),
        rationale=_coerce_text(payload.get("rationale"), field_name="rationale"),
    )


def load_llm_advisory_contract_rows(
    source: str | Path | Mapping[str, object] | Sequence[Mapping[str, object]],
) -> tuple[LLMAdvisoryContractRow, ...]:
    payload = _load_payload(source)
    rows = _normalize_contract_rows(payload)
    return tuple(LLMAdvisoryContractRow.from_payload(row) for row in rows)


def build_llm_advisory_artifact(
    contract_rows: Sequence[LLMAdvisoryContractRow | Mapping[str, object]],
    *,
    preview_order_proposals: Sequence[OrderProposal | Mapping[str, object]] = (),
    blocked_preview_orders: Sequence[BlockedPreviewOrder | Mapping[str, object]] = (),
    source: str = "operator_cli",
    provider_name: str = "offline",
    provider_model: str | None = None,
    prompt_version: str | None = None,
    runtime_health: Mapping[str, object] | None = None,
    generated_at: datetime | None = None,
) -> LLMAdvisoryArtifact:
    parsed_rows = tuple(
        row
        if isinstance(row, LLMAdvisoryContractRow)
        else LLMAdvisoryContractRow.from_payload(row)
        for row in contract_rows
    )
    parsed_proposals = tuple(
        proposal
        if isinstance(proposal, PreviewOrderProposal)
        else PreviewOrderProposal.from_payload(proposal.__dict__.copy())
        if isinstance(proposal, OrderProposal)
        else PreviewOrderProposal.from_payload(proposal)
        for proposal in preview_order_proposals
    )
    parsed_blocked = tuple(
        blocked
        if isinstance(blocked, BlockedPreviewOrder)
        else BlockedPreviewOrder.from_payload(blocked)
        for blocked in blocked_preview_orders
    )

    proposal_context_by_market = {
        proposal.market_id: {
            **proposal.to_payload(),
            "proposal_side": proposal.side,
            "proposal_action": proposal.action,
            "proposal_price": proposal.price,
            "proposal_size": proposal.size,
            "proposal_tif": proposal.tif,
            "proposal_rationale": proposal.rationale,
        }
        for proposal in parsed_proposals
    }
    blocked_context_by_market = {
        blocked.market_id: {
            **blocked.to_payload(),
            "proposal_side": blocked.side,
            "blocked_reason": blocked.blocked_reason,
        }
        for blocked in parsed_blocked
    }
    enriched_rows = tuple(
        row
        if row.preview_context is not None or row.market_id is None
        else replace(
            row,
            preview_context=proposal_context_by_market.get(row.market_id)
            or blocked_context_by_market.get(row.market_id),
        )
        for row in parsed_rows
    )

    notes: list[str] = []
    citations: list[str] = []
    for row in enriched_rows:
        if row.summary is not None:
            notes.append(row.summary)
        notes.extend(f"{row.contract_id}: {flag}" for flag in row.ambiguity_flags)
        notes.extend(f"{row.contract_id}: {note}" for note in row.consistency_notes)
        citations.extend(row.citations)
    blocked_reasons = [row.blocked_reason for row in parsed_blocked]
    if not notes:
        notes.extend(blocked_reasons)
    evidence_summary = summarize_evidence(
        notes, citations=list(_dedupe_strings(citations))
    )
    operator_memo = build_operator_memo(
        [proposal.to_order_proposal() for proposal in parsed_proposals],
        blocked_reasons=list(_dedupe_strings(blocked_reasons)),
    )
    return LLMAdvisoryArtifact(
        generated_at=(generated_at or _utc_now()).astimezone(timezone.utc),
        source=source,
        provider_name=provider_name,
        provider_model=provider_model,
        prompt_version=prompt_version,
        runtime_health=dict(runtime_health) if runtime_health is not None else None,
        evidence_summary=evidence_summary,
        operator_memo=operator_memo,
        contracts=enriched_rows,
        preview_order_proposals=parsed_proposals,
        blocked_preview_orders=parsed_blocked,
    )


def load_llm_advisory_artifact(
    source: str | Path | Mapping[str, object],
) -> LLMAdvisoryArtifact:
    payload = _load_payload(source)
    if not isinstance(payload, Mapping):
        raise ValueError("llm advisory artifact must be an object")
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != LLM_ADVISORY_SCHEMA_VERSION:
        raise ValueError(
            "unsupported llm advisory schema_version: "
            f"{schema_version} (expected {LLM_ADVISORY_SCHEMA_VERSION})"
        )
    contracts = load_llm_advisory_contract_rows(payload)
    preview_order_proposals_payload = payload.get("preview_order_proposals", [])
    if not isinstance(preview_order_proposals_payload, list):
        raise ValueError("preview_order_proposals must be a list")
    for index, item in enumerate(preview_order_proposals_payload):
        if not isinstance(item, Mapping):
            raise ValueError(f"preview_order_proposals[{index}] must be an object")
    blocked_preview_orders_payload = payload.get("blocked_preview_orders", [])
    if not isinstance(blocked_preview_orders_payload, list):
        raise ValueError("blocked_preview_orders must be a list")
    for index, item in enumerate(blocked_preview_orders_payload):
        if not isinstance(item, Mapping):
            raise ValueError(f"blocked_preview_orders[{index}] must be an object")
    runtime_health_payload = payload.get("runtime_health")
    if runtime_health_payload is not None and not isinstance(
        runtime_health_payload, Mapping
    ):
        raise ValueError("runtime_health must be an object")
    operator_memo = _coerce_text(
        payload.get("operator_memo"), field_name="operator_memo"
    )
    return LLMAdvisoryArtifact(
        schema_version=schema_version,
        generated_at=_parse_timestamp(payload.get("generated_at")),
        source=_coerce_text(payload.get("source"), field_name="source"),
        provider_name=_coerce_text(
            payload.get("provider_name"), field_name="provider_name"
        ),
        provider_model=_coerce_optional_text(payload.get("provider_model")),
        prompt_version=_coerce_optional_text(payload.get("prompt_version")),
        runtime_health=(
            dict(runtime_health_payload)
            if isinstance(runtime_health_payload, Mapping)
            else None
        ),
        evidence_summary=_deserialize_evidence_summary(payload.get("evidence_summary")),
        operator_memo=operator_memo,
        contracts=contracts,
        preview_order_proposals=tuple(
            PreviewOrderProposal.from_payload(item)
            for item in preview_order_proposals_payload
            if isinstance(item, Mapping)
        ),
        blocked_preview_orders=tuple(
            BlockedPreviewOrder.from_payload(item)
            for item in blocked_preview_orders_payload
            if isinstance(item, Mapping)
        ),
    )


def advisory_summary_payload(artifact: LLMAdvisoryArtifact) -> dict[str, object]:
    ambiguity_count = sum(1 for row in artifact.contracts if row.ambiguity_flags)
    cited_count = sum(1 for row in artifact.contracts if row.citations)
    return {
        "generated_at": artifact.generated_at.isoformat().replace("+00:00", "Z"),
        "provider_name": artifact.provider_name,
        "provider_model": artifact.provider_model,
        "prompt_version": artifact.prompt_version,
        "contract_count": len(artifact.contracts),
        "ambiguous_contract_count": ambiguity_count,
        "cited_contract_count": cited_count,
        "preview_order_proposal_count": len(artifact.preview_order_proposals),
        "blocked_preview_order_count": len(artifact.blocked_preview_orders),
        "evidence_summary": artifact.evidence_summary.summary,
    }


def render_llm_advisory_markdown(artifact: LLMAdvisoryArtifact) -> str:
    lines = ["# LLM Advisory", ""]
    lines.extend(
        [
            "## Summary",
            "",
            f"- Generated at: {artifact.generated_at.isoformat().replace('+00:00', 'Z')}",
            f"- Source: {_sanitize_rendered_text(artifact.source)}",
            f"- Provider: {_sanitize_rendered_text(artifact.provider_name)}",
            f"- Provider model: {_sanitize_rendered_text(artifact.provider_model or 'n/a')}",
            f"- Prompt version: {_sanitize_rendered_text(artifact.prompt_version or 'n/a')}",
            f"- Contracts: {len(artifact.contracts)}",
            f"- Preview proposals: {len(artifact.preview_order_proposals)}",
            f"- Blocked preview orders: {len(artifact.blocked_preview_orders)}",
            "",
            "## Evidence summary",
            "",
            _sanitize_rendered_text(artifact.evidence_summary.summary),
            "",
        ]
    )
    if artifact.evidence_summary.citations:
        lines.append(
            "Citations: "
            + ", ".join(
                _sanitize_rendered_text(citation)
                for citation in artifact.evidence_summary.citations
            )
        )
        lines.append("")
    lines.extend(
        ["## Operator memo", "", _sanitize_rendered_text(artifact.operator_memo), ""]
    )
    if artifact.runtime_health is not None:
        lines.extend(["## Runtime health", ""])
        for key in sorted(artifact.runtime_health):
            lines.append(
                f"- {_sanitize_rendered_text(key)}: "
                f"{_sanitize_rendered_text(artifact.runtime_health[key])}"
            )
        lines.append("")
    lines.extend(["## Contract summaries", ""])
    for row in artifact.contracts:
        lines.append(f"### {_sanitize_rendered_text(row.contract_id)}")
        lines.append("")
        if row.question is not None:
            lines.append(f"- Question: {_sanitize_rendered_text(row.question)}")
        if row.summary is not None:
            lines.append(f"- Summary: {_sanitize_rendered_text(row.summary)}")
        if row.llm_probability is not None:
            lines.append(f"- LLM probability: {row.llm_probability:.4f}")
        if row.deterministic_probability is not None:
            lines.append(
                f"- Deterministic probability: {row.deterministic_probability:.4f}"
            )
        if row.ambiguity_flags:
            lines.append(
                "- Ambiguity flags: "
                + ", ".join(
                    _sanitize_rendered_text(flag) for flag in row.ambiguity_flags
                )
            )
        if row.citations:
            lines.append(
                "- Citations: "
                + ", ".join(
                    _sanitize_rendered_text(citation) for citation in row.citations
                )
            )
        if row.preview_context is not None:
            if row.preview_context.get("blocked_reason") not in (None, ""):
                lines.append(
                    "- Preview blocked reason: "
                    f"{_sanitize_rendered_text(row.preview_context['blocked_reason'])}"
                )
            elif row.preview_context.get("proposal_action") not in (None, ""):
                lines.append(
                    "- Preview proposal: "
                    f"{_sanitize_rendered_text(row.preview_context['proposal_action'])} "
                    f"{_sanitize_rendered_text(row.preview_context.get('proposal_side'))} "
                    f"{_sanitize_rendered_text(row.preview_context.get('proposal_size'))} @ "
                    f"{_sanitize_rendered_text(row.preview_context.get('proposal_price'))}"
                )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def write_llm_advisory_artifacts(
    artifact: LLMAdvisoryArtifact, output_path: str | Path
) -> tuple[Path, Path]:
    json_path = Path(output_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path = json_path.with_suffix(".md")
    json_path.write_text(
        json.dumps(artifact.to_payload(), indent=2, sort_keys=True, allow_nan=False)
    )
    markdown_path.write_text(render_llm_advisory_markdown(artifact))
    return json_path, markdown_path


__all__ = [
    "LLM_ADVISORY_SCHEMA_VERSION",
    "BlockedPreviewOrder",
    "LLMAdvisoryArtifact",
    "LLMAdvisoryContractRow",
    "advisory_summary_payload",
    "build_llm_advisory_artifact",
    "load_llm_advisory_artifact",
    "load_llm_advisory_contract_rows",
    "render_llm_advisory_markdown",
    "write_llm_advisory_artifacts",
]

from __future__ import annotations

from dataclasses import dataclass

from contracts.ontology import NormalizedContractIdentity


@dataclass(frozen=True)
class ContractMatchConfidence:
    score: float
    level: str
    reasons: tuple[str, ...]


def _level_for_score(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def evaluate_contract_match_confidence(
    left: NormalizedContractIdentity,
    right: NormalizedContractIdentity,
) -> ContractMatchConfidence:
    score = 0.0
    reasons: list[str] = []
    if left.group_key == right.group_key:
        score += 0.4
        reasons.append("group key match")
    if left.event_key not in (None, "") and left.event_key == right.event_key:
        score += 0.2
        reasons.append("event key match")
    if left.sport not in (None, "") and left.sport == right.sport:
        score += 0.1
        reasons.append("sport match")
    if left.series not in (None, "") and left.series == right.series:
        score += 0.1
        reasons.append("series match")
    if left.contract_type == right.contract_type:
        score += 0.1
        reasons.append("contract type match")
    shared_labels = set(left.labels).intersection(right.labels)
    if shared_labels:
        score += min(0.1, 0.02 * len(shared_labels))
        reasons.append(f"shared labels: {', '.join(sorted(shared_labels)[:3])}")
    if left.outcome == right.outcome:
        score += 0.05
        reasons.append("outcome alignment")
    score = min(score, 1.0)
    return ContractMatchConfidence(
        score=round(score, 4),
        level=_level_for_score(score),
        reasons=tuple(reasons),
    )

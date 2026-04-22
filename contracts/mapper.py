from __future__ import annotations

from dataclasses import dataclass

from adapters import MarketSummary
from contracts.confidence import (
    ContractMatchConfidence,
    evaluate_contract_match_confidence,
)
from contracts.ontology import (
    NormalizedContractIdentity,
    market_identity_from_market,
)
from contracts.resolution_rules import ParsedContractRules, parse_contract_rules


@dataclass(frozen=True)
class MappedContract:
    identity: NormalizedContractIdentity
    rules: ParsedContractRules
    confidence: ContractMatchConfidence


def map_market_to_contract(
    market: MarketSummary,
    *,
    reference: MarketSummary | None = None,
) -> MappedContract:
    identity = market_identity_from_market(market)
    confidence = ContractMatchConfidence(
        score=1.0,
        level="high",
        reasons=("self-derived market identity",),
    )
    if reference is not None:
        confidence = evaluate_contract_match_confidence(
            identity,
            market_identity_from_market(reference),
        )
    return MappedContract(
        identity=identity,
        rules=parse_contract_rules(market),
        confidence=confidence,
    )

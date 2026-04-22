from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParsedLLMContract:
    includes_overtime: bool
    void_on_postponement: bool
    requires_player_to_start: bool | None
    resolution_source: str | None
    ambiguity_score: float


def parse_llm_contract_payload(payload: dict[str, Any]) -> ParsedLLMContract:
    ambiguity_score = float(payload.get("ambiguity_score", 0.0))
    if ambiguity_score < 0.0 or ambiguity_score > 1.0:
        raise ValueError("ambiguity_score must be between 0 and 1")
    player_rule = payload.get("requires_player_to_start")
    if player_rule not in (None, True, False):
        raise ValueError("requires_player_to_start must be true, false, or null")
    return ParsedLLMContract(
        includes_overtime=bool(payload.get("includes_overtime", False)),
        void_on_postponement=bool(payload.get("void_on_postponement", False)),
        requires_player_to_start=player_rule,
        resolution_source=(
            str(payload["resolution_source"]).strip()
            if payload.get("resolution_source") not in (None, "")
            else None
        ),
        ambiguity_score=ambiguity_score,
    )

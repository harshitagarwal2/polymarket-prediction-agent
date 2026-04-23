from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParsedLLMContract:
    includes_overtime: bool
    void_on_postponement: bool
    requires_player_to_start: bool | None
    resolution_source: str | None
    ambiguity_score: float

    def to_payload(self) -> dict[str, Any]:
        return {
            "includes_overtime": self.includes_overtime,
            "void_on_postponement": self.void_on_postponement,
            "requires_player_to_start": self.requires_player_to_start,
            "resolution_source": self.resolution_source,
            "ambiguity_score": self.ambiguity_score,
        }


def _parse_bool_flag(payload: dict[str, Any], field_name: str) -> bool:
    if field_name not in payload:
        raise ValueError(f"{field_name} is required")
    value = payload[field_name]
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be true or false")
    return value


def _parse_ambiguity_score(payload: dict[str, Any]) -> float:
    raw_value = payload.get("ambiguity_score", 0.0)
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float, str)):
        raise ValueError("ambiguity_score must be a finite number between 0 and 1")
    ambiguity_score = float(raw_value)
    if (
        not math.isfinite(ambiguity_score)
        or ambiguity_score < 0.0
        or ambiguity_score > 1.0
    ):
        raise ValueError("ambiguity_score must be between 0 and 1")
    return ambiguity_score


def parse_llm_contract_payload(payload: dict[str, Any]) -> ParsedLLMContract:
    ambiguity_score = _parse_ambiguity_score(payload)
    player_rule = payload.get("requires_player_to_start")
    if player_rule not in (None, True, False):
        raise ValueError("requires_player_to_start must be true, false, or null")
    return ParsedLLMContract(
        includes_overtime=_parse_bool_flag(payload, "includes_overtime"),
        void_on_postponement=_parse_bool_flag(payload, "void_on_postponement"),
        requires_player_to_start=player_rule,
        resolution_source=(
            str(payload["resolution_source"]).strip()
            if payload.get("resolution_source") not in (None, "")
            else None
        ),
        ambiguity_score=ambiguity_score,
    )

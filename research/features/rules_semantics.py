from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuleSemantics:
    includes_overtime: bool
    void_on_postponement: bool
    requires_player_to_start: bool | None = None
    resolution_source: str | None = None


def semantics_from_market_type(
    market_type: str | None,
    *,
    source: str | None = None,
    void_on_postponement: bool = True,
    requires_player_to_start: bool | None = None,
) -> RuleSemantics:
    normalized = (market_type or "").strip().lower()
    includes_overtime = "regulation" not in normalized
    return RuleSemantics(
        includes_overtime=includes_overtime,
        void_on_postponement=void_on_postponement,
        requires_player_to_start=requires_player_to_start,
        resolution_source=source,
    )


def compare_rule_semantics(
    left: RuleSemantics,
    right: RuleSemantics,
) -> tuple[bool, str | None]:
    if left.includes_overtime != right.includes_overtime:
        return False, "overtime/regulation mismatch"
    if left.void_on_postponement != right.void_on_postponement:
        return False, "postponement/void mismatch"
    if (
        left.requires_player_to_start is not None
        and right.requires_player_to_start is not None
        and left.requires_player_to_start != right.requires_player_to_start
    ):
        return False, "player participation rule mismatch"
    return True, None

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ResolutionRules:
    includes_overtime: bool
    void_on_postponement: bool
    requires_player_to_start: Optional[bool]
    resolution_source: Optional[str]


def rules_compatible(
    left: ResolutionRules, right: ResolutionRules
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

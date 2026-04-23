from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Mapping

from contracts.mapping_identity import (
    polymarket_contract_identity,
    sportsbook_contract_identity,
    start_time_alignment_score,
    team_overlap_score,
)
from contracts.mapping_semantics import (
    RuleSemantics,
    compare_rule_semantics,
    semantics_from_market_type,
)
from contracts.ontology import normalize_market_type


class MappingStatus(str, Enum):
    EXACT_MATCH = "exact_match"
    NORMALIZED_MATCH = "normalized_match"
    AMBIGUOUS_MATCH = "ambiguous_match"
    BLOCKED = "blocked"


_BLOCKED_REASON_CODES: dict[str, str] = {
    "market type mismatch": "market_type_mismatch",
    "event key mismatch": "event_key_mismatch",
    "game id mismatch": "game_id_mismatch",
    "overtime/regulation mismatch": "overtime_regulation_mismatch",
    "postponement/void mismatch": "postponement_void_mismatch",
    "player participation rule mismatch": "player_participation_rule_mismatch",
    "team names do not match with enough confidence": "team_name_mismatch",
    "event start times do not match": "event_start_time_mismatch",
    "ambiguous market identity": "ambiguous_market_identity",
    "missing upstream event identity": "missing_upstream_event_identity",
}


def _confidence_band(score: float | None) -> str:
    if score is None:
        return "unscored"
    if score >= 0.85:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


@dataclass(frozen=True)
class MappingBlockedReason:
    code: str
    message: str
    details: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


def mapping_blocked_reason(
    message: str,
    *,
    details: Mapping[str, object] | None = None,
) -> MappingBlockedReason:
    code = _BLOCKED_REASON_CODES.get(
        message,
        re.sub(r"[^a-z0-9]+", "_", message.strip().lower()).strip("_")
        or "unknown_blocked_reason",
    )
    return MappingBlockedReason(
        code=code,
        message=message,
        details=dict(details or {}),
    )


@dataclass(frozen=True)
class MappingConfidence:
    score: float | None
    band: str
    components: dict[str, float] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()

    def with_score(self, score: float | None) -> MappingConfidence:
        rounded = None if score is None else round(score, 4)
        return MappingConfidence(
            score=rounded,
            band=_confidence_band(rounded),
            components=dict(self.components),
            reasons=self.reasons,
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "score": self.score,
            "band": self.band,
            "components": {
                key: round(value, 4) for key, value in sorted(self.components.items())
            },
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class MappingDecision:
    polymarket_market_id: str
    sportsbook_event_id: str
    sportsbook_market_type: str
    normalized_market_type: str
    mapping_status: MappingStatus
    mapping_confidence: MappingConfidence
    blocked_reason: MappingBlockedReason | None = None
    event_key: str | None = None
    sport: str | None = None
    series: str | None = None
    game_id: str | None = None
    polymarket_semantics: RuleSemantics | None = None
    sportsbook_semantics: RuleSemantics | None = None

    @property
    def match_confidence(self) -> float:
        return round(float(self.mapping_confidence.score or 0.0), 4)

    @property
    def resolution_risk(self) -> float:
        return round(max(0.0, 1.0 - self.match_confidence), 4)

    def to_payload(
        self,
        *,
        blocked_reason_override: MappingBlockedReason | None = None,
        confidence_score_override: float | None = None,
        is_active: bool | None = None,
    ) -> dict[str, object]:
        effective_blocked_reason = blocked_reason_override or self.blocked_reason
        confidence = (
            self.mapping_confidence.with_score(confidence_score_override)
            if confidence_score_override is not None
            else self.mapping_confidence
        )
        if effective_blocked_reason is None:
            status = self.mapping_status.value
        elif effective_blocked_reason.code == "ambiguous_market_identity":
            status = MappingStatus.AMBIGUOUS_MATCH.value
        else:
            status = MappingStatus.BLOCKED.value
        return {
            "mapping_status": status,
            "is_active": is_active
            if is_active is not None
            else effective_blocked_reason is None,
            "mapping_confidence": confidence.to_payload(),
            "blocked_reason": (
                effective_blocked_reason.to_payload()
                if effective_blocked_reason is not None
                else None
            ),
            "target": {
                "sportsbook_event_id": self.sportsbook_event_id,
                "sportsbook_market_type": self.sportsbook_market_type,
                "normalized_market_type": self.normalized_market_type,
            },
            "identity": {
                "event_key": self.event_key,
                "sport": self.sport,
                "series": self.series,
                "game_id": self.game_id,
            },
            "semantics": {
                "polymarket": (
                    self.polymarket_semantics.to_payload()
                    if self.polymarket_semantics is not None
                    else None
                ),
                "sportsbook": (
                    self.sportsbook_semantics.to_payload()
                    if self.sportsbook_semantics is not None
                    else None
                ),
            },
        }


def _token_match(left: str | None, right: str | None) -> bool:
    if left in (None, "") or right in (None, ""):
        return False
    left_tokens = {
        token for token in re.split(r"[^a-z0-9]+", str(left).strip().lower()) if token
    }
    right_tokens = {
        token for token in re.split(r"[^a-z0-9]+", str(right).strip().lower()) if token
    }
    if not left_tokens or not right_tokens:
        return False
    return bool(left_tokens.intersection(right_tokens))


def _build_confidence(
    *,
    score: float | None,
    components: Mapping[str, float],
    reasons: list[str],
) -> MappingConfidence:
    rounded = None if score is None else round(score, 4)
    return MappingConfidence(
        score=rounded,
        band=_confidence_band(rounded),
        components={key: round(value, 4) for key, value in components.items()},
        reasons=tuple(reasons),
    )


def _blocked_decision(
    *,
    polymarket_market_id: str,
    sportsbook_event_id: str,
    sportsbook_market_type: str,
    normalized_market_type: str,
    blocked_reason: MappingBlockedReason,
    event_key: str | None,
    sport: str | None,
    series: str | None,
    game_id: str | None,
    explicit_identity_match: bool,
    polymarket_semantics: RuleSemantics,
    sportsbook_semantics: RuleSemantics,
) -> MappingDecision:
    return MappingDecision(
        polymarket_market_id=polymarket_market_id,
        sportsbook_event_id=sportsbook_event_id,
        sportsbook_market_type=sportsbook_market_type,
        normalized_market_type=normalized_market_type,
        mapping_status=_mapping_status_for(
            blocked_reason=blocked_reason,
            explicit_identity_match=explicit_identity_match,
        ),
        mapping_confidence=_build_confidence(score=0.0, components={}, reasons=[]),
        blocked_reason=blocked_reason,
        event_key=event_key,
        sport=sport,
        series=series,
        game_id=game_id,
        polymarket_semantics=polymarket_semantics,
        sportsbook_semantics=sportsbook_semantics,
    )


def _mapping_status_for(
    *,
    blocked_reason: MappingBlockedReason | None,
    explicit_identity_match: bool,
) -> MappingStatus:
    if blocked_reason is None:
        return (
            MappingStatus.EXACT_MATCH
            if explicit_identity_match
            else MappingStatus.NORMALIZED_MATCH
        )
    if blocked_reason.code == "ambiguous_market_identity":
        return MappingStatus.AMBIGUOUS_MATCH
    return MappingStatus.BLOCKED


def map_contract_candidate(
    pm_market: Mapping[str, object],
    sb_event: Mapping[str, object],
    *,
    sportsbook_market_type: str,
    pm_semantics: RuleSemantics | None = None,
    sb_semantics: RuleSemantics | None = None,
) -> MappingDecision:
    pm_identity = polymarket_contract_identity(pm_market)
    sb_identity = sportsbook_contract_identity(
        sb_event,
        sportsbook_market_type=sportsbook_market_type,
    )

    normalized_market_type = normalize_market_type(
        pm_identity.sports_market_type or sportsbook_market_type
    ).value
    sportsbook_normalized_market_type = normalize_market_type(
        sportsbook_market_type
    ).value

    resolved_pm_semantics = pm_semantics or semantics_from_market_type(
        pm_identity.sports_market_type or sportsbook_market_type,
        source="polymarket",
    )
    resolved_sb_semantics = sb_semantics or semantics_from_market_type(
        sportsbook_market_type,
        source="sportsbook",
    )

    polymarket_market_id = pm_identity.market_id or pm_identity.condition_id or ""
    sportsbook_event_id = sb_identity.sportsbook_event_id or ""
    event_key = pm_identity.event_key or sb_identity.event_key
    sport = pm_identity.sport or sb_identity.sport
    series = pm_identity.series or sb_identity.series
    game_id = pm_identity.game_id or sb_identity.game_id

    explicit_event_match = (
        pm_identity.event_key not in (None, "")
        and sb_identity.event_key not in (None, "")
        and pm_identity.event_key == sb_identity.event_key
    )
    explicit_game_match = (
        pm_identity.game_id not in (None, "")
        and sb_identity.game_id not in (None, "")
        and pm_identity.game_id == sb_identity.game_id
    )
    sport_match = _token_match(pm_identity.sport, sb_identity.sport)
    series_match = _token_match(pm_identity.series, sb_identity.series)
    team_score = team_overlap_score(pm_identity, sb_identity)
    time_score = start_time_alignment_score(pm_identity, sb_identity)

    if normalized_market_type != sportsbook_normalized_market_type:
        return _blocked_decision(
            polymarket_market_id=polymarket_market_id,
            sportsbook_event_id=sportsbook_event_id,
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            blocked_reason=mapping_blocked_reason("market type mismatch"),
            event_key=event_key,
            sport=sport,
            series=series,
            game_id=game_id,
            explicit_identity_match=explicit_event_match or explicit_game_match,
            polymarket_semantics=resolved_pm_semantics,
            sportsbook_semantics=resolved_sb_semantics,
        )
    if (
        pm_identity.event_key not in (None, "")
        and sb_identity.event_key not in (None, "")
        and pm_identity.event_key != sb_identity.event_key
    ):
        return _blocked_decision(
            polymarket_market_id=polymarket_market_id,
            sportsbook_event_id=sportsbook_event_id,
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            blocked_reason=mapping_blocked_reason("event key mismatch"),
            event_key=event_key,
            sport=sport,
            series=series,
            game_id=game_id,
            explicit_identity_match=explicit_event_match or explicit_game_match,
            polymarket_semantics=resolved_pm_semantics,
            sportsbook_semantics=resolved_sb_semantics,
        )
    if (
        pm_identity.game_id not in (None, "")
        and sb_identity.game_id not in (None, "")
        and pm_identity.game_id != sb_identity.game_id
    ):
        return _blocked_decision(
            polymarket_market_id=polymarket_market_id,
            sportsbook_event_id=sportsbook_event_id,
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            blocked_reason=mapping_blocked_reason("game id mismatch"),
            event_key=event_key,
            sport=sport,
            series=series,
            game_id=game_id,
            explicit_identity_match=explicit_event_match or explicit_game_match,
            polymarket_semantics=resolved_pm_semantics,
            sportsbook_semantics=resolved_sb_semantics,
        )
    compatible, mismatch_reason = compare_rule_semantics(
        resolved_pm_semantics,
        resolved_sb_semantics,
    )
    if not compatible and mismatch_reason is not None:
        return _blocked_decision(
            polymarket_market_id=polymarket_market_id,
            sportsbook_event_id=sportsbook_event_id,
            sportsbook_market_type=sportsbook_market_type,
            normalized_market_type=normalized_market_type,
            blocked_reason=mapping_blocked_reason(mismatch_reason),
            event_key=event_key,
            sport=sport,
            series=series,
            game_id=game_id,
            explicit_identity_match=explicit_event_match or explicit_game_match,
            polymarket_semantics=resolved_pm_semantics,
            sportsbook_semantics=resolved_sb_semantics,
        )

    components = {
        "market_type_alignment": 0.25,
        "rule_semantics_alignment": 0.20,
        "explicit_event_identity": 0.15 if explicit_event_match else 0.0,
        "explicit_game_identity": 0.10 if explicit_game_match else 0.0,
        "sport_alignment": 0.05 if sport_match else 0.0,
        "series_alignment": 0.05 if series_match else 0.0,
        "participant_overlap": 0.20 * team_score,
        "start_time_alignment": 0.15 * time_score,
    }
    reasons: list[str] = ["market types normalize to the same ontology"]
    blocked_reason: MappingBlockedReason | None = None
    score = sum(components.values())
    if explicit_event_match:
        reasons.append("explicit event key match")
    if explicit_game_match:
        reasons.append("explicit game id match")
    if sport_match:
        reasons.append("sport tokens overlap")
    if series_match:
        reasons.append("series tokens overlap")
    if team_score > 0.0:
        reasons.append("participant tokens overlap")
    if time_score > 0.0:
        reasons.append("start times align")
    if not explicit_event_match and not explicit_game_match:
        if team_score < 0.25:
            blocked_reason = mapping_blocked_reason(
                "team names do not match with enough confidence"
            )
        elif time_score <= 0.0:
            blocked_reason = mapping_blocked_reason("event start times do not match")
    if blocked_reason is None and score < 0.6:
        blocked_reason = mapping_blocked_reason("ambiguous market identity")

    if blocked_reason is not None and score > 0.0:
        score = min(score, 0.59)

    score = min(score, 1.0)
    confidence = _build_confidence(score=score, components=components, reasons=reasons)
    return MappingDecision(
        polymarket_market_id=polymarket_market_id,
        sportsbook_event_id=sportsbook_event_id,
        sportsbook_market_type=sportsbook_market_type,
        normalized_market_type=normalized_market_type,
        mapping_status=_mapping_status_for(
            blocked_reason=blocked_reason,
            explicit_identity_match=explicit_event_match or explicit_game_match,
        ),
        mapping_confidence=confidence,
        blocked_reason=blocked_reason,
        event_key=event_key,
        sport=sport,
        series=series,
        game_id=game_id,
        polymarket_semantics=resolved_pm_semantics,
        sportsbook_semantics=resolved_sb_semantics,
    )

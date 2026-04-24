from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


KILL_SWITCH_REASON_PREFIX = "kill switch:"


@dataclass(frozen=True)
class KillSwitchState:
    source_health_red: bool = False
    reject_burst: bool = False
    slippage_burst: bool = False
    mapping_failure_spike: bool = False
    daily_loss_breach: bool = False

    @property
    def active(self) -> bool:
        return any(
            (
                self.source_health_red,
                self.reject_burst,
                self.slippage_burst,
                self.mapping_failure_spike,
                self.daily_loss_breach,
            )
        )

    def reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.source_health_red:
            reasons.append("source health red")
        if self.reject_burst:
            reasons.append("reject burst")
        if self.slippage_burst:
            reasons.append("slippage burst")
        if self.mapping_failure_spike:
            reasons.append("mapping failure spike")
        if self.daily_loss_breach:
            reasons.append("daily loss breach")
        return tuple(reasons)


def build_kill_switch_state(
    *,
    source_health: Mapping[str, Any] | None = None,
    reject_burst: bool = False,
    slippage_burst: bool = False,
    mapping_failure_spike: bool = False,
    daily_loss_breach: bool = False,
) -> KillSwitchState:
    source_health_red = False
    if source_health:
        for row in source_health.values():
            if not isinstance(row, Mapping):
                continue
            status = str(row.get("status") or "").strip().lower()
            if status in {"red", "error", "unhealthy"}:
                source_health_red = True
                break
    return KillSwitchState(
        source_health_red=source_health_red,
        reject_burst=reject_burst,
        slippage_burst=slippage_burst,
        mapping_failure_spike=mapping_failure_spike,
        daily_loss_breach=daily_loss_breach,
    )


def format_kill_switch_reason(state: KillSwitchState) -> str | None:
    reasons = state.reasons()
    if not reasons:
        return None
    return f"{KILL_SWITCH_REASON_PREFIX} {'; '.join(reasons)}"


def extract_kill_switch_reasons(reason: str | None) -> tuple[str, ...]:
    if reason in (None, ""):
        return ()
    if not str(reason).startswith(KILL_SWITCH_REASON_PREFIX):
        return ()
    payload = str(reason)[len(KILL_SWITCH_REASON_PREFIX) :].strip()
    if payload == "":
        return ()
    return tuple(part.strip() for part in payload.split(";") if part.strip())

from __future__ import annotations

from dataclasses import dataclass


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

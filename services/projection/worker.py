from __future__ import annotations

from dataclasses import dataclass
import time

from services.projection.current_state import project_current_state_once


@dataclass(frozen=True)
class CurrentProjectionWorkerConfig:
    root: str
    refresh_interval_seconds: float = 5.0
    max_cycles: int | None = None
    max_events_per_lane: int = 1000


class CurrentProjectionWorker:
    def __init__(
        self,
        *,
        config: CurrentProjectionWorkerConfig,
        sleep_fn=time.sleep,
    ) -> None:
        self.config = config
        self.sleep_fn = sleep_fn

    def run(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        cycle = 0
        while self.config.max_cycles is None or cycle < self.config.max_cycles:
            results.append(
                project_current_state_once(
                    self.config.root,
                    max_events_per_lane=self.config.max_events_per_lane,
                )
            )
            cycle += 1
            if self.config.max_cycles is None or cycle < self.config.max_cycles:
                self.sleep_fn(max(0.0, self.config.refresh_interval_seconds))
        return results


__all__ = ["CurrentProjectionWorker", "CurrentProjectionWorkerConfig"]

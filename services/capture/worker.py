from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time

from services.capture.sportsbook import (
    SportsbookCaptureRequest,
    SportsbookCaptureSource,
    SportsbookCaptureStores,
    capture_sportsbook_odds_once,
    record_sportsbook_capture_failure,
)


@dataclass(frozen=True)
class SportsbookCaptureWorkerConfig:
    root: str
    sport: str
    market: str
    event_map_file: str | None = None
    refresh_interval_seconds: float = 60.0
    max_cycles: int | None = None
    stale_after_ms: int = 60_000


class SportsbookCaptureWorker:
    def __init__(
        self,
        *,
        source: SportsbookCaptureSource,
        config: SportsbookCaptureWorkerConfig,
        stores: SportsbookCaptureStores | None = None,
        sleep_fn=time.sleep,
    ) -> None:
        self.source = source
        self.config = config
        self.stores = stores or SportsbookCaptureStores.from_root(config.root)
        self.sleep_fn = sleep_fn

    def run(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        cycle = 0
        while self.config.max_cycles is None or cycle < self.config.max_cycles:
            observed_at = datetime.now(timezone.utc)
            request = SportsbookCaptureRequest(
                root=self.config.root,
                sport=self.config.sport,
                market=self.config.market,
                event_map_file=self.config.event_map_file,
                stale_after_ms=self.config.stale_after_ms,
            )
            try:
                results.append(
                    capture_sportsbook_odds_once(
                        request,
                        source=self.source,
                        stores=self.stores,
                        observed_at=observed_at,
                    )
                )
            except Exception as exc:
                results.append(
                    record_sportsbook_capture_failure(
                        self.stores,
                        request,
                        self.source,
                        error=exc,
                        observed_at=observed_at,
                    )
                )
            cycle += 1
            if self.config.max_cycles is None or cycle < self.config.max_cycles:
                self.sleep_fn(max(0.0, self.config.refresh_interval_seconds))
        return results

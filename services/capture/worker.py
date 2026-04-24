from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time

from services.capture.sportsbook import (
    SportsbookCaptureRequest,
    SportsbookCaptureSource,
    SportsbookCaptureStores,
    SportsbookCaptureWritePlan,
    capture_sportsbook_odds_once,
    record_sportsbook_capture_failure,
    sanitize_capture_error,
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

    def _record_result(
        self, results: list[dict[str, object]], payload: dict[str, object]
    ) -> None:
        if self.config.max_cycles is None:
            if results:
                results[0] = payload
            else:
                results.append(payload)
            return
        results.append(payload)

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
                payload = capture_sportsbook_odds_once(
                    request,
                    source=self.source,
                    stores=self.stores,
                    observed_at=observed_at,
                    write_plan=SportsbookCaptureWritePlan.raw_ingress_only(),
                )
            except Exception as exc:
                try:
                    payload = record_sportsbook_capture_failure(
                        self.stores,
                        request,
                        self.source,
                        error=exc,
                        observed_at=observed_at,
                        write_plan=SportsbookCaptureWritePlan.raw_ingress_only(),
                    )
                except Exception as failure_exc:
                    primary_error = sanitize_capture_error(exc)
                    failure_error = sanitize_capture_error(failure_exc)
                    payload = {
                        "ok": False,
                        "error_kind": primary_error["kind"],
                        "error_message": primary_error["message"],
                        "health_error_kind": failure_error["kind"],
                        "health_error_message": failure_error["message"],
                        "provider": self.source.provider_name,
                        "sport": request.sport,
                        "market": request.market,
                        "root": request.root,
                    }
            self._record_result(results, payload)
            cycle += 1
            if self.config.max_cycles is None or cycle < self.config.max_cycles:
                self.sleep_fn(max(0.0, self.config.refresh_interval_seconds))
        return results

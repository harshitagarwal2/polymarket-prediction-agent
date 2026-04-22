from __future__ import annotations

from typing import Protocol


class SportsbookOddsClient(Protocol):
    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict]: ...

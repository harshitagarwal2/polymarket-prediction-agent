from __future__ import annotations

from research.data.odds_api import fetch_odds_payload


class TheOddsApiClient:
    def __init__(
        self,
        *,
        api_key: str,
        regions: str = "us",
        odds_format: str = "decimal",
        bookmakers: str | None = None,
    ) -> None:
        self.api_key = api_key
        self.regions = regions
        self.odds_format = odds_format
        self.bookmakers = bookmakers

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict]:
        return fetch_odds_payload(
            sport_key=sport,
            api_key=self.api_key,
            regions=self.regions,
            markets=market_type,
            odds_format=self.odds_format,
            bookmakers=self.bookmakers,
        )

from __future__ import annotations

from adapters.sportsbooks.base import SportsbookJsonFeedClient


SPORTSGAMEODDS_EVENTS_URL = "https://api.sportsgameodds.com/v2/events"


class SportsGameOddsClient(SportsbookJsonFeedClient):
    def __init__(
        self,
        *,
        api_key: str,
        feed_url: str = SPORTSGAMEODDS_EVENTS_URL,
        timeout_seconds: float = 30.0,
        client=None,
    ) -> None:
        headers = {"x-api-key": api_key}
        super().__init__(
            feed_url=feed_url,
            headers=headers,
            timeout_seconds=timeout_seconds,
            client=client,
        )


__all__ = ["SPORTSGAMEODDS_EVENTS_URL", "SportsGameOddsClient"]

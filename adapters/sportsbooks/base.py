from __future__ import annotations

import http.client
import ipaddress
import json
import socket
import ssl
from typing import Any, Mapping, Protocol
from urllib.parse import urlparse, urlunparse

from engine.http_client import get_json


class SportsbookOddsClient(Protocol):
    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict]: ...


def _matches_sport(event: dict[str, Any], sport: str) -> bool:
    if sport in (None, ""):
        return True
    event_sport = event.get("sport_key") or event.get("sport")
    if event_sport in (None, ""):
        return True
    return str(event_sport).strip() == sport


def _matches_market(event: dict[str, Any], market_type: str) -> bool:
    if market_type in (None, ""):
        return True
    direct_market = event.get("market") or event.get("market_type")
    if direct_market not in (None, ""):
        return str(direct_market).strip() == market_type
    markets = event.get("markets")
    if not isinstance(markets, list) or not markets:
        return True
    for market in markets:
        if not isinstance(market, dict):
            continue
        market_key = market.get("key") or market.get("market_type")
        if market_key in (None, ""):
            continue
        if str(market_key).strip() == market_type:
            return True
    return False


def _validate_feed_url(feed_url: str, *, validate_network_target: bool) -> None:
    parsed = urlparse(feed_url)
    if parsed.scheme != "https":
        raise ValueError("feed_url must use https")
    if parsed.username or parsed.password:
        raise ValueError("feed_url must not embed credentials")
    if parsed.hostname in (None, ""):
        raise ValueError("feed_url must include a hostname")

    host = str(parsed.hostname).strip()
    if host.lower() == "localhost":
        raise ValueError("feed_url must target a public host")

    try:
        parsed_ip = ipaddress.ip_address(host)
    except ValueError:
        parsed_ip = None

    if parsed_ip is not None:
        if not parsed_ip.is_global:
            raise ValueError("feed_url must target a public host")
        return

    if not validate_network_target:
        return

    try:
        resolved = socket.getaddrinfo(
            host,
            parsed.port or 443,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise ValueError("feed_url host could not be resolved") from exc

    addresses = {
        sockaddr[0]
        for _, _, _, _, sockaddr in resolved
        if isinstance(sockaddr, tuple) and sockaddr
    }
    if not addresses:
        raise ValueError("feed_url host could not be resolved")
    if any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ValueError("feed_url must target a public host")


def _resolve_public_ip(feed_url: str) -> tuple[str, int, str, str]:
    parsed = urlparse(feed_url)
    host = str(parsed.hostname or "").strip()
    port = parsed.port or 443
    if host == "":
        raise ValueError("feed_url must include a hostname")

    try:
        parsed_ip = ipaddress.ip_address(host)
    except ValueError:
        parsed_ip = None

    if parsed_ip is not None:
        if not parsed_ip.is_global:
            raise ValueError("feed_url must target a public host")
        return str(host), port, str(host), urlunparse(parsed)

    try:
        resolved = socket.getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise ValueError("feed_url host could not be resolved") from exc

    for _, _, _, _, sockaddr in resolved:
        if not isinstance(sockaddr, tuple) or not sockaddr:
            continue
        address = str(sockaddr[0])
        if ipaddress.ip_address(address).is_global:
            rebuilt = parsed._replace(
                netloc=f"{address}:{port}" if parsed.port else address
            )
            return address, port, host, urlunparse(rebuilt)
    raise ValueError("feed_url must target a public host")


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(
        self,
        connect_host: str,
        port: int,
        *,
        server_hostname: str,
        timeout: float,
    ) -> None:
        super().__init__(connect_host, port=port, timeout=timeout)
        self._server_hostname = server_hostname
        self._ssl_context = ssl.create_default_context()

    def connect(self) -> None:
        sock = socket.create_connection((self.host, self.port), self.timeout)
        self.sock = self._ssl_context.wrap_socket(
            sock, server_hostname=self._server_hostname
        )


def _get_pinned_json(
    feed_url: str,
    *,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> Any:
    connect_host, port, server_hostname, request_url = _resolve_public_ip(feed_url)
    parsed = urlparse(request_url)
    request_path = parsed.path or "/"
    if parsed.query:
        request_path = f"{request_path}?{parsed.query}"

    request_headers = dict(headers)
    request_headers["Host"] = server_hostname

    connection = _PinnedHTTPSConnection(
        connect_host,
        port,
        server_hostname=server_hostname,
        timeout=max(0.1, float(timeout_seconds)),
    )
    try:
        connection.request("GET", request_path, headers=request_headers)
        response = connection.getresponse()
        if response.status >= 400:
            raise RuntimeError(
                f"sportsbook JSON feed request failed with status {response.status}"
            )
        return json.loads(response.read().decode("utf-8"))
    finally:
        connection.close()


class SportsbookJsonFeedClient:
    def __init__(
        self,
        *,
        feed_url: str,
        headers: Mapping[str, str] | None = None,
        timeout_seconds: float = 30.0,
        client: Any | None = None,
    ) -> None:
        if feed_url in (None, ""):
            raise ValueError("feed_url is required")
        _validate_feed_url(feed_url, validate_network_target=client is None)
        self.feed_url = feed_url
        self.headers = dict(headers or {})
        self.timeout_seconds = timeout_seconds
        self._client = client

    def fetch_upcoming(self, sport: str, market_type: str) -> list[dict[str, Any]]:
        if self._client is None:
            payload = _get_pinned_json(
                self.feed_url,
                headers=self.headers,
                timeout_seconds=self.timeout_seconds,
            )
        else:
            payload = get_json(
                self.feed_url,
                headers=self.headers,
                timeout_seconds=self.timeout_seconds,
                client=self._client,
                follow_redirects=False,
            )
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows = payload.get("events") or payload.get("data") or []
        else:
            raise RuntimeError("sportsbook JSON feed returned an unsupported payload")
        if not isinstance(rows, list):
            raise RuntimeError("sportsbook JSON feed returned a non-list event payload")
        return [
            dict(event)
            for event in rows
            if isinstance(event, dict)
            and _matches_sport(event, sport)
            and _matches_market(event, market_type)
        ]

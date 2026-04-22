from __future__ import annotations

import importlib
from typing import Any, Mapping

USER_AGENT = "prediction-market-agent/0.1.0"


def _request_headers(extra_headers: Mapping[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _httpx() -> Any:
    try:
        return importlib.import_module("httpx")
    except ImportError as exc:
        raise RuntimeError(
            "httpx is required for Polymarket HTTP access. Install the `polymarket` extra or add `httpx`."
        ) from exc


def _timeout(timeout_seconds: float) -> Any:
    return _httpx().Timeout(max(0.1, float(timeout_seconds)))


def get_json(
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float,
    client: Any | None = None,
) -> Any:
    httpx = _httpx()
    request = client.get if client is not None else httpx.get
    response = request(
        url,
        params=params,
        headers=_request_headers(headers),
        timeout=_timeout(timeout_seconds),
        follow_redirects=True,
    )
    response.raise_for_status()
    return response.json()

from __future__ import annotations

import importlib
import json
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen


USER_AGENT = "prediction-market-agent/0.1.0"


def _request_headers(extra_headers: Mapping[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def _httpx() -> Any | None:
    try:
        return importlib.import_module("httpx")
    except ImportError:
        return None


def get_json(
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float = 30.0,
    client: Any | None = None,
) -> Any:
    httpx = _httpx()
    merged_headers = _request_headers(headers)
    if httpx is not None:
        request = client.get if client is not None else httpx.get
        response = request(
            url,
            params=params,
            headers=merged_headers,
            timeout=httpx.Timeout(max(0.1, float(timeout_seconds))),
            follow_redirects=True,
        )
        response.raise_for_status()
        return response.json()

    query = f"?{urlencode(params)}" if params else ""
    request = Request(f"{url}{query}", headers=merged_headers)
    with urlopen(request, timeout=max(0.1, float(timeout_seconds))) as response:
        return json.loads(response.read().decode("utf-8"))

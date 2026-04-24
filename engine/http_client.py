from __future__ import annotations

import importlib
import json
from typing import Any, Mapping
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen


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
    follow_redirects: bool = True,
) -> Any:
    merged_headers = _request_headers(headers)
    if client is not None:
        response = client.get(
            url,
            params=params,
            headers=merged_headers,
            timeout=max(0.1, float(timeout_seconds)),
            follow_redirects=follow_redirects,
        )
        response.raise_for_status()
        return response.json()

    httpx = _httpx()
    if httpx is not None:
        response = httpx.get(
            url,
            params=params,
            headers=merged_headers,
            timeout=httpx.Timeout(max(0.1, float(timeout_seconds))),
            follow_redirects=follow_redirects,
        )
        response.raise_for_status()
        return response.json()

    query = f"?{urlencode(params)}" if params else ""
    request = Request(f"{url}{query}", headers=merged_headers)
    if follow_redirects:
        with urlopen(request, timeout=max(0.1, float(timeout_seconds))) as response:
            return json.loads(response.read().decode("utf-8"))

    class _RejectRedirectHandler(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            raise HTTPError(req.full_url, code, msg, headers, fp)

    opener = build_opener(_RejectRedirectHandler)
    with opener.open(request, timeout=max(0.1, float(timeout_seconds))) as response:
        return json.loads(response.read().decode("utf-8"))

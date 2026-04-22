from __future__ import annotations

from typing import Any, Mapping

from engine.http_client import get_json as _get_json


def get_json(
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_seconds: float,
    client: Any | None = None,
) -> Any:
    return _get_json(
        url,
        params=params,
        headers=headers,
        timeout_seconds=timeout_seconds,
        client=client,
    )
